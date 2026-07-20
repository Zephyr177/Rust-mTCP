//! Aggregation core: one logical byte stream carried over many TCP sub-connections.
//!
//! An [`Aggregator`] owns the plain "application" socket (the local app on the
//! client, or the target service on the server) and a dynamic set of framed
//! sub-connections. It is symmetric: both endpoints run the exact same logic,
//! only differing in who dials the sub-connections.
//!
//! Two independent pipelines run per logical stream:
//!
//! * **Uplink** ([`dispatch_loop`]): read the app socket, chunk it, tag each
//!   chunk with a monotonically increasing sequence, and hand it to the
//!   least-loaded live sub-connection. A FIN carrying the final sequence is
//!   broadcast once the app half-closes.
//! * **Downlink** ([`reassemble_loop`]): read frames off every sub-connection,
//!   restore ordering with a reorder buffer keyed by sequence, and write the
//!   contiguous prefix back to the app socket.
//!
//! Memory is bounded by small per-writer channels plus a bounded reassembler
//! inbox; that same send-side window transitively bounds the reorder buffer,
//! and a full app socket back-pressures the whole chain.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::protocol::{self, Frame, FrameKind, MAX_CHUNK};

/// Frames buffered per sub-connection writer before back-pressure kicks in.
/// Kept small so that at most a few frames are lost if a connection dies with
/// data still queued.
const WRITER_CHANNEL_CAP: usize = 4;

/// Frames buffered on the way into the reassembler.
const REASSEMBLER_CHANNEL_CAP: usize = 32;

/// Cloned handle to the chosen writer, returned by [`Uplink::pick`].
type PickedWriter = (mpsc::Sender<Vec<u8>>, Arc<AtomicUsize>, Arc<AtomicBool>);

/// One outbound sub-connection: a channel to its writer task plus load counters.
struct Writer {
    tx: mpsc::Sender<Vec<u8>>,
    /// Bytes queued or in-flight on this connection; drives load balancing.
    outstanding: Arc<AtomicUsize>,
    alive: Arc<AtomicBool>,
}

/// Shared uplink state: the live writer set and its wakeup signalling.
struct Uplink {
    writers: Mutex<Vec<Writer>>,
    /// Notified whenever a writer is added, so the dispatcher can wake if it is
    /// waiting for the first (or a replacement) sub-connection.
    writer_added: Notify,
    live_writers: Arc<AtomicUsize>,
    /// Set once at least one writer has ever been registered, distinguishing
    /// "not connected yet" from "all connections lost".
    ever_added: AtomicBool,
}

/// Shared downlink state used to detect when every reader has ended.
struct Downlink {
    live_readers: AtomicUsize,
    /// Notified when the last reader ends, so the reassembler can stop instead
    /// of blocking forever on a sequence that will never arrive.
    readers_done: Notify,
}

impl Uplink {
    /// Flip a writer to dead exactly once, keeping `live_writers` consistent.
    fn mark_dead(&self, alive: &AtomicBool) {
        if alive.swap(false, Ordering::AcqRel) {
            self.live_writers.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Clone the handle of the live sub-connection with the fewest outstanding
    /// bytes, or `None` if none are currently alive.
    fn pick(&self) -> Option<PickedWriter> {
        let writers = self.writers.lock().unwrap();
        writers
            .iter()
            .filter(|w| w.alive.load(Ordering::Acquire))
            .min_by_key(|w| w.outstanding.load(Ordering::Relaxed))
            .map(|w| {
                (
                    w.tx.clone(),
                    Arc::clone(&w.outstanding),
                    Arc::clone(&w.alive),
                )
            })
    }

    /// Send one encoded frame to the least-loaded connection, retrying on
    /// another connection if the chosen one has just died.
    ///
    /// Returns `false` only when the stream has no live connections left and
    /// never will, signalling the dispatcher to stop.
    async fn dispatch(&self, mut frame: Vec<u8>) -> bool {
        loop {
            if let Some((tx, outstanding, alive)) = self.pick() {
                let len = frame.len();
                outstanding.fetch_add(len, Ordering::Relaxed);
                match tx.send(frame).await {
                    Ok(()) => return true,
                    Err(returned) => {
                        outstanding.fetch_sub(len, Ordering::Relaxed);
                        self.mark_dead(&alive);
                        frame = returned.0;
                    }
                }
            } else if self.ever_added.load(Ordering::Acquire)
                && self.live_writers.load(Ordering::Acquire) == 0
            {
                return false;
            } else {
                self.writer_added.notified().await;
            }
        }
    }

    /// Broadcast a FIN to every currently-live connection so it survives the
    /// loss of any single sub-connection.
    async fn broadcast_fin(&self, final_seq: u64) {
        let senders: Vec<mpsc::Sender<Vec<u8>>> = {
            let writers = self.writers.lock().unwrap();
            writers
                .iter()
                .filter(|w| w.alive.load(Ordering::Acquire))
                .map(|w| w.tx.clone())
                .collect()
        };
        let fin = protocol::encode_fin(final_seq);
        for tx in senders {
            let _ = tx.send(fin.clone()).await;
        }
    }
}

/// Cheap, cloneable handle used to attach more sub-connections to a running
/// logical stream (the server grows a stream as its sub-connections arrive).
#[derive(Clone)]
pub struct Aggregator {
    uplink: Arc<Uplink>,
    downlink: Arc<Downlink>,
    reasm_tx: mpsc::Sender<Frame>,
}

/// Owns the uplink and downlink task handles; awaiting [`StreamTasks::join`]
/// resolves once the logical stream has fully closed in both directions.
pub struct StreamTasks {
    dispatcher: JoinHandle<()>,
    reassembler: JoinHandle<()>,
}

impl StreamTasks {
    pub async fn join(self) {
        let _ = self.dispatcher.await;
        let _ = self.reassembler.await;
    }
}

/// Wrap `app` in a logical stream, spawning its uplink and downlink pipelines.
///
/// The returned [`Aggregator`] starts with no sub-connections; the caller must
/// attach at least one via [`Aggregator::add_subconn`]. The dispatcher waits
/// for the first connection rather than failing.
pub fn spawn_stream(app: TcpStream) -> (Aggregator, StreamTasks) {
    let _ = app.set_nodelay(true);
    let (app_rd, app_wr) = app.into_split();

    let uplink = Arc::new(Uplink {
        writers: Mutex::new(Vec::new()),
        writer_added: Notify::new(),
        live_writers: Arc::new(AtomicUsize::new(0)),
        ever_added: AtomicBool::new(false),
    });
    let downlink = Arc::new(Downlink {
        live_readers: AtomicUsize::new(0),
        readers_done: Notify::new(),
    });

    let (reasm_tx, reasm_rx) = mpsc::channel(REASSEMBLER_CHANNEL_CAP);

    let dispatcher = tokio::spawn(dispatch_loop(app_rd, Arc::clone(&uplink)));
    let reassembler = tokio::spawn(reassemble_loop(app_wr, reasm_rx, Arc::clone(&downlink)));

    (
        Aggregator {
            uplink,
            downlink,
            reasm_tx,
        },
        StreamTasks {
            dispatcher,
            reassembler,
        },
    )
}

impl Aggregator {
    /// Attach a framed sub-connection to this logical stream, spawning its
    /// reader (downlink) and writer (uplink) tasks.
    pub fn add_subconn(&self, stream: TcpStream) {
        let _ = stream.set_nodelay(true);
        let (rd, wr) = stream.into_split();

        let (tx, rx) = mpsc::channel(WRITER_CHANNEL_CAP);
        let outstanding = Arc::new(AtomicUsize::new(0));
        let alive = Arc::new(AtomicBool::new(true));

        self.uplink.writers.lock().unwrap().push(Writer {
            tx,
            outstanding: Arc::clone(&outstanding),
            alive: Arc::clone(&alive),
        });
        self.uplink.live_writers.fetch_add(1, Ordering::AcqRel);
        self.uplink.ever_added.store(true, Ordering::Release);
        self.uplink.writer_added.notify_one();

        tokio::spawn(writer_loop(
            wr,
            rx,
            outstanding,
            alive,
            Arc::clone(&self.uplink.live_writers),
        ));

        self.downlink.live_readers.fetch_add(1, Ordering::AcqRel);
        tokio::spawn(reader_loop(
            rd,
            self.reasm_tx.clone(),
            Arc::clone(&self.downlink),
        ));
    }
}

/// Uplink: read the app socket, chunk, sequence, and dispatch each chunk.
async fn dispatch_loop(mut app_rd: OwnedReadHalf, uplink: Arc<Uplink>) {
    let mut seq: u64 = 0;
    let mut buf = vec![0u8; MAX_CHUNK];

    loop {
        let n = match app_rd.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let frame = protocol::encode_data(seq, &buf[..n]);
        seq += 1;
        if !uplink.dispatch(frame).await {
            // No live sub-connections remain; the downlink side will tear the
            // stream down. Nothing more we can send.
            return;
        }
    }

    uplink.broadcast_fin(seq).await;
}

/// Drains one sub-connection's writer channel to the socket, tracking load.
async fn writer_loop(
    mut wr: OwnedWriteHalf,
    mut rx: mpsc::Receiver<Vec<u8>>,
    outstanding: Arc<AtomicUsize>,
    alive: Arc<AtomicBool>,
    live_writers: Arc<AtomicUsize>,
) {
    while let Some(frame) = rx.recv().await {
        let len = frame.len();
        if wr.write_all(&frame).await.is_err() {
            if alive.swap(false, Ordering::AcqRel) {
                live_writers.fetch_sub(1, Ordering::AcqRel);
            }
            return;
        }
        outstanding.fetch_sub(len, Ordering::Relaxed);
    }
    // Channel closed: uplink is finished for this connection. Half-close our
    // write direction; the read half keeps serving the downlink.
    let _ = wr.shutdown().await;
}

/// Reads framed data off one sub-connection and forwards it to the reassembler.
async fn reader_loop(rd: OwnedReadHalf, reasm_tx: mpsc::Sender<Frame>, downlink: Arc<Downlink>) {
    let mut rd = BufReader::new(rd);
    while let Ok(Some(frame)) = protocol::read_frame(&mut rd).await {
        if reasm_tx.send(frame).await.is_err() {
            break;
        }
    }
    if downlink.live_readers.fetch_sub(1, Ordering::AcqRel) == 1 {
        downlink.readers_done.notify_one();
    }
}

/// Downlink: reorder incoming frames by sequence and write the contiguous
/// prefix to the app socket, finishing on FIN or when all readers have ended.
async fn reassemble_loop(
    mut app_wr: OwnedWriteHalf,
    mut rx: mpsc::Receiver<Frame>,
    downlink: Arc<Downlink>,
) {
    let mut next: u64 = 0;
    let mut buffer: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    let mut final_seq: Option<u64> = None;

    'outer: loop {
        let frame = tokio::select! {
            biased;
            maybe = rx.recv() => match maybe {
                Some(frame) => frame,
                None => break,
            },
            _ = downlink.readers_done.notified() => {
                // Every reader has ended; flush anything already queued, then
                // stop even if a gap remains (the missing bytes are lost).
                while let Ok(frame) = rx.try_recv() {
                    if deliver(frame, &mut next, &mut buffer, &mut final_seq, &mut app_wr).await {
                        break 'outer;
                    }
                }
                break;
            }
        };

        if deliver(frame, &mut next, &mut buffer, &mut final_seq, &mut app_wr).await {
            break;
        }
    }

    let _ = app_wr.shutdown().await;
}

/// Apply one frame to the reorder buffer, flushing the contiguous prefix.
///
/// Returns `true` when the loop should stop, either because the stream is
/// complete (FIN reached and everything below it delivered) or the app socket
/// is gone.
async fn deliver(
    frame: Frame,
    next: &mut u64,
    buffer: &mut BTreeMap<u64, Vec<u8>>,
    final_seq: &mut Option<u64>,
    app_wr: &mut OwnedWriteHalf,
) -> bool {
    match frame.kind {
        FrameKind::Fin => *final_seq = Some(frame.seq),
        FrameKind::Data => {
            if frame.seq >= *next {
                buffer.entry(frame.seq).or_insert(frame.payload);
            }
            while let Some(payload) = buffer.remove(next) {
                if app_wr.write_all(&payload).await.is_err() {
                    return true;
                }
                *next += 1;
            }
        }
    }
    final_seq.is_some_and(|final_seq| *next >= final_seq)
}
