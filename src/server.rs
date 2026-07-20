//! Server mode (mtcp2tcp): accept aggregated sub-connections, group them by
//! session, and bridge each logical stream to one plain TCP target connection.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::aggregator::{spawn_stream, Aggregator};
use crate::protocol::{self, SessionId, HANDSHAKE_LEN};

/// Sessions keyed by client-chosen id; the entry is the running logical stream.
type Sessions = Arc<Mutex<HashMap<SessionId, Aggregator>>>;

/// A bound server listener ready to accept aggregated sub-connections.
pub struct Server {
    listener: TcpListener,
    listen_addr: SocketAddr,
    target: SocketAddr,
}

impl Server {
    /// Bind the listener that accepts aggregated sub-connections.
    pub async fn bind(listen: SocketAddr, target: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("failed to bind server listener on {listen}"))?;
        let listen_addr = listener.local_addr()?;
        Ok(Self {
            listener,
            listen_addr,
            target,
        })
    }

    /// The actual bound address (useful when binding to port 0).
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Accept sub-connections forever, grouping them into logical streams.
    pub async fn run(self) -> Result<()> {
        info!(
            listen = %self.listen_addr,
            target = %self.target,
            "mtcp server listening (mtcp -> tcp)"
        );
        let sessions: Sessions = Arc::new(Mutex::new(HashMap::new()));
        loop {
            let (sub, peer) = self.listener.accept().await?;
            let target = self.target;
            let sessions = Arc::clone(&sessions);
            tokio::spawn(async move {
                if let Err(e) = handle_sub(sub, target, sessions).await {
                    debug!(%peer, error = %e, "rejected sub-connection");
                }
            });
        }
    }
}

/// Handle a freshly accepted sub-connection: read its handshake, then either
/// attach it to an existing session or start a new one bound to the target.
async fn handle_sub(mut sub: TcpStream, target: SocketAddr, sessions: Sessions) -> Result<()> {
    let mut handshake = [0u8; HANDSHAKE_LEN];
    sub.read_exact(&mut handshake)
        .await
        .context("reading handshake")?;
    let handshake = protocol::decode_handshake(&handshake)?;
    let session_id = handshake.session_id;

    // Hold the lock across target connect so that concurrent sub-connections
    // for the same new session cannot each open their own target.
    let mut sessions_guard = sessions.lock().await;

    if let Some(aggregator) = sessions_guard.get(&session_id) {
        let aggregator = aggregator.clone();
        drop(sessions_guard);
        aggregator.add_subconn(sub);
        return Ok(());
    }

    let target_conn = TcpStream::connect(target)
        .await
        .with_context(|| format!("connecting to target {target}"))?;
    debug!(?session_id, %target, streams = handshake.streams, "opened target for new session");

    let (aggregator, tasks) = spawn_stream(target_conn);
    aggregator.add_subconn(sub);
    sessions_guard.insert(session_id, aggregator);
    drop(sessions_guard);

    let sessions = Arc::clone(&sessions);
    tokio::spawn(async move {
        tasks.join().await;
        sessions.lock().await.remove(&session_id);
        debug!(?session_id, "session closed");
    });

    Ok(())
}
