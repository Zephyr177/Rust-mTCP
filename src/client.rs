//! Client mode (tcp2mtcp): accept plain local connections and carry each one
//! over a fresh bundle of aggregated sub-connections to the remote server(s).

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

use crate::aggregator::spawn_stream;
use crate::protocol::{self, SessionId};

/// A bound client listener ready to accept local application connections.
pub struct Client {
    listener: TcpListener,
    listen_addr: SocketAddr,
    remotes: Arc<Vec<SocketAddr>>,
    streams: usize,
}

impl Client {
    /// Bind the local listener. Sub-connections are spread across `remotes`.
    pub async fn bind(
        listen: SocketAddr,
        remotes: Vec<SocketAddr>,
        streams: usize,
    ) -> Result<Self> {
        ensure!(!remotes.is_empty(), "at least one --remote is required");
        ensure!(streams >= 1, "--streams must be at least 1");
        let listener = TcpListener::bind(listen)
            .await
            .with_context(|| format!("failed to bind local listener on {listen}"))?;
        let listen_addr = listener.local_addr()?;
        Ok(Self {
            listener,
            listen_addr,
            remotes: Arc::new(remotes),
            streams,
        })
    }

    /// The actual bound address (useful when binding to port 0).
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Accept local connections forever, aggregating each to the remote.
    pub async fn run(self) -> Result<()> {
        info!(
            listen = %self.listen_addr,
            remotes = ?self.remotes,
            streams = self.streams,
            "mtcp client listening (tcp -> mtcp)"
        );
        loop {
            let (app, peer) = self.listener.accept().await?;
            debug!(%peer, "accepted local connection");
            let remotes = Arc::clone(&self.remotes);
            let streams = self.streams;
            tokio::spawn(async move {
                if let Err(e) = handle_local(app, remotes, streams).await {
                    debug!(%peer, error = %e, "local connection closed");
                }
            });
        }
    }
}

/// Drive one local connection: open the sub-connection bundle and pump.
async fn handle_local(app: TcpStream, remotes: Arc<Vec<SocketAddr>>, streams: usize) -> Result<()> {
    let mut session_id: SessionId = [0u8; 16];
    rand::rng().fill_bytes(&mut session_id);

    let (aggregator, tasks) = spawn_stream(app);

    let mut connected = 0usize;
    for i in 0..streams {
        let remote = remotes[i % remotes.len()];
        match connect_sub(remote, &session_id, streams as u16).await {
            Ok(sub) => {
                aggregator.add_subconn(sub);
                connected += 1;
            }
            Err(e) => warn!(%remote, error = %e, "sub-connection failed to establish"),
        }
    }

    if connected == 0 {
        bail!("no sub-connections could be established");
    }
    debug!(connected, requested = streams, "logical stream established");

    tasks.join().await;
    Ok(())
}

/// Connect one sub-connection and send the handshake identifying its stream.
async fn connect_sub(
    remote: SocketAddr,
    session_id: &SessionId,
    streams: u16,
) -> Result<TcpStream> {
    let mut sub = TcpStream::connect(remote)
        .await
        .with_context(|| format!("connecting to remote {remote}"))?;
    let handshake = protocol::encode_handshake(session_id, streams);
    sub.write_all(&handshake)
        .await
        .with_context(|| format!("sending handshake to {remote}"))?;
    Ok(sub)
}
