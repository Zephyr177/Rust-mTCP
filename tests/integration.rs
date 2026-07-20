//! End-to-end tests that run the client and server over loopback and verify
//! that a large payload survives chunking, striping, and reordering intact.

use std::net::SocketAddr;

use rust_mtcp::client::Client;
use rust_mtcp::server::Server;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Spawn a plain TCP echo server ("app B") on an ephemeral loopback port.
async fn spawn_echo() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (mut conn, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(_) => break,
            };
            tokio::spawn(async move {
                let (mut rd, mut wr) = conn.split();
                let _ = tokio::io::copy(&mut rd, &mut wr).await;
                let _ = wr.shutdown().await;
            });
        }
    });
    addr
}

/// Bring up echo -> server -> client on loopback and return the client's
/// listen address plus the target for assertions.
async fn spawn_tunnel(streams: usize) -> SocketAddr {
    let echo_addr = spawn_echo().await;

    let server = Server::bind("127.0.0.1:0".parse().unwrap(), echo_addr)
        .await
        .unwrap();
    let server_addr = server.listen_addr();
    tokio::spawn(async move {
        let _ = server.run().await;
    });

    let client = Client::bind("127.0.0.1:0".parse().unwrap(), vec![server_addr], streams)
        .await
        .unwrap();
    let client_addr = client.listen_addr();
    tokio::spawn(async move {
        let _ = client.run().await;
    });

    client_addr
}

/// A deterministic, order-sensitive payload so a reordering bug corrupts it.
fn make_payload(len: usize) -> Vec<u8> {
    (0..len).map(|i| (i % 251) as u8).collect()
}

#[tokio::test]
async fn large_payload_roundtrip_is_intact_and_ordered() {
    let client_addr = spawn_tunnel(4).await;

    let conn = TcpStream::connect(client_addr).await.unwrap();
    let len = 8 * 1024 * 1024; // 8 MiB: far more than one 64 KiB chunk.
    let payload = make_payload(len);

    let (mut rd, mut wr) = conn.into_split();
    let to_send = payload.clone();
    let writer = tokio::spawn(async move {
        wr.write_all(&to_send).await.unwrap();
        wr.shutdown().await.unwrap();
    });

    let mut received = vec![0u8; len];
    rd.read_exact(&mut received).await.unwrap();

    // After the payload, the stream must close cleanly with no extra bytes.
    let mut trailing = [0u8; 1];
    assert_eq!(
        rd.read(&mut trailing).await.unwrap(),
        0,
        "unexpected trailing bytes after payload"
    );

    writer.await.unwrap();
    assert_eq!(received.len(), payload.len(), "length mismatch");
    assert!(received == payload, "payload corrupted or reordered");
}

#[tokio::test]
async fn many_concurrent_streams_stay_isolated() {
    // Each logical stream gets its own session; verify they don't cross-talk.
    let client_addr = spawn_tunnel(3).await;

    let mut handles = Vec::new();
    for stream_idx in 0..16u8 {
        let addr = client_addr;
        handles.push(tokio::spawn(async move {
            let conn = TcpStream::connect(addr).await.unwrap();
            let len = 512 * 1024 + stream_idx as usize; // distinct lengths
            let payload: Vec<u8> = (0..len).map(|i| (i as u8) ^ stream_idx).collect();

            let (mut rd, mut wr) = conn.into_split();
            let to_send = payload.clone();
            let writer = tokio::spawn(async move {
                wr.write_all(&to_send).await.unwrap();
                wr.shutdown().await.unwrap();
            });

            let mut received = vec![0u8; len];
            rd.read_exact(&mut received).await.unwrap();
            writer.await.unwrap();
            assert!(received == payload, "stream {stream_idx} corrupted");
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}
