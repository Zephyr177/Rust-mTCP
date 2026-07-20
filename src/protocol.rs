//! Wire protocol for rust-mtcp.
//!
//! Two message types travel over each sub-connection:
//!
//! * A one-shot **handshake** sent by the client right after connecting. It
//!   identifies the logical stream a sub-connection belongs to so the server
//!   can group sub-connections that share a `session_id`.
//! * A stream of length-prefixed **frames** carrying the aggregated payload.
//!   Each direction of a logical stream is sequenced independently with a
//!   64-bit counter so the receiver can restore ordering regardless of which
//!   sub-connection a frame arrived on.

use std::io;

use anyhow::{bail, Result};
use tokio::io::{AsyncRead, AsyncReadExt};

/// Identifies the protocol and guards against connections from unrelated
/// services landing on the listening port.
pub const MAGIC: [u8; 4] = *b"mTCP";

/// Bumped only on incompatible wire changes.
pub const VERSION: u8 = 1;

/// Length of a random session identifier, in bytes.
pub const SESSION_ID_LEN: usize = 16;

/// Total size of the fixed handshake: magic + version + session id + stream count.
pub const HANDSHAKE_LEN: usize = MAGIC.len() + 1 + SESSION_ID_LEN + 2;

/// Fixed frame header: kind + seq + payload length.
pub const FRAME_HEADER_LEN: usize = 1 + 8 + 4;

/// Maximum payload carried by a single DATA frame (64 KiB).
pub const MAX_CHUNK: usize = 64 * 1024;

const KIND_DATA: u8 = 0;
const KIND_FIN: u8 = 1;

/// A logical-stream identifier shared by every sub-connection of one stream.
pub type SessionId = [u8; SESSION_ID_LEN];

/// Handshake contents parsed from the first bytes of a sub-connection.
#[derive(Debug, Clone)]
pub struct Handshake {
    pub session_id: SessionId,
    /// Number of sub-connections the client intends to open (advisory).
    pub streams: u16,
}

/// Serialize the fixed-size handshake.
pub fn encode_handshake(session_id: &SessionId, streams: u16) -> [u8; HANDSHAKE_LEN] {
    let mut buf = [0u8; HANDSHAKE_LEN];
    buf[0..4].copy_from_slice(&MAGIC);
    buf[4] = VERSION;
    buf[5..21].copy_from_slice(session_id);
    buf[21..23].copy_from_slice(&streams.to_be_bytes());
    buf
}

/// Parse and validate a handshake, rejecting bad magic or unknown versions.
pub fn decode_handshake(buf: &[u8; HANDSHAKE_LEN]) -> Result<Handshake> {
    if buf[0..4] != MAGIC {
        bail!("bad magic: not an mtcp connection");
    }
    if buf[4] != VERSION {
        bail!("unsupported protocol version {}", buf[4]);
    }
    let mut session_id = [0u8; SESSION_ID_LEN];
    session_id.copy_from_slice(&buf[5..21]);
    let streams = u16::from_be_bytes([buf[21], buf[22]]);
    Ok(Handshake {
        session_id,
        streams,
    })
}

/// Distinguishes payload frames from the end-of-stream marker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// Carries `payload` at position `seq` in the ordered stream.
    Data,
    /// Marks the end of a direction; `seq` is the total number of DATA frames.
    Fin,
}

/// A decoded frame read from a sub-connection.
#[derive(Debug)]
pub struct Frame {
    pub kind: FrameKind,
    pub seq: u64,
    pub payload: Vec<u8>,
}

/// Encode a DATA frame for `payload` at sequence `seq`.
///
/// `payload` must not exceed [`MAX_CHUNK`]; callers chunk before encoding.
pub fn encode_data(seq: u64, payload: &[u8]) -> Vec<u8> {
    debug_assert!(payload.len() <= MAX_CHUNK);
    let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    buf.push(KIND_DATA);
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Encode a FIN frame whose `final_seq` is the number of DATA frames sent, so
/// the peer knows the ordered stream is complete once it has delivered every
/// sequence below it.
pub fn encode_fin(final_seq: u64) -> Vec<u8> {
    let mut buf = Vec::with_capacity(FRAME_HEADER_LEN);
    buf.push(KIND_FIN);
    buf.extend_from_slice(&final_seq.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf
}

/// Read one frame from `r`.
///
/// Returns `Ok(None)` on a clean EOF at a frame boundary (the peer closed the
/// connection between frames). An EOF in the middle of a frame is surfaced as
/// an error, as is a malformed header.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<Option<Frame>> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    if let Err(e) = r.read_exact(&mut header).await {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(None);
        }
        return Err(e);
    }

    let kind = header[0];
    let seq = u64::from_be_bytes(header[1..9].try_into().expect("8 bytes"));
    let len = u32::from_be_bytes(header[9..13].try_into().expect("4 bytes")) as usize;

    if len > MAX_CHUNK {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame payload exceeds maximum chunk size",
        ));
    }

    let mut payload = vec![0u8; len];
    r.read_exact(&mut payload).await?;

    let kind = match kind {
        KIND_DATA => FrameKind::Data,
        KIND_FIN => FrameKind::Fin,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown frame kind {other}"),
            ))
        }
    };

    Ok(Some(Frame { kind, seq, payload }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handshake_roundtrip() {
        let id = [7u8; SESSION_ID_LEN];
        let buf = encode_handshake(&id, 4);
        let hs = decode_handshake(&buf).unwrap();
        assert_eq!(hs.session_id, id);
        assert_eq!(hs.streams, 4);
    }

    #[test]
    fn handshake_rejects_bad_magic() {
        let mut buf = encode_handshake(&[0u8; SESSION_ID_LEN], 1);
        buf[0] = b'X';
        assert!(decode_handshake(&buf).is_err());
    }

    #[tokio::test]
    async fn data_frame_roundtrip() {
        let payload = b"hello aggregated world";
        let encoded = encode_data(42, payload);
        let mut cursor = std::io::Cursor::new(encoded);
        let frame = read_frame(&mut cursor).await.unwrap().unwrap();
        assert_eq!(frame.kind, FrameKind::Data);
        assert_eq!(frame.seq, 42);
        assert_eq!(frame.payload, payload);
    }

    #[tokio::test]
    async fn fin_frame_roundtrip() {
        let encoded = encode_fin(123);
        let mut cursor = std::io::Cursor::new(encoded);
        let frame = read_frame(&mut cursor).await.unwrap().unwrap();
        assert_eq!(frame.kind, FrameKind::Fin);
        assert_eq!(frame.seq, 123);
        assert!(frame.payload.is_empty());
    }

    #[tokio::test]
    async fn clean_eof_returns_none() {
        let mut cursor = std::io::Cursor::new(Vec::new());
        assert!(read_frame(&mut cursor).await.unwrap().is_none());
    }
}
