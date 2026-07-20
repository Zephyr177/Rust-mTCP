# rust-mtcp

A bandwidth-aggregating TCP tunnel. It stripes a **single logical byte stream
across several parallel TCP connections**, so when one connection is
rate-limited/QoS'd but the path can carry more in aggregate, throughput scales
with the number of connections.

It is a clean-room Rust reimplementation of the core idea from a reference
Node.js implementation (Node-mTCP), with its own protocol — not
wire-compatible.

```
 App A ── client ══ many TCP sub-connections ══ server ── App B
              (tcp -> mtcp)                 (mtcp -> tcp)
```

- **client (`tcp2mtcp`)**: listens on a local port; for each incoming
  connection it opens N sub-connections to the server(s) and aggregates them
  into one logical stream.
- **server (`mtcp2tcp`)**: accepts sub-connections, groups the ones that share
  a session into one logical stream, opens **one** plain TCP connection to the
  configured target, and pipes bytes between them.

## How it works

- Outbound bytes are split into ≤ 64 KiB chunks; each chunk gets a
  monotonically increasing 64-bit sequence number.
- Each chunk is sent on the **least-loaded** live sub-connection (the one with
  the fewest outstanding bytes), which is what spreads bandwidth across links.
- The receiver reorders chunks by sequence before delivering them to the app,
  since chunks race across different connections and arrive out of order.
- Both directions of a stream are aggregated and sequenced independently, so
  half-close is handled per direction.
- If a sub-connection dies, the logical stream keeps running over the remaining
  ones (as long as at least one survives).
- Memory is bounded by small per-connection send windows plus a bounded
  reassembler inbox; a slow app socket back-pressures the whole pipeline.

See [`src/protocol.rs`](src/protocol.rs) for the exact wire format and
[`src/aggregator.rs`](src/aggregator.rs) for the multiplexing core.

## Build

Requires a stable Rust toolchain.

```bash
cargo build --release
# binary: target/release/rust-mtcp
```

## Usage

Global `-v`/`--verbose` enables debug logging. Log level can also be set with
`RUST_LOG` (e.g. `RUST_LOG=debug`).

### Server (mtcp -> tcp)

```bash
rust-mtcp server --listen 0.0.0.0:15201 --target 127.0.0.1:5201
```

### Client (tcp -> mtcp)

```bash
rust-mtcp client --listen 0.0.0.0:5201 --remote host:15201 --streams 4
```

Pass `--remote` multiple times to spread sub-connections across several server
endpoints (bandwidth aggregation / failover):

```bash
rust-mtcp client --listen 0.0.0.0:5201 \
  --remote host1:15201 --remote host2:15201 --streams 4
```

### End-to-end example

Mirroring the reference layout — an app connects to the client on `5201`, which
aggregates to the server on `15201`, which forwards to a local service (e.g. a
reverse proxy) on the server host's `5201`:

```bash
# On the server host (App B side):
rust-mtcp server --listen 0.0.0.0:15201 --target 127.0.0.1:5201

# On the client host (App A side):
rust-mtcp client --listen 0.0.0.0:5201 --remote SERVER_HOST:15201 --streams 4

# Now point App A at 127.0.0.1:5201; traffic is aggregated across 4 links.
```

Defaults: `--streams 3`, server `--listen 0.0.0.0:15201`, client
`--listen 0.0.0.0:5201`. Keeping the stream count around 3–5 is recommended,
as in the reference.

## Wire protocol

Per sub-connection, the client first sends a fixed handshake:

```
[ magic "mTCP" (4) ][ version=1 (1) ][ session_id (16) ][ streams: u16 BE (2) ]
```

The server groups sub-connections by `session_id`; the first one for a new
session opens the target connection. After the handshake, both directions carry
a stream of frames:

```
[ kind: u8 ][ seq: u64 BE ][ len: u32 BE ][ payload (len bytes, <= 64 KiB) ]
```

- `kind = 0` (DATA): `payload` belongs at position `seq` in the ordered stream.
- `kind = 1` (FIN): end-of-direction marker; `seq` is the total number of DATA
  frames, so the peer knows the stream is complete once it has delivered every
  sequence below it. `len` is 0.

## Tests

```bash
cargo test --release
```

Includes an end-to-end integration test (`tests/integration.rs`) that runs the
client and server over loopback, pushes a multi-megabyte payload through an echo
target across several sub-connections, and asserts the bytes return intact and
in order.

## Limitations

- No application-level retransmission. If a sub-connection is aborted (RST)
  mid-stream, the few frames buffered on it can be lost; the logical stream then
  ends at the first gap once connections close (same behavior class as the
  reference). Cleanly closed connections lose nothing.
- Remote hostnames are resolved once at startup.
