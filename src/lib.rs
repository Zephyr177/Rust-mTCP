//! rust-mtcp: a bandwidth-aggregating TCP tunnel.
//!
//! It stripes a single logical byte stream across several parallel TCP
//! connections so that, when a single connection is rate-limited but the path
//! can carry more in aggregate, throughput scales with the number of
//! connections. See the [`protocol`] and [`aggregator`] modules for the wire
//! format and the multiplexing core.

pub mod aggregator;
pub mod client;
pub mod protocol;
pub mod server;
