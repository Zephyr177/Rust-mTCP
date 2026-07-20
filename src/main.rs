//! Command-line entry point for rust-mtcp.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use rust_mtcp::{client::Client, server::Server};

/// Bandwidth-aggregating TCP tunnel: stripe one logical stream over many
/// parallel TCP connections.
#[derive(Parser)]
#[command(name = "rust-mtcp", version, about, long_about = None)]
struct Cli {
    /// Enable verbose (debug-level) logging.
    #[arg(short, long, global = true)]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Server side (mtcp -> tcp): accept aggregated connections and forward
    /// each logical stream to a plain TCP target.
    Server {
        /// Address to listen on for aggregated sub-connections.
        #[arg(long, default_value = "0.0.0.0:15201")]
        listen: String,
        /// Plain TCP target every logical stream is forwarded to.
        #[arg(long)]
        target: String,
    },
    /// Client side (tcp -> mtcp): accept plain local connections and carry
    /// each over an aggregated bundle to the remote server(s).
    Client {
        /// Local address applications connect to.
        #[arg(long, default_value = "0.0.0.0:5201")]
        listen: String,
        /// Remote server address(es). Repeat to aggregate across endpoints.
        #[arg(long = "remote", required = true)]
        remotes: Vec<String>,
        /// Number of parallel sub-connections per logical stream.
        #[arg(long, default_value_t = 3)]
        streams: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(cli.verbose);

    match cli.command {
        Command::Server { listen, target } => {
            let listen = resolve(&listen).await?;
            let target = resolve(&target).await?;
            Server::bind(listen, target).await?.run().await
        }
        Command::Client {
            listen,
            remotes,
            streams,
        } => {
            let listen = resolve(&listen).await?;
            let mut resolved = Vec::with_capacity(remotes.len());
            for remote in &remotes {
                resolved.push(resolve(remote).await?);
            }
            Client::bind(listen, resolved, streams).await?.run().await
        }
    }
}

/// Resolve a `host:port` string to a single socket address.
async fn resolve(addr: &str) -> Result<SocketAddr> {
    tokio::net::lookup_host(addr)
        .await
        .with_context(|| format!("resolving {addr}"))?
        .next()
        .with_context(|| format!("no addresses resolved for {addr}"))
}

fn init_tracing(verbose: bool) {
    use tracing_subscriber::{fmt, EnvFilter};

    let default_level = if verbose { "debug" } else { "info" };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level));
    fmt().with_env_filter(filter).with_target(false).init();
}
