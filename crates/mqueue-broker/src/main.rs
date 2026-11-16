#![allow(dead_code)]
mod broker;
mod console;
mod console_html;
mod dedup;
mod group;
mod metadata;
mod metrics;
mod server;

use std::path::PathBuf;

use clap::Parser;
use mqueue_storage::RetentionPolicy;

/// Single-node partitioned log message queue.
#[derive(Parser, Debug)]
#[command(name = "mqueue-broker", version)]
struct Args {
    /// Directory for topic logs, indexes and consumer offsets.
    #[arg(long, default_value = "./mqueue-data")]
    data_dir: PathBuf,
    /// TCP address for the produce/fetch protocol.
    #[arg(long, default_value = "127.0.0.1:9100")]
    bind: String,
    /// HTTP address for the operations console.
    #[arg(long, default_value = "127.0.0.1:9101")]
    console: String,
    /// Max size of a single log segment before rolling.
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    segment_bytes: u64,
    /// Retention age in milliseconds (0 = keep forever by age).
    #[arg(long, default_value_t = 7 * 24 * 60 * 60 * 1000)]
    retention_age_ms: u64,
    /// Retention size floor per partition.
    #[arg(long, default_value_t = 1024 * 1024 * 1024)]
    retention_bytes: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,mqueue=debug".into()),
        )
        .init();

    let args = Args::parse();
    let defaults = RetentionPolicy {
        max_segment_bytes: args.segment_bytes,
        max_age_ms: if args.retention_age_ms == 0 {
            None
        } else {
            Some(args.retention_age_ms)
        },
        max_total_bytes: args.retention_bytes,
    };

    let broker = broker::Broker::start(args.data_dir, defaults).await?;
    let tcp = server::serve(broker.clone(), &args.bind);
    let http = console::serve(broker, &args.console);
    tokio::try_join!(tcp, http)?;
    Ok(())
}
