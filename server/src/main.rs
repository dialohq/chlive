//! `chlive-server`: serve live chlive-dialect queries over WebSocket (`/live`).
//! A thin user of the `chlive-server` library.

use anyhow::Result;
use chlive_engine::{Config, Engine};
use clap::Parser;
use tracing::info;

#[derive(Parser)]
#[command(
    about = "Serve live chlive-dialect queries over WebSocket: history from ClickHouse, then events from Kafka"
)]
struct Cli {
    /// Address to listen on
    #[arg(long, env = "CHLIVE_LISTEN", default_value = "0.0.0.0:8080")]
    listen: String,
    /// ClickHouse HTTP URL (default: the dev shell's ClickHouse)
    #[arg(
        long,
        env = "CHLIVE_CLICKHOUSE_URL",
        default_value = "http://127.0.0.1:18123"
    )]
    clickhouse_url: String,
    /// ClickHouse user (read-only is enough)
    #[arg(long, env = "CHLIVE_CLICKHOUSE_USER", default_value = "chlive_ro")]
    user: String,
    #[arg(long, env = "CHLIVE_CLICKHOUSE_PASSWORD", default_value = "chlive_ro")]
    password: String,
    /// ClickHouse database that dialect tables map to
    #[arg(long, default_value = "default")]
    database: String,
    /// Kafka bootstrap servers (default: the dev shell's Redpanda)
    #[arg(long, env = "CHLIVE_KAFKA", default_value = "127.0.0.1:9092")]
    kafka: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,chlive_server=info".into()),
        )
        .init();
    let cli = Cli::parse();
    let engine = Engine::connect(Config {
        clickhouse_url: cli.clickhouse_url,
        user: cli.user,
        password: cli.password,
        database: cli.database,
        kafka_brokers: cli.kafka,
    });
    let server = chlive_server::start(&cli.listen, engine).await?;
    info!(addr = %server.addr, "listening (WebSocket at /live)");
    tokio::select! {
        r = async { server.wait().await } => r?,
        _ = tokio::signal::ctrl_c() => info!("shutting down"),
    }
    Ok(())
}
