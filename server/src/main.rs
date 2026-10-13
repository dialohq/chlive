//! `chlive`: serve live chlive-dialect queries over WebSocket (`/live`).
//! A thin user of the `chlive-server` library.

use anyhow::Result;
use std::time::Duration;

use chlive_engine::snapshot::SnapshotConfig;
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
    /// ClickHouse HTTP URL (the dev shell: `dnvr-state get clickhouse.httpUrl`)
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
    /// Kafka bootstrap servers (the dev shell: `dnvr-state get redpanda.bootstrap`)
    #[arg(long, env = "CHLIVE_KAFKA", default_value = "127.0.0.1:9092")]
    kafka: String,
    /// ClickHouse's native protocol address, for aggregates (chDB reads states with remote())
    #[arg(
        long,
        env = "CHLIVE_CLICKHOUSE_NATIVE",
        default_value = "127.0.0.1:19000"
    )]
    clickhouse_native: String,
    /// The pause server, which pauses a table's ingestion for a snapshot (default: the dev
    /// shell's)
    #[arg(
        long,
        env = "CHLIVE_PAUSE_SERVER",
        default_value = "http://127.0.0.1:18091"
    )]
    pause_server: String,
    /// ClickHouse user creating and dropping snapshots (clones of the tables)
    #[arg(long, env = "CHLIVE_SNAPSHOT_USER", default_value = "chlive_snap")]
    snapshot_user: String,
    #[arg(long, env = "CHLIVE_SNAPSHOT_PASSWORD", default_value = "chlive_snap")]
    snapshot_password: String,
    /// Seconds between snapshots of a table in use
    #[arg(long, env = "CHLIVE_SNAPSHOT_REFRESH", default_value_t = 10)]
    snapshot_refresh: u64,
    /// Seconds without subscriptions after which a table's snapshots stop
    #[arg(long, env = "CHLIVE_SNAPSHOT_IDLE", default_value_t = 3600)]
    snapshot_idle: u64,
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
        clickhouse_native: cli.clickhouse_native,
        pause_server_url: cli.pause_server,
        snapshot_user: cli.snapshot_user,
        snapshot_password: cli.snapshot_password,
        snapshots: SnapshotConfig {
            refresh: Duration::from_secs(cli.snapshot_refresh),
            idle: Duration::from_secs(cli.snapshot_idle),
            sweep: true,
        },
    });
    let engine = std::sync::Arc::new(engine);
    let server = chlive_server::start(&cli.listen, engine.clone()).await?;
    info!(addr = %server.addr, "listening (WebSocket at /live)");
    tokio::select! {
        r = async { server.wait().await } => r?,
        _ = tokio::signal::ctrl_c() => info!("shutting down"),
    }
    engine.shutdown().await;
    // with chDB open (aggregates), the process must close it before exiting
    chlive_engine::chdb::shutdown();
    Ok(())
}
