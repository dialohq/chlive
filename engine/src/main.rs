//! `chlive-engine`: run a chlive-dialect query and print each iteration's diffs.
//! A thin user of the `chlive-engine` library.

use anyhow::Result;
use std::time::Duration;

use chlive_engine::snapshot::SnapshotConfig;
use chlive_engine::{Config, Engine};
use clap::Parser;
use futures::StreamExt;

#[derive(Parser)]
#[command(
    about = "Run a chlive-dialect query: history from ClickHouse, then live events from Kafka"
)]
struct Cli {
    /// The query, e.g. "select * from events"
    query: String,
    /// ClickHouse HTTP URL (the dev shell: `dnvr-state get clickhouse.httpUrl`)
    #[arg(
        long,
        env = "CHLIVE_CLICKHOUSE_URL",
        default_value = "http://127.0.0.1:18123"
    )]
    clickhouse_url: String,
    #[arg(long, default_value = "default")]
    user: String,
    #[arg(long, default_value = "")]
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
    #[arg(long, env = "CHLIVE_SNAPSHOT_USER", default_value = "default")]
    snapshot_user: String,
    #[arg(long, env = "CHLIVE_SNAPSHOT_PASSWORD", default_value = "")]
    snapshot_password: String,
    /// Seconds between snapshots of a table in use
    #[arg(long, env = "CHLIVE_SNAPSHOT_REFRESH", default_value_t = 10)]
    snapshot_refresh: u64,
    /// Seconds without subscriptions after which a table's snapshots stop
    #[arg(long, env = "CHLIVE_SNAPSHOT_IDLE", default_value_t = 3600)]
    snapshot_idle: u64,
    /// Print the plan instead of running it
    #[arg(long)]
    explain: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let cli = Cli::parse();
    let (query, explain) = (cli.query.clone(), cli.explain);
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
            sweep: false, // the server owns leftover clones,
        },
    });

    let result = run(&engine, &query, explain).await;
    engine.shutdown().await;
    // with chDB open (aggregates), the process must close it before exiting
    chlive_engine::chdb::shutdown();
    result
}

async fn run(engine: &Engine, query: &str, explain: bool) -> Result<()> {
    if explain {
        println!("{}", engine.explain(query).await?);
        return Ok(());
    }
    let mut subscription = engine.subscribe(query).await?;
    loop {
        tokio::select! {
            next = subscription.next() => match next {
                Some(iteration) => {
                    let iteration = iteration?;
                    if !iteration.diffs.is_empty() {
                        println!("{iteration}");
                    }
                }
                None => return Ok(()),
            },
            _ = tokio::signal::ctrl_c() => return Ok(()),
        }
    }
}
