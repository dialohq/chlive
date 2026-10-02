//! `chlive-engine`: run a chlive-dialect query and print each iteration's diffs.
//! A thin user of the `chlive-engine` library.

use anyhow::Result;
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
    /// ClickHouse HTTP URL (default: the dev shell's ClickHouse)
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
    /// Kafka bootstrap servers (default: the dev shell's Redpanda)
    #[arg(long, env = "CHLIVE_KAFKA", default_value = "127.0.0.1:9092")]
    kafka: String,
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
    let engine = Engine::connect(Config {
        clickhouse_url: cli.clickhouse_url,
        user: cli.user,
        password: cli.password,
        database: cli.database,
        kafka_brokers: cli.kafka,
    });

    if cli.explain {
        println!("{}", engine.explain(&cli.query).await?);
        return Ok(());
    }
    let mut subscription = engine.subscribe(&cli.query).await?;
    while let Some(iteration) = subscription.next().await {
        let iteration = iteration?;
        if !iteration.diffs.is_empty() {
            println!("{iteration}");
        }
    }
    Ok(())
}
