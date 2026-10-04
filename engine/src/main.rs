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
    /// ClickHouse's native protocol address, for aggregates (chDB reads states with remote())
    #[arg(
        long,
        env = "CHLIVE_CLICKHOUSE_NATIVE",
        default_value = "127.0.0.1:19000"
    )]
    clickhouse_native: String,
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
    });

    let result = run(&engine, &query, explain).await;
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
