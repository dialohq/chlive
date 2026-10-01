use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use chlive_engine::engine::{Engine, Origin};
use chlive_engine::history::ClickHouseHttp;
use chlive_engine::rules::HardcodedRules;
use chlive_engine::stream::JsonlTopics;
use clap::Parser;
use futures::StreamExt;

#[derive(Parser)]
#[command(
    about = "Run a chlive-dialect query: history from ClickHouse, then events from JSONL topics"
)]
struct Cli {
    /// The query, e.g. "select * from events"
    query: String,
    #[arg(long, default_value = "http://127.0.0.1:18123")]
    clickhouse_url: String,
    #[arg(long, default_value = "default")]
    user: String,
    #[arg(long, default_value = "")]
    password: String,
    /// ClickHouse database that dialect tables map to
    #[arg(long, default_value = "default")]
    database: String,
    /// Directory with one `<topic>.jsonl` file per topic
    #[arg(long, default_value = "engine/topics")]
    topics_dir: PathBuf,
    /// Pause before each event (milliseconds)
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
    /// Keep waiting for lines appended to the topic files
    #[arg(long)]
    follow: bool,
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
    let mut topics = JsonlTopics::new(&cli.topics_dir);
    topics.delay = Duration::from_millis(cli.delay_ms);
    topics.follow = cli.follow;
    let engine = Engine {
        rules: Arc::new(HardcodedRules {
            database: cli.database,
        }),
        history: Arc::new(ClickHouseHttp::new(
            &cli.clickhouse_url,
            &cli.user,
            &cli.password,
        )),
        topics,
    };

    let plan = engine.prepare(&cli.query).await?;
    if cli.explain {
        println!("table:   {}", plan.table);
        println!(
            "columns: {}",
            plan.columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        println!("history: {}", plan.history.sql);
        println!(
            "topics:  {:?}",
            plan.stream.topics.iter().map(|t| &t.0).collect::<Vec<_>>()
        );
        println!("filter:  {:?}", plan.stream.filter);
        return Ok(());
    }
    let paths: Vec<_> = plan
        .stream
        .topics
        .iter()
        .map(|t| engine.topics.path(t).display().to_string())
        .collect();
    tracing::info!(history = %plan.history.sql, topics = ?paths, "subscribed");

    let mut iterations = engine.run(plan);
    while let Some(it) = iterations.next().await {
        let it = it?;
        match &it.origin {
            Origin::History => {
                for d in &it.diffs {
                    println!("{d}");
                }
                println!("history: {} rows", it.diffs.len());
            }
            Origin::Event { topic, line } => {
                for d in &it.diffs {
                    println!("{topic}:{line} {d}");
                }
            }
        }
    }
    Ok(())
}
