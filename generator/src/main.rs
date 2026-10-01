use anyhow::Result;
use chlive_generator::{Generator, TsMode};
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about = "Produce synthetic events into Kafka")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:19092", env = "CHLIVE_KAFKA")]
    brokers: String,
    #[arg(long, default_value = "events")]
    topic: String,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Seed historical events with timestamps spread over the last N days.
    Seed {
        #[arg(long, default_value_t = 1_000_000)]
        count: u64,
        #[arg(long, default_value_t = 7)]
        days: u32,
    },
    /// Serve a minimal HTTP produce API (`POST /topics/{topic}`) for the simple demo.
    HttpProxy {
        #[arg(long, default_value = "127.0.0.1:18082")]
        listen: std::net::SocketAddr,
    },
    /// Stream live events at a steady rate until interrupted (or --count is reached).
    Stream {
        #[arg(long, default_value_t = 200.0)]
        rate: f64,
        #[arg(long)]
        count: Option<u64>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let cli = Cli::parse();
    if let Cmd::HttpProxy { listen } = cli.cmd {
        return chlive_generator::proxy::serve(&cli.brokers, listen).await;
    }
    let generator = Generator::new(&cli.brokers, &cli.topic)?;
    let offsets = match cli.cmd {
        Cmd::Seed { count, days } => {
            generator
                .produce(Some(count), None, TsMode::Backdated { days })
                .await?
        }
        Cmd::HttpProxy { .. } => unreachable!("handled above"),
        Cmd::Stream { rate, count } => {
            tokio::select! {
                r = generator.produce(count, Some(rate), TsMode::Now) => r?,
                _ = tokio::signal::ctrl_c() => return Ok(()),
            }
        }
    };
    tracing::info!(?offsets, "done; last offsets per partition");
    Ok(())
}
