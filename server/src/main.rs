use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Parser)]
#[command(about = "chlive: live ClickHouse queries over WebSocket")]
struct Cli {
    #[arg(long, short, default_value = "chlive.toml", env = "CHLIVE_CONFIG")]
    config: PathBuf,
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
    let cfg = chlive_server::config::Config::load(&cli.config)?;
    let server = chlive_server::start(cfg).await?;
    tokio::select! {
        r = server.wait() => r,
        _ = tokio::signal::ctrl_c() => Ok(()),
    }
}
