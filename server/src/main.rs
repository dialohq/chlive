//! `chlive`: serve live chlive-dialect queries over WebSocket. A thin user of the
//! `chlive-server` library; configured by a TOML file, env vars and flags (see `config`).

use std::path::PathBuf;

use anyhow::Result;
use chlive_engine::Engine;
use chlive_server::config::{self, Config, Overrides};
use clap::{Parser, Subcommand};
use tracing::{info, warn};

#[derive(Parser)]
#[command(
    about = "Serve live chlive-dialect queries over WebSocket: history from ClickHouse, then events from Kafka",
    after_help = "Settings come from the defaults, then --config, then CHLIVE_<SECTION>__<KEY> env vars \
                  (e.g. CHLIVE_KAFKA__BROKERS), then flags. `chlive config show` prints the result."
)]
struct Cli {
    /// The config file (TOML)
    #[arg(long, env = "CHLIVE_CONFIG", global = true)]
    config: Option<PathBuf>,
    /// Set any key, e.g. `--set server.max_connections=500` (the value as TOML, or a string)
    #[arg(long = "set", value_name = "KEY=VALUE", global = true)]
    sets: Vec<String>,
    /// = --set server.listen=…
    #[arg(long, global = true)]
    listen: Option<String>,
    /// = --set clickhouse.url=…
    #[arg(long, global = true)]
    clickhouse_url: Option<String>,
    /// = --set kafka.brokers=…
    #[arg(long, global = true)]
    kafka: Option<String>,
    /// = --set pause_server.url=…
    #[arg(long, global = true)]
    pause_server: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// The configuration
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Check a config file (with env vars and flags on top), as the server would load it
    Check { file: Option<PathBuf> },
    /// Print the effective config, each value with where it came from (passwords hidden)
    Show,
}

impl Cli {
    fn overrides(&self) -> Result<Overrides> {
        let mut o = Overrides {
            file: self.config.clone(),
            ..Default::default()
        };
        // shortcuts first, so an explicit --set of the same key wins
        let shortcuts = [
            ("server.listen", &self.listen),
            ("clickhouse.url", &self.clickhouse_url),
            ("kafka.brokers", &self.kafka),
            ("pause_server.url", &self.pause_server),
        ];
        for (key, value) in shortcuts {
            if let Some(v) = value {
                // a string, whatever it looks like
                o.sets
                    .push((key.into(), toml::Value::String(v.clone()).to_string()));
            }
        }
        for s in &self.sets {
            o.set(s)?;
        }
        Ok(o)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let overrides = cli.overrides()?;
    match cli.command {
        Some(Command::Config {
            action: ConfigAction::Check { file },
        }) => {
            match file {
                Some(f) => config::check_file(&f, overrides)?,
                None => Config::load(&overrides)?,
            };
            println!("ok");
            return Ok(());
        }
        Some(Command::Config {
            action: ConfigAction::Show,
        }) => {
            print!("{}", config::show(&overrides)?);
            return Ok(());
        }
        None => {}
    }

    let config = Config::load(&overrides)?;
    chlive_server::log::init(&config.log)?;
    if config.tables.tables.is_empty()
        && config.tables.expose == chlive_engine::rules::Expose::Listed
    {
        warn!("no tables listed in [tables]: nothing can be queried");
    }
    let engine = std::sync::Arc::new(Engine::connect(config.engine()?)?);
    let mut server = chlive_server::start_with(
        &config.server.listen,
        config.server_settings(),
        engine.clone(),
    )
    .await?;
    info!(addr = %server.addr, path = %config.server.path, "listening");
    tokio::select! {
        r = server.wait() => r?,
        _ = tokio::signal::ctrl_c() => info!("shutting down"),
    }
    // the shared results, then the engine
    server.shutdown().await;
    // with chDB open (aggregates), the process must close it before exiting
    chlive_engine::chdb::shutdown();
    Ok(())
}
