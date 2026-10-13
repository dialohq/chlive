//! `chlive-tui`: run live queries against a chlive-server and watch the result as a table,
//! updated on every diff. A user of `chlive-client`.

mod app;
mod ui;

use std::time::{Duration, Instant};

use anyhow::Result;
use chlive_client::{ClientOptions, LiveClient, Params, Row, Subscription, Update};
use clap::Parser;
use crossterm::event::{Event, EventStream, KeyEventKind};
use futures::StreamExt;
use ratatui::DefaultTerminal;
use serde_json::Value;

use app::{Action, App};

#[derive(Parser)]
#[command(about = "Run live chlive queries and watch their results update in a table")]
struct Cli {
    /// The chlive-server's WebSocket (the dev shell: `dnvr-state get server.url`)
    #[arg(long, env = "CHLIVE_URL", default_value = "ws://127.0.0.1:8080/live")]
    url: String,
    /// A query parameter's value, for `{name:Type}`: `--param id=abcdef00-…`. A value that
    /// parses as JSON is used as such (`5`, `true`, `null`); anything else is a string.
    #[arg(long = "param", value_name = "NAME=VALUE", value_parser = parse_param)]
    params: Vec<(String, Value)>,
    /// A query to run at start
    sql: Option<String>,
}

fn parse_param(s: &str) -> Result<(String, Value), String> {
    let (name, value) = s
        .split_once('=')
        .ok_or_else(|| format!("expected NAME=VALUE, got {s:?}"))?;
    let value = serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.into()));
    Ok((name.to_string(), value))
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut terminal = ratatui::init();
    let result = run(&mut terminal, cli).await;
    ratatui::restore();
    result
}

async fn run(terminal: &mut DefaultTerminal, cli: Cli) -> Result<()> {
    let client = LiveClient::new(ClientOptions::new(cli.url.clone()));
    let params: Params = cli.params.into_iter().collect();
    let mut app = App::new(cli.url, cli.sql.unwrap_or_default());
    let mut sub: Option<Subscription<Row>> = None;
    let start = app.input.trim().to_string();
    if !start.is_empty() {
        app.run(&start);
        sub = Some(client.subscribe_with(start, params.clone()));
    }

    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(250));
    loop {
        terminal.draw(|f| ui::draw(f, &mut app))?;
        tokio::select! {
            event = events.next() => match event {
                Some(Ok(Event::Key(key))) if key.kind == KeyEventKind::Press => {
                    match app.on_key(key) {
                        Action::Quit => break,
                        Action::Run(sql) => {
                            // dropping the old subscription unsubscribes
                            drop(sub.take());
                            app.run(&sql);
                            sub = Some(client.subscribe_with(sql, params.clone()));
                        }
                        Action::None => {}
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => break,
            },
            update = next(&mut sub) => match update {
                Some(u) => app.on_update(u),
                // after an error
                None => sub = None,
            },
            _ = tick.tick() => app.tick(Instant::now()),
        }
    }
    drop(sub);
    client.close().await;
    Ok(())
}

/// The subscription's next update; never, without one.
async fn next(sub: &mut Option<Subscription<Row>>) -> Option<Update<Row>> {
    match sub {
        Some(s) => s.next().await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn params_are_json_or_strings() {
        assert_eq!(parse_param("n=5").unwrap(), ("n".into(), json!(5)));
        assert_eq!(parse_param("b=true").unwrap(), ("b".into(), json!(true)));
        assert_eq!(
            parse_param("id=abcdef00-0000-0000-0000-000000000001").unwrap(),
            ("id".into(), json!("abcdef00-0000-0000-0000-000000000001"))
        );
        assert_eq!(parse_param("s=a=b").unwrap(), ("s".into(), json!("a=b")));
        assert!(parse_param("nope").is_err());
    }
}
