//! Logging, as `[log]` says: a level (an `EnvFilter` directive, unless `RUST_LOG` is set) and
//! text or JSON lines.

use anyhow::{anyhow, Result};
use tracing::Subscriber;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::EnvFilter;

use crate::config::{Log, LogFormat};

/// `RUST_LOG` if set (and not empty), else `level`.
pub fn filter(level: &str, rust_log: Option<&str>) -> Result<EnvFilter> {
    let directive = rust_log.filter(|v| !v.trim().is_empty()).unwrap_or(level);
    EnvFilter::try_new(directive).map_err(|e| anyhow!("log level {directive:?}: {e}"))
}

/// The subscriber `[log]` describes, writing to `writer` (text in colors with `ansi`).
pub fn subscriber<W>(
    log: &Log,
    rust_log: Option<&str>,
    writer: W,
    ansi: bool,
) -> Result<Box<dyn Subscriber + Send + Sync>>
where
    W: for<'w> MakeWriter<'w> + Send + Sync + 'static,
{
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter(&log.level, rust_log)?)
        .with_ansi(ansi)
        .with_writer(writer);
    Ok(match log.format {
        LogFormat::Text => Box::new(builder.finish()),
        LogFormat::Json => Box::new(builder.json().finish()),
    })
}

/// Log to stdout for the rest of the process.
pub fn init(log: &Log) -> Result<()> {
    let rust_log = std::env::var("RUST_LOG").ok();
    let ansi = std::io::IsTerminal::is_terminal(&std::io::stdout());
    tracing::subscriber::set_global_default(subscriber(
        log,
        rust_log.as_deref(),
        std::io::stdout,
        ansi,
    )?)
    .map_err(|e| anyhow!("setting up logging: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct Buf(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'w> MakeWriter<'w> for Buf {
        type Writer = Buf;
        fn make_writer(&'w self) -> Buf {
            self.clone()
        }
    }

    fn logged(log: &Log, rust_log: Option<&str>) -> String {
        let buf = Buf::default();
        let sub = subscriber(log, rust_log, buf.clone(), false).unwrap();
        tracing::subscriber::with_default(sub, || {
            tracing::info!(conn = 1, "connected");
            tracing::debug!("details");
        });
        let out = buf.0.lock().unwrap().clone();
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn level_format_and_rust_log() {
        let text = Log::default();
        let out = logged(&text, None);
        assert!(out.contains("connected") && out.contains("conn=1"), "{out}");
        assert!(!out.contains("details"), "info by default: {out}");
        assert!(!out.trim_start().starts_with('{'), "text: {out}");

        let debug = Log {
            level: "debug".into(),
            ..Log::default()
        };
        assert!(logged(&debug, None).contains("details"));

        // RUST_LOG wins over the config; an empty one doesn't count
        assert!(!logged(&debug, Some("warn")).contains("connected"));
        assert!(logged(&text, Some("debug")).contains("details"));
        assert!(!logged(&text, Some(" ")).contains("details"));

        let json = Log {
            format: LogFormat::Json,
            ..Log::default()
        };
        let out = logged(&json, None);
        let line: serde_json::Value = serde_json::from_str(out.lines().next().unwrap()).unwrap();
        assert_eq!(line["fields"]["message"], "connected");
        assert_eq!(line["fields"]["conn"], 1);
        assert_eq!(line["level"], "INFO");

        assert!(filter("a=b=c", None).is_err());
    }
}
