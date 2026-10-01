//! The engine's entry point: [`Engine::explain`] plans a query, [`Engine::subscribe`] runs it.
//!
//! A subscription yields the history from ClickHouse first, then one [`Iteration`] per event of
//! the topics the query needs, each carrying the diffs it produced.

use std::fmt;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::{self, BoxStream, Stream, StreamExt};

use crate::binder::{bind, BindError, Catalog};
use crate::diff::Diff;
use crate::history::{ClickHouseHttp, HistoryError, HistorySource};
use crate::parser::parse;
use crate::plan::{plan, Plan};
use crate::rules::{HardcodedRules, NamingRules, TopicId};
use crate::stream::JsonlTopics;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Rendered with line and column.
    #[error("syntax error: {0}")]
    Syntax(String),
    /// The query doesn't fit the catalog (unknown table/column, …); rendered with position.
    #[error("{0}")]
    Bind(String),
    #[error(transparent)]
    History(#[from] HistoryError),
}

/// Everything [`Engine::connect`] needs for the standard setup: ClickHouse over HTTP, the
/// hard-coded naming rules, and JSONL files as topics.
#[derive(Debug, Clone)]
pub struct Config {
    pub clickhouse_url: String,
    pub user: String,
    pub password: String,
    /// ClickHouse database that dialect tables map to.
    pub database: String,
    /// Directory with one `<topic>.jsonl` file per topic.
    pub topics_dir: PathBuf,
    /// Pause before each event, to simulate events arriving over time.
    pub delay: Duration,
    /// Keep waiting for lines appended to the topic files.
    pub follow: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            clickhouse_url: "http://127.0.0.1:18123".into(),
            user: "default".into(),
            password: String::new(),
            database: "default".into(),
            topics_dir: "engine/topics".into(),
            delay: Duration::ZERO,
            follow: false,
        }
    }
}

pub struct Engine {
    rules: Arc<dyn NamingRules>,
    history: Arc<dyn HistorySource>,
    topics: JsonlTopics,
}

impl Engine {
    /// The standard setup from a [`Config`].
    pub fn connect(config: Config) -> Self {
        let mut topics = JsonlTopics::new(config.topics_dir);
        topics.delay = config.delay;
        topics.follow = config.follow;
        Self::new(
            HardcodedRules {
                database: config.database,
            },
            ClickHouseHttp::new(&config.clickhouse_url, &config.user, &config.password),
            topics,
        )
    }

    /// Any naming rules and history source, e.g. fakes in tests.
    pub fn new(
        rules: impl NamingRules + 'static,
        history: impl HistorySource + 'static,
        topics: JsonlTopics,
    ) -> Self {
        Self {
            rules: Arc::new(rules),
            history: Arc::new(history),
            topics,
        }
    }

    /// Parse, bind and plan a query without running it.
    pub async fn explain(&self, sql: &str) -> Result<Plan, EngineError> {
        let query = parse(sql).map_err(|e| EngineError::Syntax(e.render(sql)))?;
        let catalog = Catalog {
            rules: self.rules.as_ref(),
            history: self.history.as_ref(),
            topics: &self.topics,
        };
        let bound = bind(&query, &catalog).await.map_err(|e| match e {
            BindError::History(e) => EngineError::History(e),
            e => EngineError::Bind(e.render(sql)),
        })?;
        Ok(plan(&bound))
    }

    /// Plan a query and start it: the history first, then one iteration per event of the
    /// plan's topics.
    pub async fn subscribe(&self, sql: &str) -> Result<Subscription, EngineError> {
        let plan = self.explain(sql).await?;
        let files: Vec<_> = plan
            .stream
            .topics
            .iter()
            .map(|t| self.topics.path(t).display().to_string())
            .collect();
        tracing::info!(history = %plan.history.sql, topics = ?files, "subscribed");
        let iterations = self.iterations(&plan);
        Ok(Subscription { plan, iterations })
    }

    fn iterations(&self, plan: &Plan) -> BoxStream<'static, Result<Iteration, EngineError>> {
        let history = self.history.clone();
        let sql = plan.history.sql.clone();
        let initial = stream::once(async move {
            let rows = history.fetch(&sql).await?;
            Ok(Iteration {
                origin: Origin::History,
                diffs: rows.into_iter().map(Diff::Insert).collect(),
            })
        });
        let stream_plan = plan.stream.clone();
        let events = self
            .topics
            .subscribe(&stream_plan.topics, &stream_plan.required_keys())
            .map(move |event| {
                // decode by column types, then filter, then project
                let diffs = match stream_plan.decode(&event.value) {
                    Ok(row) if stream_plan.matches(&row) => vec![Diff::Insert(stream_plan.project(&row))],
                    Ok(_) => Vec::new(),
                    Err(e) => {
                        // ClickHouse's ingestion would reject this message too
                        tracing::warn!(topic = %event.topic, line = event.line, error = %e, "skipping event");
                        Vec::new()
                    }
                };
                Ok(Iteration {
                    origin: Origin::Event {
                        topic: event.topic,
                        line: event.line,
                    },
                    diffs,
                })
            });
        initial.chain(events).boxed()
    }
}

/// A running query: a stream of [`Iteration`]s, plus the plan it runs.
pub struct Subscription {
    pub plan: Plan,
    iterations: BoxStream<'static, Result<Iteration, EngineError>>,
}

impl Stream for Subscription {
    type Item = Result<Iteration, EngineError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.iterations.poll_next_unpin(cx)
    }
}

/// Where an iteration's diffs came from.
#[derive(Debug, Clone, PartialEq)]
pub enum Origin {
    /// The initial result, from ClickHouse.
    History,
    /// One event of a subscribed topic.
    Event { topic: TopicId, line: usize },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Iteration {
    pub origin: Origin,
    /// Empty when the event was not relevant to the query.
    pub diffs: Vec<Diff>,
}

/// One line per diff: `+ {row}` for the history (followed by `history: N rows`), and
/// `<topic>:<line> + {row}` for events. Irrelevant events print nothing.
impl fmt::Display for Iteration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.origin {
            Origin::History => {
                for d in &self.diffs {
                    writeln!(f, "{d}")?;
                }
                write!(f, "history: {} rows", self.diffs.len())
            }
            Origin::Event { topic, line } => {
                let mut first = true;
                for d in &self.diffs {
                    if !first {
                        writeln!(f)?;
                    }
                    first = false;
                    write!(f, "{topic}:{line} {d}")?;
                }
                Ok(())
            }
        }
    }
}
