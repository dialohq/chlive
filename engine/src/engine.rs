//! The engine's entry point: [`Engine::explain`] plans a query, [`Engine::subscribe`] runs it.
//!
//! A subscription yields the history from ClickHouse first, then one [`Iteration`] per event of
//! the topics the query needs, each carrying the diffs it produced.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream, Stream, StreamExt};

use crate::binder::{bind, BindError, Catalog};
use crate::diff::{Diff, Row};
use crate::history::{ClickHouseHttp, HistoryError, HistorySource};
use crate::order::{OrderedState, WindowState};
use crate::parser::parse;
use crate::plan::{plan, Plan, StreamPlan};
use crate::rules::{HardcodedRules, NamingRules, TopicId};
use crate::source::{Event, EventSource, KafkaSource, SourceError, TopicStart};
use crate::watermark::Watermark;

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
    #[error(transparent)]
    Source(#[from] SourceError),
}

/// Everything [`Engine::connect`] needs for the standard setup: ClickHouse over HTTP, Kafka,
/// and the hard-coded naming rules.
#[derive(Debug, Clone)]
pub struct Config {
    pub clickhouse_url: String,
    pub user: String,
    pub password: String,
    /// ClickHouse database that dialect tables map to.
    pub database: String,
    /// Kafka bootstrap servers, e.g. `127.0.0.1:9092`.
    pub kafka_brokers: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            clickhouse_url: "http://127.0.0.1:18123".into(),
            user: "default".into(),
            password: String::new(),
            database: "default".into(),
            kafka_brokers: "127.0.0.1:9092".into(),
        }
    }
}

pub struct Engine {
    rules: Arc<dyn NamingRules>,
    history: Arc<dyn HistorySource>,
    source: Arc<dyn EventSource>,
}

impl Engine {
    /// The standard setup from a [`Config`].
    pub fn connect(config: Config) -> Self {
        Self::new(
            HardcodedRules {
                database: config.database,
            },
            ClickHouseHttp::new(&config.clickhouse_url, &config.user, &config.password),
            KafkaSource::new(&config.kafka_brokers),
        )
    }

    /// Any naming rules, history and event source, e.g. fakes in tests.
    pub fn new(
        rules: impl NamingRules + 'static,
        history: impl HistorySource + 'static,
        source: impl EventSource + 'static,
    ) -> Self {
        Self {
            rules: Arc::new(rules),
            history: Arc::new(history),
            source: Arc::new(source),
        }
    }

    /// Parse, bind and plan a query without running it.
    pub async fn explain(&self, sql: &str) -> Result<Plan, EngineError> {
        let query = parse(sql).map_err(|e| EngineError::Syntax(e.render(sql)))?;
        let catalog = Catalog {
            rules: self.rules.as_ref(),
            history: self.history.as_ref(),
            source: self.source.as_ref(),
        };
        let bound = bind(&query, &catalog).await.map_err(|e| match e {
            BindError::History(e) => EngineError::History(e),
            BindError::Source(e) => EngineError::Source(e),
            e => EngineError::Bind(e.render(sql)),
        })?;
        Ok(plan(&bound))
    }

    /// Plan a query and start it: the history first, then one iteration per event of the
    /// plan's topics.
    ///
    /// History and stream meet at the [`Watermark`], read now: history is every row up to it,
    /// and each partition is consumed from right after it. So every event is in exactly one of
    /// them, wherever ClickHouse's ingestion is at.
    pub async fn subscribe(&self, sql: &str) -> Result<Subscription, EngineError> {
        let plan = self.explain(sql).await?;
        let kafka = &plan.history.kafka;
        let watermark = Watermark::fetch(
            self.history.as_ref(),
            &plan.history.table,
            &kafka.partition,
            &kafka.offset,
        )
        .await?;
        let history_sql = plan.history.sql(Some(&watermark));
        let mut starts = Vec::new();
        for topic in &plan.stream.topics {
            let partitions = self
                .source
                .partitions(topic)
                .await?
                .ok_or_else(|| SourceError(format!("topic {topic} disappeared")))?;
            starts.push(TopicStart {
                topic: topic.clone(),
                partitions: partitions
                    .into_iter()
                    .map(|p| (p, watermark.start(p)))
                    .collect(),
            });
        }
        tracing::info!(history = %history_sql, watermark = %watermark.describe(), "subscribed");
        let events = self
            .source
            .subscribe(&starts, &plan.stream.required_keys())
            .await?;
        let iterations = self.iterations(&plan, history_sql.clone(), events);
        Ok(Subscription {
            plan,
            watermark,
            history_sql,
            iterations,
        })
    }

    fn iterations(
        &self,
        plan: &Plan,
        history_sql: String,
        events: BoxStream<'static, Event>,
    ) -> BoxStream<'static, Result<Iteration, EngineError>> {
        enum Item {
            History(Result<Vec<Row>, HistoryError>),
            Event(Event),
        }
        let history = self.history.clone();
        let initial = stream::once(async move { Item::History(history.fetch(&history_sql).await) });
        let stream_plan = plan.stream.clone();
        let state = ResultState::for_plan(&stream_plan);
        initial
            .chain(events.map(Item::Event))
            .scan(state, move |state, item| {
                let iteration = match item {
                    Item::History(rows) => history_iteration(&stream_plan, state, rows),
                    Item::Event(event) => Ok(event_iteration(&stream_plan, state, event)),
                };
                futures::future::ready(Some(iteration))
            })
            .boxed()
    }
}

/// What a subscription keeps about its result to place new rows.
enum ResultState {
    /// Unordered, unlimited: every row is appended.
    Append,
    /// LIMIT without ORDER BY: the latest N rows.
    Window(WindowState),
    /// ORDER BY (and maybe LIMIT): the sort keys of the result, in tie groups.
    Ordered(OrderedState),
}

impl ResultState {
    fn for_plan(plan: &StreamPlan) -> Self {
        match (&plan.order, plan.limit) {
            (Some(o), limit) => Self::Ordered(OrderedState::with_limit(o.keys.clone(), limit)),
            (None, Some(n)) => Self::Window(WindowState::new(n)),
            (None, None) => Self::Append,
        }
    }
}

/// The history as one iteration. Seeds the state: with ORDER BY the sort keys, in ClickHouse's
/// order (dropping the hidden ORDER BY fields from the rows); with only a LIMIT the row count.
fn history_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    rows: Result<Vec<Row>, HistoryError>,
) -> Result<Iteration, EngineError> {
    let mut rows = rows?;
    match (state, &plan.order) {
        (ResultState::Ordered(state), Some(order)) => {
            for row in &mut rows {
                let key = order
                    .history_key(row)
                    .map_err(|e| HistoryError::ClickHouse(format!("history row: {e}")))?;
                state.push_history(key);
            }
        }
        (ResultState::Window(w), _) => rows.iter().for_each(|_| w.push_history()),
        _ => {}
    }
    Ok(Iteration {
        origin: Origin::History,
        diffs: rows.into_iter().map(Diff::insert).collect(),
    })
}

/// One event: decode by column types, filter, project, and place it. An insert and the delete
/// it causes come in the same iteration, insert first, so they always land in the same diff.
fn event_iteration(plan: &StreamPlan, state: &mut ResultState, event: Event) -> Iteration {
    let diffs = match plan.decode(&event.value) {
        Ok(row) if plan.matches(&row) => match state {
            ResultState::Append => vec![Diff::insert(plan.project(&row))],
            ResultState::Window(w) => match w.push() {
                None => Vec::new(),
                Some(false) => vec![Diff::insert(plan.project(&row))],
                Some(true) => vec![Diff::insert(plan.project(&row)), Diff::Delete { index: 0 }],
            },
            ResultState::Ordered(s) => {
                let order = plan.order.as_ref().expect("ordered plan");
                match s.insert(order.event_key(&row)) {
                    None => Vec::new(),
                    Some(p) => {
                        let mut diffs = vec![Diff::Insert {
                            row: plan.project(&row),
                            index: Some(p.index),
                        }];
                        if let Some(index) = p.evicts {
                            diffs.push(Diff::Delete { index });
                        }
                        diffs
                    }
                }
            }
        },
        Ok(_) => Vec::new(),
        Err(e) => {
            // ClickHouse's ingestion skips this message too
            tracing::warn!(
                topic = %event.topic,
                partition = event.partition,
                offset = event.offset,
                error = %e,
                "skipping event"
            );
            Vec::new()
        }
    };
    Iteration {
        origin: Origin::Event {
            topic: event.topic,
            partition: event.partition,
            offset: event.offset,
        },
        diffs,
    }
}

/// A running query: a stream of [`Iteration`]s, plus the plan it runs.
pub struct Subscription {
    pub plan: Plan,
    /// Where history ends and the stream begins.
    pub watermark: Watermark,
    /// The history query as run: the plan's, bounded by the watermark.
    pub history_sql: String,
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
    Event {
        topic: TopicId,
        partition: i32,
        offset: i64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Iteration {
    pub origin: Origin,
    /// Empty when the event was not relevant to the query.
    pub diffs: Vec<Diff>,
}

/// One line per diff: `+ {row}` for the history (followed by `history: N rows`), and
/// `<topic>/<partition>:<offset> + {row}` for events. Irrelevant events print nothing.
impl fmt::Display for Iteration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.origin {
            Origin::History => {
                for d in &self.diffs {
                    writeln!(f, "{d}")?;
                }
                write!(f, "history: {} rows", self.diffs.len())
            }
            Origin::Event {
                topic,
                partition,
                offset,
            } => {
                let mut first = true;
                for d in &self.diffs {
                    if !first {
                        writeln!(f)?;
                    }
                    first = false;
                    write!(f, "{topic}/{partition}:{offset} {d}")?;
                }
                Ok(())
            }
        }
    }
}
