//! Runs a query: history from ClickHouse first, then the event loop over the subscribed
//! topics. Each step yields an [`Iteration`] with the diffs it produced.

use std::sync::Arc;

use futures::stream::{self, BoxStream, StreamExt};

use crate::diff::Diff;
use crate::history::{HistoryError, HistorySource};
use crate::parser::parse;
use crate::plan::{plan, Plan};
use crate::rules::{NamingRules, TopicId};
use crate::schema::resolve;
use crate::stream::JsonlTopics;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Rendered with line and column.
    #[error("syntax error: {0}")]
    Syntax(String),
    #[error(transparent)]
    History(#[from] HistoryError),
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

pub struct Engine {
    pub rules: Arc<dyn NamingRules>,
    pub history: Arc<dyn HistorySource>,
    pub topics: JsonlTopics,
}

impl Engine {
    /// Parse and plan a query.
    pub async fn prepare(&self, sql: &str) -> Result<Plan, EngineError> {
        let query = parse(sql).map_err(|e| EngineError::Syntax(e.render(sql)))?;
        let schema = resolve(&query.from.name, self.rules.as_ref(), self.history.as_ref()).await?;
        Ok(plan(&query, self.rules.as_ref(), &schema))
    }

    /// The query's iterations: the history first, then one per event of the plan's topics.
    pub fn run(&self, plan: Plan) -> BoxStream<'static, Result<Iteration, EngineError>> {
        let history = self.history.clone();
        let sql = plan.history.sql.clone();
        let initial = stream::once(async move {
            let rows = history.fetch(&sql).await?;
            Ok(Iteration {
                origin: Origin::History,
                diffs: rows.into_iter().map(Diff::Insert).collect(),
            })
        });
        let stream_plan = plan.stream;
        let events = self
            .topics
            .subscribe(&stream_plan.topics)
            .map(move |event| {
                let diffs = if stream_plan.filter.matches(&event.value) {
                    vec![Diff::Insert(stream_plan.project(&event.value))]
                } else {
                    Vec::new()
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
