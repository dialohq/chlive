//! Simulated Kafka: each topic is `<dir>/<topic>.jsonl`, one JSON object (event) per line.

use std::path::PathBuf;
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::warn;

use crate::diff::Row;
use crate::rules::TopicId;

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub topic: TopicId,
    /// 1-based line in the topic file; plays the role of the Kafka offset.
    pub line: usize,
    pub value: Row,
}

#[derive(Debug, Clone)]
pub struct JsonlTopics {
    pub dir: PathBuf,
    /// Pause before each event, to simulate events arriving over time.
    pub delay: Duration,
    /// Keep waiting for lines appended to the files, like `tail -f`.
    pub follow: bool,
}

impl JsonlTopics {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            delay: Duration::ZERO,
            follow: false,
        }
    }

    pub fn path(&self, topic: &TopicId) -> PathBuf {
        self.dir.join(format!("{}.jsonl", topic.0))
    }

    /// Events of exactly these topics, merged as they become available. No other file is
    /// opened.
    pub fn subscribe(&self, topics: &[TopicId]) -> BoxStream<'static, Event> {
        let streams: Vec<_> = topics
            .iter()
            .map(|t| self.topic_stream(t.clone()))
            .collect();
        stream::select_all(streams).boxed()
    }

    fn topic_stream(&self, topic: TopicId) -> BoxStream<'static, Event> {
        struct State {
            topic: TopicId,
            path: PathBuf,
            reader: Option<BufReader<File>>,
            line: usize,
            partial: String,
            delay: Duration,
            follow: bool,
        }
        const POLL: Duration = Duration::from_millis(200);
        let state = State {
            path: self.path(&topic),
            topic,
            reader: None,
            line: 0,
            partial: String::new(),
            delay: self.delay,
            follow: self.follow,
        };
        stream::unfold(state, |mut s| async move {
            loop {
                let reader = match &mut s.reader {
                    Some(r) => r,
                    None => match File::open(&s.path).await {
                        Ok(f) => s.reader.insert(BufReader::new(f)),
                        Err(e) if s.follow => {
                            tracing::debug!(path = %s.path.display(), error = %e, "topic file not there yet");
                            tokio::time::sleep(POLL).await;
                            continue;
                        }
                        Err(e) => {
                            warn!(path = %s.path.display(), error = %e, "cannot open topic file");
                            return None;
                        }
                    },
                };
                let mut chunk = String::new();
                match reader.read_line(&mut chunk).await {
                    Ok(0) if s.follow => {
                        tokio::time::sleep(POLL).await;
                        continue;
                    }
                    Ok(0) => return None,
                    Ok(_) => {
                        s.partial.push_str(&chunk);
                        // In follow mode a writer may not have finished the line yet.
                        if !s.partial.ends_with('\n') && s.follow {
                            continue;
                        }
                        let text = std::mem::take(&mut s.partial);
                        s.line += 1;
                        let text = text.trim();
                        if text.is_empty() {
                            continue;
                        }
                        match serde_json::from_str::<serde_json::Value>(text) {
                            Ok(serde_json::Value::Object(value)) => {
                                if !s.delay.is_zero() {
                                    tokio::time::sleep(s.delay).await;
                                }
                                let event = Event { topic: s.topic.clone(), line: s.line, value };
                                return Some((event, s));
                            }
                            Ok(_) => warn!(path = %s.path.display(), line = s.line, "skipping non-object JSON line"),
                            Err(e) => warn!(path = %s.path.display(), line = s.line, error = %e, "skipping malformed line"),
                        }
                    }
                    Err(e) => {
                        warn!(path = %s.path.display(), error = %e, "read error");
                        return None;
                    }
                }
            }
        })
        .boxed()
    }
}
