//! Simulated Kafka: each topic is `<dir>/<topic>.jsonl`, one JSON object (event) per line.

use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde_json::Value;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::warn;

use crate::diff::Row;
use crate::rules::TopicId;

/// Which topics exist in the event stream (used by the binder for FROM).
pub trait TopicCatalog: Send + Sync {
    fn exists(&self, topic: &TopicId) -> bool;
}

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub topic: TopicId,
    /// 1-based line in the topic file; plays the role of the Kafka offset.
    pub line: usize,
    /// Only the keys the query needs (see [`JsonlTopics::subscribe`]).
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
    /// opened. Each event is decoded keeping only `keys`; other keys are skipped without
    /// being parsed into values.
    pub fn subscribe(&self, topics: &[TopicId], keys: &[String]) -> BoxStream<'static, Event> {
        let keys: Arc<HashSet<String>> = Arc::new(keys.iter().cloned().collect());
        let streams: Vec<_> = topics
            .iter()
            .map(|t| self.topic_stream(t.clone(), keys.clone()))
            .collect();
        stream::select_all(streams).boxed()
    }

    fn topic_stream(
        &self,
        topic: TopicId,
        keys: Arc<HashSet<String>>,
    ) -> BoxStream<'static, Event> {
        struct State {
            keys: Arc<HashSet<String>>,
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
            keys,
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
                        match decode_pruned(text, &s.keys) {
                            Ok(value) => {
                                if !s.delay.is_zero() {
                                    tokio::time::sleep(s.delay).await;
                                }
                                let event = Event { topic: s.topic.clone(), line: s.line, value };
                                return Some((event, s));
                            }
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

impl TopicCatalog for JsonlTopics {
    /// A topic exists if its file does.
    fn exists(&self, topic: &TopicId) -> bool {
        self.path(topic).is_file()
    }
}

/// Decode one JSON object, keeping only `keys`. Values of other keys are skipped with
/// [`IgnoredAny`], so they are validated but never built (no allocation for nested objects or
/// long strings). Anything other than an object is an error.
pub fn decode_pruned(text: &str, keys: &HashSet<String>) -> Result<Row, serde_json::Error> {
    let mut de = serde_json::Deserializer::from_str(text);
    let row = Pruned(keys).deserialize(&mut de)?;
    de.end()?;
    Ok(row)
}

struct Pruned<'a>(&'a HashSet<String>);

impl<'de> DeserializeSeed<'de> for Pruned<'_> {
    type Value = Row;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Row, D::Error> {
        deserializer.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for Pruned<'_> {
    type Value = Row;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Row, A::Error> {
        let mut row = Row::new();
        while let Some(key) = map.next_key::<String>()? {
            if self.0.contains(&key) {
                let value: Value = map.next_value()?;
                row.insert(key, value);
            } else {
                map.next_value::<IgnoredAny>()?;
            }
        }
        Ok(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(k: &[&str]) -> HashSet<String> {
        k.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn keeps_only_required_keys() {
        let row = decode_pruned(
            r#"{"ts": "t", "big": {"nested": [1, 2, {"x": "y"}]}, "user_id": "u", "note": "long text"}"#,
            &keys(&["user_id", "ts"]),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&row).unwrap(),
            r#"{"ts":"t","user_id":"u"}"#
        );
    }

    #[test]
    fn missing_keys_are_simply_absent_and_bad_input_is_an_error() {
        assert!(decode_pruned(r#"{"ts": "t"}"#, &keys(&["user_id"]))
            .unwrap()
            .is_empty());
        assert!(decode_pruned("[1, 2]", &keys(&["ts"])).is_err());
        assert!(decode_pruned("not json", &keys(&["ts"])).is_err());
        assert!(decode_pruned(r#"{"ts": "t"} trailing"#, &keys(&["ts"])).is_err());
        // skipped values are still validated
        assert!(decode_pruned(r#"{"ts": "t", "x": [1,}"#, &keys(&["ts"])).is_err());
    }
}
