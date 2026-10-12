//! Where events come from: Kafka ([`KafkaSource`]), or memory for tests ([`MemorySource`]).
//!
//! A source is asked for exactly the topics a query needs, starting each partition at a given
//! offset (right after the history watermark), and decodes only the JSON keys the query needs.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::stream::{self, BoxStream, StreamExt};
use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde_json::Value;
use tracing::warn;

use crate::diff::Row;
use crate::rules::TopicId;

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub topic: TopicId,
    pub partition: i32,
    pub offset: i64,
    /// Only the keys the query needs (see [`decode_pruned`]).
    pub value: Row,
}

#[derive(Debug, thiserror::Error)]
#[error("event source: {0}")]
pub struct SourceError(pub String);

/// Where to start reading one topic: `(partition, first offset)` for every partition.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicStart {
    pub topic: TopicId,
    pub partitions: Vec<(i32, i64)>,
}

#[async_trait]
pub trait EventSource: Send + Sync {
    /// The topic's partitions, or `None` if the topic doesn't exist.
    async fn partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError>;

    /// Per partition, the first offset still kept and the next to be written, if known. A resume
    /// cursor at or past the end comes from different data (the topic was recreated); one that
    /// needs offsets before the start can't be resumed (they were deleted by retention).
    async fn offset_bounds(
        &self,
        _topic: &TopicId,
    ) -> Result<Option<BTreeMap<i32, (i64, i64)>>, SourceError> {
        Ok(None)
    }

    /// The topic's partitions, read anew from the source rather than from any cache: for when
    /// a snapshot knows a partition [`EventSource::partitions`] doesn't (partitions were added).
    async fn refresh_partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        self.partitions(topic).await
    }

    /// Events of exactly these topics and partitions, from the given offsets on; each decoded
    /// keeping only `keys`. Undecodable messages are skipped with a warning.
    async fn subscribe(
        &self,
        starts: &[TopicStart],
        keys: &[String],
    ) -> Result<BoxStream<'static, Event>, SourceError>;
}

/// How often [`KafkaSource`] reloads the topics' partitions in the background.
pub const METADATA_REFRESH: Duration = Duration::from_secs(30);
/// A topic missing from metadata fetched longer ago than this is looked up again (it may have
/// been created since); a more recent miss means it doesn't exist.
const MISS_RETRY: Duration = Duration::from_secs(1);
const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// Every topic's partitions, as of one metadata fetch.
#[derive(Debug, Default)]
struct TopicCache {
    partitions: HashMap<String, Vec<i32>>,
    fetched_at: Option<Instant>,
}

#[derive(Debug, PartialEq)]
enum Lookup {
    /// The answer: the partitions, or `None` if the topic doesn't exist.
    Known(Option<Vec<i32>>),
    /// The cache can't answer: fetch the metadata.
    Fetch,
}

impl TopicCache {
    /// What the cache knows of `topic` at `now`, if it's younger than `max_age`.
    fn lookup(&self, topic: &str, now: Instant, max_age: Duration) -> Lookup {
        let Some(at) = self.fetched_at else {
            return Lookup::Fetch;
        };
        let age = now.saturating_duration_since(at);
        match self.partitions.get(topic) {
            _ if age >= max_age => Lookup::Fetch,
            Some(p) => Lookup::Known(Some(p.clone())),
            None if age < MISS_RETRY => Lookup::Known(None),
            None => Lookup::Fetch,
        }
    }

    /// The topics of a new fetch, replacing all others (deleted topics go).
    fn replace(&mut self, partitions: HashMap<String, Vec<i32>>, now: Instant) {
        self.partitions = partitions;
        self.fetched_at = Some(now);
    }
}

/// Topic metadata for a [`KafkaSource`]: one long-lived client, and every topic's partitions,
/// fetched at start and refreshed in the background.
struct Metadata {
    brokers: String,
    /// Created on first use. An admin client polls itself (a consumer would have to be polled).
    client: Mutex<Option<Arc<AdminClient<DefaultClientContext>>>>,
    cache: RwLock<TopicCache>,
    /// Held while fetching, so concurrent misses fetch once.
    fetching: tokio::sync::Mutex<()>,
    /// The cache is trusted for this long (a few refresh intervals).
    max_age: Duration,
    fetches: AtomicU64,
}

impl Metadata {
    fn client(&self) -> Result<Arc<AdminClient<DefaultClientContext>>, SourceError> {
        let mut client = self.client.lock().unwrap();
        if let Some(c) = client.as_ref() {
            return Ok(c.clone());
        }
        let c: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.brokers)
            .create()
            .map_err(|e| SourceError(format!("creating the metadata client: {e}")))?;
        let c = Arc::new(c);
        *client = Some(c.clone());
        Ok(c)
    }

    /// Fetch every topic's partitions (one request) into the cache, unless a fetch finished at
    /// or after `since` (e.g. while this one waited for it).
    async fn fetch(&self, since: Instant) -> Result<(), SourceError> {
        let _fetching = self.fetching.lock().await;
        let fetched_at = self.cache.read().unwrap().fetched_at;
        if fetched_at.is_some_and(|at| at >= since) {
            return Ok(());
        }
        let client = self.client()?;
        let partitions = tokio::task::spawn_blocking(move || {
            let md = client
                .inner()
                .fetch_metadata(None, METADATA_TIMEOUT)
                .map_err(|e| SourceError(format!("fetching metadata: {e}")))?;
            Ok::<_, SourceError>(
                md.topics()
                    .iter()
                    .filter(|t| t.error().is_none() && !t.partitions().is_empty())
                    .map(|t| {
                        let parts = t.partitions().iter().map(|p| p.id()).collect();
                        (t.name().to_string(), parts)
                    })
                    .collect(),
            )
        })
        .await
        .map_err(|e| SourceError(e.to_string()))??;
        self.fetches.fetch_add(1, Ordering::Relaxed);
        self.cache
            .write()
            .unwrap()
            .replace(partitions, Instant::now());
        Ok(())
    }

    async fn partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        let lookup = self
            .cache
            .read()
            .unwrap()
            .lookup(&topic.0, Instant::now(), self.max_age);
        if let Lookup::Known(p) = lookup {
            return Ok(p);
        }
        self.fetch(Instant::now()).await?;
        Ok(self.cache.read().unwrap().partitions.get(&topic.0).cloned())
    }
}

/// Reload the metadata now and every `every`, until the source is dropped.
async fn refresh(metadata: Weak<Metadata>, every: Duration) {
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        let Some(m) = metadata.upgrade() else { return };
        // not again if a lookup just fetched (e.g. the first one, racing the load at start)
        let since = Instant::now()
            .checked_sub(every / 2)
            .unwrap_or_else(Instant::now);
        if let Err(e) = m.fetch(since).await {
            warn!(error = %e, "refreshing Kafka topic metadata");
        }
    }
}

/// Kafka (or Redpanda) via rdkafka. Partitions are assigned explicitly at the requested offsets;
/// nothing is ever committed, so subscriptions don't interfere with each other or with other
/// consumers. Each subscription has its own consumer; topic metadata (partitions, offsets)
/// goes through one shared client, and the partitions are cached: read at start and every
/// [`METADATA_REFRESH`], so subscribing doesn't ask Kafka.
pub struct KafkaSource {
    brokers: String,
    metadata: Arc<Metadata>,
}

impl KafkaSource {
    pub fn new(brokers: &str) -> Self {
        Self::with_metadata_refresh(brokers, METADATA_REFRESH)
    }

    /// Reload the topics' partitions every `every`. The background reload runs when created
    /// inside a tokio runtime; otherwise (and if it falls behind) a lookup on a cache older
    /// than three intervals fetches anew.
    pub fn with_metadata_refresh(brokers: &str, every: Duration) -> Self {
        let metadata = Arc::new(Metadata {
            brokers: brokers.to_string(),
            client: Mutex::new(None),
            cache: RwLock::new(TopicCache::default()),
            fetching: tokio::sync::Mutex::new(()),
            max_age: every * 3,
            fetches: AtomicU64::new(0),
        });
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(refresh(Arc::downgrade(&metadata), every));
        }
        Self {
            brokers: brokers.to_string(),
            metadata,
        }
    }

    /// How many metadata fetches this source has made (for tests).
    #[doc(hidden)]
    pub fn metadata_fetches(&self) -> u64 {
        self.metadata.fetches.load(Ordering::Relaxed)
    }

    fn config(&self) -> ClientConfig {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", &self.brokers)
            // required by librdkafka even with manual assignment; nothing is committed
            .set("group.id", format!("chlive-engine-{}", std::process::id()))
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("allow.auto.create.topics", "false")
            .set("fetch.wait.max.ms", "10");
        c
    }
}

#[async_trait]
impl EventSource for KafkaSource {
    async fn partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        self.metadata.partitions(topic).await
    }

    async fn refresh_partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        self.metadata.fetch(Instant::now()).await?;
        Ok(self
            .metadata
            .cache
            .read()
            .unwrap()
            .partitions
            .get(&topic.0)
            .cloned())
    }

    async fn offset_bounds(
        &self,
        topic: &TopicId,
    ) -> Result<Option<BTreeMap<i32, (i64, i64)>>, SourceError> {
        let Some(partitions) = self.partitions(topic).await? else {
            return Ok(None);
        };
        let client = self.metadata.client()?;
        let name = topic.0.clone();
        tokio::task::spawn_blocking(move || {
            let mut bounds = BTreeMap::new();
            for p in partitions {
                let (low, high) = client
                    .inner()
                    .fetch_watermarks(&name, p, METADATA_TIMEOUT)
                    .map_err(|e| SourceError(format!("fetching offsets: {e}")))?;
                bounds.insert(p, (low, high));
            }
            Ok(Some(bounds))
        })
        .await
        .map_err(|e| SourceError(e.to_string()))?
    }

    async fn subscribe(
        &self,
        starts: &[TopicStart],
        keys: &[String],
    ) -> Result<BoxStream<'static, Event>, SourceError> {
        let consumer: StreamConsumer = self
            .config()
            .create()
            .map_err(|e| SourceError(e.to_string()))?;
        let mut tpl = TopicPartitionList::new();
        for s in starts {
            for (partition, offset) in &s.partitions {
                tpl.add_partition_offset(&s.topic.0, *partition, Offset::Offset(*offset))
                    .map_err(|e| SourceError(e.to_string()))?;
            }
        }
        consumer
            .assign(&tpl)
            .map_err(|e| SourceError(format!("assigning partitions: {e}")))?;
        let keys: Arc<HashSet<String>> = Arc::new(keys.iter().cloned().collect());
        Ok(stream::unfold((Arc::new(consumer), keys), |(consumer, keys)| async move {
            loop {
                let msg = match consumer.recv().await {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(error = %e, "kafka error");
                        continue;
                    }
                };
                let (topic, partition, offset) = (msg.topic().to_string(), msg.partition(), msg.offset());
                match decode_pruned(msg.payload().unwrap_or_default(), &keys) {
                    Ok(value) => {
                        let event = Event { topic: TopicId(topic), partition, offset, value };
                        drop(msg);
                        return Some((event, (consumer, keys)));
                    }
                    Err(e) => warn!(topic, partition, offset, error = %e, "skipping undecodable message"),
                }
            }
        })
        .boxed())
    }
}

/// Events held in memory, for tests and examples: `topic → partition → messages` (a message's
/// offset is its index). Finite: the stream ends after the last message.
#[derive(Debug, Clone, Default)]
pub struct MemorySource {
    topics: BTreeMap<TopicId, BTreeMap<i32, Vec<String>>>,
}

impl MemorySource {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a raw message (normally JSON) to a topic partition.
    pub fn push(&mut self, topic: &str, partition: i32, message: impl Into<String>) -> &mut Self {
        self.topics
            .entry(TopicId(topic.into()))
            .or_default()
            .entry(partition)
            .or_default()
            .push(message.into());
        self
    }

    /// Declare a topic partition without messages.
    pub fn partition(&mut self, topic: &str, partition: i32) -> &mut Self {
        self.topics
            .entry(TopicId(topic.into()))
            .or_default()
            .entry(partition)
            .or_default();
        self
    }
}

#[async_trait]
impl EventSource for MemorySource {
    async fn partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        Ok(self.topics.get(topic).map(|p| p.keys().copied().collect()))
    }

    async fn offset_bounds(
        &self,
        topic: &TopicId,
    ) -> Result<Option<BTreeMap<i32, (i64, i64)>>, SourceError> {
        Ok(self.topics.get(topic).map(|p| {
            p.iter()
                .map(|(part, msgs)| (*part, (0, msgs.len() as i64)))
                .collect()
        }))
    }

    async fn subscribe(
        &self,
        starts: &[TopicStart],
        keys: &[String],
    ) -> Result<BoxStream<'static, Event>, SourceError> {
        let keys: HashSet<String> = keys.iter().cloned().collect();
        let mut events = Vec::new();
        for s in starts {
            for (partition, start) in &s.partitions {
                let messages = self.topics.get(&s.topic).and_then(|t| t.get(partition));
                for (offset, text) in messages
                    .into_iter()
                    .flatten()
                    .enumerate()
                    .skip((*start).max(0) as usize)
                {
                    match decode_pruned(text.as_bytes(), &keys) {
                        Ok(value) => events.push(Event {
                            topic: s.topic.clone(),
                            partition: *partition,
                            offset: offset as i64,
                            value,
                        }),
                        Err(e) => {
                            warn!(topic = %s.topic, partition, offset, error = %e, "skipping undecodable message")
                        }
                    }
                }
            }
        }
        Ok(stream::iter(events).boxed())
    }
}

/// Decode one JSON object, keeping only `keys`. Values of other keys are skipped with
/// [`IgnoredAny`], so they are validated but never built (no allocation for nested objects or
/// long strings). Anything other than an object is an error.
pub fn decode_pruned(bytes: &[u8], keys: &HashSet<String>) -> Result<Row, serde_json::Error> {
    let mut de = serde_json::Deserializer::from_slice(bytes);
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
            br#"{"ts": "t", "big": {"nested": [1, 2, {"x": "y"}]}, "user_id": "u", "note": "long text"}"#,
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
        assert!(decode_pruned(br#"{"ts": "t"}"#, &keys(&["user_id"]))
            .unwrap()
            .is_empty());
        assert!(decode_pruned(b"[1, 2]", &keys(&["ts"])).is_err());
        assert!(decode_pruned(b"not json", &keys(&["ts"])).is_err());
        assert!(decode_pruned(br#"{"ts": "t"} trailing"#, &keys(&["ts"])).is_err());
        // skipped values are still validated
        assert!(decode_pruned(br#"{"ts": "t", "x": [1,}"#, &keys(&["ts"])).is_err());
    }

    #[test]
    fn the_topic_cache_answers_while_fresh_and_asks_again_after() {
        let max_age = Duration::from_secs(90);
        let t0 = Instant::now();
        let mut cache = TopicCache::default();
        assert_eq!(
            cache.lookup("events", t0, max_age),
            Lookup::Fetch,
            "never fetched"
        );
        cache.replace([("events".to_string(), vec![0, 1, 2])].into(), t0);
        let at = |ms: u64| t0 + Duration::from_millis(ms);
        assert_eq!(
            cache.lookup("events", at(500), max_age),
            Lookup::Known(Some(vec![0, 1, 2]))
        );
        // a miss right after a fetch: the topic doesn't exist
        assert_eq!(cache.lookup("nope", at(500), max_age), Lookup::Known(None));
        // later, it may have been created since
        assert_eq!(cache.lookup("nope", at(1500), max_age), Lookup::Fetch);
        // too old for anything
        assert_eq!(cache.lookup("events", at(90_000), max_age), Lookup::Fetch);
        // a fetch replaces every topic: deleted ones go
        cache.replace([("orders".to_string(), vec![0])].into(), at(91_000));
        assert_eq!(
            cache.lookup("events", at(91_100), max_age),
            Lookup::Known(None)
        );
        assert_eq!(
            cache.lookup("orders", at(91_100), max_age),
            Lookup::Known(Some(vec![0]))
        );
    }

    #[tokio::test]
    async fn memory_source_starts_at_the_requested_offsets() {
        let mut src = MemorySource::new();
        src.push("events", 0, r#"{"a": 0}"#)
            .push("events", 0, r#"{"a": 1}"#)
            .push("events", 1, r#"{"a": 2}"#);
        src.push("other", 0, r#"{"a": 9}"#);
        assert_eq!(
            src.partitions(&TopicId("events".into())).await.unwrap(),
            Some(vec![0, 1])
        );
        assert_eq!(src.partitions(&TopicId("nope".into())).await.unwrap(), None);
        let starts = [TopicStart {
            topic: TopicId("events".into()),
            partitions: vec![(0, 1), (1, 0)],
        }];
        let events: Vec<(i32, i64)> = src
            .subscribe(&starts, &["a".into()])
            .await
            .unwrap()
            .map(|e| (e.partition, e.offset))
            .collect()
            .await;
        assert_eq!(events, [(0, 1), (1, 0)]);
    }
}
