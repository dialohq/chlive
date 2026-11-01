//! One event of a topic, as a source hands it over (the engine's sources: Kafka, memory).

use crate::diff::Row;
use crate::rules::TopicId;

#[derive(Debug, Clone, PartialEq)]
pub struct Event {
    pub topic: TopicId,
    pub partition: i32,
    pub offset: i64,
    /// The message's timestamp, in milliseconds (as Kafka Connect's `InsertField` stores it).
    pub timestamp: Option<i64>,
    /// Only the keys the query needs.
    pub value: Row,
}
