//! Minimal HTTP → Kafka produce proxy for local development.
//!
//! Speaks the subset of the Kafka REST / Redpanda HTTP proxy API the simple demo uses:
//! `POST /topics/{topic}` with `{"records": [{"value": <json>, "partition"?: n}]}` produces
//! each `value` as one JSON message and answers `{"offsets": [{"partition", "offset"}]}`.
//! Apache Kafka has no built-in HTTP proxy, unlike Redpanda.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
struct ProduceRequest {
    records: Vec<RecordIn>,
}

#[derive(Deserialize)]
struct RecordIn {
    value: Value,
    partition: Option<i32>,
}

pub async fn serve(brokers: &str, listen: SocketAddr) -> Result<()> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        // one click = one message: send immediately
        .set("linger.ms", "0")
        .create()
        .context("creating kafka producer")?;
    let app = Router::new()
        .route("/topics/{topic}", post(produce))
        .with_state(producer);
    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .with_context(|| format!("binding {listen}"))?;
    tracing::info!(%listen, brokers, "HTTP produce proxy listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn produce(
    State(producer): State<FutureProducer>,
    Path(topic): Path<String>,
    Json(req): Json<ProduceRequest>,
) -> Response {
    if topic.is_empty()
        || !topic
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c))
    {
        return (StatusCode::BAD_REQUEST, "invalid topic name").into_response();
    }
    let mut offsets = Vec::with_capacity(req.records.len());
    for r in req.records {
        let payload = r.value.to_string();
        let mut record = FutureRecord::<(), _>::to(&topic).payload(&payload);
        if let Some(p) = r.partition {
            record = record.partition(p);
        }
        match producer.send(record, Duration::from_secs(10)).await {
            Ok(d) => offsets.push(json!({"partition": d.partition, "offset": d.offset})),
            Err((e, _)) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    format!("kafka delivery failed: {e}"),
                )
                    .into_response()
            }
        }
    }
    (
        [(header::CONTENT_TYPE, "application/vnd.kafka.v2+json")],
        Json(json!({ "offsets": offsets })),
    )
        .into_response()
}
