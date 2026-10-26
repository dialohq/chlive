//! The connection: one task owning the WebSocket. It opens when there's a subscription,
//! (re)subscribes every active one on each open, from the cursor of the last message it handed
//! it (so the server can resume rather than snapshot), hands each subscription the messages for
//! it, and reconnects with exponential backoff when the socket drops.
//!
//! Messages are only read here; applying them is the subscriptions' work, when their streams
//! are polled. Each subscription's queue is bounded: a full one makes this task wait, which
//! stops reading the socket, which slows the server (it falls behind on Kafka, never drops).

use std::collections::BTreeMap;

use chlive_protocol::{ClientMsg, Params, ServerMsg};
use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use crate::ClientOptions;

pub(crate) enum Cmd {
    Subscribe {
        id: String,
        sql: String,
        params: Params,
        events: mpsc::Sender<Event>,
    },
    Unsubscribe(String),
    Close(oneshot::Sender<()>),
}

/// What a subscription hears from the connection.
pub(crate) enum Event {
    /// Its `subscribe` was sent.
    Subscribed,
    Message {
        msg: ServerMsg,
        bytes: usize,
    },
    /// A message for it that isn't valid (e.g. an unknown change op).
    Unreadable {
        error: String,
        bytes: usize,
    },
    Disconnected,
}

struct Sub {
    sql: String,
    params: Params,
    events: mpsc::Sender<Event>,
    /// The `at` of the last message handed to it: everything handed over is applied in order,
    /// so it's where the subscription's rows will be.
    at: Option<String>,
}

pub(crate) fn spawn(opts: ClientOptions) -> mpsc::UnboundedSender<Cmd> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run(opts, rx));
    tx
}

/// Whether to keep going.
enum Flow {
    Go,
    Stop,
}

/// A command while disconnected: subscriptions are only recorded (sent on the next open).
fn offline(cmd: Option<Cmd>, subs: &mut BTreeMap<String, Sub>) -> Flow {
    match cmd {
        None => Flow::Stop,
        Some(Cmd::Subscribe {
            id,
            sql,
            params,
            events,
        }) => {
            subs.insert(
                id,
                Sub {
                    sql,
                    params,
                    events,
                    at: None,
                },
            );
            Flow::Go
        }
        Some(Cmd::Unsubscribe(id)) => {
            subs.remove(&id);
            Flow::Go
        }
        Some(Cmd::Close(done)) => {
            let _ = done.send(());
            Flow::Stop
        }
    }
}

fn text(msg: &ClientMsg) -> Message {
    Message::text(serde_json::to_string(msg).expect("client messages serialize"))
}

fn subscribe(id: &str, sub: &Sub) -> Message {
    text(&ClientMsg::Subscribe {
        id: id.to_string(),
        sql: sub.sql.clone(),
        since: sub.at.clone(),
        params: sub.params.clone(),
    })
}

async fn run(opts: ClientOptions, mut cmds: mpsc::UnboundedReceiver<Cmd>) {
    let mut subs: BTreeMap<String, Sub> = BTreeMap::new();
    let mut backoff = opts.min_backoff;
    loop {
        // connect only when there's something to subscribe to
        while subs.is_empty() {
            if let Flow::Stop = offline(cmds.recv().await, &mut subs) {
                return;
            }
        }
        let connecting = tokio_tungstenite::connect_async_with_config(
            opts.url.as_str(),
            Some(ws_config()),
            false,
        );
        tokio::pin!(connecting);
        let ws = loop {
            tokio::select! {
                r = &mut connecting => break r,
                cmd = cmds.recv() => if let Flow::Stop = offline(cmd, &mut subs) {
                    return;
                },
            }
        };
        match ws {
            Err(e) => tracing::warn!(url = %opts.url, error = %e, "chlive: can't connect"),
            Ok((ws, _)) => {
                backoff = opts.min_backoff;
                if let Flow::Stop = connected(ws, &mut subs, &mut cmds).await {
                    return;
                }
                for sub in subs.values() {
                    let _ = sub.events.send(Event::Disconnected).await;
                }
            }
        }
        // wait, still taking commands
        let wait = tokio::time::sleep(backoff);
        tokio::pin!(wait);
        backoff = (backoff * 2).min(opts.max_backoff);
        loop {
            tokio::select! {
                _ = &mut wait => break,
                cmd = cmds.recv() => if let Flow::Stop = offline(cmd, &mut subs) {
                    return;
                },
            }
        }
    }
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// One open connection, until it drops (`Go`: reconnect) or the client closes (`Stop`).
async fn connected(
    ws: Socket,
    subs: &mut BTreeMap<String, Sub>,
    cmds: &mut mpsc::UnboundedReceiver<Cmd>,
) -> Flow {
    let (mut sink, mut stream) = ws.split();
    for (id, sub) in subs.iter() {
        if sink.send(subscribe(id, sub)).await.is_err() {
            return Flow::Go;
        }
        let _ = sub.events.send(Event::Subscribed).await;
    }
    loop {
        tokio::select! {
            cmd = cmds.recv() => match cmd {
                None => {
                    let _ = sink.close().await;
                    return Flow::Stop;
                }
                Some(Cmd::Close(done)) => {
                    let _ = sink.close().await;
                    subs.clear();
                    let _ = done.send(());
                    return Flow::Stop;
                }
                Some(Cmd::Subscribe {
                    id,
                    sql,
                    params,
                    events,
                }) => {
                    let sub = Sub {
                        sql,
                        params,
                        events,
                        at: None,
                    };
                    let sent = sink.send(subscribe(&id, &sub)).await.is_ok();
                    if sent {
                        let _ = sub.events.send(Event::Subscribed).await;
                    }
                    subs.insert(id, sub);
                    if !sent {
                        return Flow::Go;
                    }
                }
                Some(Cmd::Unsubscribe(id)) => {
                    if subs.remove(&id).is_some()
                        && sink.send(text(&ClientMsg::Unsubscribe { id })).await.is_err()
                    {
                        return Flow::Go;
                    }
                }
            },
            frame = stream.next() => match frame {
                Some(Ok(Message::Text(t))) => dispatch(subs, t.as_str()).await,
                Some(Ok(Message::Close(_))) | None => return Flow::Go,
                Some(Err(e)) => {
                    tracing::warn!(error = %e, "chlive: connection lost");
                    return Flow::Go;
                }
                Some(Ok(_)) => {}
            },
        }
    }
}

/// Hand a message to its subscription. One that doesn't parse fails only that subscription.
async fn dispatch(subs: &mut BTreeMap<String, Sub>, text: &str) {
    let bytes = text.len();
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "chlive: a message that isn't JSON");
            return;
        }
    };
    let Some(id) = value.get("id").and_then(|v| v.as_str()) else {
        tracing::warn!(message = %value, "chlive: a message for no subscription");
        return;
    };
    // messages for subscriptions that are gone are ignored
    let Some(sub) = subs.get_mut(id) else { return };
    let event = match serde_json::from_value::<ServerMsg>(value.clone()) {
        Ok(msg) => {
            if let ServerMsg::Snapshot { at, .. }
            | ServerMsg::Diff { at, .. }
            | ServerMsg::Progress { at, .. } = &msg
            {
                sub.at = Some(at.clone());
            }
            Event::Message { msg, bytes }
        }
        Err(e) => Event::Unreadable {
            error: format!("unsupported message ({e}): {value}"),
            bytes,
        },
    };
    let _ = sub.events.send(event).await;
}

/// No size limit on messages: a snapshot is the whole result in one message, so a big result
/// is a big message (tungstenite's default, 16 MiB, would drop the connection on it). Browsers
/// don't limit them either.
fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None)
}
