//! The connection: one task owning the WebSocket. It opens when there's a subscription,
//! (re)subscribes every active one on each open, from the cursor of the last message it handed
//! it (so the server can resume rather than snapshot), hands each subscription the messages for
//! it, and reconnects with exponential backoff when the socket drops.
//!
//! Messages are only read here; applying them is the subscriptions' work, when their streams
//! are polled. Each subscription's queue is bounded: a full one makes this task wait, which
//! stops reading the socket, which slows the server (it falls behind on Kafka, never drops).

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chlive_protocol::{ClientMsg, Params, Row, ServerMsg};
use futures::future::BoxFuture;
use futures::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

use crate::{Ack, ClientOptions, LiveError, CLIENT_ERROR};

pub(crate) enum Cmd {
    Subscribe {
        id: String,
        sql: String,
        params: Params,
        complete: bool,
        events: mpsc::Sender<Event>,
    },
    Unsubscribe(String),
    Insert {
        id: String,
        table: String,
        rows: Vec<Row>,
        done: oneshot::Sender<Result<Ack, LiveError>>,
    },
    Close(oneshot::Sender<()>),
}

/// An insert waiting for its answer.
pub(crate) struct Insert {
    table: String,
    rows: Vec<Row>,
    done: oneshot::Sender<Result<Ack, LiveError>>,
}

/// Inserts not sent yet (sent on the next open), and sent ones waiting for their `ack`: a drop
/// fails those, since whether they went in is unknown.
#[derive(Default)]
struct Inserts {
    queued: BTreeMap<String, Insert>,
    sent: BTreeMap<String, Insert>,
}

impl Inserts {
    /// The connection dropped: what was sent and not answered may or may not have gone in.
    fn disconnected(&mut self) {
        for (_, i) in std::mem::take(&mut self.sent) {
            let _ = i.done.send(Err(LiveError {
                code: chlive_protocol::DISCONNECTED.into(),
                message: "the connection dropped before the server answered; the rows may or may not have gone in".into(),
            }));
        }
    }

    /// The client closed: nothing more is sent.
    fn closed(&mut self) {
        self.disconnected();
        for (_, i) in std::mem::take(&mut self.queued) {
            let _ = i.done.send(Err(crate::query::closed()));
        }
    }
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
    /// Sent with every `subscribe`: the snapshot holds everything in Kafka when subscribing.
    complete: bool,
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
    /// The credential expired: reconnect at once, with a new one.
    Now,
    Stop,
}

/// A command while disconnected: subscriptions and inserts are only recorded (sent on the next
/// open).
fn offline(cmd: Option<Cmd>, subs: &mut BTreeMap<String, Sub>, inserts: &mut Inserts) -> Flow {
    match cmd {
        None => {
            inserts.closed();
            Flow::Stop
        }
        Some(Cmd::Insert {
            id,
            table,
            rows,
            done,
        }) => {
            inserts.queued.insert(id, Insert { table, rows, done });
            Flow::Go
        }
        Some(Cmd::Subscribe {
            id,
            sql,
            params,
            complete,
            events,
        }) => {
            subs.insert(
                id,
                Sub {
                    sql,
                    params,
                    complete,
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
            inserts.closed();
            let _ = done.send(());
            Flow::Stop
        }
    }
}

fn text(msg: &ClientMsg) -> Message {
    Message::text(serde_json::to_string(msg).expect("client messages serialize"))
}

fn insert(id: &str, i: &Insert) -> Message {
    text(&ClientMsg::Insert {
        id: id.to_string(),
        table: i.table.clone(),
        rows: i.rows.clone(),
    })
}

fn subscribe(id: &str, sub: &Sub) -> Message {
    text(&ClientMsg::Subscribe {
        id: id.to_string(),
        sql: sub.sql.clone(),
        since: sub.at.clone(),
        params: sub.params.clone(),
        complete: sub.complete,
        optimistic: false,
    })
}

async fn run(opts: ClientOptions, mut cmds: mpsc::UnboundedReceiver<Cmd>) {
    let mut subs: BTreeMap<String, Sub> = BTreeMap::new();
    let mut inserts = Inserts::default();
    let mut backoff = opts.min_backoff;
    loop {
        // connect only when there's something to subscribe to, or to insert
        while subs.is_empty() && inserts.queued.is_empty() {
            if let Flow::Stop = offline(cmds.recv().await, &mut subs, &mut inserts) {
                return;
            }
        }
        let flow = 'attempt: {
            // the credential first: the server gives a connection only so long to present it
            let token = match &opts.credentials {
                None => None,
                Some(c) => match taking((c.0)(), &mut cmds, &mut subs, &mut inserts).await {
                    None => return,
                    Some(Ok(token)) => Some(token),
                    Some(Err(e)) => {
                        auth_error(&opts, CLIENT_ERROR, format!("the credentials failed: {e}"));
                        break 'attempt Flow::Go;
                    }
                },
            };
            let connecting = tokio_tungstenite::connect_async_with_config(
                opts.url.as_str(),
                Some(ws_config()),
                false,
            );
            let Some(ws) = taking(connecting, &mut cmds, &mut subs, &mut inserts).await else {
                return;
            };
            match ws {
                Err(e) => {
                    tracing::warn!(url = %opts.url, error = %e, "chlive: can't connect");
                    Flow::Go
                }
                Ok((ws, _)) => {
                    backoff = opts.min_backoff;
                    let flow =
                        connected(ws, &opts, token, &mut subs, &mut inserts, &mut cmds).await;
                    inserts.disconnected();
                    if let Flow::Stop = flow {
                        return;
                    }
                    for sub in subs.values() {
                        let _ = sub.events.send(Event::Disconnected).await;
                    }
                    flow
                }
            }
        };
        if let Flow::Now = flow {
            continue;
        }
        // wait, still taking commands
        let wait = tokio::time::sleep(backoff);
        backoff = (backoff * 2).min(opts.max_backoff);
        if taking(wait, &mut cmds, &mut subs, &mut inserts)
            .await
            .is_none()
        {
            return;
        }
    }
}

/// `fut`'s output, taking commands while disconnected meanwhile; `None`: the client closed.
async fn taking<F: Future>(
    fut: F,
    cmds: &mut mpsc::UnboundedReceiver<Cmd>,
    subs: &mut BTreeMap<String, Sub>,
    inserts: &mut Inserts,
) -> Option<F::Output> {
    tokio::pin!(fut);
    loop {
        tokio::select! {
            out = &mut fut => return Some(out),
            cmd = cmds.recv() => if let Flow::Stop = offline(cmd, subs, inserts) {
                return None;
            },
        }
    }
}

/// `f`'s output, or never without one.
async fn opt<F: Future + Unpin>(f: &mut Option<F>) -> F::Output {
    match f {
        Some(f) => f.await,
        None => std::future::pending().await,
    }
}

/// Tell the application (and the log) that a credential was refused, or couldn't be had.
fn auth_error(opts: &ClientOptions, code: &str, message: impl Into<String>) {
    let e = LiveError {
        code: code.into(),
        message: message.into(),
    };
    tracing::warn!(error = %e, "chlive: authentication");
    if let Some(handler) = &opts.on_auth_error {
        (handler.0)(&e);
    }
}

/// How long until a credential expiring at `expires_at` (Unix ms) is renewed: `before` its
/// expiry, or halfway if it has less than twice that left.
fn renewal(before: Duration, expires_at: i64) -> Duration {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default();
    let left = (expires_at - now).max(0) as u64;
    let before = (before.as_millis() as u64).min(left / 2);
    Duration::from_millis(left - before)
}

/// A provider that failed to renew is asked again this much later (while the credential lasts).
const RENEW_RETRY: Duration = Duration::from_secs(5);

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// One open connection, until it drops (`Go`: reconnect), its credential expires (`Now`:
/// reconnect at once, with a new one) or the client closes (`Stop`).
async fn connected(
    ws: Socket,
    opts: &ClientOptions,
    token: Option<String>,
    subs: &mut BTreeMap<String, Sub>,
    inserts: &mut Inserts,
    cmds: &mut mpsc::UnboundedReceiver<Cmd>,
) -> Flow {
    let (mut sink, mut stream) = ws.split();
    let hello = match (token, &opts.user) {
        (Some(token), _) => Some(ClientMsg::Hello {
            user: None,
            token: Some(token),
        }),
        (None, Some(user)) => Some(ClientMsg::Hello {
            user: Some(user.clone()),
            token: None,
        }),
        (None, None) => None,
    };
    // subscriptions and inserts may follow at once: the server takes them after the hello
    if let Some(hello) = hello {
        if sink.send(text(&hello)).await.is_err() {
            return Flow::Go;
        }
    }
    for (id, sub) in subs.iter() {
        if sink.send(subscribe(id, sub)).await.is_err() {
            return Flow::Go;
        }
        let _ = sub.events.send(Event::Subscribed).await;
    }
    for (id, i) in std::mem::take(&mut inserts.queued) {
        let sent = sink.send(insert(&id, &i)).await.is_ok();
        inserts.sent.insert(id, i);
        if !sent {
            return Flow::Go;
        }
    }
    // the credential: verified (so an expiry is a reason to reconnect at once), when to ask for
    // the next one, and the asking
    let mut authenticated = false;
    let mut renew_at: Option<Pin<Box<tokio::time::Sleep>>> = None;
    let mut renewing: Option<BoxFuture<'static, Result<String, String>>> = None;
    loop {
        tokio::select! {
            cmd = cmds.recv() => match cmd {
                None => {
                    let _ = sink.close().await;
                    inserts.closed();
                    return Flow::Stop;
                }
                Some(Cmd::Close(done)) => {
                    let _ = sink.close().await;
                    subs.clear();
                    inserts.closed();
                    let _ = done.send(());
                    return Flow::Stop;
                }
                Some(Cmd::Subscribe {
                    id,
                    sql,
                    params,
                    complete,
                    events,
                }) => {
                    let sub = Sub {
                        sql,
                        params,
                        complete,
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
                Some(Cmd::Insert { id, table, rows, done }) => {
                    let i = Insert { table, rows, done };
                    let sent = sink.send(insert(&id, &i)).await.is_ok();
                    inserts.sent.insert(id, i);
                    if !sent {
                        return Flow::Go;
                    }
                }
            },
            _ = opt(&mut renew_at) => {
                renew_at = None;
                renewing = opts.credentials.as_ref().map(|c| (c.0)());
            }
            token = opt(&mut renewing) => {
                renewing = None;
                match token {
                    Ok(token) => {
                        if sink.send(text(&ClientMsg::Renew { token })).await.is_err() {
                            return Flow::Go;
                        }
                    }
                    Err(e) => {
                        auth_error(opts, CLIENT_ERROR, format!("the credentials failed: {e}"));
                        renew_at = Some(Box::pin(tokio::time::sleep(RENEW_RETRY)));
                    }
                }
            }
            frame = stream.next() => match frame {
                Some(Ok(Message::Text(t))) => match dispatch(subs, inserts, t.as_str()).await {
                    Some(ServerMsg::Authenticated { expires_at, .. }) => {
                        authenticated = true;
                        if opts.credentials.is_some() {
                            let wait = renewal(opts.refresh_before, expires_at);
                            renew_at = Some(Box::pin(tokio::time::sleep(wait)));
                        }
                    }
                    // the hello's or a renewal's credential refused (a refused hello's
                    // connection then closes)
                    Some(ServerMsg::Error { code, message, .. })
                        if chlive_protocol::is_auth_error(&code)
                            || code == chlive_protocol::PERMISSION_DENIED =>
                    {
                        auth_error(opts, &code, message);
                    }
                    Some(other) => tracing::warn!(message = ?other, "chlive: a message for no subscription"),
                    None => {}
                },
                Some(Ok(Message::Close(frame))) => {
                    let expired = frame.is_some_and(|f| {
                        u16::from(f.code) == chlive_protocol::AUTH_CLOSE
                            && f.reason.as_str() == chlive_protocol::AUTH_EXPIRED
                    });
                    // expired after it was verified: a new credential, at once (one refused
                    // at the hello waits like any failed connect)
                    return if expired && authenticated { Flow::Now } else { Flow::Go };
                }
                None => return Flow::Go,
                Some(Err(e)) => {
                    tracing::warn!(error = %e, "chlive: connection lost");
                    return Flow::Go;
                }
                Some(Ok(_)) => {}
            },
        }
    }
}

/// Hand a message to its insert or subscription. One that doesn't parse fails only that
/// subscription. The connection's own messages (no id) are returned.
async fn dispatch(
    subs: &mut BTreeMap<String, Sub>,
    inserts: &mut Inserts,
    text: &str,
) -> Option<ServerMsg> {
    let bytes = text.len();
    let value: serde_json::Value = match serde_json::from_str(text) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "chlive: a message that isn't JSON");
            return None;
        }
    };
    let Some(id) = value.get("id").and_then(|v| v.as_str()) else {
        return match serde_json::from_value::<ServerMsg>(value.clone()) {
            Ok(msg) => Some(msg),
            Err(e) => {
                tracing::warn!(message = %value, error = %e, "chlive: a message the client doesn't know");
                None
            }
        };
    };
    if let Some(i) = inserts.sent.remove(id) {
        let answer = match serde_json::from_value::<ServerMsg>(value.clone()) {
            Ok(ServerMsg::Ack { offsets, .. }) => Ok(Ack { offsets }),
            Ok(ServerMsg::Error { code, message, .. }) => Err(LiveError { code, message }),
            _ => Err(LiveError {
                code: crate::CLIENT_ERROR.into(),
                message: format!("an unexpected answer to an insert: {value}"),
            }),
        };
        let _ = i.done.send(answer);
        return None;
    }
    // messages for subscriptions that are gone are ignored
    let sub = subs.get_mut(id)?;
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
    None
}

/// No size limit on messages: a snapshot is the whole result in one message, so a big result
/// is a big message (tungstenite's default, 16 MiB, would drop the connection on it). Browsers
/// don't limit them either.
fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(None)
        .max_frame_size(None)
}
