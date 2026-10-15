//! chlive server: a thin WebSocket layer over [`chlive_engine`]. The engine plans and runs
//! each query (ClickHouse history, then Kafka events); the server only speaks the
//! [`protocol`] with clients.

pub mod config;
pub mod hub;
pub mod log;
pub mod protocol;
pub mod store;
pub mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use chlive_engine::Engine;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub use hub::{CacheSettings, Hub};
pub use ws::{router, router_with, ServerSettings};

/// A running server (see [`start`]).
pub struct Server {
    pub addr: SocketAddr,
    task: JoinHandle<std::io::Result<()>>,
    engine: Arc<Engine>,
    hub: Arc<Hub>,
}

impl Server {
    /// Until the server stops.
    pub async fn wait(&mut self) -> std::io::Result<()> {
        (&mut self.task).await.map_err(std::io::Error::other)?
    }

    /// The shared results it serves.
    pub fn hub(&self) -> &Arc<Hub> {
        &self.hub
    }

    pub fn stop(&self) {
        self.task.abort();
    }

    /// Stop serving, then the shared results, then the engine (dropping its snapshots).
    pub async fn shutdown(&self) {
        self.stop();
        self.hub.shutdown().await;
        self.engine.shutdown().await;
    }
}

/// Listen on `addr` (port 0 picks a free one) and serve `engine` in the background, with the
/// default [`ServerSettings`].
pub async fn start(addr: &str, engine: impl Into<Arc<Engine>>) -> std::io::Result<Server> {
    start_with(addr, ServerSettings::default(), engine).await
}

/// [`start`] with these settings.
pub async fn start_with(
    addr: &str,
    settings: ServerSettings,
    engine: impl Into<Arc<Engine>>,
) -> std::io::Result<Server> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let engine = engine.into();
    let hub = Hub::new(
        engine.clone(),
        settings.cache.clone(),
        settings.progress_interval,
    );
    let router = router_with(hub.clone(), settings);
    let task = tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(Server {
        addr,
        task,
        engine,
        hub,
    })
}
