//! chlive server: a thin WebSocket layer over [`chlive_engine`]. The engine plans and runs
//! each query (ClickHouse history, then Kafka events); the server only speaks the
//! [`protocol`] with clients.

pub mod protocol;
pub mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use chlive_engine::Engine;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub use ws::router;

/// A running server (see [`start`]).
pub struct Server {
    pub addr: SocketAddr,
    task: JoinHandle<std::io::Result<()>>,
    engine: Arc<Engine>,
}

impl Server {
    /// Until the server stops.
    pub async fn wait(self) -> std::io::Result<()> {
        self.task.await.map_err(std::io::Error::other)?
    }

    pub fn stop(&self) {
        self.task.abort();
    }

    /// Stop serving, then the engine (dropping its snapshots).
    pub async fn shutdown(&self) {
        self.stop();
        self.engine.shutdown().await;
    }
}

/// Listen on `addr` (port 0 picks a free one) and serve `engine` in the background.
pub async fn start(addr: &str, engine: impl Into<Arc<Engine>>) -> std::io::Result<Server> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let engine = engine.into();
    let router = router(engine.clone());
    let task = tokio::spawn(async move { axum::serve(listener, router).await });
    Ok(Server { addr, task, engine })
}
