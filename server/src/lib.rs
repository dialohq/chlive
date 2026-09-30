pub mod cache_engine;
pub mod ch_http;
pub mod config;
pub mod diff;
pub mod rewriter;
pub mod subscriptions;
pub mod tail;
pub mod watermark;
pub mod ws;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::broadcast;
use tracing::{info, warn};

use cache_engine::{CacheEngine, ClickHouseCache};
use ch_http::{ChHttp, Params};
use config::Config;
use rewriter::{Catalog, FunctionCatalog};
use subscriptions::Manager;
use tail::TableRuntime;

/// A running server.
pub struct Server {
    pub addr: SocketAddr,
    pub manager: Arc<Manager>,
    pub tables: HashMap<String, Arc<TableRuntime>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Server {
    pub async fn wait(mut self) -> Result<()> {
        let http = self.tasks.remove(0);
        http.await.context("http server task")?;
        Ok(())
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        for t in &self.tasks {
            t.abort();
        }
    }
}

async fn load_functions(main: &ChHttp) -> Result<FunctionCatalog> {
    let aggs = main
        .query(
            "SELECT name, case_insensitive FROM system.functions WHERE is_aggregate",
            &Params::new(),
        )
        .await?;
    let combs = main
        .query(
            "SELECT name FROM system.aggregate_function_combinators",
            &Params::new(),
        )
        .await?;
    Ok(FunctionCatalog::new(
        aggs.data.iter().filter_map(|r| {
            Some((
                r["name"].as_str()?.to_string(),
                r["case_insensitive"].as_u64().unwrap_or(0) == 1,
            ))
        }),
        combs
            .data
            .iter()
            .filter_map(|r| r["name"].as_str().map(str::to_string)),
    ))
}

/// Prepare the cache, start tail consumers and serve HTTP/WebSocket on `cfg.listen`.
pub async fn start(cfg: Config) -> Result<Server> {
    let main = ChHttp::new(
        &cfg.main.url,
        &cfg.main.user,
        &cfg.main.password,
        &cfg.main.database,
    );
    let cache_root = ChHttp::new(
        &cfg.cache.url,
        &cfg.cache.user,
        &cfg.cache.password,
        "default",
    );
    // The cache holds no durable state: start from an empty database every time.
    let db = &cfg.cache.database;
    cache_root
        .exec(
            &format!("DROP DATABASE IF EXISTS {db} SYNC"),
            &Params::new(),
        )
        .await?;
    cache_root
        .exec(&format!("CREATE DATABASE {db}"), &Params::new())
        .await?;
    let cache: Arc<dyn CacheEngine> = Arc::new(ClickHouseCache(cache_root.with_database(db)));

    let functions = match load_functions(&main).await {
        Ok(f) => f,
        Err(e) => {
            warn!(error = %e, "could not load aggregate function list from main; using builtin list");
            FunctionCatalog::builtin()
        }
    };

    let (tx, rx) = broadcast::channel(1024);
    let mut tables = HashMap::new();
    let mut schemas = HashMap::new();
    let mut tasks = Vec::new();
    for t in &cfg.tables {
        let schema = tail::describe(&main, &t.name).await?;
        tail::create_tail_table(cache.as_ref(), &schema).await?;
        let partitions = tail::partition_count(&cfg.kafka, &t.topic)?;
        let w0 = tail::fetch_watermark(&main, &t.name, partitions).await?;
        let rt = Arc::new(TableRuntime {
            schema: schema.clone(),
            topic: t.topic.clone(),
            partitions,
            tail_max: RwLock::new(w0.clone()),
            w0,
        });
        tasks.push(tail::spawn(
            cfg.kafka.clone(),
            cache.clone(),
            rt.clone(),
            tx.clone(),
        )?);
        schemas.insert(t.name.clone(), schema);
        tables.insert(t.name.clone(), rt);
    }

    let catalog = Arc::new(Catalog {
        database: cfg.main.database.clone(),
        tables: schemas,
        functions,
        rows_limit_cap: cfg.rows_limit_cap,
        source_template: cfg.cache.remote_source.clone(),
    });
    let manager = Manager::new(
        catalog,
        tables.clone(),
        main,
        cache,
        Duration::from_millis(cfg.debounce_ms),
    );
    tasks.push(manager.spawn_router(rx));

    let listener = tokio::net::TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("binding {}", cfg.listen))?;
    let addr = listener.local_addr()?;
    let app = ws::router(manager.clone());
    tasks.insert(
        0,
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(error = %e, "http server failed");
            }
        }),
    );
    info!(%addr, "chlive listening");
    Ok(Server {
        addr,
        manager,
        tables,
        tasks,
    })
}
