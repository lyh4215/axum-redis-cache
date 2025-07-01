// src/cache.rs

use sqlx::{Database, Pool};
use std::future::Future;
use tokio::time::{sleep, Duration};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use colored::*;
use futures_util::StreamExt;

/// Cache system config.
/// - `redis_url`: Redis server URL
/// - `write_duration`: Write-behind worker interval (sec)
#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub redis_url: String,
    pub write_duration: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig {
            redis_url: "redis://127.0.0.1/".to_string(),
            write_duration: 60,
        }
    }
}

impl CacheConfig {
    /// Build with default settings.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set custom redis URL.
    pub fn with_url(mut self, url: &str) -> Self {
        self.redis_url = url.to_string();
        self
    }
    /// Set custom write-behind interval.
    pub fn with_write_duration(mut self, duration: u64) -> Self {
        self.write_duration = duration;
        self
    }
}

/// Main cache connection bundle.
/// Owns: redis client/conn, db pool, config.
pub struct CacheConnection<DB: Database> {
    pub client: redis::Client,
    pub conn: MultiplexedConnection,
    pub db: Pool<DB>,
    pub config: CacheConfig,
}

impl<DB: Database> CacheConnection<DB> {
    /// Create with default config.
    pub async fn new(db: Pool<DB>) -> CacheConnection<DB> {
        let redis_client = redis::Client::open("redis://127.0.0.1/").expect("Invalid Redis URL");
        let mut con = redis_client.get_connection().unwrap();
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("Ex")
            .query(&mut con)
            .unwrap();
        let conn = redis_client.get_multiplexed_async_connection().await.unwrap();

        CacheConnection { client: redis_client, conn, db, config: CacheConfig::default() }
    }

    /// Create with custom config.
    pub async fn new_with_config(
        db: Pool<DB>,
        config: CacheConfig,
    ) -> CacheConnection<DB> {
        let redis_client = redis::Client::open(config.redis_url.clone()).expect("Invalid Redis URL");
        let mut con = redis_client.get_connection().expect("Failed to connect to Redis");
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("Ex")
            .query(&mut con)
            .expect("Failed to set Redis config (PubSub)");
        let conn = redis_client.get_multiplexed_async_connection().await
            .expect("Failed to get Redis multiplexed connection");

        CacheConnection { client: redis_client, conn, db, config }
    }

    /// Build cache manager + spawn background workers.
    ///
    /// - `put_function`: DB writer for write-behind
    /// - `delete_function`: DB remover for delete events
    /// - `put_cache_function`: Cache body merger for PUT
    pub fn get_manager<F, G, Fut1, Fut2>(
        &self,
        key: String,
        put_function: F,
        delete_function: G,
        put_cache_function: fn(String, String) -> String,
    ) -> CacheManager
    where
        F: Fn(Pool<DB>, String) -> Fut1 + Send + Sync + 'static,
        G: Fn(Pool<DB>, String) -> Fut2 + Send + Sync + 'static,
        Fut1: Future<Output = ()> + Send + 'static,
        Fut2: Future<Output = ()> + Send + 'static,
    {
        CacheManager::new(self.client.clone(), self.conn.clone(), self.db.clone(), key, self.config.clone(), put_function, delete_function, put_cache_function)
    }
}

/// Central cache manager struct.
/// Background workers start on creation.
pub struct CacheManager {
    pub conn: MultiplexedConnection,
    pub key: String,
    pub config: CacheConfig,
    put_cache_function: fn(String, String) -> String,
}

impl CacheManager {
    /// Construct new manager, spawns background workers.
    #[allow(clippy::too_many_arguments)]
    fn new<F, G, Fut1, Fut2, DB: Database>(
        client: redis::Client,
        conn: MultiplexedConnection,
        db: Pool<DB>,
        key: String,
        config: CacheConfig,
        put_function: F,
        delete_function: G,
        put_cache_function: fn(String, String) -> String,
    ) -> CacheManager
    where
        F: Fn(Pool<DB>, String) -> Fut1 + Send + Sync + 'static,
        G: Fn(Pool<DB>, String) -> Fut2 + Send + Sync + 'static,
        Fut1: Future<Output = ()> + Send + 'static,
        Fut2: Future<Output = ()> + Send + 'static,
    {
        // Write-behind + delete event listeners
        tokio::spawn(write_behind(conn.clone(), db.clone(), key.clone(), config.write_duration.clone(), put_function));
        tokio::spawn(delete_event_listener(client, db.clone(), key.clone(), delete_function));
        CacheManager {
            conn,
            key,
            config,
            put_cache_function,
        }
    }

    /// Return CacheState for Axum middleware injection.
    pub fn get_state(&self) -> CacheState {
        CacheState {
            conn: self.conn.clone(),
            write_to_cache: self.put_cache_function,
        }
    }
}

/// Minimal state for middleware.
/// - `conn`: multiplexed redis connection
/// - `write_to_cache`: custom JSON merge function for PUT
#[derive(Clone)]
pub struct CacheState {
    pub conn: MultiplexedConnection,
    pub write_to_cache: fn(String, String) -> String,
}

/// Write-behind background worker.
/// Every N seconds, scans dirty:* keys and writes to DB, then cleans up.
async fn write_behind<F, Fut, DB>(
    mut conn: MultiplexedConnection,
    db: Pool<DB>,
    root_key: String,
    duration: u64,
    write_function: F,
)
where
    F: Fn(Pool<DB>, String) -> Fut,
    Fut: Future<Output = ()>,
    DB: Database,
{
    println!("{} Redis write behind thread", "Start".green().bold());
    loop {
        // Scan for dirty keys
        let dirty_key = format!("dirty:{}:*", root_key);
        let keys: Vec<String> = match conn.keys(&dirty_key).await {
            Ok(k) => k,
            Err(e) => {
                eprintln!("❌ Failed to get keys: {e}");
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        for key in keys {
            println!("key : {key}");
            if let Ok(Some(bytes)) = conn.get::<_, Option<String>>(&key).await {
                // Write to DB
                write_function(db.clone(), bytes.clone()).await;

                // Clean up dirty key, set clean with short TTL
                let _: () = conn.del(&key).await.unwrap_or(());
                let clean_key = key.strip_prefix("dirty:").unwrap_or(&key).to_string();
                let _: () = conn.set_ex(&clean_key, bytes, 10).await.unwrap_or(());
                println!("Write behind for : {key}");
            }
        }
        // Sleep interval
        sleep(Duration::from_secs(duration)).await;
    }
}

/// Background task: listens for Redis expire (delete) events.
/// On expire, invokes user-provided delete function.
async fn delete_event_listener<F, Fut, DB: Database>(
    client: redis::Client,
    db: Pool<DB>,
    root_key: String,
    delete_function: F,
)
where
    F: Fn(Pool<DB>, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut pubsub_conn = client.get_async_pubsub().await.unwrap();

    // Subscribe to Redis key expire events
    pubsub_conn.subscribe("__keyevent@0__:expired").await.unwrap();

    println!("{} Redis expired event listening", "Start".green().bold());
    while let Some(msg) = pubsub_conn.on_message().next().await {
        let expired_key: String = msg.get_payload().unwrap();

        let prefix = format!("delete:{}:", root_key);
        if let Some(post_id_str) = expired_key.strip_prefix(&prefix) {
            // Call delete handler
            delete_function(db.clone(), post_id_str.to_string()).await;
        }
    }
}
