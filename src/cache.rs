// src/cache.rs

use sqlx::{Database, Pool};
use std::future::Future;
use redis::{aio::MultiplexedConnection};
use colored::*;
use tokio_util::sync::CancellationToken;
use tokio::task::JoinHandle;
use std::sync::{Arc, Mutex};

use std::thread::sleep;
use std::time::Duration;
use redis::{Client, Connection};

use crate::cache_sync;

/// Cache system config.
/// - `redis_url`: Redis server URL
#[derive(Debug, Clone)]
pub struct CacheConnConfig {
    pub redis_url: String,
}


impl Default for CacheConnConfig {
    fn default() -> Self {
        CacheConnConfig {
            redis_url: "redis://127.0.0.1/".to_string(),
        }
    }
}

impl CacheConnConfig {
    /// Build with default settings.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set custom redis URL.
    pub fn with_url(mut self, url: &str) -> Self {
        self.redis_url = url.to_string();
        self
    }
}

/// Main cache connection bundle.
/// Owns: redis client/conn, db pool, config.
pub struct CacheConnection<DB: Database> {
    pub client: redis::Client,
    pub conn: MultiplexedConnection,
    pub db: Pool<DB>,
    pub config: CacheConnConfig,
}

impl<DB: Database> CacheConnection<DB> {
    /// Create with default config.
    pub async fn new(db: Pool<DB>) -> CacheConnection<DB> {
        let config = CacheConnConfig::default();
        CacheConnection::new_with_config(db, config).await
    }

    /// Create with custom config.
    pub async fn new_with_config(
        db: Pool<DB>,
        config: CacheConnConfig,
    ) -> CacheConnection<DB> {
        let redis_client = redis::Client::open(config.redis_url.clone()).expect("Invalid Redis URL");
        let mut con = get_redis_connection_with_retry(&redis_client);
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
        CacheManager::new(self.db.clone(),
                        self.client.clone(),
                        self.conn.clone(),
                        key,
                        put_function,
                        delete_function,
                        put_cache_function)
    }
}


#[derive(Debug, Clone, Copy)]
pub struct CacheConfig {
    pub write_duration: u64,
    pub ttl_clean: u64,
    pub ttl_deleted: u64,
}


impl CacheConfig {
    /// Build with default settings.
    pub fn new() -> Self {
        CacheConfig {
            write_duration: 5, // Default to 5 seconds
            ttl_clean: 60,     // Default to 60 seconds
            ttl_deleted: 10,   // Default to 10 seconds
        }
    }
    /// Set custom write-behind interval.
    pub fn with_write_duration(mut self, duration: u64) -> Self {
        self.write_duration = duration;
        self
    }

    /// Set custom TTL for clean entries.
    pub fn with_clean_ttl(mut self, ttl: u64) -> Self {
        self.ttl_clean = ttl;
        self
    }

    /// Set custom TTL for deleted entries.
    pub fn with_deleted_ttl(mut self, ttl: u64) -> Self {
        self.ttl_deleted = ttl;
        self
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        CacheConfig::new()
    }
}

/// Central cache manager struct.
/// Background workers start on creation.
pub struct CacheManager {
    pub conn: MultiplexedConnection,
    pub key: String,
    pub config: Arc<Mutex<CacheConfig>>,

    /* Handler for Cache Write-behind */
    put_cache_function: fn(String, String) -> String,
    write_behind_handle: JoinHandle<()>,
    delete_event_handle: JoinHandle<()>,

    /* For graceful Shutdown */
    cancellation_token: CancellationToken,
}

impl CacheManager {
    /// Construct new manager, spawns background workers.
    #[allow(clippy::too_many_arguments)]
    fn new<F, G, Fut1, Fut2, DB: Database>(
        /* Datebase */
        db: Pool<DB>,

        /* redis setting */
        client: redis::Client,
        conn: MultiplexedConnection,
        key: String,

        /* user-defined function */
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
        let cancellation_token = CancellationToken::new();
        let config = Arc::new(Mutex::new(CacheConfig::default()));

        // Write-behind + delete event listeners
        let write_behind_handle = tokio::spawn(cache_sync::write_behind(conn.clone(), db.clone(), key.clone(), Arc::clone(&config), put_function, cancellation_token.clone()));
        let delete_event_handle = tokio::spawn(cache_sync::delete_event_listener(client, db.clone(), key.clone(), delete_function, cancellation_token.clone()));
        
        CacheManager {
            conn,
            key,
            config,
            put_cache_function,
            write_behind_handle,
            delete_event_handle,
            cancellation_token,
        }
    }

    /// Set a new cache configuration.
    pub fn with_config(self, config: CacheConfig) -> Self {
        *self.config.lock().unwrap() = config;
        self
    }

    /// Return CacheState for Axum middleware injection.
    pub fn get_state(&self) -> CacheState {
        CacheState {
            conn: self.conn.clone(),
            write_to_cache: self.put_cache_function,
            config: self.config.clone(),
        }
    }

    /// Signals shutdown and waits for background tasks to complete.
    pub async fn shutdown(self) {
        println!("{} Cache manager graceful shutdown", "Shutdown".red().bold());
        // Signal shutdown to background tasks
        self.cancellation_token.cancel();
        // Wait for tasks to finish

        let _ = tokio::join!(self.write_behind_handle, self.delete_event_handle);
        println!("{} Cache manager shutdown gracefully.", "Done".green().bold());
    }

}

/// Minimal state for `middleware`.
/// - `conn`: multiplexed redis connection
/// - `write_to_cache`: custom JSON merge function for PUT
#[derive(Clone)]
pub struct CacheState {
    pub conn: MultiplexedConnection,
    pub write_to_cache: fn(String, String) -> String,
    pub config: Arc<Mutex<CacheConfig>>,
}

fn get_redis_connection_with_retry(redis_client: &Client) -> Connection {
    let mut attempts = 0;
    let max_attempts = 6;
    let wait_duration = Duration::from_secs(10);

    loop {
        match redis_client.get_connection() {
            Ok(conn) => return conn,
            Err(err) => {
                attempts += 1;
                if attempts >= max_attempts {
                    panic!("Failed to connect to Redis after {} attempts: {}", attempts, err);
                } else {
                    eprintln!(
                        "Redis connection failed (attempt {}/{}), retrying in {}s...: {}",
                        attempts,
                        max_attempts,
                        wait_duration.as_secs(),
                        err
                    );
                    sleep(wait_duration);
                }
            }
        }
    }
}