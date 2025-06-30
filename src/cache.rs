//src/cache.rs

use sqlx::{
    SqlitePool,
};

use std::future::Future;
use tokio::time::{sleep, Duration};

use redis::{aio::MultiplexedConnection, AsyncCommands};

use colored::*;
use futures_util::StreamExt;

use sqlx::{Pool, Sqlite};

pub struct CacheConnection {
    client : redis::Client,
    conn : MultiplexedConnection,
    db : SqlitePool
}

impl CacheConnection {
    pub async fn new(db : SqlitePool) -> CacheConnection {
        let redis_client =redis::Client::open("redis://127.0.0.1/").unwrap();
        let mut con = redis_client.get_connection().unwrap();
        let _: () = redis::cmd("CONFIG")
            .arg("SET")
            .arg("notify-keyspace-events")
            .arg("Ex")
            .query(&mut con).unwrap();
        let conn = redis_client.get_multiplexed_async_connection().await.unwrap();
        
        CacheConnection {client: redis_client, conn, db}
    }

    pub fn get_manager<F, G, Fut1, Fut2> (
        &self,
        key: String,
        put_function: F,
        delete_function: G,
        put_cache_function: fn(String, String) -> String
    ) -> CacheManager 
    where
        F: Fn(Pool<Sqlite>, String) -> Fut1 + Send + Sync + 'static,
        G: Fn(Pool<Sqlite>, String) -> Fut2 + Send + Sync + 'static,
        Fut1: Future<Output = ()> + Send + 'static,
        Fut2: Future<Output = ()> + Send + 'static,
    {
        CacheManager::new(self.client.clone(), self.conn.clone(), self.db.clone(), key, put_function, delete_function, put_cache_function)
    }
}
pub struct CacheManager {
    pub conn : MultiplexedConnection,
    key: String,
    // put_function: Arc<dyn Fn(Pool<Sqlite>, String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    // delete_function: Arc<dyn Fn(Pool<Sqlite>, String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>,
    put_cache_function: fn(String, String) -> String,
}

impl CacheManager {
    fn new<F, G, Fut1, Fut2>(
        client : redis::Client,
        conn: MultiplexedConnection,
        db : SqlitePool,
        key: String,
        put_function: F,
        delete_function: G,
        put_cache_function: fn(String, String) -> String,
    ) -> CacheManager
    where
        F: Fn(Pool<Sqlite>, String) -> Fut1 + Send + Sync + 'static,
        G: Fn(Pool<Sqlite>, String) -> Fut2 + Send + Sync + 'static,
        Fut1: Future<Output = ()> + Send + 'static,
        Fut2: Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(write_behind(conn.clone(), db.clone(), key.clone(), put_function));
        tokio::spawn(delete_event_listener(client, db.clone(), key.clone(), delete_function));
        CacheManager {
            conn,
            key,
            put_cache_function,
            // put_function: Arc::new(move |pool, s| Box::pin(put_function(pool, s))),
            // delete_function: Arc::new(move |pool, s| Box::pin(delete_function(pool, s))),),
        }
    }

    pub fn get_state(&self) -> CacheState {
        CacheState {
            conn: self.conn.clone(),
            write_to_cache : self.put_cache_function
        }
    }
    // pub fn get_layer(&self) -> impl tower::Layer<axum::routing::Route> + Clone + Send + 'static
    // {
    //     axum::middleware::from_fn_with_state::<_,_,Request<Body>>(self.conn.clone(), middleware)
    // }
    

}

#[derive(Clone)]
pub struct CacheState {
    pub conn: MultiplexedConnection,
    pub write_to_cache: fn(String, String) -> String,
}

pub async fn init_cache() ->  redis::Client {
    //redis setting (keyevent notification channel)
    let redis_client =redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut conn = redis_client.get_connection().unwrap();
    let _: () = redis::cmd("CONFIG")
        .arg("SET")
        .arg("notify-keyspace-events")
        .arg("Ex")
        .query(&mut conn).unwrap();
    redis_client
}

//background worker
pub async fn write_behind<F, Fut> (
    mut conn: MultiplexedConnection,
    db: SqlitePool,
    root_key: String,
    write_function :F
)
where
    F: Fn(SqlitePool, String) -> Fut,
    Fut: Future<Output = ()>,
{
    println!("{} Redis write behind thread", "Start".green().bold());
    loop {
        // Redis에서 post:* 키들을 스캔
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

                //for write-behind
                write_function(db.clone(), bytes.clone()).await;

                //delete key
                let _: () = conn.del(&key).await.unwrap_or(());
                let clean_key = key.strip_prefix("dirty:").unwrap_or(&key).to_string();
                let _: () = conn.set_ex(&clean_key, bytes, 10).await.unwrap_or(()); // 예: 1시간 만료
                println!("Write behind for : {key}");
            }
        }
        // 10초마다 반복
        sleep(Duration::from_secs(10)).await;
    }
}


pub async fn delete_event_listener<F, Fut>(
    client: redis::Client,
    db: SqlitePool,
    root_key: String,
    delete_function: F,
)
where
    F: Fn(SqlitePool, String) -> Fut,
    Fut: Future<Output = ()>,
{
    
    let mut pubsub_conn = client.get_async_pubsub().await.unwrap();

    // expire 이벤트 구독
    pubsub_conn.subscribe("__keyevent@0__:expired").await.unwrap();
    
    println!("{} Redis expired event listening", "Start".green().bold());
    while let Some(msg) = pubsub_conn.on_message().next().await {
        println!("감지됨");
        let expired_key: String = msg.get_payload().unwrap();

        // delete:/posts/ 만 감지
        let prefix = format!("delete:{}:", root_key);
        if let Some(post_id_str) = expired_key.strip_prefix(&prefix) {
            delete_function(db.clone(), post_id_str.to_string()).await;
        }
    }
}


