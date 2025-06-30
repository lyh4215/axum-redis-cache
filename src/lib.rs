//Write-Behind Redis Cache
use axum::{
    extract::{Path, State}, http::{response, Request, Response, StatusCode}, middleware::{self, FromFnLayer, Next}, routing::delete, Json
};
use sqlx::{
    SqlitePool,
};
use std::sync::Arc;
use std::any::Any;
use std::pin::Pin;
use std::future::Future;
use tokio::time::{sleep, Duration};

use redis::{aio::MultiplexedConnection, AsyncCommands, RedisResult};
use axum::http::Method;
use http_body_util::BodyExt;
use bytes::Bytes;
use axum::body::Body;
use colored::*;
use futures_util::StreamExt;

use sqlx::{Pool, Sqlite};
use serde_json::{self, json};


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
        tokio::spawn(write_behind(conn.clone(), db.clone(), put_function));
        tokio::spawn(delete_event_listener(client, db.clone(), delete_function));
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
    write_to_cache: fn(String, String) -> String,
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
pub async fn write_behind<F, Fut>(mut conn: MultiplexedConnection, db: SqlitePool, write_function :F)
where
    F: Fn(SqlitePool, String) -> Fut,
    Fut: Future<Output = ()>,
{
    println!("{} Redis write behind thread", "Start".green().bold());
    loop {
        // Redis에서 post:* 키들을 스캔
        let keys: Vec<String> = match conn.keys("dirty:/posts/*").await {
            Ok(k) => k,
            Err(e) => {
                eprintln!("❌ Failed to get keys: {e}");
                sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        for key in keys {
            println!("key : {key}");
            if let Ok(Some(bytes)) = (conn.get::<_, Option<String>>(&key).await) {

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
        if let Some(post_id_str) = expired_key.strip_prefix("delete:/posts/") {
            delete_function(db.clone(), post_id_str.to_string()).await;
        }
    }
}
//middleware
pub async fn middleware(
    State(state): State<CacheState>,
    req: Request<Body>,
    next: Next
) -> Result<Response<Body>, StatusCode> {
    let key = req
    .uri()
    .path_and_query()
    .map(|pq| pq.as_str().to_string()) // 👈 복사
    .unwrap_or_else(|| "".to_string());

    // 이미 삭제된거면, 안보이게 해야 함 
    let del_key = String::from("delete:") + &key;
    let mut conn = state.conn;
    let write_to_cache = state.write_to_cache;
    if conn.exists(&del_key).await.unwrap() {
        let final_response = Response::builder()
            .status(404)
            .body(Body::empty())
            .unwrap();

        return Ok(final_response);
    }

    //삭제 안됐을때.
    
    //get 요청
    match req.method() {
        &Method::GET => {
            println!("GET");
            // 캐시 로직 등 수행
            // Redis에 캐시된 응답이 있는지 확인
            match conn.get::<_, Option<String>>(&key).await {
                Ok(Some(cached_body)) => {
                    println!("🔄 Redis cache hit for {}", key);
                    let response = Response::builder()
                        .status(200)
                        .header("X-Cache", "HIT")
                        .header("Content-Type", "application/json")
                        .body(Body::from(cached_body))
                        .unwrap();
                    return Ok(response);
                }
                Ok(None) => {
                    println!("❌ Cache miss, find in dirty");
                    let dirty = String::from("dirty:") + &key;
                    match conn.get::<_, Option<String>>(&dirty).await {
                        Ok(Some(cached_body)) => {
                            println!("🔄 Redis cache hit for {}", key);
                            let response = Response::builder()
                                .status(200)
                                .header("X-Cache", "HIT")
                                .header("Content-Type", "application/json")
                                .body(Body::from(cached_body))
                                .unwrap();
                            return Ok(response);
                        }
                        Ok(None) => {
                            println!("Cache Missed again");
                        }
                        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
                    }
                    // 계속 진행
                }
                Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            }
        },
        &Method::PUT => {
            println!("PUT");
            let dirty_key = String::from("dirty:") + &key;
            match conn.get::<_, Option<String>>(&dirty_key).await {
                Ok(Some(cached_body)) => {
                    println!("✅ Redis dirty hit {}", key);

                    let (parts, body ) = req.into_parts(); //consume
                    let collected = body.collect().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                    let bytes: Bytes = collected.to_bytes(); // bytes로 변환
                    let new = String::from_utf8_lossy(&bytes).to_string();

                    let response_json = write_to_cache(cached_body, new);
                    let response_bytes = response_json.into_bytes();
                    let cloned_bytes = response_bytes.clone();
                    let dirty_key = String::from("dirty:") + &key;
                    conn.set(dirty_key, cloned_bytes).await.unwrap_or(());
                    let _ :RedisResult<i32>  = conn.del(key).await;
                    
                    let final_response = Response::builder()
                        .status(200)
                        .header("X-Cache", "HIT")
                        .header("Content-Type", "application/json")
                        .body(Body::from(response_bytes))
                        .unwrap();

                    return Ok(final_response);
                }
                Ok(None) => {
                    match conn.get::<_, Option<String>>(&key).await {
                        Ok(Some(cached_body)) => {
                            println!("✅ Redis cache hit for {}", key);

                            let (parts, body ) = req.into_parts(); //consume
                            let collected = body.collect().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                            let bytes: Bytes = collected.to_bytes(); // bytes로 변환
                            let new = String::from_utf8_lossy(&bytes).to_string();

                            let response_json = write_to_cache(cached_body, new);

                            let response_bytes = response_json.into_bytes();
                            let cloned_bytes = response_bytes.clone();
                            let dirty_key = String::from("dirty:") + &key;
                            conn.set(dirty_key, cloned_bytes).await.unwrap_or(());
                            let _ :RedisResult<i32>  = conn.del(key).await;
                            
                            let final_response = Response::builder()
                                .status(200)
                                .header("X-Cache", "HIT")
                                .header("Content-Type", "application/json")
                                .body(Body::from(response_bytes))
                                .unwrap();
        
                            return Ok(final_response);
                        }
                        Ok(None) => {
                            println!("❌ Cache miss");
                            // 계속 진행
                        }
                        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
                    }
                }
            Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            
            }
            
            // write-back 캐시 등 수행
        },
        &Method::DELETE => { //DELETE일때는 캐시에 delete tag 붙여서 기록
            println!("DEL");
            let result :RedisResult<i32> = conn.del(&key).await;
            match result {
                Ok(i)   => println!("deleted {i}"),
                Err(e) => println!("err {e}"),
            }
            let dirty_key = String::from("dirty:") + &key;
            let _  :RedisResult<i32>= conn.del(&dirty_key).await;
            let del_key = String::from("delete:") + &key;
            let _ : RedisResult<()>= conn.set_ex(&del_key, "1", 10).await;
            let response = Response::builder()
                .status(204)
                .body(axum::body::Body::empty())
                .unwrap();
            return Ok(response);
        }
        _ => {
            println!("🔴 기타 요청");
        }
    }

    let method = req.method().clone();
    // 없으면 요청을 처리
    let response = next.run(req).await;

    //GET : Cache miss
    //PUT : Cache miss
    //DELETE : not reached
    match method {
        Method::GET | Method::PUT => {
            // 바디 추출
            let (parts, body) = response.into_parts();
            let collected = body.collect().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let bytes: Bytes = collected.to_bytes(); // bytes로 변환
            let string_body = String::from_utf8_lossy(&bytes).to_string();
            // Redis에 저장 (TTL: 60초)
            let _: () = conn.set_ex::<_, _, ()>(key, string_body, 60).await.unwrap_or(());

            //response 재조립립
            let final_response = Response::from_parts(parts, Body::from(bytes));
            Ok(final_response)
        },
        _ => Err(StatusCode::INTERNAL_SERVER_ERROR)
    }

}
