// src/cache_sync.rs

use redis::{aio::MultiplexedConnection,
            AsyncCommands,
            Script};
use tokio_util::sync::CancellationToken;
use sqlx::{Database, Pool};
use tokio::time::{Duration};
use colored::*;
use futures_util::StreamExt;


/// Write-behind background worker.
/// Every N seconds, scans dirty:* keys and writes to DB, then cleans up.
pub async fn write_behind<F, Fut, DB>(
    mut conn: MultiplexedConnection,
    db: Pool<DB>,
    root_key: String,
    config: std::sync::Arc<std::sync::Mutex<crate::cache::CacheConfig>>,
    write_function: F,
    token: CancellationToken,
)
where
    F: Fn(Pool<DB>, String) -> Fut,
    Fut: std::future::Future<Output = ()>,
    DB: Database,
{
    println!("{} Redis write behind thread", "Start".green().bold());
    loop {
        let duration = config.lock().unwrap().write_duration;
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(duration)) => {
                // Scan for dirty keys
                let dirty_key = format!("dirty:{}:*", root_key);
                let keys: Vec<String> = match conn.keys(&dirty_key).await {
                    Ok(k) => k,
                    Err(e) => {
                        eprintln!("❌ Failed to get keys: {e}");
                        continue;
                    }
                };

                for key in keys {
                    println!("key : {key}");
                    if let Ok(Some(bytes)) = conn.get::<_, Option<String>>(&key).await {
                        // Write to DB
                        write_function(db.clone(), bytes.clone()).await;

                        let clean_key = key.strip_prefix("dirty:").unwrap_or(&key).to_string();

                        // Clean up dirty key, set clean with short TTL
                        /*let _: () = conn.del(&key).await.unwrap_or(());
                        
                        let _: () = conn.set_ex(&clean_key, bytes, 10).await.unwrap_or(());
                        println!("  Write behind for : {key}");*/
                        let ttl_sec = config.lock().unwrap().ttl_clean;
                        /* atomic version (using redis script lua) */
                        let script = Script::new(
                            r#"
                            local dirty_key = KEYS[1]
                            local clean_key = KEYS[2]
                            local value = ARGV[1]
                            local ttl_sec = ARGV[2]
                            redis.call('del', dirty_key)
                            redis.call('setex', clean_key, ttl_sec, value)
                            return 1
                            "#,
                        );

                        let _: i32 = script
                            .key(key)
                            .key(clean_key)
                            .arg(bytes)
                            .arg(ttl_sec)
                            .invoke_async(&mut conn)
                            .await
                            .expect("Failed to execute write-behind script");
                    }
                }
            }
            _ = token.cancelled() => {
                println!("{} Write-behind task shutting down...", "Shutdown".red().bold());
                // Perform one final write for all dirty keys before exiting
                let dirty_key = format!("dirty:{}:*", root_key);
                if let Ok(keys) = conn.keys::<_, Vec<String>>(&dirty_key).await {
                    for key in keys {
                        if let Ok(Some(bytes)) = conn.get::<_, Option<String>>(&key).await {

                            // let _: () = conn.del(&key).await.unwrap_or(());
                            // let clean_key = key.strip_prefix("dirty:").unwrap_or(&key).to_string();
                            // let _: () = conn.set_ex(&clean_key, bytes, 10).await.unwrap_or(());
                            // println!("  Final write for: {key}");
                            let script = Script::new(
                                r#"
                                local dirty_key = KEYS[1]
                                local clean_key = KEYS[2]
                                local value = ARGV[1]
                                local ttl_sec = tonumber(ARGV[2])
                            
                                redis.call('del', dirty_key)
                                redis.call('setex', clean_key, ttl_sec, value)
                            
                                return 1
                                "#
                            );
                            
                            let clean_key = key.strip_prefix("dirty:").unwrap_or(&key).to_string();
                            
                            let _: i32 = script
                                .key(&key)            // dirty key
                                .key(&clean_key)      // clean key
                                .arg(bytes.clone())   // value
                                .arg(10)              // TTL
                                .invoke_async(&mut conn)
                                .await
                                .unwrap_or(0);
                            
                            println!("  Final write (atomic) for: {key}");
                            write_function(db.clone(), bytes.clone()).await;
                        }
                    }
                }
                break;
            }
        }
    }
}

/// Background task: listens for Redis expire (delete) events.
/// On expire, invokes user-provided delete function.
pub async fn delete_event_listener<F, Fut, DB: Database>(
    client: redis::Client,
    db: Pool<DB>,
    root_key: String,
    delete_function: F,
    token: CancellationToken,
)
where
    F: Fn(Pool<DB>, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let mut pubsub_conn = match client.get_async_pubsub().await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("❌ Failed to get pubsub connection: {e}");
            return;
        }
    };

    //TODO : not create in here.
    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("❌ Failed to get multiplexed connection: {e}");
            return;
        }
    };

    // Subscribe to Redis key expire events
    if let Err(e) = pubsub_conn.subscribe("__keyevent@0__:expired").await {
        eprintln!("❌ Failed to subscribe to key events: {e}");
        return;
    }
    let mut pubsub_stream = pubsub_conn.on_message();

    println!("{} Redis expired event listening", "Start".green().bold());
    loop {
        tokio::select! {
            Some(msg) = pubsub_stream.next() => {
                let expired_key: String = match msg.get_payload() {
                    Ok(key) => key,
                    Err(_) => continue,
                };

                let prefix = format!("delete:{}:", root_key);
                if let Some(post_id_str) = expired_key.strip_prefix(&prefix) {
                    // Call delete handler
                    delete_function(db.clone(), post_id_str.to_string()).await;
                }
            }
            _ = token.cancelled() => {
                println!("{} Delete event listener shutting down...", "Shutdown".red().bold());
                let delete_key = format!("delete:{}:*", root_key);
                if let Ok(keys) = conn.keys::<_, Vec<String>>(&delete_key).await {
                    let prefix = format!("delete:{}:", root_key);
                    for key in keys {
                        if let Some(post_id_str) = key.strip_prefix(&prefix) {
                            // Call delete handler
                            delete_function(db.clone(), post_id_str.to_string()).await;
                            let _: () = conn.del(&key).await.unwrap_or(());
                            println!("Final delete for: {key}");
                        }
                    }
                    
                break;
                }
            }
        }
    }
}
