
use std::panic;
use axum_redis_cache::{
    CacheConnection,
    CacheConfig,
    CacheConnConfig,
    CacheManager,
};

#[path = "common.rs"]
mod common;


#[tokio::test]
#[should_panic(expected = "CacheManager dropped without calling shutdown()")]
async fn drop_without_shutdown() {
    //binding
    let pgstruct  = common::start_postgres().await;
    let pool = pgstruct.pool;

    let redisstruct = common::start_redis().await;
    let redis_url = redisstruct.url;

    let cache_conn_config = CacheConnConfig::new()
    .with_url(&redis_url);
    let cache = CacheConnection::new_with_config(pool.clone(), cache_conn_config).await;
    let manager = cache.get_manager(
        "posts".to_string(),
        |_db, _s| Box::pin(async {}),
        |_db, _s| Box::pin(async {}),
        common::merge_json,
    );
}

#[tokio::test]
async fn drop_with_shutdown() {
    //binding
    let pgstruct  = common::start_postgres().await;
    let pool = pgstruct.pool;

    let redisstruct = common::start_redis().await;
    let redis_url = redisstruct.url;

    let cache_conn_config = CacheConnConfig::new()
        .with_url(&redis_url);
    let cache = CacheConnection::new_with_config(pool.clone(), cache_conn_config).await;
    let mut manager = cache.get_manager(
        "posts".to_string(),
        |_db, _s| Box::pin(async {}),
        |_db, _s| Box::pin(async {}),
        common::merge_json,
    );

    manager.shutdown().await;

    assert!(true, "CacheManager dropped cleanly after shutdown");
}