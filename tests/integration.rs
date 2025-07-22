// tests/integration.rs

use axum::{
    Router,
    routing::{get, put, delete},
    body::Body,
    http::{Request, StatusCode},
    middleware::from_fn_with_state,
};
use tower::ServiceExt;
use axum_redis_cache::{CacheConnection, CacheConfig, CacheConnConfig}; // 경로에 따라 조정
use std::{time::Duration};
use tokio::time::sleep;
use redis::AsyncCommands;

#[path = "common.rs"]
mod common;

/// 테이블 생성 SQL (테스트용)
const INIT_SQL_POSTS: &str = r#"
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL
);"#;

const INIT_SQL_POSTS_TTL: &str = r#"
CREATE TABLE IF NOT EXISTS posts_ttl (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL
);"#;

const INIT_SQL_POSTS_DELETE_TTL: &str = r#"
CREATE TABLE IF NOT EXISTS posts_delete_ttl (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL
);"#;



#[tokio::test]
async fn test_cache_middleware_postgres() {
    //binding
    let pgstruct  = common::start_postgres().await;
    let pool = pgstruct.pool;

    let redisstruct = common::start_redis().await;
    let redis_url = redisstruct.url;

    // (3) 테스트 테이블 준비 (없으면 생성)
    sqlx::query(INIT_SQL_POSTS).execute(&pool).await.unwrap();

    // (4) CacheConnection, CacheManager 준비
    let cache_conn_config = CacheConnConfig::new()
        .with_url(&redis_url);
    let cache = CacheConnection::new_with_config(pool.clone(), cache_conn_config).await;
    let mut manager = cache.get_manager(
        "posts".to_string(),
        |_db, _s| Box::pin(async {}),
        |_db, _s| Box::pin(async {}),
        common::merge_json,
    );

    // (5) axum 라우터 준비
    let app = Router::new()
        .route("/posts/:id", get(|| async { "hello" }))
        .route("/posts/:id", put(|| async { "updated" }))
        .route("/posts/:id", delete(|| async { "" }))
        .with_state(pool.clone())
        .layer(from_fn_with_state(manager.get_state(), axum_redis_cache::middleware));

    // (6) GET 테스트 (처음은 캐시 미스)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/posts/123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // (7) PUT (dirty 캐시 생성)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri("/posts/123")
                .body(Body::from(r#"{"foo":"bar"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // (8) DELETE
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/posts/123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // (9) DELETE 후 GET → 404
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/posts/123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    // (10) CacheManager shutdown
    manager.shutdown().await;
}

#[tokio::test]
async fn test_cache_ttl() {
    let pgstruct  = common::start_postgres().await;
    let pool = pgstruct.pool;

    let redisstruct = common::start_redis().await;
    let redis_url = redisstruct.url;

    sqlx::query(INIT_SQL_POSTS_TTL).execute(&pool).await.unwrap();

    let cache_conn_config = CacheConnConfig::new()
        .with_url(&redis_url);
    let mut cache = CacheConnection::new_with_config(pool.clone(), cache_conn_config).await;
    let cache_config = CacheConfig::new().with_clean_ttl(5); // 5초 TTL
    let mut manager = cache.get_manager(
        "posts_ttl".to_string(),
        |_db, _s| Box::pin(async {}),
        |_db, _s| Box::pin(async {}),
        common::merge_json,
    ).with_config(cache_config);

    let app = Router::new()
        .route("/posts_ttl/:id", get(|| async { "ttl test" }))
        .with_state(pool.clone())
        .layer(from_fn_with_state(manager.get_state(), axum_redis_cache::middleware));

    // (1) GET 요청으로 캐시 생성
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/posts_ttl/ttl_test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // (2) 3초 후, 키가 아직 존재하는지 확인
    sleep(Duration::from_secs(3)).await;
    let key_exists: bool = cache.conn.exists("posts_ttl:ttl_test").await.unwrap();
    assert!(key_exists);

    // (3) 추가 3초 후 (총 6초), 키가 만료되었는지 확인
    sleep(Duration::from_secs(3)).await;
    let key_exists: bool = cache.conn.exists("posts_ttl:ttl_test").await.unwrap();
    assert!(!key_exists);
    // (4) CacheManager shutdown
    manager.shutdown().await;
}

#[tokio::test]
async fn test_cache_delete_ttl() {
    let pgstruct  = common::start_postgres().await;
    let pool = pgstruct.pool;

    let redisstruct = common::start_redis().await;
    let redis_url = redisstruct.url;

    sqlx::query(INIT_SQL_POSTS_DELETE_TTL).execute(&pool).await.unwrap();

    let cache_conn_config = CacheConnConfig::new()
        .with_url(&redis_url);
    let mut cache = CacheConnection::new_with_config(pool.clone(), cache_conn_config).await;
    let cache_config = CacheConfig::new().with_deleted_ttl(5); // 5초 TTL
    let mut manager = cache.get_manager(
        "posts_delete_ttl".to_string(),
        |_db, _s| Box::pin(async {}),
        |_db, _s| Box::pin(async {}),
        common::merge_json,
    ).with_config(cache_config);

    let app = Router::new()
        .route("/posts_delete_ttl/:id", delete(|| async { "" }))
        .with_state(pool.clone())
        .layer(from_fn_with_state(manager.get_state(), axum_redis_cache::middleware));

    // (1) DELETE 요청으로 delete: 키 생성
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri("/posts_delete_ttl/delete_test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // (2) 3초 후, delete: 키가 아직 존재하는지 확인
    sleep(Duration::from_secs(3)).await;
    let key_exists: bool = cache.conn.exists("delete:posts_delete_ttl:delete_test").await.unwrap();
    assert!(key_exists);

    // (3) 추가 3초 후 (총 6초), delete: 키가 만료되었는지 확인
    sleep(Duration::from_secs(3)).await;
    let key_exists: bool = cache.conn.exists("delete:posts_delete_ttl:delete_test").await.unwrap();
    assert!(!key_exists);

    // (4) CacheManager shutdown
    manager.shutdown().await;
}