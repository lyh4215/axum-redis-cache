// tests/integration.rs

use axum::{
    Router,
    routing::{get, put, delete},
    body::Body,
    http::{Request, StatusCode},
    middleware::from_fn_with_state,
    extract::State,
    response::IntoResponse,
};
use tower::ServiceExt;
use sqlx::{Pool, Postgres, postgres::PgPoolOptions};
use axum_redis_cache::{CacheConnection, middleware as my_middleware}; // 경로에 따라 조정
use std::{env, time::Duration};
use tokio::time::sleep;

/// 테이블 생성 SQL (테스트용)
const INIT_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS posts (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL
);"#;

/// Merge 함수 (예시)
fn merge_json(_old: String, new: String) -> String {
    new
}

#[tokio::test]
async fn test_cache_middleware_postgres() {
    // (1) docker-compose로 postgres, redis 올려둘 것!
    //     docker-compose up -d

    // (2) DB 커넥션 정보 (docker-compose와 맞춰야 함)
    let db_url = "postgres://testuser:testpw@localhost:5432/testdb";
    let pool = loop {
        match PgPoolOptions::new().max_connections(5).connect(db_url).await {
            Ok(pool) => break pool,
            Err(e) => {
                eprintln!("DB 연결 재시도: {e}");
                sleep(Duration::from_secs(1)).await;
            }
        }
    };

    // (3) 테스트 테이블 준비 (없으면 생성)
    sqlx::query(INIT_SQL).execute(&pool).await.unwrap();

    // (4) CacheConnection, CacheManager 준비
    let cache = CacheConnection::new(pool.clone()).await;
    let manager = cache.get_manager(
        "posts".to_string(),
        |_db, _s| Box::pin(async {}),
        |_db, _s| Box::pin(async {}),
        merge_json,
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
}
