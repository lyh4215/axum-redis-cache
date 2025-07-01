// src/middleware.rs

use axum::{
    extract::State,
    http::{Request, Response, StatusCode},
    middleware::Next,
};
use redis::{AsyncCommands, RedisResult, aio::MultiplexedConnection};
use axum::http::Method;
use http_body_util::BodyExt;
use bytes::Bytes;
use axum::body::Body;

use crate::cache;

/// Main middleware for cache handling.
///
/// Handles GET, PUT, DELETE logic with Redis backend.
/// - Returns cached data if present
/// - Marks as dirty on PUT
/// - Soft-deletes via `delete:` key on DELETE
pub async fn middleware(
    State(state): State<cache::CacheState>,
    req: Request<Body>,
    next: Next,
) -> Result<Response<Body>, StatusCode> {
    // Extract key from path and query
    let key = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string());

    let key = match key {
        Some(k) => k,
        None => return Err(StatusCode::BAD_REQUEST),
    };

    let key = normalize_path(&key);

    // Check for deleted marker in Redis
    let del_key = String::from("delete:") + &key;
    let mut conn = state.conn;
    let write_to_cache = state.write_to_cache;
    if conn.exists(&del_key).await.unwrap() {
        let final_response = Response::builder()
            .status(404)
            .body(Body::empty());
        match final_response {
            Ok(resp) => return Ok(resp),
            Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        };
    }

    // Dispatch based on HTTP method
    match req.method() {
        &Method::GET => {
            // Try dirty or clean cache hit
            if let Some(cached_body) = get_dirty_or_clean(&mut conn, &key).await? {
                return Ok(build_cached_response(cached_body));
            }
            // Continue if cache miss
        }
        &Method::PUT => {
            if let Some(cached_body) = get_dirty_or_clean(&mut conn, &key).await? {
                let (_, body) = req.into_parts();
                let collected = body.collect().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                let new_body = String::from_utf8_lossy(&collected.to_bytes()).to_string();

                // Call custom cache merger (usually JSON merge)
                let response_json = write_to_cache(cached_body, new_body);
                let response_bytes = response_json.into_bytes();

                // Store as dirty, delete clean
                let dirty_key = format!("dirty:{}", key);
                let _: () = conn.set(&dirty_key, &response_bytes).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                let _: RedisResult<i32> = conn.del(&key).await;

                return Ok(
                    Response::builder()
                        .status(200)
                        .header("X-Cache", "HIT")
                        .header("Content-Type", "application/json")
                        .body(Body::from(response_bytes))
                        .unwrap(),
                );
            }
            // Continue if cache miss
        }
        &Method::DELETE => {
            // Remove both dirty/clean, mark deleted for soft delete TTL
            let _: RedisResult<i32> = conn.del(&key).await;
            let _: RedisResult<i32> = conn.del(&format!("dirty:{}", key)).await;
            let _: RedisResult<()> = conn.set_ex(&format!("delete:{}", key), "1", 10).await;

            return Ok(
                Response::builder()
                    .status(204)
                    .body(Body::empty())
                    .unwrap(),
            );
        }
        _ => (),
    }

    let method = req.method().clone();
    // Forward to real handler if cache miss
    let response = next.run(req).await;

    // After handler: Optionally cache (GET, PUT) result
    match method {
        Method::GET | Method::PUT => {
            // Extract response body
            let (parts, body) = response.into_parts();
            let collected = body.collect().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let bytes: Bytes = collected.to_bytes();
            let string_body = String::from_utf8_lossy(&bytes).to_string();
            // Store in Redis (TTL: 60s)
            match conn.set_ex::<_, _, ()>(key, string_body, 60).await {
                Ok(_) => (),
                Err(_) => {
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }
            // Reassemble response
            let final_response = Response::from_parts(parts, Body::from(bytes));
            Ok(final_response)
        }
        _ => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Try dirty cache first, then clean cache.
///
/// Returns: Some(body) if hit, None if miss.
async fn get_dirty_or_clean(
    conn: &mut MultiplexedConnection,
    key: &str,
) -> Result<Option<String>, StatusCode> {
    let dirty_key = format!("dirty:{}", key);

    match conn.get::<_, Option<String>>(&dirty_key).await {
        Ok(Some(val)) => {
            println!("✅ Redis dirty cache hit: {}", key);
            return Ok(Some(val));
        }
        Ok(None) => {}
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }

    match conn.get::<_, Option<String>>(key).await {
        Ok(Some(val)) => {
            println!("✅ Redis clean cache hit: {}", key);
            Ok(Some(val))
        }
        Ok(None) => {
            println!("❌ Cache miss: {}", key);
            Ok(None)
        }
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Build an Axum Response from cached data.
fn build_cached_response(body: String) -> Response<Body> {
    Response::builder()
        .status(200)
        .header("X-Cache", "HIT")
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

/// Normalize path to redis key (ex: "/foo/bar" => "foo:bar")
fn normalize_path(path: &str) -> String {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    trimmed.replace('/', ":")
}
