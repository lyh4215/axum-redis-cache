//src/middleware.rs

use axum::{
    extract::{State}, 
    http::{Request, Response, StatusCode},
    middleware::{Next},
};

use redis::{AsyncCommands, RedisResult, aio::MultiplexedConnection};
use axum::http::Method;
use http_body_util::BodyExt;
use bytes::Bytes;
use axum::body::Body;

use crate::cache;

//middleware
pub async fn middleware(
    State(state): State<cache::CacheState>,
    req: Request<Body>,
    next: Next
) -> Result<Response<Body>, StatusCode> {
    let key = req
    .uri()
    .path_and_query()
    .map(|pq| pq.as_str().to_string()); // 👈 복사

    let key = match key {
        Some(k) => k,
        None => return Err(StatusCode::BAD_REQUEST),
    };

    let key = normalize_path(&key);

    // if already deleted, return 404
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

    //in db
    match req.method() {
        &Method::GET => {

            if let Some(cached_body) = get_dirty_or_clean(&mut conn, &key).await? {
                return Ok(build_cached_response(cached_body));
            }
            // 계속 진행
        }

        &Method::PUT => {
            if let Some(cached_body) = get_dirty_or_clean(&mut conn, &key).await? {
                let (_, body) = req.into_parts();
                let collected = body.collect().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                let new_body = String::from_utf8_lossy(&collected.to_bytes()).to_string();

                let response_json = write_to_cache(cached_body, new_body);
                let response_bytes = response_json.into_bytes();

                let dirty_key = format!("dirty:{}", key);
                let _: () = conn.set(&dirty_key, &response_bytes).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                let _: RedisResult<i32> = conn.del(&key).await;

                return Ok(
                    Response::builder()
                        .status(200)
                        .header("X-Cache", "HIT")
                        .header("Content-Type", "application/json")
                        .body(Body::from(response_bytes))
                        .unwrap()
                );
            }

            // 계속 진행
        }

        &Method::DELETE => {
            let _: RedisResult<i32> = conn.del(&key).await;
            let _: RedisResult<i32> = conn.del(&format!("dirty:{}", key)).await;
            let _: RedisResult<()> = conn.set_ex(&format!("delete:{}", key), "1", 10).await;

            return Ok(
                Response::builder()
                    .status(204)
                    .body(Body::empty())
                    .unwrap()
            );
        }
        _ => ()
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
            match conn.set_ex::<_, _, ()>(key, string_body, 60).await {
                Ok(_) => (),
                Err(e) => {
                    return Err(StatusCode::INTERNAL_SERVER_ERROR);
                }
            }

            //response 재조립
            let final_response = Response::from_parts(parts, Body::from(bytes));
            Ok(final_response)
        },
        _ => Err(StatusCode::INTERNAL_SERVER_ERROR)
    }

}


/// dirty → clean 순으로 조회하는 공통 헬퍼
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

/// 캐시 HIT 시 응답 생성
fn build_cached_response(body: String) -> Response<Body> {
    Response::builder()
        .status(200)
        .header("X-Cache", "HIT")
        .header("Content-Type", "application/json")
        .body(Body::from(body))
        .unwrap()
}


fn normalize_path(path: &str) -> String {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    trimmed.replace('/', ":")
}