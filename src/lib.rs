// src/lib.rs

//! # Redis-backed Write-behind Cache for Axum
//!
//! - GET/PUT/DELETE cache middleware
//! - Write-behind worker
//! - Expired event listener
//! 
//! # Example
//! 
//! You can use sqlx DB.
//! Use Example (with no custom config : you can't change config in this case):
//! ```rust,ignore
//! use tokio::sync::CancellationToken;
//! 
//! // Graceful shutdown setup
//! let shutdown_token = CancellationToken::new();
//! let token_clone = shutdown_token.clone();
//! tokio::spawn(async move {
//!     tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
//!     println!("
//! Ctrl-C received, initiating shutdown...");
//!     token_clone.cancel();
//! });
//! 
//! let db : sqlx::Pool<DB> = /* your DB */;
//! let cache_connection = axum_redis_cache::CacheConnection::new(db.clone()).await;
//! let key = String::from("posts");
//! 
//! struct Post {
//!     id: i32,
//!     content: String,
//!     writer : i32,
//! }
//! 
//! struct PostUpdate {
//!    content: Option<String>,
//! }
//! 
//! async fn write_callback<DB>(db: sqlx::Pool<DB>, body: String) {
//!     let json : Post = serde_json::from_str(&body).unwrap();
//!     sqlx::query(
//!         "UPDATE posts
//!         SET content = $2
//!         WHERE id = $1")
//!     .bind(json.id)
//!     .bind(json.content)
//!     .execute(&db)
//!     .await
//!     .unwrap();
//! }
//! 
//! 
//! async fn delete_callback<DB>(db: sqlx::Pool<DB>, id: String) {
//!     if let Ok(id) = id.parse::<i32>() {
//!         sqlx::query("DELETE FROM posts WHERE id = $1")
//!            .bind(id)
//!            .execute(&db)
//!            .await
//!            .unwrap(); 
//!     }
//! }
//! 
//! async fn update_entity(old: String, new: String) -> String {
//!     let mut post: Post = serde_json::from_str(&old).unwrap();
//!     let new_post: PostUpdate = serde_json::from_str(&new).unwrap();
//!     if let Some(content) = new_post.content {
//!        post.content = content;
//!     }
//!     serde_json::to_string(&post).unwrap()
//! }
//! 
//! let mut cache_manager = cache_connection.get_manager(
//!     key,
//!     write_callback,
//!     delete_callback,
//!     update_entity,
//!     shutdown_token.clone(),
//! );
//! 
//! let routes = axum::Router::new()
//!     .route("/posts/:id", get(/* your get handler */)
//!                         .delete( /* your delete handler */)
//!                         .put( /* your put handler */))
//!     .layer(axum::middleware::from_fn_with_state(
//!         cache_manager.get_state(),
//!         axum_redis_cache::middleware
//!     ));
//! 
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
//! axum::serve(listener, routes)
//!     .with_graceful_shutdown(shutdown_token.cancelled())
//!     .await
//!     .unwrap();
//! 
//! cache_manager.shutdown().await;
//! ```
//! 
//! By this, you can use Axum middleware to cache your data.''
//!
//!
//!
//!
//! For usage and examples, see [README](https://github.com/lyh4215/axum-redis-cache).

mod cache;
mod middleware;

pub use cache::*;
pub use middleware::*;