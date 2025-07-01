// src/lib.rs

//! # Redis-backed Write-behind Cache for Axum
//!
//! - GET/PUT/DELETE cache middleware
//! - Write-behind worker
//! - Expired event listener
//!
//!
//!
//!
//! For usage and examples, see [README](https://github.com/lyh4215/axum-redis-cache).

mod cache;
mod middleware;

pub use cache::*;
pub use middleware::*;
