use std::fmt::Result;

use testcontainers::{
    bollard::container,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
    GenericImage,
    TestcontainersError,
    ContainerAsync
};
use testcontainers_modules::{postgres, redis::Redis};
use testcontainers_modules::postgres::Postgres;

use sqlx::{Pool, postgres::{PgPool, PgPoolOptions, PgConnection, PgConnectOptions}};

pub struct PgStruct {
    pub pool: PgPool,
    pub container: ContainerAsync<Postgres>,
}

pub struct RedisStruct {
    pub url : String,
    pub container: ContainerAsync<Redis>,
}
/// Redis 컨테이너 시작
pub async fn start_redis() -> RedisStruct {
    let container = Redis::default().start().await.unwrap();
    let host = container.get_host().await.unwrap();
    let host_port = container.get_host_port_ipv4(6379).await.unwrap();

    let url = format!("redis://{}:{}", host, host_port);
    RedisStruct {
        url,
        container,
    }
}

/// Postgres 컨테이너 시작
pub async fn start_postgres() -> PgStruct {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host_port = container.get_host_port_ipv4(5432).await.unwrap();
    let connection_string = &format!(
        "postgres://postgres:postgres@127.0.0.1:{host_port}/postgres",
    );

    let mut opts : PgConnectOptions = connection_string.parse().unwrap();

    let pool = PgPool::connect_with(opts).await.unwrap();

    PgStruct {
        pool,
        container,
    }
}


/// Merge 함수 (예시)
pub fn merge_json(_old: String, new: String) -> String {
    new
}