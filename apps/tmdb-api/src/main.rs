use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use anyhow::Context;
use sqlx::PgPool;
use tmdb_api::{
    ApiState, DatabaseAdminStore, DatabaseReadinessProbe, ShutdownError,
    build_admin_router_with_operations_and_auth, build_router, shutdown_signal, supervise_shutdown,
};
use tmdb_config::{AppConfig, EnvSource, Environment, load_database_for_role};
use tmdb_db::{PoolPolicy, connect_direct_for_startup};
use tmdb_observability::{Metrics, init_tracing_from_env};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing_from_env(env!("CARGO_PKG_NAME")).map_err(|error| anyhow::anyhow!(error))?;
    tracing::info!(event = "api_starting");
    let config = AppConfig::load(&EnvSource).context("load API configuration")?;
    let database_pools = connect_api_database_pools(config.environment).await?;

    let metrics = Metrics::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), "unknown");
    let state = ApiState::new(
        Arc::new(DatabaseReadinessProbe::new(
            database_pools.read_pool.clone(),
            database_pools.reader_username.clone(),
        )),
        metrics.clone(),
    );
    let tmdb_v3_router = tmdb_api::build_tmdb_v3_router(
        database_pools.read_pool.clone(),
        database_pools.write_pool.clone(),
        config.media_base_url.clone(),
    );
    let public_listener = tokio::net::TcpListener::bind(config.api_bind)
        .await
        .context("bind public API listener")?;
    let admin_listener = tokio::net::TcpListener::bind(config.admin_bind)
        .await
        .context("bind admin API listener")?;
    tracing::info!(event = "listener_started", listener = "public");
    tracing::info!(event = "listener_started", listener = "admin");
    tracing::info!(event = "api_ready", public_surface = "tmdb_v3",);

    let cancellation = CancellationToken::new();
    let signal_cancellation = cancellation.clone();
    let signal_failed = Arc::new(AtomicBool::new(false));
    let signal_failed_task = signal_failed.clone();
    let signal_task = tokio::spawn(async move {
        match shutdown_signal().await {
            Ok(()) => {
                tracing::info!(event = "shutdown_requested");
                signal_cancellation.cancel();
            }
            Err(error) => {
                signal_failed_task.store(true, Ordering::SeqCst);
                tracing::error!(event = "shutdown_signal_failed", error = %error);
                signal_cancellation.cancel();
            }
        }
    });

    let public_server = axum::serve(public_listener, build_router(state).merge(tmdb_v3_router))
        .with_graceful_shutdown(cancellation.clone().cancelled_owned())
        .into_future();
    let admin_server = axum::serve(
        admin_listener,
        build_admin_router_with_operations_and_auth(
            metrics,
            config.admin_api_key,
            Arc::new(DatabaseAdminStore::new(
                database_pools.admin_read_pool,
                database_pools.write_pool,
            )),
        ),
    )
    .with_graceful_shutdown(cancellation.clone().cancelled_owned())
    .into_future();
    let result = supervise_shutdown(
        public_server,
        admin_server,
        cancellation,
        std::time::Duration::from_secs(30),
    )
    .await;
    signal_task.abort();
    let _ = signal_task.await;
    if signal_failed.load(Ordering::SeqCst) {
        return Err(anyhow::anyhow!("shutdown signal setup failed"));
    }
    if let Err(error) = result {
        if matches!(error, ShutdownError::DeadlineExceeded) {
            tracing::error!(event = "shutdown_deadline_exceeded");
        }
        return Err(anyhow::anyhow!(error));
    }
    tracing::info!(event = "shutdown_complete");
    Ok(())
}

struct ApiDatabasePools {
    read_pool: PgPool,
    write_pool: PgPool,
    admin_read_pool: PgPool,
    reader_username: String,
}

async fn connect_api_database_pools(environment: Environment) -> anyhow::Result<ApiDatabasePools> {
    let reader_database = load_database_for_role(&EnvSource, environment, "api_reader")
        .context("load API reader database configuration")?;
    let submitter_database = load_database_for_role(&EnvSource, environment, "api_job_submitter")
        .context("load API submitter database configuration")?;
    let monitor_database = load_database_for_role(&EnvSource, environment, "monitor")
        .context("load API monitor database configuration")?;
    let read_pool = connect_direct_for_startup(&reader_database, PoolPolicy::ReadOnly)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect API read pool")?;
    let write_pool = connect_direct_for_startup(&submitter_database, PoolPolicy::ReadWrite)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect API administrative write pool")?;
    let admin_read_pool = connect_direct_for_startup(&monitor_database, PoolPolicy::ReadOnly)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect API administrative read pool")?;

    Ok(ApiDatabasePools {
        read_pool,
        write_pool,
        admin_read_pool,
        reader_username: reader_database.username,
    })
}
