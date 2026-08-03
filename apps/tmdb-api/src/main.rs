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
use tmdb_db::{CatalogRepository, PoolPolicy, connect_direct};
use tmdb_observability::{Metrics, init_tracing_from_env};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing_from_env(env!("CARGO_PKG_NAME")).map_err(|error| anyhow::anyhow!(error))?;
    tracing::info!(event = "api_starting");
    let config = AppConfig::load(&EnvSource).context("load API configuration")?;
    let allow_local_media = load_bool("ALLOW_LOCAL_MEDIA")?;
    let media_base_url = load_optional_string("TMDB_MEDIA_BASE_URL")?;
    let local_media_url_configured = media_base_url.is_some();
    let database_pools = connect_api_database_pools(config.environment).await?;

    let metrics = Metrics::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), "unknown");
    let state = ApiState::new(
        Arc::new(DatabaseReadinessProbe::new(
            database_pools.read_pool.clone(),
            database_pools.reader_username.clone(),
        )),
        metrics.clone(),
    );
    let catalog_router = tmdb_api::build_catalog_router_with_media(
        Arc::new(CatalogRepository::new(database_pools.read_pool.clone())),
        allow_local_media,
        media_base_url,
    );
    let public_listener = tokio::net::TcpListener::bind(config.api_bind)
        .await
        .context("bind public API listener")?;
    let admin_listener = tokio::net::TcpListener::bind(config.admin_bind)
        .await
        .context("bind admin API listener")?;
    tracing::info!(event = "listener_started", listener = "public");
    tracing::info!(event = "listener_started", listener = "admin");
    tracing::info!(
        event = "api_ready",
        local_media_enabled = allow_local_media,
        local_media_url_configured,
    );

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

    let public_server = axum::serve(public_listener, build_router(state).merge(catalog_router))
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
    let read_pool = connect_direct(&reader_database, PoolPolicy::ReadOnly)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect API read pool")?;
    let write_pool = connect_direct(&submitter_database, PoolPolicy::ReadWrite)
        .await
        .map_err(|error| anyhow::anyhow!(error))
        .context("connect API administrative write pool")?;
    let admin_read_pool = connect_direct(&monitor_database, PoolPolicy::ReadOnly)
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

fn load_bool(name: &str) -> anyhow::Result<bool> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| anyhow::anyhow!("configuration field {name} is invalid")),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow::anyhow!(
            "configuration field {name} is not valid Unicode"
        )),
    }
}

fn load_optional_string(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) => Err(anyhow::anyhow!("configuration field {name} is invalid")),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(anyhow::anyhow!(
            "configuration field {name} is not valid Unicode"
        )),
    }
}
