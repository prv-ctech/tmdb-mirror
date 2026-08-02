//! HTTP health and telemetry surface for the TMDB API.

mod admin_api;
mod app;
mod catalog_api;
pub mod health;
pub mod problem;

pub use admin_api::{
    AdminApiError, AdminApiStore, AdminBackupKind, AdminBackupStatus, AdminBuildStatus,
    AdminCatalogCounts, AdminComponentHealth, AdminDatabaseStatus, AdminJob, AdminJobDetail,
    AdminJobEvent, AdminJobListRequest, AdminJobPage, AdminOperation, AdminPoolStatus,
    AdminQueueSummary, AdminStatus, AdminSubmission, DatabaseAdminStore,
};
pub use app::{
    ApiState, DatabaseReadinessProbe, ProbeError, REQUEST_TIMEOUT, ReadinessProbe, RequestId,
    ShutdownError, build_admin_router, build_admin_router_with_auth,
    build_admin_router_with_auth_and_timeout, build_admin_router_with_operations_and_auth,
    build_admin_router_with_timeout, build_router, build_router_with_timeout, build_test_router,
    shutdown_signal, supervise_shutdown,
};
pub use catalog_api::{
    CatalogApiError, CatalogApiStore, build_catalog_router, build_catalog_router_with_media,
};
pub use tmdb_db::ReadinessReport;
