//! HTTP health and telemetry surface for the TMDB API.

mod admin_api;
mod app;
pub mod health;
pub mod problem;
mod tmdb_v3_api;

pub use admin_api::{
    AdminApiError, AdminApiStore, AdminBackupKind, AdminBackupStatus, AdminBuildStatus,
    AdminCatalogCounts, AdminCatalogPhase, AdminCatalogWork, AdminComponentHealth,
    AdminDatabaseStatus, AdminJob, AdminJobDetail, AdminJobEvent, AdminJobListRequest,
    AdminJobPage, AdminMediaRequestItem, AdminMediaRequestOutcome, AdminMediaRequestStatus,
    AdminMediaRequestSubmission, AdminMediaWorkerAction, AdminMediaWorkerStatus, AdminOperation,
    AdminPoolStatus, AdminQueueSummary, AdminScanMode, AdminStatus, AdminSubmission,
    AdminWorkerAction, AdminWorkerStatus, DatabaseAdminStore,
};
pub use app::{
    ApiState, DatabaseReadinessProbe, ProbeError, REQUEST_TIMEOUT, ReadinessProbe, RequestId,
    ShutdownError, build_admin_router, build_admin_router_with_auth,
    build_admin_router_with_auth_and_timeout, build_admin_router_with_operations_and_auth,
    build_admin_router_with_timeout, build_router, build_router_with_timeout, build_test_router,
    shutdown_signal, supervise_shutdown,
};
pub use tmdb_db::ReadinessReport;
pub use tmdb_v3_api::build_tmdb_v3_router;
