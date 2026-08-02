//! Database access primitives for the TMDB mirror.

mod catalog;
mod migrate;
mod options;
mod pool;
mod readiness;

pub use catalog::{
    AnimeScope, CatalogAlternateTitle, CatalogCollection, CatalogCompany, CatalogCredit,
    CatalogDetail, CatalogEpisode, CatalogError, CatalogExternalIds, CatalogFacets, CatalogFilters,
    CatalogGenre, CatalogImageAsset, CatalogImageVariant, CatalogKeyword, CatalogLanguage,
    CatalogMovieDetails, CatalogNetwork, CatalogPage, CatalogPerson, CatalogRecentPage,
    CatalogReleaseDate, CatalogRepository, CatalogSeason, CatalogTag, CatalogTitle, CatalogTopPage,
    CatalogTranslation, CatalogTrend, CatalogTvDetails, CatalogVideo, PopularCursor, RecentCursor,
    TopCursor,
};
pub use migrate::{MIGRATOR, MigrationReport, migrate};
pub use pool::{PoolPolicy, connect_direct};
pub use readiness::{ReadinessReport, readiness};

/// A sanitized database failure suitable for command-line and readiness boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum DbError {
    /// A `PostgreSQL` connection could not be established.
    #[error("database connection failed")]
    Connection,
    /// A database query failed.
    #[error("database query failed")]
    Query,
    /// The connected role is not authorized for the requested operation.
    #[error("database role is not authorized for this operation")]
    WrongRole,
    /// `SQLx` could not validate or apply the embedded migrations.
    #[error("database migration failed")]
    Migration,
    /// The database does not match the supported readiness contract.
    #[error("database readiness check failed")]
    Unready,
}
