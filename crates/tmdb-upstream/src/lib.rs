//! Bounded, typed transport primitives for TMDB and its daily source files.
//!
//! The client deliberately owns retry, rate-limit, response-size, and challenge
//! classification policy. Callers receive sanitized errors and must decide how
//! to persist the returned models or whether an allowlisted Trawl fallback is
//! appropriate.

mod client;
mod model;
mod parser;
mod policy;

pub use client::{
    DailyExportDownload, MAX_DAILY_EXPORT_BYTES, ResponseClass, TmdbClient, TmdbClientError,
    TrawlDecision, classify_response, trawl_decision,
};
pub use model::{
    ChangeGroup, ChangeHistory, ChangeItem, ChangePage, ChangedId, DailyExportRecord,
    TmdbCollection, TmdbCompany, TmdbCredit, TmdbCredits, TmdbEpisode, TmdbGenre, TmdbKeyword,
    TmdbMovie, TmdbNetwork, TmdbPerson, TmdbSeason, TmdbSeasonSummary, TmdbTv,
};
pub use parser::{
    DailyExportParser, ExportParseError, parse_change_history, parse_change_page,
    parse_daily_export,
};
pub use policy::{PolicyError, RateLimitPolicy, RetryPolicy};
