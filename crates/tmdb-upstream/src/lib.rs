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
    DailyExportDownload, IMAGE_GALLERY_QUERY_STRING, MAX_DAILY_EXPORT_BYTES,
    MOVIE_DETAIL_QUERY_STRING, ResponseClass, TV_DETAIL_QUERY_STRING, TmdbClient, TmdbClientError,
    TrawlDecision, VIDEO_GALLERY_QUERY_STRING, classify_response,
    trawl_decision,
};
pub use model::{
    ChangeGroup, ChangeHistory, ChangeItem, ChangePage, ChangedId, DailyExportRecord,
    TmdbAlternateTitle, TmdbCollection, TmdbCompany, TmdbContentRating, TmdbContentRatings,
    TmdbCredit, TmdbCredits, TmdbEpisode, TmdbExternalIds, TmdbGenre, TmdbImage, TmdbImages,
    TmdbKeyword, TmdbMovie, TmdbNetwork, TmdbPerson, TmdbReleaseDate, TmdbReleaseDateCountry,
    TmdbReleaseDates, TmdbSeason, TmdbSeasonSummary, TmdbTranslation, TmdbTranslationData,
    TmdbTranslations, TmdbTrendingItem, TmdbTrendingPage, TmdbTv, TmdbVideo, TmdbVideos,
};
pub use parser::{
    DailyExportParser, ExportParseError, parse_change_history, parse_change_page,
    parse_daily_export,
};
pub use policy::{PolicyError, RateLimitPolicy, RetryPolicy};
