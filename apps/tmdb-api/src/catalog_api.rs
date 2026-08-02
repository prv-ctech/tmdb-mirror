use std::{num::NonZeroU32, str::FromStr, sync::Arc};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Extension, Path, RawQuery},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use serde_json::{Value, json};
use tmdb_db::{
    AnimeScope, CatalogCredit, CatalogDetail, CatalogEpisode, CatalogError, CatalogFilters,
    CatalogImageAsset, CatalogPage, CatalogPerson, CatalogRecentPage, CatalogRepository,
    CatalogSeason, CatalogTitle, CatalogTopPage, CatalogTrend, PopularCursor, RecentCursor,
    TopCursor,
};
use tmdb_domain::{MediaType, TitleKey};

use crate::{RequestId, problem};

const DEFAULT_LIMIT: u16 = 20;
const MAX_LIMIT: u16 = 100;
const MAX_QUERY_BYTES: usize = 4_096;
const MAX_SEARCH_CHARS: usize = 256;

/// Errors at the public catalog boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CatalogApiError {
    /// A query parameter or path identifier violated the public contract.
    #[error("catalog query is invalid")]
    InvalidQuery,
    /// The catalog read dependency did not complete successfully.
    #[error("catalog dependency is unavailable")]
    Unavailable,
    /// A named ordering is reserved but its stable database ranking is not ready yet.
    #[error("catalog ordering is not implemented")]
    Unsupported,
}

/// Object-safe read boundary used by catalog handlers.
#[async_trait]
pub trait CatalogApiStore: Send + Sync + 'static {
    /// Reads one title by media namespace and TMDB identifier.
    async fn get_title(&self, key: TitleKey) -> Result<Option<CatalogTitle>, CatalogError>;

    /// Reads one title while enforcing the route's anime partition.
    async fn get_title_scoped(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<CatalogTitle>, CatalogError> {
        let title = self.get_title(key).await?;
        Ok(title.filter(|title| match anime_scope {
            AnimeScope::OnlyAnime => title.is_anime,
            AnimeScope::OnlyNonAnime => !title.is_anime,
        }))
    }

    /// Reads one scope-isolated detail row. Implementations can override this to include facets.
    async fn get_detail(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<CatalogDetail>, CatalogError> {
        self.get_title_scoped(key, anime_scope)
            .await
            .map(|title| title.map(detail_from_title))
    }

    /// Reads a deterministic popularity page in an explicitly selected scope.
    async fn list_popular(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<PopularCursor>,
    ) -> Result<CatalogPage, CatalogError>;

    /// Reads a top-rated page using a stable rating cursor.
    async fn list_top(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<TopCursor>,
    ) -> Result<CatalogTopPage, CatalogError>;

    /// Reads a date-ordered recent page.
    async fn list_recent(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<RecentCursor>,
    ) -> Result<CatalogRecentPage, CatalogError>;

    /// Searches an explicitly selected media and anime scope.
    async fn search(
        &self,
        term: &str,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError>;

    async fn list_popular_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<PopularCursor>,
    ) -> Result<CatalogPage, CatalogError> {
        if filters.is_empty() {
            self.list_popular(media_type, anime_scope, limit, after)
                .await
        } else {
            Err(CatalogError::InvalidInput)
        }
    }

    async fn list_top_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<TopCursor>,
    ) -> Result<CatalogTopPage, CatalogError> {
        if filters.is_empty() {
            self.list_top(media_type, anime_scope, limit, after).await
        } else {
            Err(CatalogError::InvalidInput)
        }
    }

    async fn list_recent_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<RecentCursor>,
    ) -> Result<CatalogRecentPage, CatalogError> {
        if filters.is_empty() {
            self.list_recent(media_type, anime_scope, limit, after)
                .await
        } else {
            Err(CatalogError::InvalidInput)
        }
    }

    async fn search_filtered(
        &self,
        term: &str,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        if filters.is_empty() {
            self.search(term, media_type, anime_scope, limit).await
        } else {
            Err(CatalogError::InvalidInput)
        }
    }

    async fn list_people(
        &self,
        _term: Option<&str>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<CatalogPerson>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_credits(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogCredit>>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_seasons(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogSeason>>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_episodes(
        &self,
        _key: TitleKey,
        _season_number: i32,
        _anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogEpisode>>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_images(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogImageAsset>>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_translations(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<Vec<tmdb_db::CatalogTranslation>>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_alternate_titles(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<Vec<tmdb_db::CatalogAlternateTitle>>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn external_ids(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<tmdb_db::CatalogExternalIds>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_videos(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<Vec<tmdb_db::CatalogVideo>>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_release_dates(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<Vec<tmdb_db::CatalogReleaseDate>>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_trending(
        &self,
        _trend_window: &str,
        _media_type: Option<MediaType>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<CatalogTrend>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_calendar(
        &self,
        _media_type: MediaType,
        _anime_scope: AnimeScope,
        _start: NaiveDate,
        _end: NaiveDate,
        _limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_company_entities(
        &self,
        _term: Option<&str>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogCompany>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_network_entities(
        &self,
        _term: Option<&str>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogNetwork>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_collections(
        &self,
        _term: Option<&str>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogCollection>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_genre_entities(
        &self,
        _term: Option<&str>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogGenre>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_keyword_entities(
        &self,
        _term: Option<&str>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogKeyword>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_tag_entities(
        &self,
        _term: Option<&str>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogTag>, CatalogError> {
        Err(CatalogError::Query)
    }
    async fn list_language_entities(
        &self,
        _term: Option<&str>,
        _anime_scope: AnimeScope,
        _limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogLanguage>, CatalogError> {
        Err(CatalogError::Query)
    }
}

#[async_trait]
impl CatalogApiStore for CatalogRepository {
    async fn get_title(&self, key: TitleKey) -> Result<Option<CatalogTitle>, CatalogError> {
        Self::get_title(self, key).await
    }

    async fn get_title_scoped(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<CatalogTitle>, CatalogError> {
        Self::get_title_scoped(self, key, anime_scope).await
    }

    async fn get_detail(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<CatalogDetail>, CatalogError> {
        Self::get_detail(self, key, anime_scope).await
    }

    async fn list_popular(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<PopularCursor>,
    ) -> Result<CatalogPage, CatalogError> {
        Self::list_popular(self, media_type, anime_scope, limit, after).await
    }

    async fn list_top(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<TopCursor>,
    ) -> Result<CatalogTopPage, CatalogError> {
        Self::list_top(self, media_type, anime_scope, limit, after).await
    }

    async fn list_recent(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<RecentCursor>,
    ) -> Result<CatalogRecentPage, CatalogError> {
        Self::list_recent(self, media_type, anime_scope, limit, after).await
    }

    async fn search(
        &self,
        term: &str,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        Self::search(self, term, media_type, anime_scope, limit).await
    }

    async fn list_popular_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<PopularCursor>,
    ) -> Result<CatalogPage, CatalogError> {
        Self::list_popular_filtered(self, media_type, anime_scope, filters, limit, after).await
    }

    async fn list_top_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<TopCursor>,
    ) -> Result<CatalogTopPage, CatalogError> {
        Self::list_top_filtered(self, media_type, anime_scope, filters, limit, after).await
    }

    async fn list_recent_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<RecentCursor>,
    ) -> Result<CatalogRecentPage, CatalogError> {
        Self::list_recent_filtered(self, media_type, anime_scope, filters, limit, after).await
    }

    async fn search_filtered(
        &self,
        term: &str,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        Self::search_filtered(self, term, media_type, anime_scope, filters, limit).await
    }

    async fn list_people(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogPerson>, CatalogError> {
        Self::list_people(self, term, anime_scope, limit).await
    }
    async fn list_credits(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogCredit>>, CatalogError> {
        Self::list_credits(self, key, anime_scope).await
    }
    async fn list_seasons(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogSeason>>, CatalogError> {
        Self::list_seasons(self, key, anime_scope).await
    }
    async fn list_episodes(
        &self,
        key: TitleKey,
        season_number: i32,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogEpisode>>, CatalogError> {
        Self::list_episodes(self, key, season_number, anime_scope).await
    }
    async fn list_images(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogImageAsset>>, CatalogError> {
        Self::list_images(self, key, anime_scope).await
    }
    async fn list_translations(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<tmdb_db::CatalogTranslation>>, CatalogError> {
        Self::list_translations(self, key, anime_scope).await
    }
    async fn list_alternate_titles(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<tmdb_db::CatalogAlternateTitle>>, CatalogError> {
        Self::list_alternate_titles(self, key, anime_scope).await
    }
    async fn external_ids(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<tmdb_db::CatalogExternalIds>, CatalogError> {
        Self::external_ids(self, key, anime_scope).await
    }
    async fn list_videos(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<tmdb_db::CatalogVideo>>, CatalogError> {
        Self::list_videos(self, key, anime_scope).await
    }
    async fn list_release_dates(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<tmdb_db::CatalogReleaseDate>>, CatalogError> {
        Self::list_release_dates(self, key, anime_scope).await
    }
    async fn list_trending(
        &self,
        trend_window: &str,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogTrend>, CatalogError> {
        Self::list_trending(self, trend_window, media_type, anime_scope, limit).await
    }
    async fn list_calendar(
        &self,
        media_type: MediaType,
        anime_scope: AnimeScope,
        start: NaiveDate,
        end: NaiveDate,
        limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        Self::list_calendar(self, media_type, anime_scope, start, end, limit).await
    }
    async fn list_company_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogCompany>, CatalogError> {
        Self::list_company_entities(self, term, anime_scope, limit).await
    }
    async fn list_network_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogNetwork>, CatalogError> {
        Self::list_network_entities(self, term, anime_scope, limit).await
    }
    async fn list_collections(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogCollection>, CatalogError> {
        Self::list_collections(self, term, anime_scope, limit).await
    }

    async fn list_genre_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogGenre>, CatalogError> {
        Self::list_genre_entities(self, term, anime_scope, limit).await
    }
    async fn list_keyword_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogKeyword>, CatalogError> {
        Self::list_keyword_entities(self, term, anime_scope, limit).await
    }
    async fn list_tag_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogTag>, CatalogError> {
        Self::list_tag_entities(self, term, anime_scope, limit).await
    }
    async fn list_language_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<tmdb_db::CatalogLanguage>, CatalogError> {
        Self::list_language_entities(self, term, anime_scope, limit).await
    }
}

#[derive(Clone)]
struct CatalogApiState {
    store: Arc<dyn CatalogApiStore>,
    allow_local_media: bool,
    media_base_url: Option<String>,
}

impl std::fmt::Debug for CatalogApiState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CatalogApiState")
            .field("allow_local_media", &self.allow_local_media)
            .field("media_base_url_configured", &self.media_base_url.is_some())
            .finish_non_exhaustive()
    }
}

/// Builds the public catalog routes over a repository implementation.
///
/// The returned router intentionally has no fallback or application state. It
/// can be merged into [`crate::build_router`] so health, request IDs, timeout,
/// panic conversion, and metrics middleware remain shared by every endpoint.
#[must_use = "merge the catalog routes into the application router"]
pub fn build_catalog_router(store: Arc<dyn CatalogApiStore>) -> Router {
    build_catalog_router_with_media(store, false, None)
}

/// Builds catalog routes with local-media URL behavior configured explicitly.
pub fn build_catalog_router_with_media(
    store: Arc<dyn CatalogApiStore>,
    allow_local_media: bool,
    media_base_url: Option<String>,
) -> Router {
    let state = CatalogApiState {
        store,
        allow_local_media,
        media_base_url,
    };
    let current = build_catalog_routes(state);
    let versioned = current.clone().route("/openapi.json", get(openapi));
    Router::new().merge(current).nest("/v1", versioned)
}

fn build_catalog_routes(state: CatalogApiState) -> Router {
    Router::new()
        // Keep reserved collection names before the `/{tmdb_id}` routes.
        .route("/movies/popular", get(list_movies))
        .route("/movies/recent", get(list_movies_recent))
        .route("/movies/top-rated", get(list_movies_top))
        .route("/movies", get(list_movies))
        .route("/movies/{tmdb_id}", get(get_movie))
        .route("/movies/{tmdb_id}/translations", get(movie_translations))
        .route(
            "/movies/{tmdb_id}/alternate-titles",
            get(movie_alternate_titles),
        )
        .route("/movies/{tmdb_id}/external-ids", get(movie_external_ids))
        .route("/movies/{tmdb_id}/videos", get(movie_videos))
        .route("/movies/{tmdb_id}/release-dates", get(movie_release_dates))
        .route("/tv/popular", get(list_tv))
        .route("/tv/recent", get(list_tv_recent))
        .route("/tv/top-rated", get(list_tv_top))
        .route("/tv", get(list_tv))
        .route("/tv/{tmdb_id}", get(get_tv))
        .route("/tv/{tmdb_id}/translations", get(tv_translations))
        .route("/tv/{tmdb_id}/alternate-titles", get(tv_alternate_titles))
        .route("/tv/{tmdb_id}/external-ids", get(tv_external_ids))
        .route("/tv/{tmdb_id}/videos", get(tv_videos))
        .route("/tv/{tmdb_id}/certifications", get(tv_release_dates))
        .route("/anime/popular", get(list_anime))
        .route("/anime/recent", get(list_anime_recent))
        .route("/anime/top-rated", get(list_anime_top))
        .route("/anime", get(list_anime))
        .route("/anime/{media_type}/{tmdb_id}", get(get_anime))
        .route(
            "/anime/{media_type}/{tmdb_id}/translations",
            get(anime_translations),
        )
        .route(
            "/anime/{media_type}/{tmdb_id}/alternate-titles",
            get(anime_alternate_titles),
        )
        .route(
            "/anime/{media_type}/{tmdb_id}/external-ids",
            get(anime_external_ids),
        )
        .route("/anime/{media_type}/{tmdb_id}/videos", get(anime_videos))
        .route(
            "/anime/{media_type}/{tmdb_id}/release-dates",
            get(anime_release_dates),
        )
        .route("/anime/{media_type}/{tmdb_id}/images", get(anime_images))
        .route("/trending/{trend_window}", get(list_trending))
        .route("/anime/trending/{trend_window}", get(list_anime_trending))
        .route("/calendar/movies", get(movie_calendar))
        .route("/calendar/tv", get(tv_calendar))
        .route("/search", get(search_titles))
        .route("/genres", get(list_genres))
        .route("/languages", get(list_languages))
        .route("/keywords", get(list_keywords))
        .route("/tags", get(list_tags))
        .route("/people", get(list_people))
        .route("/companies", get(list_companies))
        .route("/networks", get(list_networks))
        .route("/collections", get(list_collections))
        .route("/movies/{tmdb_id}/credits", get(movie_credits))
        .route("/tv/{tmdb_id}/credits", get(tv_credits))
        .route("/movies/{tmdb_id}/images", get(movie_images))
        .route("/tv/{tmdb_id}/images", get(tv_images))
        .route("/tv/{tmdb_id}/seasons", get(tv_seasons))
        .route("/tv/{tmdb_id}/seasons/{season_number}", get(tv_season))
        .route(
            "/tv/{tmdb_id}/seasons/{season_number}/episodes",
            get(tv_episodes),
        )
        .route(
            "/tv/{tmdb_id}/seasons/{season_number}/episodes/{episode_number}",
            get(tv_episode),
        )
        .layer(Extension(state))
}

#[allow(
    clippy::too_many_lines,
    reason = "the public OpenAPI document is intentionally a single auditable static route declaration"
)]
async fn openapi() -> axum::Json<Value> {
    // This is generated in-process so the published document is kept beside
    // the route registration. The unversioned endpoints remain compatible;
    // this document intentionally lists only their stable `/v1` aliases.
    let routes = [
        ("/v1/openapi.json", "OpenAPI document"),
        ("/v1/health/live", "Process liveness"),
        ("/v1/health/ready", "Database and schema readiness"),
        ("/v1/movies", "List non-anime movies"),
        ("/v1/movies/popular", "List popular non-anime movies"),
        ("/v1/movies/recent", "List recent non-anime movies"),
        ("/v1/movies/top-rated", "List top-rated non-anime movies"),
        ("/v1/movies/{tmdb_id}", "Get non-anime movie metadata"),
        (
            "/v1/movies/{tmdb_id}/translations",
            "Get movie translations",
        ),
        (
            "/v1/movies/{tmdb_id}/alternate-titles",
            "Get movie alternate titles",
        ),
        (
            "/v1/movies/{tmdb_id}/external-ids",
            "Get movie external identifiers",
        ),
        ("/v1/movies/{tmdb_id}/videos", "Get movie videos"),
        (
            "/v1/movies/{tmdb_id}/release-dates",
            "Get movie regional release dates and certifications",
        ),
        ("/v1/movies/{tmdb_id}/credits", "Get movie credits"),
        ("/v1/movies/{tmdb_id}/images", "Get movie image metadata"),
        ("/v1/tv", "List non-anime TV series"),
        ("/v1/tv/popular", "List popular non-anime TV series"),
        ("/v1/tv/recent", "List recent non-anime TV series"),
        ("/v1/tv/top-rated", "List top-rated non-anime TV series"),
        ("/v1/tv/{tmdb_id}", "Get non-anime TV metadata"),
        ("/v1/tv/{tmdb_id}/translations", "Get TV translations"),
        (
            "/v1/tv/{tmdb_id}/alternate-titles",
            "Get TV alternate titles",
        ),
        (
            "/v1/tv/{tmdb_id}/external-ids",
            "Get TV external identifiers",
        ),
        ("/v1/tv/{tmdb_id}/videos", "Get TV videos"),
        (
            "/v1/tv/{tmdb_id}/certifications",
            "Get TV regional certifications",
        ),
        ("/v1/tv/{tmdb_id}/credits", "Get TV credits"),
        ("/v1/tv/{tmdb_id}/images", "Get TV image metadata"),
        ("/v1/tv/{tmdb_id}/seasons", "List TV seasons"),
        (
            "/v1/tv/{tmdb_id}/seasons/{season_number}",
            "Get a TV season",
        ),
        (
            "/v1/tv/{tmdb_id}/seasons/{season_number}/episodes",
            "List season episodes",
        ),
        (
            "/v1/tv/{tmdb_id}/seasons/{season_number}/episodes/{episode_number}",
            "Get a TV episode",
        ),
        ("/v1/anime", "List isolated anime movies and TV series"),
        ("/v1/anime/popular", "List popular isolated anime"),
        ("/v1/anime/recent", "List recent isolated anime"),
        ("/v1/anime/top-rated", "List top-rated isolated anime"),
        (
            "/v1/anime/{media_type}/{tmdb_id}",
            "Get isolated anime metadata",
        ),
        (
            "/v1/anime/{media_type}/{tmdb_id}/translations",
            "Get anime translations",
        ),
        (
            "/v1/anime/{media_type}/{tmdb_id}/alternate-titles",
            "Get anime alternate titles",
        ),
        (
            "/v1/anime/{media_type}/{tmdb_id}/external-ids",
            "Get anime external identifiers",
        ),
        (
            "/v1/anime/{media_type}/{tmdb_id}/videos",
            "Get anime videos",
        ),
        (
            "/v1/anime/{media_type}/{tmdb_id}/release-dates",
            "Get anime regional release dates and certifications",
        ),
        (
            "/v1/anime/{media_type}/{tmdb_id}/credits",
            "Get anime credits",
        ),
        (
            "/v1/anime/{media_type}/{tmdb_id}/images",
            "Get anime image metadata",
        ),
        (
            "/v1/trending/{trend_window}",
            "List non-anime trending titles",
        ),
        (
            "/v1/anime/trending/{trend_window}",
            "List isolated anime trending titles",
        ),
        (
            "/v1/calendar/movies",
            "List upcoming/recent movie calendar entries",
        ),
        (
            "/v1/calendar/tv",
            "List upcoming/recent TV calendar entries",
        ),
        ("/v1/search", "Search non-anime titles"),
        ("/v1/genres", "List catalog genres"),
        ("/v1/languages", "List catalog languages"),
        ("/v1/keywords", "List catalog keywords"),
        ("/v1/tags", "List catalog tags"),
        ("/v1/people", "List cast and crew people"),
        ("/v1/companies", "List production companies"),
        ("/v1/networks", "List TV networks"),
        ("/v1/collections", "List movie collections"),
    ];
    let mut paths = serde_json::Map::with_capacity(routes.len());
    for (path, summary) in routes {
        paths.insert(
            path.to_owned(),
            json!({
                "get": {
                    "summary": summary,
                    "responses": {
                        "200": {"description": "Successful response"},
                        "400": {"$ref": "#/components/responses/Problem"},
                        "404": {"$ref": "#/components/responses/Problem"},
                        "503": {"$ref": "#/components/responses/Problem"}
                    }
                }
            }),
        );
    }
    axum::Json(json!({
        "openapi": "3.1.0",
        "info": {
            "title": "TMDB Mirror Catalog API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "Unversioned catalog paths remain compatible aliases. Movie and TV routes exclude anime; anime routes search anime movies and TV series together unless media_type is specified."
        },
        "paths": paths,
        "components": {
            "responses": {
                "Problem": {
                    "description": "RFC 9457 problem response",
                    "content": {"application/problem+json": {"schema": {"$ref": "#/components/schemas/Problem"}}}
                }
            },
            "schemas": {
                "Problem": {
                    "type": "object",
                    "required": ["type", "title", "status"],
                    "properties": {
                        "type": {"type": "string"},
                        "title": {"type": "string"},
                        "status": {"type": "integer"},
                        "detail": {"type": "string"}
                    }
                }
            }
        }
    }))
}

#[derive(Clone, Debug)]
struct CatalogQuery {
    limit: u16,
    cursor: Option<CatalogCursor>,
    term: Option<String>,
    media_type: Option<MediaType>,
    anime: Option<bool>,
    filters: CatalogFilters,
}

#[derive(Clone, Debug)]
enum CatalogCursor {
    Popular(PopularCursor),
    Recent(RecentCursor),
    Top(TopCursor),
}

impl Default for CatalogQuery {
    fn default() -> Self {
        Self {
            limit: DEFAULT_LIMIT,
            cursor: None,
            term: None,
            media_type: None,
            anime: None,
            filters: CatalogFilters::default(),
        }
    }
}

#[allow(clippy::too_many_lines)]
fn parse_query(raw_query: Option<&str>) -> Result<CatalogQuery, CatalogApiError> {
    let Some(raw_query) = raw_query else {
        return Ok(CatalogQuery::default());
    };
    if raw_query.len() > MAX_QUERY_BYTES {
        return Err(CatalogApiError::InvalidQuery);
    }

    let mut query = CatalogQuery::default();
    let mut seen_limit = false;
    let mut seen_cursor = false;
    let mut seen_term = false;
    let mut seen_media_type = false;
    let mut seen_anime = false;
    let mut seen_genre = false;
    let mut seen_keyword = false;
    let mut seen_tag = false;
    let mut seen_language = false;
    let mut seen_runtime_min = false;
    let mut seen_runtime_max = false;
    let mut seen_person = false;
    let mut seen_company = false;
    let mut seen_network = false;
    let mut seen_year = false;
    let mut seen_status = false;
    for component in raw_query
        .split('&')
        .filter(|component| !component.is_empty())
    {
        let (raw_key, raw_value) = component.split_once('=').unwrap_or((component, ""));
        let key = decode_query_component(raw_key)?;
        let value = decode_query_component(raw_value)?;
        match key.as_str() {
            "limit" => {
                if seen_limit {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_limit = true;
                query.limit = value
                    .parse::<u16>()
                    .ok()
                    .filter(|limit| (1..=MAX_LIMIT).contains(limit))
                    .ok_or(CatalogApiError::InvalidQuery)?;
            }
            "cursor" => {
                if seen_cursor {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_cursor = true;
                query.cursor = Some(parse_cursor(&value)?);
            }
            "q" => {
                if seen_term {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_term = true;
                let term = value.trim();
                if term.is_empty()
                    || term.chars().count() > MAX_SEARCH_CHARS
                    || term.chars().any(char::is_control)
                {
                    return Err(CatalogApiError::InvalidQuery);
                }
                query.term = Some(term.to_owned());
            }
            "type" | "mediaType" => {
                if seen_media_type {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_media_type = true;
                query.media_type =
                    Some(MediaType::from_str(&value).map_err(|_| CatalogApiError::InvalidQuery)?);
            }
            "anime" => {
                if seen_anime {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_anime = true;
                query.anime = Some(match value.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(CatalogApiError::InvalidQuery),
                });
            }
            "genre" | "genreId" => {
                if seen_genre {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_genre = true;
                query.filters.genre_id = Some(parse_positive_filter_id(&value)?);
            }
            "keyword" | "keywordId" => {
                if seen_keyword {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_keyword = true;
                query.filters.keyword_id = Some(parse_positive_filter_id(&value)?);
            }
            "tag" | "tagId" => {
                if seen_tag {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_tag = true;
                query.filters.tag_id = Some(parse_positive_filter_id(&value)?);
            }
            "language" | "lang" => {
                if seen_language {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_language = true;
                let language = value.trim().to_ascii_lowercase();
                if !(2..=16).contains(&language.len()) || !language.is_ascii() {
                    return Err(CatalogApiError::InvalidQuery);
                }
                query.filters.language = Some(language);
            }
            "runtimeMin" | "lengthMin" => {
                if seen_runtime_min {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_runtime_min = true;
                query.filters.runtime_min = Some(parse_runtime_filter(&value)?);
            }
            "runtimeMax" | "lengthMax" => {
                if seen_runtime_max {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_runtime_max = true;
                query.filters.runtime_max = Some(parse_runtime_filter(&value)?);
            }
            "person" | "personId" | "actor" | "actorId" => {
                if seen_person {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_person = true;
                query.filters.person_id = Some(parse_positive_filter_id(&value)?);
            }
            "company" | "companyId" | "studio" | "studioId" => {
                if seen_company {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_company = true;
                query.filters.company_id = Some(parse_positive_filter_id(&value)?);
            }
            "network" | "networkId" => {
                if seen_network {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_network = true;
                query.filters.network_id = Some(parse_positive_filter_id(&value)?);
            }
            "year" => {
                if seen_year {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_year = true;
                query.filters.year = Some(
                    value
                        .parse::<i32>()
                        .ok()
                        .filter(|year| (1800..=2200).contains(year))
                        .ok_or(CatalogApiError::InvalidQuery)?,
                );
            }
            "status" => {
                if seen_status {
                    return Err(CatalogApiError::InvalidQuery);
                }
                seen_status = true;
                let status = value.trim();
                if status.is_empty() || status.len() > 64 || status.chars().any(char::is_control) {
                    return Err(CatalogApiError::InvalidQuery);
                }
                query.filters.status = Some(status.to_owned());
            }
            _ => return Err(CatalogApiError::InvalidQuery),
        }
    }
    Ok(query)
}

fn parse_positive_filter_id(value: &str) -> Result<i64, CatalogApiError> {
    value
        .parse::<i64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or(CatalogApiError::InvalidQuery)
}

fn parse_runtime_filter(value: &str) -> Result<i32, CatalogApiError> {
    value
        .parse::<i32>()
        .ok()
        .filter(|value| (0..=10_000).contains(value))
        .ok_or(CatalogApiError::InvalidQuery)
}

fn decode_query_component(value: &str) -> Result<String, CatalogApiError> {
    let mut bytes = Vec::with_capacity(value.len());
    let mut characters = value.as_bytes().iter().copied();
    while let Some(byte) = characters.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = characters
                    .next()
                    .and_then(hex_value)
                    .ok_or(CatalogApiError::InvalidQuery)?;
                let low = characters
                    .next()
                    .and_then(hex_value)
                    .ok_or(CatalogApiError::InvalidQuery)?;
                bytes.push((high << 4) | low);
            }
            _ => bytes.push(byte),
        }
    }
    String::from_utf8(bytes).map_err(|_| CatalogApiError::InvalidQuery)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn parse_cursor(value: &str) -> Result<CatalogCursor, CatalogApiError> {
    let components = value.split(':').collect::<Vec<_>>();
    if components.len() == 3 {
        let vote_average = components[0]
            .parse::<f64>()
            .map_err(|_| CatalogApiError::InvalidQuery)?;
        let vote_count = components[1]
            .parse::<i64>()
            .map_err(|_| CatalogApiError::InvalidQuery)?;
        let title_id = components[2]
            .parse::<i64>()
            .map_err(|_| CatalogApiError::InvalidQuery)?;
        return TopCursor::try_new(vote_average, vote_count, title_id)
            .map(CatalogCursor::Top)
            .map_err(|_| CatalogApiError::InvalidQuery);
    }
    if components.len() != 2 {
        return Err(CatalogApiError::InvalidQuery);
    }
    let [ordering, raw_title_id] = components.as_slice() else {
        return Err(CatalogApiError::InvalidQuery);
    };
    let title_id = raw_title_id
        .parse::<i64>()
        .map_err(|_| CatalogApiError::InvalidQuery)?;
    if let Ok(popularity) = ordering.parse::<f64>()
        && let Ok(cursor) = PopularCursor::try_new(popularity, title_id)
    {
        return Ok(CatalogCursor::Popular(cursor));
    }
    let date = NaiveDate::parse_from_str(ordering, "%Y-%m-%d")
        .map_err(|_| CatalogApiError::InvalidQuery)?;
    RecentCursor::try_new(date, title_id)
        .map(CatalogCursor::Recent)
        .map_err(|_| CatalogApiError::InvalidQuery)
}

fn popular_cursor(query: &CatalogQuery) -> Result<Option<PopularCursor>, CatalogApiError> {
    match query.cursor.as_ref() {
        None => Ok(None),
        Some(CatalogCursor::Popular(cursor)) => Ok(Some(*cursor)),
        Some(CatalogCursor::Recent(_) | CatalogCursor::Top(_)) => {
            Err(CatalogApiError::InvalidQuery)
        }
    }
}

fn recent_cursor(query: &CatalogQuery) -> Result<Option<RecentCursor>, CatalogApiError> {
    match query.cursor.as_ref() {
        None => Ok(None),
        Some(CatalogCursor::Recent(cursor)) => Ok(Some(*cursor)),
        Some(CatalogCursor::Popular(_) | CatalogCursor::Top(_)) => {
            Err(CatalogApiError::InvalidQuery)
        }
    }
}

fn top_cursor(query: &CatalogQuery) -> Result<Option<TopCursor>, CatalogApiError> {
    match query.cursor.as_ref() {
        None => Ok(None),
        Some(CatalogCursor::Top(cursor)) => Ok(Some(*cursor)),
        Some(CatalogCursor::Popular(_) | CatalogCursor::Recent(_)) => {
            Err(CatalogApiError::InvalidQuery)
        }
    }
}

fn list_query(query: &CatalogQuery) -> Result<(), CatalogApiError> {
    if query.term.is_some() || query.anime.is_some() || query.media_type.is_some() {
        return Err(CatalogApiError::InvalidQuery);
    }
    Ok(())
}

fn anime_query(query: &CatalogQuery) -> Result<(), CatalogApiError> {
    if query.anime.is_some() || (query.term.is_some() && query.cursor.is_some()) {
        return Err(CatalogApiError::InvalidQuery);
    }
    Ok(())
}

fn search_query(query: &CatalogQuery) -> Result<&str, CatalogApiError> {
    if query.anime.is_some() || query.cursor.is_some() {
        return Err(CatalogApiError::InvalidQuery);
    }
    query.term.as_deref().ok_or(CatalogApiError::InvalidQuery)
}

async fn list_trending(
    Extension(state): Extension<CatalogApiState>,
    Path(trend_window): Path<String>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    trending_response(
        state,
        trend_window,
        raw_query,
        request_id,
        AnimeScope::OnlyNonAnime,
    )
    .await
}

async fn list_anime_trending(
    Extension(state): Extension<CatalogApiState>,
    Path(trend_window): Path<String>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    trending_response(
        state,
        trend_window,
        raw_query,
        request_id,
        AnimeScope::OnlyAnime,
    )
    .await
}

async fn trending_response(
    state: CatalogApiState,
    trend_window: String,
    raw_query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    anime_scope: AnimeScope,
) -> Response {
    let request_id = request_id_string(request_id);
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query)
            if query.cursor.is_none()
                && query.term.is_none()
                && query.anime.is_none()
                && query.filters.is_empty() =>
        {
            query
        }
        Ok(_) | Err(_) => return error_response(CatalogApiError::InvalidQuery, &request_id),
    };
    match state
        .store
        .list_trending(&trend_window, query.media_type, anime_scope, query.limit)
        .await
    {
        Ok(data) => (StatusCode::OK, Json(serde_json::json!({"data": data}))).into_response(),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn movie_calendar(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    calendar_response(state, raw_query, request_id, MediaType::Movie).await
}

async fn tv_calendar(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    calendar_response(state, raw_query, request_id, MediaType::Tv).await
}

async fn calendar_response(
    state: CatalogApiState,
    raw_query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    media_type: MediaType,
) -> Response {
    let request_id = request_id_string(request_id);
    let query = match parse_calendar_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(error) => return error_response(error, &request_id),
    };
    match state
        .store
        .list_calendar(
            media_type,
            AnimeScope::OnlyNonAnime,
            query.start,
            query.end,
            query.limit,
        )
        .await
    {
        Ok(data) => items_response(data),
        Err(error) => store_error_response(error, &request_id),
    }
}

#[derive(Clone, Copy, Debug)]
struct CalendarQuery {
    start: NaiveDate,
    end: NaiveDate,
    limit: u16,
}

fn parse_calendar_query(raw_query: Option<&str>) -> Result<CalendarQuery, CatalogApiError> {
    let raw_query = raw_query.ok_or(CatalogApiError::InvalidQuery)?;
    if raw_query.is_empty() || raw_query.len() > MAX_QUERY_BYTES {
        return Err(CatalogApiError::InvalidQuery);
    }
    let mut start = None;
    let mut end = None;
    let mut limit = DEFAULT_LIMIT;
    let mut seen_limit = false;
    for component in raw_query
        .split('&')
        .filter(|component| !component.is_empty())
    {
        let (raw_key, raw_value) = component.split_once('=').unwrap_or((component, ""));
        let key = decode_query_component(raw_key)?;
        let value = decode_query_component(raw_value)?;
        match key.as_str() {
            "start" if start.is_none() => {
                start = Some(
                    NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                        .map_err(|_| CatalogApiError::InvalidQuery)?,
                );
            }
            "end" if end.is_none() => {
                end = Some(
                    NaiveDate::parse_from_str(&value, "%Y-%m-%d")
                        .map_err(|_| CatalogApiError::InvalidQuery)?,
                );
            }
            "limit" if !seen_limit => {
                seen_limit = true;
                limit = value
                    .parse::<u16>()
                    .ok()
                    .filter(|value| (1..=MAX_LIMIT).contains(value))
                    .ok_or(CatalogApiError::InvalidQuery)?;
            }
            _ => return Err(CatalogApiError::InvalidQuery),
        }
    }
    let (Some(start), Some(end)) = (start, end) else {
        return Err(CatalogApiError::InvalidQuery);
    };
    if start > end || end.signed_duration_since(start).num_days() > 366 {
        return Err(CatalogApiError::InvalidQuery);
    }
    Ok(CalendarQuery { start, end, limit })
}

async fn list_movies(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_fixed_media_order(
        state,
        raw_query,
        request_id,
        MediaType::Movie,
        ListOrder::Popular,
    )
    .await
}

async fn list_tv(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_fixed_media_order(
        state,
        raw_query,
        request_id,
        MediaType::Tv,
        ListOrder::Popular,
    )
    .await
}

async fn list_movies_top(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_fixed_media_order(
        state,
        raw_query,
        request_id,
        MediaType::Movie,
        ListOrder::Top,
    )
    .await
}

async fn list_tv_top(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_fixed_media_order(state, raw_query, request_id, MediaType::Tv, ListOrder::Top).await
}

async fn list_movies_recent(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_fixed_media_order(
        state,
        raw_query,
        request_id,
        MediaType::Movie,
        ListOrder::Recent,
    )
    .await
}

async fn list_tv_recent(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_fixed_media_order(
        state,
        raw_query,
        request_id,
        MediaType::Tv,
        ListOrder::Recent,
    )
    .await
}

#[derive(Clone, Copy, Debug)]
enum ListOrder {
    Popular,
    Top,
    Recent,
}

#[allow(clippy::too_many_lines)]
async fn list_fixed_media_order(
    state: CatalogApiState,
    raw_query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    media_type: MediaType,
    order: ListOrder,
) -> Response {
    let request_id = request_id_string(request_id);
    let query = match parse_query(raw_query.as_deref())
        .and_then(|query| list_query(&query).map(|()| query))
    {
        Ok(query) => query,
        Err(error) => return error_response(error, &request_id),
    };
    match order {
        ListOrder::Top => {
            let cursor = match top_cursor(&query) {
                Ok(cursor) => cursor,
                Err(error) => return error_response(error, &request_id),
            };
            let result = if query.filters.is_empty() {
                state
                    .store
                    .list_top(
                        Some(media_type),
                        AnimeScope::OnlyNonAnime,
                        query.limit,
                        cursor,
                    )
                    .await
            } else {
                state
                    .store
                    .list_top_filtered(
                        Some(media_type),
                        AnimeScope::OnlyNonAnime,
                        &query.filters,
                        query.limit,
                        cursor,
                    )
                    .await
            };
            match result {
                Ok(page) => top_page_response(page),
                Err(error) => store_error_response(error, &request_id),
            }
        }
        ListOrder::Popular => {
            let cursor = match popular_cursor(&query) {
                Ok(cursor) => cursor,
                Err(error) => return error_response(error, &request_id),
            };
            let result = if query.filters.is_empty() {
                state
                    .store
                    .list_popular(
                        Some(media_type),
                        AnimeScope::OnlyNonAnime,
                        query.limit,
                        cursor,
                    )
                    .await
            } else {
                state
                    .store
                    .list_popular_filtered(
                        Some(media_type),
                        AnimeScope::OnlyNonAnime,
                        &query.filters,
                        query.limit,
                        cursor,
                    )
                    .await
            };
            match result {
                Ok(page) => page_response(page),
                Err(error) => store_error_response(error, &request_id),
            }
        }
        ListOrder::Recent => {
            let cursor = match recent_cursor(&query) {
                Ok(cursor) => cursor,
                Err(error) => return error_response(error, &request_id),
            };
            let result = if query.filters.is_empty() {
                state
                    .store
                    .list_recent(
                        Some(media_type),
                        AnimeScope::OnlyNonAnime,
                        query.limit,
                        cursor,
                    )
                    .await
            } else {
                state
                    .store
                    .list_recent_filtered(
                        Some(media_type),
                        AnimeScope::OnlyNonAnime,
                        &query.filters,
                        query.limit,
                        cursor,
                    )
                    .await
            };
            match result {
                Ok(page) => recent_page_response(page),
                Err(error) => store_error_response(error, &request_id),
            }
        }
    }
}

async fn list_anime(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_anime_order(state, raw_query, request_id, ListOrder::Popular).await
}

async fn list_anime_top(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_anime_order(state, raw_query, request_id, ListOrder::Top).await
}

async fn list_anime_recent(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_anime_order(state, raw_query, request_id, ListOrder::Recent).await
}

#[allow(clippy::too_many_lines)]
async fn list_anime_order(
    state: CatalogApiState,
    raw_query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    order: ListOrder,
) -> Response {
    let request_id = request_id_string(request_id);
    let query = match parse_query(raw_query.as_deref())
        .and_then(|query| anime_query(&query).map(|()| query))
    {
        Ok(query) => query,
        Err(error) => return error_response(error, &request_id),
    };
    if matches!(order, ListOrder::Popular) {
        if let Some(term) = query.term.as_deref() {
            let result = if query.filters.is_empty() {
                state
                    .store
                    .search(term, query.media_type, AnimeScope::OnlyAnime, query.limit)
                    .await
            } else {
                state
                    .store
                    .search_filtered(
                        term,
                        query.media_type,
                        AnimeScope::OnlyAnime,
                        &query.filters,
                        query.limit,
                    )
                    .await
            };
            return match result {
                Ok(items) => items_response(items),
                Err(error) => store_error_response(error, &request_id),
            };
        }
    } else if query.term.is_some() {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    match order {
        ListOrder::Top => {
            let cursor = match top_cursor(&query) {
                Ok(cursor) => cursor,
                Err(error) => return error_response(error, &request_id),
            };
            let result = if query.filters.is_empty() {
                state
                    .store
                    .list_top(query.media_type, AnimeScope::OnlyAnime, query.limit, cursor)
                    .await
            } else {
                state
                    .store
                    .list_top_filtered(
                        query.media_type,
                        AnimeScope::OnlyAnime,
                        &query.filters,
                        query.limit,
                        cursor,
                    )
                    .await
            };
            match result {
                Ok(page) => top_page_response(page),
                Err(error) => store_error_response(error, &request_id),
            }
        }
        ListOrder::Popular => {
            let cursor = match popular_cursor(&query) {
                Ok(cursor) => cursor,
                Err(error) => return error_response(error, &request_id),
            };
            let result = if query.filters.is_empty() {
                state
                    .store
                    .list_popular(query.media_type, AnimeScope::OnlyAnime, query.limit, cursor)
                    .await
            } else {
                state
                    .store
                    .list_popular_filtered(
                        query.media_type,
                        AnimeScope::OnlyAnime,
                        &query.filters,
                        query.limit,
                        cursor,
                    )
                    .await
            };
            match result {
                Ok(page) => page_response(page),
                Err(error) => store_error_response(error, &request_id),
            }
        }
        ListOrder::Recent => {
            let cursor = match recent_cursor(&query) {
                Ok(cursor) => cursor,
                Err(error) => return error_response(error, &request_id),
            };
            let result = if query.filters.is_empty() {
                state
                    .store
                    .list_recent(query.media_type, AnimeScope::OnlyAnime, query.limit, cursor)
                    .await
            } else {
                state
                    .store
                    .list_recent_filtered(
                        query.media_type,
                        AnimeScope::OnlyAnime,
                        &query.filters,
                        query.limit,
                        cursor,
                    )
                    .await
            };
            match result {
                Ok(page) => recent_page_response(page),
                Err(error) => store_error_response(error, &request_id),
            }
        }
    }
}

async fn search_titles(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let request_id = request_id_string(request_id);
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(error) => return error_response(error, &request_id),
    };
    let term = match search_query(&query) {
        Ok(term) => term,
        Err(error) => return error_response(error, &request_id),
    };
    let result = if query.filters.is_empty() {
        state
            .store
            .search(
                term,
                query.media_type,
                AnimeScope::OnlyNonAnime,
                query.limit,
            )
            .await
    } else {
        state
            .store
            .search_filtered(
                term,
                query.media_type,
                AnimeScope::OnlyNonAnime,
                &query.filters,
                query.limit,
            )
            .await
    };
    match result {
        Ok(items) => items_response(items),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn get_movie(
    Extension(state): Extension<CatalogApiState>,
    Path(tmdb_id): Path<String>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    get_fixed_media(state, tmdb_id, raw_query, request_id, MediaType::Movie).await
}

async fn get_tv(
    Extension(state): Extension<CatalogApiState>,
    Path(tmdb_id): Path<String>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    get_fixed_media(state, tmdb_id, raw_query, request_id, MediaType::Tv).await
}

async fn get_anime(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_media_type, raw_id)): Path<(String, String)>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let request_id = request_id_string(request_id);
    if raw_query.is_some_and(|query| !query.is_empty()) {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let Ok(media_type) = MediaType::from_str(&raw_media_type) else {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    };
    let tmdb_id = match parse_tmdb_id(&raw_id) {
        Ok(id) => id,
        Err(error) => return error_response(error, &request_id),
    };
    match state
        .store
        .get_detail(TitleKey::new(media_type, tmdb_id), AnimeScope::OnlyAnime)
        .await
    {
        Ok(Some(detail)) if detail.title.media_type == media_type && detail.title.is_anime => {
            detail_response(detail)
        }
        Ok(Some(_) | None) => not_found_response(&request_id),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn get_fixed_media(
    state: CatalogApiState,
    raw_id: String,
    raw_query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    media_type: MediaType,
) -> Response {
    let request_id = request_id_string(request_id);
    if raw_query.is_some_and(|query| !query.is_empty()) {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let tmdb_id = match parse_tmdb_id(&raw_id) {
        Ok(id) => id,
        Err(error) => return error_response(error, &request_id),
    };
    let key = TitleKey::new(media_type, tmdb_id);
    match state.store.get_detail(key, AnimeScope::OnlyNonAnime).await {
        Ok(Some(detail)) if detail.title.media_type == media_type && !detail.title.is_anime => {
            detail_response(detail)
        }
        Ok(Some(_) | None) => not_found_response(&request_id),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn list_people(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let request_id = request_id_string(request_id);
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(error) => return error_response(error, &request_id),
    };
    if query.cursor.is_some() || query.media_type.is_some() || !query.filters.is_empty() {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let scope = query.anime.map_or(AnimeScope::OnlyNonAnime, |anime| {
        if anime {
            AnimeScope::OnlyAnime
        } else {
            AnimeScope::OnlyNonAnime
        }
    });
    match state
        .store
        .list_people(query.term.as_deref(), scope, query.limit)
        .await
    {
        Ok(items) => (StatusCode::OK, Json(serde_json::json!({ "data": items }))).into_response(),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn list_genres(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_dimension(raw_query, request_id, state, DimensionKind::Genres).await
}

async fn list_keywords(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_dimension(raw_query, request_id, state, DimensionKind::Keywords).await
}

async fn list_tags(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_dimension(raw_query, request_id, state, DimensionKind::Tags).await
}

async fn list_languages(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_dimension(raw_query, request_id, state, DimensionKind::Languages).await
}

async fn list_companies(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_dimension(raw_query, request_id, state, DimensionKind::Companies).await
}

async fn list_networks(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_dimension(raw_query, request_id, state, DimensionKind::Networks).await
}
async fn list_collections(
    Extension(state): Extension<CatalogApiState>,
    RawQuery(raw_query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    list_dimension(raw_query, request_id, state, DimensionKind::Collections).await
}

#[derive(Clone, Copy)]
enum DimensionKind {
    Genres,
    Languages,
    Keywords,
    Tags,
    Companies,
    Networks,
    Collections,
}

#[allow(clippy::too_many_lines)]
async fn list_dimension(
    raw_query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    state: CatalogApiState,
    kind: DimensionKind,
) -> Response {
    let request_id = request_id_string(request_id);
    let query = match parse_query(raw_query.as_deref()) {
        Ok(query) => query,
        Err(error) => return error_response(error, &request_id),
    };
    if query.cursor.is_some() || query.media_type.is_some() || !query.filters.is_empty() {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let scope = query.anime.map_or(AnimeScope::OnlyNonAnime, |anime| {
        if anime {
            AnimeScope::OnlyAnime
        } else {
            AnimeScope::OnlyNonAnime
        }
    });
    let result = match kind {
        DimensionKind::Genres => state
            .store
            .list_genre_entities(query.term.as_deref(), scope, query.limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| serde_json::json!({ "id": item.id, "name": item.name }))
                    .collect::<Vec<_>>()
            }),
        DimensionKind::Languages => state
            .store
            .list_language_entities(query.term.as_deref(), scope, query.limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| {
                        serde_json::json!({
                            "iso6391": item.iso_639_1,
                            "englishName": item.english_name,
                            "name": item.name,
                            "isOriginal": item.is_original,
                        })
                    })
                    .collect::<Vec<_>>()
            }),
        DimensionKind::Keywords => state
            .store
            .list_keyword_entities(query.term.as_deref(), scope, query.limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| serde_json::json!({ "id": item.id, "name": item.name }))
                    .collect::<Vec<_>>()
            }),
        DimensionKind::Tags => state
            .store
            .list_tag_entities(query.term.as_deref(), scope, query.limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| serde_json::json!({ "id": item.id, "name": item.name }))
                    .collect::<Vec<_>>()
            }),
        DimensionKind::Companies => state
            .store
            .list_company_entities(query.term.as_deref(), scope, query.limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| {
                        serde_json::json!({
                            "id": item.id,
                            "name": item.name,
                            "originCountry": item.origin_country,
                            "logoPath": item.logo_path,
                            "companyRole": item.company_role,
                        })
                    })
                    .collect::<Vec<_>>()
            }),
        DimensionKind::Networks => state
            .store
            .list_network_entities(query.term.as_deref(), scope, query.limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| {
                        serde_json::json!({
                            "id": item.id,
                            "name": item.name,
                            "originCountry": item.origin_country,
                            "logoPath": item.logo_path,
                        })
                    })
                    .collect::<Vec<_>>()
            }),
        DimensionKind::Collections => state
            .store
            .list_collections(query.term.as_deref(), scope, query.limit)
            .await
            .map(|items| {
                items
                    .into_iter()
                    .map(|item| serde_json::to_value(item).unwrap_or_default())
                    .collect::<Vec<_>>()
            }),
    };
    match result {
        Ok(data) => (StatusCode::OK, Json(serde_json::json!({"data": data}))).into_response(),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn scoped_resource(
    state: CatalogApiState,
    raw_id: String,
    raw_query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    media_type: MediaType,
    kind: ResourceKind,
    anime_scope: AnimeScope,
) -> Response {
    let request_id = request_id_string(request_id);
    if raw_query.is_some_and(|query| !query.is_empty()) {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let tmdb_id = match parse_tmdb_id(&raw_id) {
        Ok(id) => id,
        Err(error) => return error_response(error, &request_id),
    };
    let key = TitleKey::new(media_type, tmdb_id);
    let result = match kind {
        ResourceKind::Credits => state
            .store
            .list_credits(key, anime_scope)
            .await
            .map(|value| value.map(|items| serde_json::to_value(items).unwrap_or_default())),
        ResourceKind::Images => state
            .store
            .list_images(key, anime_scope)
            .await
            .map(|value| {
                value.map(|items| {
                    let items = items
                        .into_iter()
                        .map(|item| {
                            image_response(
                                item,
                                state.allow_local_media,
                                state.media_base_url.as_deref(),
                            )
                        })
                        .collect::<Vec<_>>();
                    serde_json::to_value(items).unwrap_or_default()
                })
            }),
        ResourceKind::Translations => state
            .store
            .list_translations(key, anime_scope)
            .await
            .map(|value| value.map(|items| serde_json::to_value(items).unwrap_or_default())),
        ResourceKind::AlternateTitles => state
            .store
            .list_alternate_titles(key, anime_scope)
            .await
            .map(|value| value.map(|items| serde_json::to_value(items).unwrap_or_default())),
        ResourceKind::ExternalIds => state
            .store
            .external_ids(key, anime_scope)
            .await
            .map(|value| value.map(|item| serde_json::to_value(item).unwrap_or_default())),
        ResourceKind::Videos => state
            .store
            .list_videos(key, anime_scope)
            .await
            .map(|value| value.map(|items| serde_json::to_value(items).unwrap_or_default())),
        ResourceKind::ReleaseDates => state
            .store
            .list_release_dates(key, anime_scope)
            .await
            .map(|value| value.map(|items| serde_json::to_value(items).unwrap_or_default())),
    };
    match result {
        Ok(Some(data)) => (StatusCode::OK, Json(serde_json::json!({"data": data}))).into_response(),
        Ok(None) => not_found_response(&request_id),
        Err(error) => store_error_response(error, &request_id),
    }
}

#[derive(Clone, Copy)]
enum ResourceKind {
    Credits,
    Images,
    Translations,
    AlternateTitles,
    ExternalIds,
    Videos,
    ReleaseDates,
}

async fn movie_resource(
    state: CatalogApiState,
    raw_id: String,
    query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    kind: ResourceKind,
) -> Response {
    scoped_resource(
        state,
        raw_id,
        query,
        request_id,
        MediaType::Movie,
        kind,
        AnimeScope::OnlyNonAnime,
    )
    .await
}

async fn tv_resource(
    state: CatalogApiState,
    raw_id: String,
    query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    kind: ResourceKind,
) -> Response {
    scoped_resource(
        state,
        raw_id,
        query,
        request_id,
        MediaType::Tv,
        kind,
        AnimeScope::OnlyNonAnime,
    )
    .await
}

async fn anime_resource(
    state: CatalogApiState,
    raw_media_type: String,
    raw_id: String,
    query: Option<String>,
    request_id: Option<Extension<RequestId>>,
    kind: ResourceKind,
) -> Response {
    let Ok(media_type) = MediaType::from_str(&raw_media_type) else {
        return error_response(
            CatalogApiError::InvalidQuery,
            &request_id_string(request_id),
        );
    };
    scoped_resource(
        state,
        raw_id,
        query,
        request_id,
        media_type,
        kind,
        AnimeScope::OnlyAnime,
    )
    .await
}

fn image_response(
    image: CatalogImageAsset,
    allow_local_media: bool,
    media_base_url: Option<&str>,
) -> serde_json::Value {
    let mut value = serde_json::to_value(image).unwrap_or_default();
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    let remote = object
        .get("sourceUrl")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);
    let local = if allow_local_media {
        object
            .get("storagePath")
            .and_then(serde_json::Value::as_str)
            .filter(|path| tmdb_media::is_public_relative(path))
            .map(|path| match media_base_url {
                Some(base) => format!("{}/{}", base.trim_end_matches('/'), path),
                None => format!("/media/{path}"),
            })
    } else {
        None
    };
    object.insert(
        "url".to_owned(),
        serde_json::Value::String(local.or(remote).unwrap_or_default()),
    );
    if let Some(variants) = object
        .get_mut("variants")
        .and_then(serde_json::Value::as_array_mut)
    {
        for variant in variants {
            let Some(variant_object) = variant.as_object_mut() else {
                continue;
            };
            let local = if allow_local_media {
                variant_object
                    .get("storagePath")
                    .and_then(serde_json::Value::as_str)
                    .filter(|path| tmdb_media::is_public_relative(path))
                    .map(|path| match media_base_url {
                        Some(base) => format!("{}/{}", base.trim_end_matches('/'), path),
                        None => format!("/media/{path}"),
                    })
            } else {
                None
            };
            if let Some(local) = local {
                variant_object.insert("url".to_owned(), serde_json::Value::String(local));
            }
        }
    }
    value
}

async fn movie_credits(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    scoped_resource(
        state,
        raw_id,
        query,
        request_id,
        MediaType::Movie,
        ResourceKind::Credits,
        AnimeScope::OnlyNonAnime,
    )
    .await
}
async fn tv_credits(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    scoped_resource(
        state,
        raw_id,
        query,
        request_id,
        MediaType::Tv,
        ResourceKind::Credits,
        AnimeScope::OnlyNonAnime,
    )
    .await
}
async fn movie_images(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    scoped_resource(
        state,
        raw_id,
        query,
        request_id,
        MediaType::Movie,
        ResourceKind::Images,
        AnimeScope::OnlyNonAnime,
    )
    .await
}
async fn tv_images(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    scoped_resource(
        state,
        raw_id,
        query,
        request_id,
        MediaType::Tv,
        ResourceKind::Images,
        AnimeScope::OnlyNonAnime,
    )
    .await
}

async fn anime_images(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_media_type, raw_id)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let Ok(media_type) = MediaType::from_str(&raw_media_type) else {
        return error_response(
            CatalogApiError::InvalidQuery,
            &request_id_string(request_id),
        );
    };
    scoped_resource(
        state,
        raw_id,
        query,
        request_id,
        media_type,
        ResourceKind::Images,
        AnimeScope::OnlyAnime,
    )
    .await
}

async fn movie_translations(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    movie_resource(state, raw_id, query, request_id, ResourceKind::Translations).await
}

async fn movie_alternate_titles(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    movie_resource(
        state,
        raw_id,
        query,
        request_id,
        ResourceKind::AlternateTitles,
    )
    .await
}

async fn movie_external_ids(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    movie_resource(state, raw_id, query, request_id, ResourceKind::ExternalIds).await
}

async fn movie_videos(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    movie_resource(state, raw_id, query, request_id, ResourceKind::Videos).await
}

async fn movie_release_dates(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    movie_resource(state, raw_id, query, request_id, ResourceKind::ReleaseDates).await
}

async fn tv_translations(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    tv_resource(state, raw_id, query, request_id, ResourceKind::Translations).await
}

async fn tv_alternate_titles(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    tv_resource(
        state,
        raw_id,
        query,
        request_id,
        ResourceKind::AlternateTitles,
    )
    .await
}

async fn tv_external_ids(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    tv_resource(state, raw_id, query, request_id, ResourceKind::ExternalIds).await
}

async fn tv_videos(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    tv_resource(state, raw_id, query, request_id, ResourceKind::Videos).await
}

async fn tv_release_dates(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    tv_resource(state, raw_id, query, request_id, ResourceKind::ReleaseDates).await
}

async fn anime_translations(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_media_type, raw_id)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    anime_resource(
        state,
        raw_media_type,
        raw_id,
        query,
        request_id,
        ResourceKind::Translations,
    )
    .await
}

async fn anime_alternate_titles(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_media_type, raw_id)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    anime_resource(
        state,
        raw_media_type,
        raw_id,
        query,
        request_id,
        ResourceKind::AlternateTitles,
    )
    .await
}

async fn anime_external_ids(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_media_type, raw_id)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    anime_resource(
        state,
        raw_media_type,
        raw_id,
        query,
        request_id,
        ResourceKind::ExternalIds,
    )
    .await
}

async fn anime_videos(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_media_type, raw_id)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    anime_resource(
        state,
        raw_media_type,
        raw_id,
        query,
        request_id,
        ResourceKind::Videos,
    )
    .await
}

async fn anime_release_dates(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_media_type, raw_id)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    anime_resource(
        state,
        raw_media_type,
        raw_id,
        query,
        request_id,
        ResourceKind::ReleaseDates,
    )
    .await
}

async fn tv_seasons(
    Extension(state): Extension<CatalogApiState>,
    Path(raw_id): Path<String>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let request_id = request_id_string(request_id);
    if query.is_some_and(|value| !value.is_empty()) {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let tmdb_id = match parse_tmdb_id(&raw_id) {
        Ok(id) => id,
        Err(error) => return error_response(error, &request_id),
    };
    match state
        .store
        .list_seasons(
            TitleKey::new(MediaType::Tv, tmdb_id),
            AnimeScope::OnlyNonAnime,
        )
        .await
    {
        Ok(Some(data)) => (StatusCode::OK, Json(serde_json::json!({"data": data}))).into_response(),
        Ok(None) => not_found_response(&request_id),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn tv_episodes(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_id, raw_season)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let request_id = request_id_string(request_id);
    if query.is_some_and(|value| !value.is_empty()) {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let tmdb_id = match parse_tmdb_id(&raw_id) {
        Ok(id) => id,
        Err(error) => return error_response(error, &request_id),
    };
    let Some(season_number) = raw_season
        .parse::<i32>()
        .ok()
        .filter(|value| (0..=1000).contains(value))
    else {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    };
    match state
        .store
        .list_episodes(
            TitleKey::new(MediaType::Tv, tmdb_id),
            season_number,
            AnimeScope::OnlyNonAnime,
        )
        .await
    {
        Ok(Some(data)) => (StatusCode::OK, Json(serde_json::json!({"data": data}))).into_response(),
        Ok(None) => not_found_response(&request_id),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn tv_episode(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_id, raw_season, raw_episode)): Path<(String, String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let request_id = request_id_string(request_id);
    if query.is_some_and(|value| !value.is_empty()) {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let tmdb_id = match parse_tmdb_id(&raw_id) {
        Ok(id) => id,
        Err(error) => return error_response(error, &request_id),
    };
    let Some(season_number) = raw_season
        .parse::<i32>()
        .ok()
        .filter(|value| (0..=1000).contains(value))
    else {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    };
    let Some(episode_number) = raw_episode
        .parse::<i32>()
        .ok()
        .filter(|value| (0..=100_000).contains(value))
    else {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    };
    match state
        .store
        .list_episodes(
            TitleKey::new(MediaType::Tv, tmdb_id),
            season_number,
            AnimeScope::OnlyNonAnime,
        )
        .await
    {
        Ok(Some(data)) => match data
            .into_iter()
            .find(|episode| episode.episode_number == episode_number)
        {
            Some(data) => (StatusCode::OK, Json(serde_json::json!({"data": data}))).into_response(),
            None => not_found_response(&request_id),
        },
        Ok(None) => not_found_response(&request_id),
        Err(error) => store_error_response(error, &request_id),
    }
}

async fn tv_season(
    Extension(state): Extension<CatalogApiState>,
    Path((raw_id, raw_season)): Path<(String, String)>,
    RawQuery(query): RawQuery,
    request_id: Option<Extension<RequestId>>,
) -> Response {
    let request_id = request_id_string(request_id);
    if query.is_some_and(|value| !value.is_empty()) {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    }
    let tmdb_id = match parse_tmdb_id(&raw_id) {
        Ok(id) => id,
        Err(error) => return error_response(error, &request_id),
    };
    let Some(season_number) = raw_season
        .parse::<i32>()
        .ok()
        .filter(|value| (0..=1000).contains(value))
    else {
        return error_response(CatalogApiError::InvalidQuery, &request_id);
    };
    match state
        .store
        .list_seasons(
            TitleKey::new(MediaType::Tv, tmdb_id),
            AnimeScope::OnlyNonAnime,
        )
        .await
    {
        Ok(Some(data)) => match data
            .into_iter()
            .find(|season| season.season_number == season_number)
        {
            Some(data) => (StatusCode::OK, Json(serde_json::json!({"data": data}))).into_response(),
            None => not_found_response(&request_id),
        },
        Ok(None) => not_found_response(&request_id),
        Err(error) => store_error_response(error, &request_id),
    }
}

fn parse_tmdb_id(value: &str) -> Result<NonZeroU32, CatalogApiError> {
    value
        .parse::<u32>()
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or(CatalogApiError::InvalidQuery)
}

fn request_id_string(request_id: Option<Extension<RequestId>>) -> String {
    request_id.map_or_else(String::new, |value| value.0.0)
}

fn error_response(error: CatalogApiError, request_id: &str) -> Response {
    match error {
        CatalogApiError::InvalidQuery => problem::response(
            StatusCode::BAD_REQUEST,
            "Bad Request",
            "The catalog query is invalid.",
            request_id,
        ),
        CatalogApiError::Unavailable => problem::response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Service Unavailable",
            "The catalog is temporarily unavailable.",
            request_id,
        ),
        CatalogApiError::Unsupported => problem::response(
            StatusCode::NOT_IMPLEMENTED,
            "Not Implemented",
            "This catalog ordering is not available yet.",
            request_id,
        ),
    }
}

fn store_error_response(error: CatalogError, request_id: &str) -> Response {
    match error {
        CatalogError::InvalidInput => error_response(CatalogApiError::InvalidQuery, request_id),
        CatalogError::Query => error_response(CatalogApiError::Unavailable, request_id),
    }
}

fn not_found_response(request_id: &str) -> Response {
    problem::response(
        StatusCode::NOT_FOUND,
        "Not Found",
        "The requested catalog item was not found.",
        request_id,
    )
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogListResponse {
    data: Vec<CatalogTitleResponse>,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogDetailResponse {
    data: CatalogDetailBody,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogDetailBody {
    #[serde(flatten)]
    title: CatalogTitleResponse,
    movie: Option<CatalogMovieDetailsResponse>,
    tv: Option<CatalogTvDetailsResponse>,
    tagline: Option<String>,
    status: Option<String>,
    original_language: Option<String>,
    last_air_date: Option<NaiveDate>,
    runtime_minutes: Option<i32>,
    adult: bool,
    video: bool,
    homepage: Option<String>,
    poster_path: Option<String>,
    backdrop_path: Option<String>,
    source_updated_at: Option<DateTime<Utc>>,
    facets: CatalogFacetsResponse,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogMovieDetailsResponse {
    budget: Option<i64>,
    revenue: Option<i64>,
    runtime_minutes: Option<i32>,
    imdb_id: Option<String>,
    collection_id: Option<i64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogTvDetailsResponse {
    in_production: Option<bool>,
    number_of_episodes: Option<i32>,
    number_of_seasons: Option<i32>,
    series_type: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogFacetsResponse {
    genres: Vec<CatalogGenreResponse>,
    keywords: Vec<CatalogKeywordResponse>,
    tags: Vec<CatalogTagResponse>,
    languages: Vec<CatalogLanguageResponse>,
    companies: Vec<CatalogCompanyResponse>,
    networks: Vec<CatalogNetworkResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogGenreResponse {
    id: i64,
    name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogKeywordResponse {
    id: i64,
    name: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogTagResponse {
    id: i64,
    name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogLanguageResponse {
    iso_639_1: String,
    english_name: Option<String>,
    name: Option<String>,
    is_original: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogCompanyResponse {
    id: i64,
    name: Option<String>,
    origin_country: Option<String>,
    logo_path: Option<String>,
    company_role: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogNetworkResponse {
    id: i64,
    name: Option<String>,
    origin_country: Option<String>,
    logo_path: Option<String>,
}

impl From<CatalogDetail> for CatalogDetailBody {
    fn from(detail: CatalogDetail) -> Self {
        let CatalogDetail {
            title,
            movie,
            tv,
            tagline,
            status,
            original_language,
            last_air_date,
            runtime_minutes,
            adult,
            video,
            homepage,
            poster_path,
            backdrop_path,
            source_updated_at,
            facets,
        } = detail;
        Self {
            title: title.into(),
            movie: movie.map(Into::into),
            tv: tv.map(Into::into),
            tagline,
            status,
            original_language,
            last_air_date,
            runtime_minutes,
            adult,
            video,
            homepage,
            poster_path,
            backdrop_path,
            source_updated_at,
            facets: facets.into(),
        }
    }
}

impl From<tmdb_db::CatalogMovieDetails> for CatalogMovieDetailsResponse {
    fn from(details: tmdb_db::CatalogMovieDetails) -> Self {
        Self {
            budget: details.budget,
            revenue: details.revenue,
            runtime_minutes: details.runtime_minutes,
            imdb_id: details.imdb_id,
            collection_id: details.collection_id,
        }
    }
}

impl From<tmdb_db::CatalogTvDetails> for CatalogTvDetailsResponse {
    fn from(details: tmdb_db::CatalogTvDetails) -> Self {
        Self {
            in_production: details.in_production,
            number_of_episodes: details.number_of_episodes,
            number_of_seasons: details.number_of_seasons,
            series_type: details.series_type,
        }
    }
}

impl From<tmdb_db::CatalogFacets> for CatalogFacetsResponse {
    fn from(facets: tmdb_db::CatalogFacets) -> Self {
        Self {
            genres: facets.genres.into_iter().map(Into::into).collect(),
            keywords: facets.keywords.into_iter().map(Into::into).collect(),
            tags: facets.tags.into_iter().map(Into::into).collect(),
            languages: facets.languages.into_iter().map(Into::into).collect(),
            companies: facets.companies.into_iter().map(Into::into).collect(),
            networks: facets.networks.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<tmdb_db::CatalogGenre> for CatalogGenreResponse {
    fn from(value: tmdb_db::CatalogGenre) -> Self {
        Self {
            id: value.id,
            name: value.name,
        }
    }
}

impl From<tmdb_db::CatalogKeyword> for CatalogKeywordResponse {
    fn from(value: tmdb_db::CatalogKeyword) -> Self {
        Self {
            id: value.id,
            name: value.name,
        }
    }
}

impl From<tmdb_db::CatalogTag> for CatalogTagResponse {
    fn from(value: tmdb_db::CatalogTag) -> Self {
        Self {
            id: value.id,
            name: value.name,
        }
    }
}

impl From<tmdb_db::CatalogLanguage> for CatalogLanguageResponse {
    fn from(value: tmdb_db::CatalogLanguage) -> Self {
        Self {
            iso_639_1: value.iso_639_1,
            english_name: value.english_name,
            name: value.name,
            is_original: value.is_original,
        }
    }
}

impl From<tmdb_db::CatalogCompany> for CatalogCompanyResponse {
    fn from(value: tmdb_db::CatalogCompany) -> Self {
        Self {
            id: value.id,
            name: value.name,
            origin_country: value.origin_country,
            logo_path: value.logo_path,
            company_role: value.company_role,
        }
    }
}

impl From<tmdb_db::CatalogNetwork> for CatalogNetworkResponse {
    fn from(value: tmdb_db::CatalogNetwork) -> Self {
        Self {
            id: value.id,
            name: value.name,
            origin_country: value.origin_country,
            logo_path: value.logo_path,
        }
    }
}

fn detail_from_title(title: CatalogTitle) -> CatalogDetail {
    CatalogDetail {
        title,
        movie: None,
        tv: None,
        tagline: None,
        status: None,
        original_language: None,
        last_air_date: None,
        runtime_minutes: None,
        adult: false,
        video: false,
        homepage: None,
        poster_path: None,
        backdrop_path: None,
        source_updated_at: None,
        facets: tmdb_db::CatalogFacets::default(),
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CatalogTitleResponse {
    /// TMDB's public identifier, suitable for `/movies/{id}` or `/tv/{id}`.
    id: i64,
    /// Internal database identifier, exposed for diagnostics and cache keys.
    database_id: i64,
    media_type: MediaType,
    tmdb_id: i64,
    display_title: Option<String>,
    original_title: Option<String>,
    overview: Option<String>,
    popularity: Option<f64>,
    vote_average: Option<f64>,
    vote_count: Option<i64>,
    release_date: Option<NaiveDate>,
    is_anime: bool,
}

impl From<CatalogTitle> for CatalogTitleResponse {
    fn from(title: CatalogTitle) -> Self {
        Self {
            id: title.tmdb_id,
            database_id: title.id,
            media_type: title.media_type,
            tmdb_id: title.tmdb_id,
            display_title: title.display_title,
            original_title: title.original_title,
            overview: title.overview,
            popularity: title.popularity,
            vote_average: title.vote_average,
            vote_count: title.vote_count,
            release_date: title.release_date,
            is_anime: title.is_anime,
        }
    }
}

fn page_response(page: CatalogPage) -> Response {
    let next_cursor = page.next.map(encode_cursor);
    let body = CatalogListResponse {
        data: page.items.into_iter().map(Into::into).collect(),
        next_cursor,
    };
    (StatusCode::OK, Json(body)).into_response()
}

fn recent_page_response(page: CatalogRecentPage) -> Response {
    let next_cursor = page.next.map(encode_recent_cursor);
    let body = CatalogListResponse {
        data: page.items.into_iter().map(Into::into).collect(),
        next_cursor,
    };
    (StatusCode::OK, Json(body)).into_response()
}

fn top_page_response(page: CatalogTopPage) -> Response {
    let next_cursor = page.next.map(encode_top_cursor);
    let body = CatalogListResponse {
        data: page.items.into_iter().map(Into::into).collect(),
        next_cursor,
    };
    (StatusCode::OK, Json(body)).into_response()
}

fn items_response(items: Vec<CatalogTitle>) -> Response {
    let body = CatalogListResponse {
        data: items.into_iter().map(Into::into).collect(),
        next_cursor: None,
    };
    (StatusCode::OK, Json(body)).into_response()
}

fn detail_response(detail: CatalogDetail) -> Response {
    (
        StatusCode::OK,
        Json(CatalogDetailResponse {
            data: detail.into(),
        }),
    )
        .into_response()
}

fn encode_cursor(cursor: PopularCursor) -> String {
    format!("{}:{}", cursor.popularity(), cursor.title_id())
}

fn encode_recent_cursor(cursor: RecentCursor) -> String {
    format!("{}:{}", cursor.date(), cursor.title_id())
}

fn encode_top_cursor(cursor: TopCursor) -> String {
    format!(
        "{}:{}:{}",
        cursor.vote_average(),
        cursor.vote_count(),
        cursor.title_id()
    )
}
