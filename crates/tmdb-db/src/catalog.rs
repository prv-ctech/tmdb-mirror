use std::str::FromStr;

use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{FromRow, PgPool, Postgres, QueryBuilder, types::Json};
use tmdb_domain::{MediaType, TitleKey};

/// Errors returned by the catalog read repository.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CatalogError {
    /// The caller supplied a bounded-query value that cannot be represented safely.
    #[error("catalog input is invalid")]
    InvalidInput,
    /// `PostgreSQL` rejected or could not complete the read query.
    #[error("catalog query failed")]
    Query,
}

/// Allowlisted, bounded discovery filters shared by the read API and repository.
///
/// Relationship filters use canonical TMDB/local identifiers so the database can use its
/// reverse indexes.  Text facets are resolved through the dedicated facet endpoints first;
/// accepting a short language/status value here keeps the hot title query deterministic.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CatalogFilters {
    pub genre_id: Option<i64>,
    pub keyword_id: Option<i64>,
    pub tag_id: Option<i64>,
    pub language: Option<String>,
    pub runtime_min: Option<i32>,
    pub runtime_max: Option<i32>,
    pub person_id: Option<i64>,
    pub company_id: Option<i64>,
    pub network_id: Option<i64>,
    pub year: Option<i32>,
    pub status: Option<String>,
}

impl CatalogFilters {
    /// Returns whether no additional discovery predicate was requested.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.genre_id.is_none()
            && self.keyword_id.is_none()
            && self.tag_id.is_none()
            && self.language.is_none()
            && self.runtime_min.is_none()
            && self.runtime_max.is_none()
            && self.person_id.is_none()
            && self.company_id.is_none()
            && self.network_id.is_none()
            && self.year.is_none()
            && self.status.is_none()
    }

    /// Validates identifier, range, and bounded text values before they reach SQL.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an out-of-range or malformed filter.
    pub fn validate(&self) -> Result<(), CatalogError> {
        for id in [
            self.genre_id,
            self.keyword_id,
            self.tag_id,
            self.person_id,
            self.company_id,
            self.network_id,
        ]
        .into_iter()
        .flatten()
        {
            if id <= 0 {
                return Err(CatalogError::InvalidInput);
            }
        }
        if self.language.as_deref().is_some_and(|language| {
            language.len() < 2 || language.len() > 16 || !language.is_ascii()
        }) {
            return Err(CatalogError::InvalidInput);
        }
        if self
            .runtime_min
            .is_some_and(|runtime| !(0..=10_000).contains(&runtime))
            || self
                .runtime_max
                .is_some_and(|runtime| !(0..=10_000).contains(&runtime))
            || matches!((self.runtime_min, self.runtime_max), (Some(min), Some(max)) if min > max)
        {
            return Err(CatalogError::InvalidInput);
        }
        if self.year.is_some_and(|year| !(1800..=2200).contains(&year)) {
            return Err(CatalogError::InvalidInput);
        }
        if self.status.as_deref().is_some_and(|status| {
            status.is_empty() || status.len() > 64 || status.chars().any(char::is_control)
        }) {
            return Err(CatalogError::InvalidInput);
        }
        Ok(())
    }
}

/// The explicit anime partition a public route is allowed to read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnimeScope {
    /// Return only titles classified as anime.
    OnlyAnime,
    /// Return only titles that are not classified as anime.
    OnlyNonAnime,
}

impl AnimeScope {
    const fn predicate(self) -> &'static str {
        match self {
            Self::OnlyAnime => "is_anime",
            Self::OnlyNonAnime => "NOT is_anime",
        }
    }

    const fn qualified_predicate(self) -> &'static str {
        match self {
            Self::OnlyAnime => "title.is_anime",
            Self::OnlyNonAnime => "NOT title.is_anime",
        }
    }
}

/// The stable key used by popularity lists for cursor pagination.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PopularCursor {
    popularity: f64,
    title_id: i64,
}

impl PopularCursor {
    /// Creates a cursor from the last returned row.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for a non-finite score or non-positive title ID.
    pub fn try_new(popularity: f64, title_id: i64) -> Result<Self, CatalogError> {
        if !popularity.is_finite() || title_id <= 0 {
            return Err(CatalogError::InvalidInput);
        }
        Ok(Self {
            popularity,
            title_id,
        })
    }

    /// Returns the popularity component of this cursor.
    #[must_use]
    pub const fn popularity(self) -> f64 {
        self.popularity
    }

    /// Returns the internal title ID component of this cursor.
    #[must_use]
    pub const fn title_id(self) -> i64 {
        self.title_id
    }
}

/// A bounded page of popular titles ordered by popularity and internal ID.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogPage {
    /// Rows in descending popularity order.
    pub items: Vec<CatalogTitle>,
    /// Cursor for the next page, when the page was full.
    pub next: Option<PopularCursor>,
}

/// The shared title fields needed by list, detail, and search routes.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogTitle {
    /// Internal database identity.
    pub id: i64,
    /// TMDB media namespace.
    pub media_type: MediaType,
    /// TMDB's public entity ID.
    pub tmdb_id: i64,
    /// Display title (`title` for movies and `name` for television in the source API).
    pub display_title: Option<String>,
    /// Original-language title/name.
    pub original_title: Option<String>,
    /// Synopsis text.
    pub overview: Option<String>,
    /// Popularity score supplied by TMDB.
    pub popularity: Option<f64>,
    /// Average vote score supplied by TMDB.
    pub vote_average: Option<f64>,
    /// Vote count supplied by TMDB.
    pub vote_count: Option<i64>,
    /// Movie release date or TV first-air date.
    pub release_date: Option<NaiveDate>,
    /// Derived anime classification used by public-route isolation.
    pub is_anime: bool,
}

/// The movie-specific columns available on a catalog detail row.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogMovieDetails {
    /// Production budget in TMDB's reported currency units.
    pub budget: Option<i64>,
    /// Gross revenue in TMDB's reported currency units.
    pub revenue: Option<i64>,
    /// Runtime reported by the movie detail endpoint.
    pub runtime_minutes: Option<i32>,
    /// `IMDb` identifier, when TMDB provides one.
    pub imdb_id: Option<String>,
    /// TMDB collection identity, when the movie belongs to a collection.
    pub collection_id: Option<i64>,
}

/// The television-specific columns available on a catalog detail row.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogTvDetails {
    /// Whether TMDB currently reports that the series is in production.
    pub in_production: Option<bool>,
    /// Number of episodes reported by TMDB.
    pub number_of_episodes: Option<i32>,
    /// Number of seasons reported by TMDB.
    pub number_of_seasons: Option<i32>,
    /// TMDB's series type, such as `Scripted`.
    pub series_type: Option<String>,
}

/// A genre dimension row attached to a title.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogGenre {
    /// TMDB genre identity.
    pub id: i64,
    /// Localized or source-provided genre name.
    pub name: Option<String>,
}

/// A keyword dimension row attached to a title.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogKeyword {
    /// TMDB keyword identity.
    pub id: i64,
    /// Source-provided keyword name.
    pub name: Option<String>,
}

/// An administrator-defined tag attached to a title.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogTag {
    /// Local tag identity.
    pub id: i64,
    /// Local tag label.
    pub name: String,
}

/// A production-company dimension row attached to a title.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogCompany {
    /// TMDB company identity.
    pub id: i64,
    /// Source-provided company name.
    pub name: Option<String>,
    /// ISO country code supplied by TMDB.
    pub origin_country: Option<String>,
    /// Relative logo path supplied by TMDB.
    pub logo_path: Option<String>,
    /// Optional local role annotation for the relationship.
    pub company_role: Option<String>,
}

/// A broadcast-network dimension row attached to a title.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogNetwork {
    /// TMDB network identity.
    pub id: i64,
    /// Source-provided network name.
    pub name: Option<String>,
    /// ISO country code supplied by TMDB.
    pub origin_country: Option<String>,
    /// Relative logo path supplied by TMDB.
    pub logo_path: Option<String>,
}

/// A spoken-language dimension row attached to a title.
#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogLanguage {
    /// ISO-639-1/extended language code.
    pub iso_639_1: String,
    /// English display name supplied by TMDB.
    pub english_name: Option<String>,
    /// Native display name supplied by TMDB.
    pub name: Option<String>,
    /// Whether this is the title's original language.
    pub is_original: bool,
}

/// All currently committed title dimensions returned alongside a detail row.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogFacets {
    /// Attached genres.
    pub genres: Vec<CatalogGenre>,
    /// Attached TMDB keywords.
    pub keywords: Vec<CatalogKeyword>,
    /// Attached administrator-defined tags.
    pub tags: Vec<CatalogTag>,
    /// Attached spoken languages.
    pub languages: Vec<CatalogLanguage>,
    /// Attached production companies.
    pub companies: Vec<CatalogCompany>,
    /// Attached broadcast networks.
    pub networks: Vec<CatalogNetwork>,
}

/// A canonical person row shared by title and episode credits.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPerson {
    pub id: i64,
    pub name: Option<String>,
    pub known_for_department: Option<String>,
    pub gender: Option<i16>,
    pub biography: Option<String>,
    pub birthday: Option<NaiveDate>,
    pub deathday: Option<NaiveDate>,
    pub place_of_birth: Option<String>,
    pub homepage: Option<String>,
    pub imdb_id: Option<String>,
    pub adult: bool,
    pub popularity: Option<f64>,
    pub profile_path: Option<String>,
}

/// A title or episode credit edge.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogCredit {
    pub credit_id: String,
    pub person: CatalogPerson,
    pub credit_type: String,
    pub department: Option<String>,
    pub job: Option<String>,
    pub character: Option<String>,
    pub cast_order: Option<i32>,
    pub episode_count: Option<i32>,
}

/// A television season attached to a title.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogSeason {
    pub id: i64,
    pub title_id: i64,
    pub season_number: i32,
    pub name: Option<String>,
    pub overview: Option<String>,
    pub air_date: Option<NaiveDate>,
    pub episode_count: Option<i32>,
    pub poster_path: Option<String>,
}

/// An episode attached to a season and television title.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogEpisode {
    pub id: i64,
    pub season_id: i64,
    pub title_id: i64,
    pub episode_number: i32,
    pub name: Option<String>,
    pub overview: Option<String>,
    pub air_date: Option<NaiveDate>,
    pub runtime_minutes: Option<i32>,
    pub still_path: Option<String>,
    pub vote_average: Option<f64>,
    pub vote_count: Option<i64>,
}

/// A persisted image metadata row owned by exactly one catalog entity.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogImageAsset {
    pub id: i64,
    pub image_kind: String,
    pub source: String,
    pub source_key: String,
    pub source_url: Option<String>,
    pub storage_path: Option<String>,
    pub mime_type: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub file_size_bytes: Option<i64>,
    pub sha256: Option<String>,
    pub status: String,
    pub iso_639_1: Option<String>,
}

/// A canonical collection resource.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogCollection {
    pub id: i64,
    pub name: Option<String>,
    pub poster_path: Option<String>,
    pub backdrop_path: Option<String>,
}

/// The complete title detail supported by the committed catalog schema.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct CatalogDetail {
    /// Shared title identity and list fields.
    pub title: CatalogTitle,
    /// Optional movie-only detail row.
    pub movie: Option<CatalogMovieDetails>,
    /// Optional television-only detail row.
    pub tv: Option<CatalogTvDetails>,
    /// Additional shared title columns.
    pub tagline: Option<String>,
    /// TMDB lifecycle status.
    pub status: Option<String>,
    /// ISO language code for the original title.
    pub original_language: Option<String>,
    /// Last TV air date, when known.
    pub last_air_date: Option<NaiveDate>,
    /// Shared runtime field in minutes.
    pub runtime_minutes: Option<i32>,
    /// TMDB adult-content marker.
    pub adult: bool,
    /// TMDB video marker.
    pub video: bool,
    /// Official homepage URL, when provided.
    pub homepage: Option<String>,
    /// Relative poster path, when provided.
    pub poster_path: Option<String>,
    /// Relative backdrop path, when provided.
    pub backdrop_path: Option<String>,
    /// Last source update timestamp.
    pub source_updated_at: Option<DateTime<Utc>>,
    /// Dimension relationships attached to this title.
    pub facets: CatalogFacets,
}

/// The stable key used by date-ordered recent lists.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecentCursor {
    date: NaiveDate,
    title_id: i64,
}

impl RecentCursor {
    /// Creates a cursor from the last returned title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for a non-positive title ID.
    pub fn try_new(date: NaiveDate, title_id: i64) -> Result<Self, CatalogError> {
        if title_id <= 0 {
            return Err(CatalogError::InvalidInput);
        }
        Ok(Self { date, title_id })
    }

    /// Returns the date component of this cursor.
    #[must_use]
    pub const fn date(self) -> NaiveDate {
        self.date
    }

    /// Returns the internal title ID component of this cursor.
    #[must_use]
    pub const fn title_id(self) -> i64 {
        self.title_id
    }
}

/// A bounded page of recent titles ordered by release/first-air date.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogRecentPage {
    /// Rows in descending date order.
    pub items: Vec<CatalogTitle>,
    /// Cursor for the next page, when the page was full.
    pub next: Option<RecentCursor>,
}

/// The stable key used by top-rated lists for cursor pagination.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TopCursor {
    vote_average: f64,
    vote_count: i64,
    title_id: i64,
}

impl TopCursor {
    /// Creates a cursor from the last returned top-rated title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for non-finite scores, negative vote counts, or
    /// non-positive title IDs.
    pub fn try_new(
        vote_average: f64,
        vote_count: i64,
        title_id: i64,
    ) -> Result<Self, CatalogError> {
        if !vote_average.is_finite() || vote_count < 0 || title_id <= 0 {
            return Err(CatalogError::InvalidInput);
        }
        Ok(Self {
            vote_average,
            vote_count,
            title_id,
        })
    }

    /// Returns the vote-average component of this cursor.
    #[must_use]
    pub const fn vote_average(self) -> f64 {
        self.vote_average
    }

    /// Returns the vote-count component of this cursor.
    #[must_use]
    pub const fn vote_count(self) -> i64 {
        self.vote_count
    }

    /// Returns the internal title ID component of this cursor.
    #[must_use]
    pub const fn title_id(self) -> i64 {
        self.title_id
    }
}

/// A bounded page of top-rated titles ordered by score, vote count, and ID.
#[derive(Clone, Debug, PartialEq)]
pub struct CatalogTopPage {
    /// Rows in descending rating order.
    pub items: Vec<CatalogTitle>,
    /// Cursor for the next page, when the page was full.
    pub next: Option<TopCursor>,
}

/// PostgreSQL-backed catalog read primitives.  The repository never accepts raw SQL,
/// identifiers, or an unbounded page size from callers.
#[derive(Clone, Debug)]
pub struct CatalogRepository {
    pool: PgPool,
}

impl CatalogRepository {
    /// Creates a repository over a bounded read pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Fetches one title by its globally unique `(media_type, tmdb_id)` identity.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read or returns an
    /// unsupported media type.
    pub async fn get_title(&self, key: TitleKey) -> Result<Option<CatalogTitle>, CatalogError> {
        let media_type = key.media_type().to_string();
        let row = sqlx::query_as::<_, TitleRow>(
            "SELECT id, media_type, tmdb_id, display_title, original_title, overview,
                    popularity, vote_average, vote_count,
                    CASE WHEN media_type = 'movie' THEN release_date ELSE first_air_date END
                        AS release_date,
                    is_anime
               FROM catalog.titles
              WHERE media_type = $1 AND tmdb_id = $2 AND active",
        )
        .bind(media_type)
        .bind(i64::from(key.tmdb_id().get()))
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| CatalogError::Query)?;
        row.map(TitleRow::try_into_title).transpose()
    }

    /// Fetches one title while enforcing the route's explicit anime partition.
    ///
    /// This scoped variant is the method public movie/TV/anime detail handlers should use.
    /// The legacy [`Self::get_title`] method remains available for internal/admin callers that
    /// intentionally read either partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read or returns an
    /// unsupported media type.
    pub async fn get_title_scoped(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<CatalogTitle>, CatalogError> {
        let media_type = key.media_type().to_string();
        let statement = format!(
            "SELECT id, media_type, tmdb_id, display_title, original_title, overview,
                    popularity, vote_average, vote_count,
                    CASE WHEN media_type = 'movie' THEN release_date ELSE first_air_date END
                        AS release_date,
                    is_anime
               FROM catalog.titles
              WHERE media_type = $1 AND tmdb_id = $2 AND active AND {scope}",
            scope = anime_scope.predicate(),
        );
        let row = sqlx::query_as::<_, TitleRow>(sqlx::AssertSqlSafe(statement))
            .bind(media_type)
            .bind(i64::from(key.tmdb_id().get()))
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        row.map(TitleRow::try_into_title).transpose()
    }

    /// Fetches one complete, scope-isolated detail row and every facet supported by the
    /// committed catalog schema.
    ///
    /// The six facet collections are aggregated by `PostgreSQL` in lateral subqueries.  This
    /// keeps one detail request to one bounded database round trip while the relationship
    /// indexes remain usable for each title ID.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read or a stored media
    /// type cannot be decoded.
    #[allow(clippy::too_many_lines)]
    pub async fn get_detail(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<CatalogDetail>, CatalogError> {
        let statement = format!(
            "SELECT title.id,
                    title.media_type,
                    title.tmdb_id,
                    title.display_title,
                    title.original_title,
                    title.overview,
                    title.popularity,
                    title.vote_average,
                    title.vote_count,
                    CASE WHEN title.media_type = 'movie'
                         THEN title.release_date ELSE title.first_air_date END AS release_date,
                    title.is_anime,
                    title.tagline,
                    title.status,
                    title.original_language,
                    title.last_air_date,
                    title.runtime_minutes,
                    title.adult,
                    title.video,
                    title.homepage,
                    title.poster_path,
                    title.backdrop_path,
                    title.source_updated_at,
                    movie.title_id AS movie_title_id,
                    movie.budget AS movie_budget,
                    movie.revenue AS movie_revenue,
                    movie.runtime_minutes AS movie_runtime_minutes,
                    movie.imdb_id AS movie_imdb_id,
                    movie.collection_id AS movie_collection_id,
                    tv.title_id AS tv_title_id,
                    tv.in_production AS tv_in_production,
                    tv.number_of_episodes AS tv_number_of_episodes,
                    tv.number_of_seasons AS tv_number_of_seasons,
                    tv.series_type AS tv_series_type,
                    genres.value AS genres,
                    keywords.value AS keywords,
                    tags.value AS tags,
                    languages.value AS languages,
                    companies.value AS companies,
                    networks.value AS networks
               FROM catalog.titles AS title
               LEFT JOIN catalog.movie_details AS movie ON movie.title_id = title.id
               LEFT JOIN catalog.tv_details AS tv ON tv.title_id = title.id
               LEFT JOIN LATERAL (
                    SELECT coalesce(
                        jsonb_agg(
                            jsonb_build_object('id', genre.id, 'name', genre.name)
                            ORDER BY genre.id
                        ),
                        '[]'::jsonb
                    ) AS value
                      FROM catalog.title_genres AS relation
                      JOIN catalog.genres AS genre ON genre.id = relation.genre_id
                     WHERE relation.title_id = title.id
               ) AS genres ON TRUE
               LEFT JOIN LATERAL (
                    SELECT coalesce(
                        jsonb_agg(
                            jsonb_build_object('id', keyword.id, 'name', keyword.name)
                            ORDER BY keyword.id
                        ),
                        '[]'::jsonb
                    ) AS value
                      FROM catalog.title_keywords AS relation
                      JOIN catalog.keywords AS keyword ON keyword.id = relation.keyword_id
                     WHERE relation.title_id = title.id
               ) AS keywords ON TRUE
               LEFT JOIN LATERAL (
                    SELECT coalesce(
                        jsonb_agg(
                            jsonb_build_object('id', tag.id, 'name', tag.name)
                            ORDER BY tag.id
                        ),
                        '[]'::jsonb
                    ) AS value
                      FROM catalog.title_tags AS relation
                      JOIN catalog.tags AS tag ON tag.id = relation.tag_id
                     WHERE relation.title_id = title.id
               ) AS tags ON TRUE
               LEFT JOIN LATERAL (
                    SELECT coalesce(
                        jsonb_agg(
                            jsonb_build_object(
                                'iso_639_1', language.iso_639_1,
                                'english_name', language.english_name,
                                'name', language.name,
                                'is_original', relation.is_original
                            )
                            ORDER BY language.iso_639_1
                        ),
                        '[]'::jsonb
                    ) AS value
                      FROM catalog.title_languages AS relation
                      JOIN catalog.languages AS language
                        ON language.iso_639_1 = relation.language_id
                     WHERE relation.title_id = title.id
               ) AS languages ON TRUE
               LEFT JOIN LATERAL (
                    SELECT coalesce(
                        jsonb_agg(
                            jsonb_build_object(
                                'id', company.id,
                                'name', company.name,
                                'origin_country', company.origin_country,
                                'logo_path', company.logo_path,
                                'company_role', relation.company_role
                            )
                            ORDER BY company.id
                        ),
                        '[]'::jsonb
                    ) AS value
                      FROM catalog.title_companies AS relation
                      JOIN catalog.companies AS company ON company.id = relation.company_id
                     WHERE relation.title_id = title.id
               ) AS companies ON TRUE
               LEFT JOIN LATERAL (
                    SELECT coalesce(
                        jsonb_agg(
                            jsonb_build_object(
                                'id', network.id,
                                'name', network.name,
                                'origin_country', network.origin_country,
                                'logo_path', network.logo_path
                            )
                            ORDER BY network.id
                        ),
                        '[]'::jsonb
                    ) AS value
                      FROM catalog.title_networks AS relation
                      JOIN catalog.networks AS network ON network.id = relation.network_id
                     WHERE relation.title_id = title.id
               ) AS networks ON TRUE
              WHERE title.media_type = $1
                AND title.tmdb_id = $2
                AND title.active
                AND {scope}",
            scope = anime_scope.predicate(),
        );
        let row = sqlx::query_as::<_, DetailRow>(sqlx::AssertSqlSafe(statement))
            .bind(key.media_type().to_string())
            .bind(i64::from(key.tmdb_id().get()))
            .fetch_optional(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        row.map(DetailRow::try_into_detail).transpose()
    }

    /// Fetches all facets for a scope-isolated title without exposing the detail payload.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read.
    pub async fn get_facets(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<CatalogFacets>, CatalogError> {
        self.get_detail(key, anime_scope)
            .await
            .map(|detail| detail.map(|detail| detail.facets))
    }

    /// Lists genres for a scope-isolated title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read.
    pub async fn list_genres(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogGenre>>, CatalogError> {
        self.get_facets(key, anime_scope)
            .await
            .map(|facets| facets.map(|facets| facets.genres))
    }

    /// Lists keywords for a scope-isolated title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read.
    pub async fn list_keywords(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogKeyword>>, CatalogError> {
        self.get_facets(key, anime_scope)
            .await
            .map(|facets| facets.map(|facets| facets.keywords))
    }

    /// Lists local tags for a scope-isolated title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read.
    pub async fn list_tags(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogTag>>, CatalogError> {
        self.get_facets(key, anime_scope)
            .await
            .map(|facets| facets.map(|facets| facets.tags))
    }

    /// Lists spoken languages for a scope-isolated title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read.
    pub async fn list_languages(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogLanguage>>, CatalogError> {
        self.get_facets(key, anime_scope)
            .await
            .map(|facets| facets.map(|facets| facets.languages))
    }

    /// Lists production companies for a scope-isolated title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read.
    pub async fn list_companies(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogCompany>>, CatalogError> {
        self.get_facets(key, anime_scope)
            .await
            .map(|facets| facets.map(|facets| facets.companies))
    }

    /// Lists broadcast networks for a scope-isolated title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when the database rejects the read.
    pub async fn list_networks(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogNetwork>>, CatalogError> {
        self.get_facets(key, anime_scope)
            .await
            .map(|facets| facets.map(|facets| facets.networks))
    }

    /// Lists people that have at least one credit in the requested anime partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an invalid term or limit, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_people(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogPerson>, CatalogError> {
        if !(1..=100).contains(&limit)
            || term.is_some_and(|value| value.trim().is_empty() || value.chars().count() > 256)
        {
            return Err(CatalogError::InvalidInput);
        }
        let term_clause = if term.is_some() {
            " AND person.normalized_name % lower(public.unaccent($1))"
        } else {
            ""
        };
        let limit_placeholder = if term.is_some() { "$2" } else { "$1" };
        let statement = format!(
            "SELECT DISTINCT person.id, person.name, person.known_for_department,
                    person.gender, person.biography, person.birthday, person.deathday,
                    person.place_of_birth, person.homepage, person.imdb_id, person.adult,
                    person.popularity, person.profile_path
               FROM catalog.people AS person
               JOIN catalog.title_credits AS credit ON credit.person_id = person.id
               JOIN catalog.titles AS title ON title.id = credit.title_id
              WHERE title.active AND {scope}{term_clause}
              ORDER BY person.popularity DESC NULLS LAST, person.id DESC
              LIMIT {limit_placeholder}",
            scope = anime_scope.predicate(),
        );
        let rows = if let Some(term) = term {
            sqlx::query_as::<_, PersonRow>(sqlx::AssertSqlSafe(statement))
                .bind(term.trim())
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query_as::<_, PersonRow>(sqlx::AssertSqlSafe(statement))
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|_| CatalogError::Query)?;
        Ok(rows.into_iter().map(PersonRow::into_person).collect())
    }

    /// Lists production companies referenced by titles in an anime partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an invalid term or limit, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_company_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogCompany>, CatalogError> {
        if !(1..=100).contains(&limit)
            || term.is_some_and(|value| value.trim().is_empty() || value.chars().count() > 256)
        {
            return Err(CatalogError::InvalidInput);
        }
        let clause = if term.is_some() {
            " AND company.name ILIKE '%' || $1 || '%'"
        } else {
            ""
        };
        let limit_placeholder = if term.is_some() { "$2" } else { "$1" };
        let statement = format!(
            "SELECT DISTINCT company.id, company.name, company.origin_country, company.logo_path FROM catalog.companies AS company JOIN catalog.title_companies AS relation ON relation.company_id = company.id JOIN catalog.titles AS title ON title.id = relation.title_id WHERE title.active AND {scope}{clause} ORDER BY company.id LIMIT {limit_placeholder}",
            scope = anime_scope.predicate()
        );
        let rows = if let Some(term) = term {
            sqlx::query_as::<_, CompanyRow>(sqlx::AssertSqlSafe(statement))
                .bind(term.trim())
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query_as::<_, CompanyRow>(sqlx::AssertSqlSafe(statement))
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|_| CatalogError::Query)?;
        Ok(rows
            .into_iter()
            .map(|row| CatalogCompany {
                id: row.id,
                name: row.name,
                origin_country: row.origin_country,
                logo_path: row.logo_path,
                company_role: None,
            })
            .collect())
    }

    /// Lists broadcast networks referenced by titles in an anime partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an invalid term or limit, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_network_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogNetwork>, CatalogError> {
        if !(1..=100).contains(&limit)
            || term.is_some_and(|value| value.trim().is_empty() || value.chars().count() > 256)
        {
            return Err(CatalogError::InvalidInput);
        }
        let clause = if term.is_some() {
            " AND network.name ILIKE '%' || $1 || '%'"
        } else {
            ""
        };
        let limit_placeholder = if term.is_some() { "$2" } else { "$1" };
        let statement = format!(
            "SELECT DISTINCT network.id, network.name, network.origin_country, network.logo_path FROM catalog.networks AS network JOIN catalog.title_networks AS relation ON relation.network_id = network.id JOIN catalog.titles AS title ON title.id = relation.title_id WHERE title.active AND {scope}{clause} ORDER BY network.id LIMIT {limit_placeholder}",
            scope = anime_scope.predicate()
        );
        let rows = if let Some(term) = term {
            sqlx::query_as::<_, NetworkRow>(sqlx::AssertSqlSafe(statement))
                .bind(term.trim())
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query_as::<_, NetworkRow>(sqlx::AssertSqlSafe(statement))
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|_| CatalogError::Query)?;
        Ok(rows
            .into_iter()
            .map(|row| CatalogNetwork {
                id: row.id,
                name: row.name,
                origin_country: row.origin_country,
                logo_path: row.logo_path,
            })
            .collect())
    }

    /// Lists collections referenced by titles in an anime partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an invalid term or limit, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_collections(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogCollection>, CatalogError> {
        if !(1..=100).contains(&limit)
            || term.is_some_and(|value| value.trim().is_empty() || value.chars().count() > 256)
        {
            return Err(CatalogError::InvalidInput);
        }
        let clause = if term.is_some() {
            " AND collection.name ILIKE '%' || $1 || '%'"
        } else {
            ""
        };
        let limit_placeholder = if term.is_some() { "$2" } else { "$1" };
        let statement = format!(
            "SELECT DISTINCT collection.id, collection.name, collection.poster_path, collection.backdrop_path FROM catalog.collections AS collection JOIN catalog.title_collections AS relation ON relation.collection_id = collection.id JOIN catalog.titles AS title ON title.id = relation.title_id WHERE title.active AND {scope}{clause} ORDER BY collection.id LIMIT {limit_placeholder}",
            scope = anime_scope.predicate()
        );
        let rows = if let Some(term) = term {
            sqlx::query_as::<_, CollectionRow>(sqlx::AssertSqlSafe(statement))
                .bind(term.trim())
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
        } else {
            sqlx::query_as::<_, CollectionRow>(sqlx::AssertSqlSafe(statement))
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await
        }
        .map_err(|_| CatalogError::Query)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Lists genres referenced by active titles in the requested anime partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an invalid term or limit, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_genre_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogGenre>, CatalogError> {
        if !(1..=100).contains(&limit)
            || term.is_some_and(|value| value.trim().is_empty() || value.chars().count() > 256)
        {
            return Err(CatalogError::InvalidInput);
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT DISTINCT genre.id, genre.name
               FROM catalog.genres AS genre
               JOIN catalog.title_genres AS relation ON relation.genre_id = genre.id
               JOIN catalog.titles AS title ON title.id = relation.title_id
              WHERE title.active AND ",
        );
        query.push(anime_scope.qualified_predicate());
        if let Some(term) = term {
            query
                .push(" AND lower(public.unaccent(coalesce(genre.name, ''))) LIKE '%' || lower(public.unaccent(")
                .push_bind(term.trim())
                .push(")) || '%' ");
        }
        query
            .push(" ORDER BY genre.name NULLS LAST, genre.id LIMIT ")
            .push_bind(i64::from(limit));
        let rows = query
            .build_query_as::<GenreEntityRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Lists keywords referenced by active titles in the requested anime partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an invalid term or limit, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_keyword_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogKeyword>, CatalogError> {
        if !(1..=100).contains(&limit)
            || term.is_some_and(|value| value.trim().is_empty() || value.chars().count() > 256)
        {
            return Err(CatalogError::InvalidInput);
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT DISTINCT keyword.id, keyword.name
               FROM catalog.keywords AS keyword
               JOIN catalog.title_keywords AS relation ON relation.keyword_id = keyword.id
               JOIN catalog.titles AS title ON title.id = relation.title_id
              WHERE title.active AND ",
        );
        query.push(anime_scope.qualified_predicate());
        if let Some(term) = term {
            query
                .push(" AND lower(public.unaccent(coalesce(keyword.name, ''))) LIKE '%' || lower(public.unaccent(")
                .push_bind(term.trim())
                .push(")) || '%' ");
        }
        query
            .push(" ORDER BY keyword.name NULLS LAST, keyword.id LIMIT ")
            .push_bind(i64::from(limit));
        let rows = query
            .build_query_as::<KeywordEntityRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Lists local tags referenced by active titles in the requested anime partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an invalid term or limit, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_tag_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogTag>, CatalogError> {
        if !(1..=100).contains(&limit)
            || term.is_some_and(|value| value.trim().is_empty() || value.chars().count() > 256)
        {
            return Err(CatalogError::InvalidInput);
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT DISTINCT tag.id, tag.name
               FROM catalog.tags AS tag
               JOIN catalog.title_tags AS relation ON relation.tag_id = tag.id
               JOIN catalog.titles AS title ON title.id = relation.title_id
              WHERE title.active AND ",
        );
        query.push(anime_scope.qualified_predicate());
        if let Some(term) = term {
            query
                .push(" AND lower(public.unaccent(tag.name)) LIKE '%' || lower(public.unaccent(")
                .push_bind(term.trim())
                .push(")) || '%' ");
        }
        query
            .push(" ORDER BY tag.name, tag.id LIMIT ")
            .push_bind(i64::from(limit));
        let rows = query
            .build_query_as::<TagEntityRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Lists languages spoken by active titles in the requested anime partition.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an invalid term or limit, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_language_entities(
        &self,
        term: Option<&str>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogLanguage>, CatalogError> {
        if !(1..=100).contains(&limit)
            || term.is_some_and(|value| value.trim().is_empty() || value.chars().count() > 256)
        {
            return Err(CatalogError::InvalidInput);
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT DISTINCT language.iso_639_1, language.english_name, language.name
               FROM catalog.languages AS language
               JOIN catalog.title_languages AS relation ON relation.language_id = language.iso_639_1
               JOIN catalog.titles AS title ON title.id = relation.title_id
              WHERE title.active AND ",
        );
        query.push(anime_scope.qualified_predicate());
        if let Some(term) = term {
            query
                .push(" AND (lower(public.unaccent(coalesce(language.english_name, ''))) LIKE '%' || lower(public.unaccent(")
                .push_bind(term.trim())
                .push(")) || '%' OR lower(public.unaccent(coalesce(language.name, ''))) LIKE '%' || lower(public.unaccent(")
                .push_bind(term.trim())
                .push(")) || '%' OR language.iso_639_1 = lower(")
                .push_bind(term.trim())
                .push(") )");
        }
        query
            .push(" ORDER BY language.iso_639_1 LIMIT ")
            .push_bind(i64::from(limit));
        let rows = query
            .build_query_as::<LanguageEntityRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        Ok(rows
            .into_iter()
            .map(|row| CatalogLanguage {
                iso_639_1: row.iso_639_1,
                english_name: row.english_name,
                name: row.name,
                is_original: false,
            })
            .collect())
    }

    /// Reads title and episode credits while enforcing title anime isolation.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for an unsupported scope, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_credits(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogCredit>>, CatalogError> {
        let statement = format!(
            "SELECT person.id AS person_id, person.name, person.known_for_department,
                    person.gender, person.biography, person.birthday, person.deathday,
                    person.place_of_birth, person.homepage, person.imdb_id, person.adult,
                    person.popularity, person.profile_path, credit.credit_id,
                    credit.credit_type, credit.department, credit.job, credit.character,
                    credit.cast_order, credit.episode_count
               FROM catalog.titles AS title
               JOIN catalog.title_credits AS credit ON credit.title_id = title.id
               JOIN catalog.people AS person ON person.id = credit.person_id
              WHERE title.media_type = $1 AND title.tmdb_id = $2 AND title.active AND {scope}
              ORDER BY credit.cast_order NULLS LAST, person.id, credit.credit_id",
            scope = anime_scope.predicate(),
        );
        let rows = sqlx::query_as::<_, CreditRow>(sqlx::AssertSqlSafe(statement))
            .bind(key.media_type().to_string())
            .bind(i64::from(key.tmdb_id().get()))
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        if rows.is_empty() {
            let title_exists: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT EXISTS (SELECT 1 FROM catalog.titles WHERE media_type = $1 AND tmdb_id = $2 AND active AND {})", anime_scope.predicate())))
                .bind(key.media_type().to_string()).bind(i64::from(key.tmdb_id().get())).fetch_one(&self.pool).await.map_err(|_| CatalogError::Query)?;
            return Ok(title_exists.then(Vec::new));
        }
        Ok(Some(rows.into_iter().map(CreditRow::into_credit).collect()))
    }

    /// Lists TV seasons for a scope-isolated title.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for a non-TV key, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_seasons(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogSeason>>, CatalogError> {
        if key.media_type() != MediaType::Tv {
            return Err(CatalogError::InvalidInput);
        }
        let statement = format!(
            "SELECT season.id, season.title_id, season.season_number, season.name,
                    season.overview, season.air_date, season.episode_count, season.poster_path
               FROM catalog.titles AS title JOIN catalog.seasons AS season ON season.title_id = title.id
              WHERE title.media_type = 'tv' AND title.tmdb_id = $1 AND title.active AND {scope}
              ORDER BY season.season_number, season.id",
            scope = anime_scope.predicate(),
        );
        let rows = sqlx::query_as::<_, SeasonRow>(sqlx::AssertSqlSafe(statement))
            .bind(i64::from(key.tmdb_id().get()))
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        if !rows.is_empty() {
            return Ok(Some(rows.into_iter().map(Into::into).collect()));
        }
        let exists: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT EXISTS (SELECT 1 FROM catalog.titles WHERE media_type = 'tv' AND tmdb_id = $1 AND active AND {})", anime_scope.predicate()))).bind(i64::from(key.tmdb_id().get())).fetch_one(&self.pool).await.map_err(|_| CatalogError::Query)?;
        Ok(exists.then(Vec::new))
    }

    /// Lists episodes for one TV season.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for a non-TV key or invalid season,
    /// or [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_episodes(
        &self,
        key: TitleKey,
        season_number: i32,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogEpisode>>, CatalogError> {
        if key.media_type() != MediaType::Tv || !(0..=1000).contains(&season_number) {
            return Err(CatalogError::InvalidInput);
        }
        let statement = format!(
            "SELECT episode.id, episode.season_id, episode.title_id, episode.episode_number,
                    episode.name, episode.overview, episode.air_date, episode.runtime_minutes,
                    episode.still_path, episode.vote_average, episode.vote_count
               FROM catalog.titles AS title JOIN catalog.seasons AS season ON season.title_id = title.id
               JOIN catalog.episodes AS episode ON episode.season_id = season.id
              WHERE title.media_type = 'tv' AND title.tmdb_id = $1 AND season.season_number = $2
                AND title.active AND {scope}
              ORDER BY episode.episode_number, episode.id",
            scope = anime_scope.predicate(),
        );
        let rows = sqlx::query_as::<_, EpisodeRow>(sqlx::AssertSqlSafe(statement))
            .bind(i64::from(key.tmdb_id().get()))
            .bind(season_number)
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        if !rows.is_empty() {
            return Ok(Some(rows.into_iter().map(Into::into).collect()));
        }
        let exists: bool = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT EXISTS (SELECT 1 FROM catalog.titles AS title JOIN catalog.seasons AS season ON season.title_id = title.id WHERE title.media_type = 'tv' AND title.tmdb_id = $1 AND season.season_number = $2 AND title.active AND {})", anime_scope.predicate()))).bind(i64::from(key.tmdb_id().get())).bind(season_number).fetch_one(&self.pool).await.map_err(|_| CatalogError::Query)?;
        Ok(exists.then(Vec::new))
    }

    /// Lists title-owned image metadata without exposing image bytes.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_images(
        &self,
        key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogImageAsset>>, CatalogError> {
        let title_id: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT id FROM catalog.titles WHERE media_type = $1 AND tmdb_id = $2 AND active AND {}", anime_scope.predicate()))).bind(key.media_type().to_string()).bind(i64::from(key.tmdb_id().get())).fetch_optional(&self.pool).await.map_err(|_| CatalogError::Query)?;
        let Some(title_id) = title_id else {
            return Ok(None);
        };
        let rows = sqlx::query_as::<_, ImageRow>("SELECT id, image_kind, source, source_key, source_url, storage_path, mime_type, width, height, file_size_bytes, sha256, status, iso_639_1 FROM assets.image_assets WHERE title_id = $1 ORDER BY image_kind, id").bind(title_id).fetch_all(&self.pool).await.map_err(|_| CatalogError::Query)?;
        Ok(Some(rows.into_iter().map(Into::into).collect()))
    }

    /// Lists active titles in deterministic popularity order with optional indexed filters.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for invalid filters or limits, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_popular_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<PopularCursor>,
    ) -> Result<CatalogPage, CatalogError> {
        filters.validate()?;
        if filters.is_empty() {
            return self
                .list_popular(media_type, anime_scope, limit, after)
                .await;
        }
        if !(1..=100).contains(&limit) {
            return Err(CatalogError::InvalidInput);
        }
        let mut query = QueryBuilder::<Postgres>::new(TITLE_SELECT_FROM);
        push_scope_and_media(&mut query, anime_scope, media_type);
        push_catalog_filters(&mut query, filters);
        if let Some(cursor) = after {
            query
                .push(" AND (coalesce(title.popularity, 0::double precision), title.id) < (")
                .push_bind(cursor.popularity())
                .push(", ")
                .push_bind(cursor.title_id())
                .push(")");
        }
        query
            .push(" ORDER BY coalesce(title.popularity, 0::double precision) DESC, title.id DESC LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let mut rows = query
            .build_query_as::<TitleRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        let has_next = rows.len() > usize::from(limit);
        rows.truncate(usize::from(limit));
        let items = rows
            .into_iter()
            .map(TitleRow::try_into_title)
            .collect::<Result<Vec<_>, _>>()?;
        let next = has_next.then(|| {
            items.last().and_then(|item| {
                PopularCursor::try_new(item.popularity.unwrap_or(0.0), item.id).ok()
            })
        });
        Ok(CatalogPage {
            items,
            next: next.flatten(),
        })
    }

    /// Lists titles by rating with optional indexed filters.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for invalid filters or limits, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_top_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<TopCursor>,
    ) -> Result<CatalogTopPage, CatalogError> {
        filters.validate()?;
        if filters.is_empty() {
            return self.list_top(media_type, anime_scope, limit, after).await;
        }
        if !(1..=100).contains(&limit) {
            return Err(CatalogError::InvalidInput);
        }
        let mut query = QueryBuilder::<Postgres>::new(TITLE_SELECT_FROM);
        push_scope_and_media(&mut query, anime_scope, media_type);
        query.push(" AND title.vote_average IS NOT NULL AND title.vote_count IS NOT NULL");
        push_catalog_filters(&mut query, filters);
        if let Some(cursor) = after {
            query
                .push(" AND (title.vote_average, title.vote_count, title.id) < (")
                .push_bind(cursor.vote_average())
                .push(", ")
                .push_bind(cursor.vote_count())
                .push(", ")
                .push_bind(cursor.title_id())
                .push(")");
        }
        query
            .push(" ORDER BY title.vote_average DESC, title.vote_count DESC, title.id DESC LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let mut rows = query
            .build_query_as::<TitleRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        let has_next = rows.len() > usize::from(limit);
        rows.truncate(usize::from(limit));
        let items = rows
            .into_iter()
            .map(TitleRow::try_into_title)
            .collect::<Result<Vec<_>, _>>()?;
        let next = has_next.then(|| {
            items
                .last()
                .and_then(|item| match (item.vote_average, item.vote_count) {
                    (Some(average), Some(count)) => {
                        TopCursor::try_new(average, count, item.id).ok()
                    }
                    _ => None,
                })
        });
        Ok(CatalogTopPage {
            items,
            next: next.flatten(),
        })
    }

    /// Lists recent titles with optional indexed filters.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for invalid filters or limits, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_recent_filtered(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
        after: Option<RecentCursor>,
    ) -> Result<CatalogRecentPage, CatalogError> {
        filters.validate()?;
        if filters.is_empty() {
            return self
                .list_recent(media_type, anime_scope, limit, after)
                .await;
        }
        if !(1..=100).contains(&limit) {
            return Err(CatalogError::InvalidInput);
        }
        let date_expression = "CASE WHEN title.media_type = 'movie' THEN title.release_date ELSE title.first_air_date END";
        let mut query = QueryBuilder::<Postgres>::new(TITLE_SELECT_FROM);
        push_scope_and_media(&mut query, anime_scope, media_type);
        query
            .push(" AND (")
            .push(date_expression)
            .push(") IS NOT NULL");
        push_catalog_filters(&mut query, filters);
        if let Some(cursor) = after {
            query
                .push(" AND (")
                .push(date_expression)
                .push(", title.id) < (")
                .push_bind(cursor.date())
                .push(", ")
                .push_bind(cursor.title_id())
                .push(")");
        }
        query
            .push(" ORDER BY ")
            .push(date_expression)
            .push(" DESC, title.id DESC LIMIT ")
            .push_bind(i64::from(limit) + 1);
        let mut rows = query
            .build_query_as::<TitleRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        let has_next = rows.len() > usize::from(limit);
        rows.truncate(usize::from(limit));
        let items = rows
            .into_iter()
            .map(TitleRow::try_into_title)
            .collect::<Result<Vec<_>, _>>()?;
        let next = has_next.then(|| {
            items.last().and_then(|item| {
                item.release_date
                    .and_then(|date| RecentCursor::try_new(date, item.id).ok())
            })
        });
        Ok(CatalogRecentPage {
            items,
            next: next.flatten(),
        })
    }

    /// Searches titles using the maintained projection and optional relationship filters.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for invalid terms, filters, or limits, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn search_filtered(
        &self,
        term: &str,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        filters: &CatalogFilters,
        limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        filters.validate()?;
        if filters.is_empty() {
            return self.search(term, media_type, anime_scope, limit).await;
        }
        let term = term.trim();
        if term.is_empty() || term.chars().count() > 256 || !(1..=100).contains(&limit) {
            return Err(CatalogError::InvalidInput);
        }
        let mut query = QueryBuilder::<Postgres>::new(
            "SELECT title.id, title.media_type, title.tmdb_id, title.display_title,
                    title.original_title, title.overview, title.popularity,
                    title.vote_average, title.vote_count,
                    CASE WHEN title.media_type = 'movie' THEN title.release_date
                         ELSE title.first_air_date END AS release_date,
                    title.is_anime
               FROM catalog.titles AS title
               JOIN search.search_documents AS document
                 ON document.title_id = title.id AND document.locale = ''
              WHERE title.active AND ",
        );
        query.push(anime_scope.qualified_predicate());
        if let Some(media_type) = media_type {
            query
                .push(" AND title.media_type = ")
                .push_bind(media_type.to_string());
        }
        push_catalog_filters(&mut query, filters);
        query
            .push(" AND (document.search_vector @@ websearch_to_tsquery('simple', public.unaccent(")
            .push_bind(term)
            .push(")) OR document.normalized_title % lower(public.unaccent(")
            .push_bind(term)
            .push(")) OR document.normalized_original_title % lower(public.unaccent(")
            .push_bind(term)
            .push(")) OR document.normalized_aliases % lower(public.unaccent(")
            .push_bind(term)
            .push("))) ORDER BY CASE WHEN document.normalized_title = lower(public.unaccent(")
            .push_bind(term)
            .push(")) THEN 0 WHEN left(document.normalized_title, char_length(lower(public.unaccent(")
            .push_bind(term)
            .push(")))) = lower(public.unaccent(")
            .push_bind(term)
            .push(")) THEN 1 ELSE 2 END, ts_rank_cd(document.search_vector, websearch_to_tsquery('simple', public.unaccent(")
            .push_bind(term)
            .push("))) DESC, greatest(similarity(document.normalized_title, lower(public.unaccent(")
            .push_bind(term)
            .push("))), similarity(document.normalized_original_title, lower(public.unaccent(")
            .push_bind(term)
            .push(")))) DESC, coalesce(title.popularity, 0::double precision) DESC, title.id DESC LIMIT ")
            .push_bind(i64::from(limit));
        let rows = query
            .build_query_as::<TitleRow>()
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;
        rows.into_iter().map(TitleRow::try_into_title).collect()
    }

    /// Lists active titles in deterministic popularity order with keyset pagination.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] when `limit` is outside `1..=100`, or
    /// [`CatalogError::Query`] when the database rejects the read.
    pub async fn list_popular(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<PopularCursor>,
    ) -> Result<CatalogPage, CatalogError> {
        if !(1..=100).contains(&limit) {
            return Err(CatalogError::InvalidInput);
        }
        let media_filter = match media_type {
            Some(MediaType::Movie) => " AND media_type = 'movie'",
            Some(MediaType::Tv) => " AND media_type = 'tv'",
            None => "",
        };
        let select = "SELECT id, media_type, tmdb_id, display_title, original_title, overview,
                    popularity, vote_average, vote_count,
                    CASE WHEN media_type = 'movie' THEN release_date ELSE first_air_date END
                        AS release_date,
                    is_anime
               FROM catalog.titles";
        let where_clause = format!(
            " WHERE active AND {}{}",
            anime_scope.predicate(),
            media_filter
        );
        let fetch_limit = i64::from(limit) + 1;
        let mut rows = if let Some(cursor) = after {
            let statement = format!(
                "{select}{where_clause}
                 AND (coalesce(popularity, 0::double precision), id) < ($1, $2)
                 ORDER BY coalesce(popularity, 0::double precision) DESC, id DESC
                 LIMIT $3"
            );
            // The only interpolated fragments come from the exhaustive enum matches above;
            // all user input remains a prepared bind parameter.
            sqlx::query_as::<_, TitleRow>(sqlx::AssertSqlSafe(statement))
                .bind(cursor.popularity)
                .bind(cursor.title_id)
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|_| CatalogError::Query)?
        } else {
            let statement = format!(
                "{select}{where_clause}
                 ORDER BY coalesce(popularity, 0::double precision) DESC, id DESC
                 LIMIT $1"
            );
            sqlx::query_as::<_, TitleRow>(sqlx::AssertSqlSafe(statement))
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|_| CatalogError::Query)?
        };

        let has_next = rows.len() > usize::from(limit);
        rows.truncate(usize::from(limit));
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(row.try_into_title()?);
        }
        let next = has_next.then(|| {
            items.last().and_then(|item| {
                PopularCursor::try_new(item.popularity.unwrap_or(0.0), item.id).ok()
            })
        });
        Ok(CatalogPage {
            items,
            next: next.flatten(),
        })
    }

    /// Lists titles by rating, vote count, and stable ID with keyset pagination.
    ///
    /// Titles without both a vote average and vote count are omitted because they cannot have
    /// a stable three-component cursor.  This keeps a top-rated route deterministic while
    /// allowing the popularity route to retain its separate ranking semantics.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] when `limit` is outside `1..=100`, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_top(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<TopCursor>,
    ) -> Result<CatalogTopPage, CatalogError> {
        if !(1..=100).contains(&limit) {
            return Err(CatalogError::InvalidInput);
        }
        let media_filter = match media_type {
            Some(MediaType::Movie) => " AND media_type = 'movie'",
            Some(MediaType::Tv) => " AND media_type = 'tv'",
            None => "",
        };
        let where_clause = format!(
            " WHERE active AND {scope}{media_filter}
                AND vote_average IS NOT NULL
                AND vote_count IS NOT NULL",
            scope = anime_scope.predicate(),
        );
        let select = "SELECT id, media_type, tmdb_id, display_title, original_title, overview,
                    popularity, vote_average, vote_count,
                    CASE WHEN media_type = 'movie' THEN release_date ELSE first_air_date END
                        AS release_date,
                    is_anime
               FROM catalog.titles";
        let fetch_limit = i64::from(limit) + 1;
        let mut rows = if let Some(cursor) = after {
            let statement = format!(
                "{select}{where_clause}
                 AND (vote_average, vote_count, id) < ($1, $2, $3)
                 ORDER BY vote_average DESC, vote_count DESC, id DESC
                 LIMIT $4"
            );
            // All interpolated fragments are exhaustive constants from enum matches; scores,
            // counts, IDs, and limits remain prepared bind parameters.
            sqlx::query_as::<_, TitleRow>(sqlx::AssertSqlSafe(statement))
                .bind(cursor.vote_average)
                .bind(cursor.vote_count)
                .bind(cursor.title_id)
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|_| CatalogError::Query)?
        } else {
            let statement = format!(
                "{select}{where_clause}
                 ORDER BY vote_average DESC, vote_count DESC, id DESC
                 LIMIT $1"
            );
            sqlx::query_as::<_, TitleRow>(sqlx::AssertSqlSafe(statement))
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|_| CatalogError::Query)?
        };

        let has_next = rows.len() > usize::from(limit);
        rows.truncate(usize::from(limit));
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(row.try_into_title()?);
        }
        let next = has_next.then(|| {
            items
                .last()
                .and_then(|item| match (item.vote_average, item.vote_count) {
                    (Some(vote_average), Some(vote_count)) => {
                        TopCursor::try_new(vote_average, vote_count, item.id).ok()
                    }
                    _ => None,
                })
        });
        Ok(CatalogTopPage {
            items,
            next: next.flatten(),
        })
    }

    /// Lists active titles by release date (movies) or first-air date (TV) with keyset
    /// pagination.  Titles with no applicable date are omitted because they cannot have a
    /// stable recent cursor.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] when `limit` is outside `1..=100`, or
    /// [`CatalogError::Query`] when `PostgreSQL` rejects the read.
    pub async fn list_recent(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        after: Option<RecentCursor>,
    ) -> Result<CatalogRecentPage, CatalogError> {
        if !(1..=100).contains(&limit) {
            return Err(CatalogError::InvalidInput);
        }
        let media_filter = match media_type {
            Some(MediaType::Movie) => " AND media_type = 'movie'",
            Some(MediaType::Tv) => " AND media_type = 'tv'",
            None => "",
        };
        let date_expression =
            "CASE WHEN media_type = 'movie' THEN release_date ELSE first_air_date END";
        let where_clause = format!(
            " WHERE active AND {scope}{media_filter}
                AND ({date_expression}) IS NOT NULL",
            scope = anime_scope.predicate(),
        );
        let select = format!(
            "SELECT id, media_type, tmdb_id, display_title, original_title, overview,
                    popularity, vote_average, vote_count,
                    {date_expression} AS release_date,
                    is_anime
               FROM catalog.titles",
        );
        let fetch_limit = i64::from(limit) + 1;
        let mut rows = if let Some(cursor) = after {
            let statement = format!(
                "{select}{where_clause}
                 AND ({date_expression}, id) < ($1, $2)
                 ORDER BY {date_expression} DESC, id DESC
                 LIMIT $3",
            );
            // All interpolated fragments are exhaustive constants from enum matches; dates and
            // limits remain prepared bind parameters.
            sqlx::query_as::<_, TitleRow>(sqlx::AssertSqlSafe(statement))
                .bind(cursor.date)
                .bind(cursor.title_id)
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|_| CatalogError::Query)?
        } else {
            let statement = format!(
                "{select}{where_clause}
                 ORDER BY {date_expression} DESC, id DESC
                 LIMIT $1",
            );
            sqlx::query_as::<_, TitleRow>(sqlx::AssertSqlSafe(statement))
                .bind(fetch_limit)
                .fetch_all(&self.pool)
                .await
                .map_err(|_| CatalogError::Query)?
        };

        let has_next = rows.len() > usize::from(limit);
        rows.truncate(usize::from(limit));
        let mut items = Vec::with_capacity(rows.len());
        for row in rows {
            items.push(row.try_into_title()?);
        }
        let next = has_next.then(|| {
            items.last().and_then(|item| {
                item.release_date
                    .and_then(|date| RecentCursor::try_new(date, item.id).ok())
            })
        });
        Ok(CatalogRecentPage {
            items,
            next: next.flatten(),
        })
    }

    /// Searches active titles using the maintained FTS/trigram projection.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::InvalidInput`] for empty/oversized terms or a page limit
    /// outside `1..=100`, and [`CatalogError::Query`] when the database rejects the read.
    pub async fn search(
        &self,
        term: &str,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        let term = term.trim();
        if term.is_empty() || term.chars().count() > 256 || !(1..=100).contains(&limit) {
            return Err(CatalogError::InvalidInput);
        }
        let media_filter = match media_type {
            Some(MediaType::Movie) => " AND title.media_type = 'movie'",
            Some(MediaType::Tv) => " AND title.media_type = 'tv'",
            None => "",
        };
        let statement = format!(
            "SELECT title.id, title.media_type, title.tmdb_id, title.display_title,
                    title.original_title, title.overview, title.popularity,
                    title.vote_average, title.vote_count,
                    CASE WHEN title.media_type = 'movie' THEN title.release_date
                         ELSE title.first_air_date END AS release_date,
                    title.is_anime
               FROM catalog.titles AS title
               JOIN search.search_documents AS document
                 ON document.title_id = title.id AND document.locale = ''
              WHERE title.active
                AND {anime_predicate}
                {media_filter}
                AND (
                    document.search_vector @@ websearch_to_tsquery('simple', public.unaccent($1))
                    OR document.normalized_title % lower(public.unaccent($1))
                    OR document.normalized_original_title % lower(public.unaccent($1))
                    OR document.normalized_aliases % lower(public.unaccent($1))
                )
              ORDER BY
                CASE
                    WHEN document.normalized_title = lower(public.unaccent($1)) THEN 0
                    WHEN left(
                        document.normalized_title,
                        char_length(lower(public.unaccent($1)))
                    ) = lower(public.unaccent($1)) THEN 1
                    ELSE 2
                END,
                ts_rank_cd(
                    document.search_vector,
                    websearch_to_tsquery('simple', public.unaccent($1))
                ) DESC,
                greatest(
                    similarity(document.normalized_title, lower(public.unaccent($1))),
                    similarity(document.normalized_original_title, lower(public.unaccent($1)))
                ) DESC,
                coalesce(title.popularity, 0::double precision) DESC,
                title.id DESC
              LIMIT $2",
            anime_predicate = anime_scope.predicate(),
        );
        // The scope/media fragments are exhaustive constants; term and limit are binds.
        let rows = sqlx::query_as::<_, TitleRow>(sqlx::AssertSqlSafe(statement))
            .bind(term)
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await
            .map_err(|_| CatalogError::Query)?;

        rows.into_iter().map(TitleRow::try_into_title).collect()
    }
}

const TITLE_SELECT_FROM: &str = "SELECT title.id, title.media_type, title.tmdb_id, title.display_title, title.original_title, title.overview, title.popularity, title.vote_average, title.vote_count, CASE WHEN title.media_type = 'movie' THEN title.release_date ELSE title.first_air_date END AS release_date, title.is_anime FROM catalog.titles AS title WHERE title.active AND ";

fn push_scope_and_media(
    query: &mut QueryBuilder<Postgres>,
    anime_scope: AnimeScope,
    media_type: Option<MediaType>,
) {
    query.push(anime_scope.qualified_predicate());
    if let Some(media_type) = media_type {
        query
            .push(" AND title.media_type = ")
            .push_bind(media_type.to_string());
    }
}

fn push_catalog_filters(query: &mut QueryBuilder<Postgres>, filters: &CatalogFilters) {
    if let Some(id) = filters.genre_id {
        query
            .push(" AND EXISTS (SELECT 1 FROM catalog.title_genres AS filter_genre WHERE filter_genre.title_id = title.id AND filter_genre.genre_id = ")
            .push_bind(id)
            .push(")");
    }
    if let Some(id) = filters.keyword_id {
        query
            .push(" AND EXISTS (SELECT 1 FROM catalog.title_keywords AS filter_keyword WHERE filter_keyword.title_id = title.id AND filter_keyword.keyword_id = ")
            .push_bind(id)
            .push(")");
    }
    if let Some(id) = filters.tag_id {
        query
            .push(" AND EXISTS (SELECT 1 FROM catalog.title_tags AS filter_tag WHERE filter_tag.title_id = title.id AND filter_tag.tag_id = ")
            .push_bind(id)
            .push(")");
    }
    if let Some(language) = &filters.language {
        query
            .push(" AND EXISTS (SELECT 1 FROM catalog.title_languages AS filter_language WHERE filter_language.title_id = title.id AND filter_language.language_id = ")
            .push_bind(language)
            .push(")");
    }
    if let Some(id) = filters.person_id {
        query
            .push(" AND EXISTS (SELECT 1 FROM catalog.title_credits AS filter_credit WHERE filter_credit.title_id = title.id AND filter_credit.person_id = ")
            .push_bind(id)
            .push(")");
    }
    if let Some(id) = filters.company_id {
        query
            .push(" AND EXISTS (SELECT 1 FROM catalog.title_companies AS filter_company WHERE filter_company.title_id = title.id AND filter_company.company_id = ")
            .push_bind(id)
            .push(")");
    }
    if let Some(id) = filters.network_id {
        query
            .push(" AND EXISTS (SELECT 1 FROM catalog.title_networks AS filter_network WHERE filter_network.title_id = title.id AND filter_network.network_id = ")
            .push_bind(id)
            .push(")");
    }
    if let Some(runtime_min) = filters.runtime_min {
        query
            .push(" AND title.runtime_minutes >= ")
            .push_bind(runtime_min);
    }
    if let Some(runtime_max) = filters.runtime_max {
        query
            .push(" AND title.runtime_minutes <= ")
            .push_bind(runtime_max);
    }
    if let Some(year) = filters.year
        && let (Some(start), Some(end)) = (
            NaiveDate::from_ymd_opt(year, 1, 1),
            NaiveDate::from_ymd_opt(year + 1, 1, 1),
        )
    {
        query
                .push(" AND (CASE WHEN title.media_type = 'movie' THEN title.release_date ELSE title.first_air_date END) >= ")
                .push_bind(start)
                .push(" AND (CASE WHEN title.media_type = 'movie' THEN title.release_date ELSE title.first_air_date END) < ")
                .push_bind(end);
    }
    if let Some(status) = &filters.status {
        query.push(" AND title.status = ").push_bind(status);
    }
}

#[derive(Debug, FromRow)]
struct CompanyRow {
    id: i64,
    name: Option<String>,
    origin_country: Option<String>,
    logo_path: Option<String>,
}

#[derive(Debug, FromRow)]
struct GenreEntityRow {
    id: i64,
    name: Option<String>,
}

impl From<GenreEntityRow> for CatalogGenre {
    fn from(row: GenreEntityRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
        }
    }
}

#[derive(Debug, FromRow)]
struct KeywordEntityRow {
    id: i64,
    name: Option<String>,
}

impl From<KeywordEntityRow> for CatalogKeyword {
    fn from(row: KeywordEntityRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
        }
    }
}

#[derive(Debug, FromRow)]
struct TagEntityRow {
    id: i64,
    name: String,
}

impl From<TagEntityRow> for CatalogTag {
    fn from(row: TagEntityRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
        }
    }
}

#[derive(Debug, FromRow)]
struct LanguageEntityRow {
    iso_639_1: String,
    english_name: Option<String>,
    name: Option<String>,
}
#[derive(Debug, FromRow)]
struct NetworkRow {
    id: i64,
    name: Option<String>,
    origin_country: Option<String>,
    logo_path: Option<String>,
}
#[derive(Debug, FromRow)]
struct CollectionRow {
    id: i64,
    name: Option<String>,
    poster_path: Option<String>,
    backdrop_path: Option<String>,
}
impl From<CollectionRow> for CatalogCollection {
    fn from(row: CollectionRow) -> Self {
        Self {
            id: row.id,
            name: row.name,
            poster_path: row.poster_path,
            backdrop_path: row.backdrop_path,
        }
    }
}

#[derive(Debug, FromRow)]
struct PersonRow {
    id: i64,
    name: Option<String>,
    known_for_department: Option<String>,
    gender: Option<i16>,
    biography: Option<String>,
    birthday: Option<NaiveDate>,
    deathday: Option<NaiveDate>,
    place_of_birth: Option<String>,
    homepage: Option<String>,
    imdb_id: Option<String>,
    adult: bool,
    popularity: Option<f64>,
    profile_path: Option<String>,
}
impl PersonRow {
    fn into_person(self) -> CatalogPerson {
        CatalogPerson {
            id: self.id,
            name: self.name,
            known_for_department: self.known_for_department,
            gender: self.gender,
            biography: self.biography,
            birthday: self.birthday,
            deathday: self.deathday,
            place_of_birth: self.place_of_birth,
            homepage: self.homepage,
            imdb_id: self.imdb_id,
            adult: self.adult,
            popularity: self.popularity,
            profile_path: self.profile_path,
        }
    }
}

#[derive(Debug, FromRow)]
struct CreditRow {
    person_id: i64,
    name: Option<String>,
    known_for_department: Option<String>,
    gender: Option<i16>,
    biography: Option<String>,
    birthday: Option<NaiveDate>,
    deathday: Option<NaiveDate>,
    place_of_birth: Option<String>,
    homepage: Option<String>,
    imdb_id: Option<String>,
    adult: bool,
    popularity: Option<f64>,
    profile_path: Option<String>,
    credit_id: String,
    credit_type: String,
    department: Option<String>,
    job: Option<String>,
    character: Option<String>,
    cast_order: Option<i32>,
    episode_count: Option<i32>,
}
impl CreditRow {
    fn into_credit(self) -> CatalogCredit {
        CatalogCredit {
            credit_id: self.credit_id,
            person: CatalogPerson {
                id: self.person_id,
                name: self.name,
                known_for_department: self.known_for_department,
                gender: self.gender,
                biography: self.biography,
                birthday: self.birthday,
                deathday: self.deathday,
                place_of_birth: self.place_of_birth,
                homepage: self.homepage,
                imdb_id: self.imdb_id,
                adult: self.adult,
                popularity: self.popularity,
                profile_path: self.profile_path,
            },
            credit_type: self.credit_type,
            department: self.department,
            job: self.job,
            character: self.character,
            cast_order: self.cast_order,
            episode_count: self.episode_count,
        }
    }
}

#[derive(Debug, FromRow)]
struct SeasonRow {
    id: i64,
    title_id: i64,
    season_number: i32,
    name: Option<String>,
    overview: Option<String>,
    air_date: Option<NaiveDate>,
    episode_count: Option<i32>,
    poster_path: Option<String>,
}
impl From<SeasonRow> for CatalogSeason {
    fn from(row: SeasonRow) -> Self {
        Self {
            id: row.id,
            title_id: row.title_id,
            season_number: row.season_number,
            name: row.name,
            overview: row.overview,
            air_date: row.air_date,
            episode_count: row.episode_count,
            poster_path: row.poster_path,
        }
    }
}

#[derive(Debug, FromRow)]
struct EpisodeRow {
    id: i64,
    season_id: i64,
    title_id: i64,
    episode_number: i32,
    name: Option<String>,
    overview: Option<String>,
    air_date: Option<NaiveDate>,
    runtime_minutes: Option<i32>,
    still_path: Option<String>,
    vote_average: Option<f64>,
    vote_count: Option<i64>,
}
impl From<EpisodeRow> for CatalogEpisode {
    fn from(row: EpisodeRow) -> Self {
        Self {
            id: row.id,
            season_id: row.season_id,
            title_id: row.title_id,
            episode_number: row.episode_number,
            name: row.name,
            overview: row.overview,
            air_date: row.air_date,
            runtime_minutes: row.runtime_minutes,
            still_path: row.still_path,
            vote_average: row.vote_average,
            vote_count: row.vote_count,
        }
    }
}

#[derive(Debug, FromRow)]
struct ImageRow {
    id: i64,
    image_kind: String,
    source: String,
    source_key: String,
    source_url: Option<String>,
    storage_path: Option<String>,
    mime_type: Option<String>,
    width: Option<i32>,
    height: Option<i32>,
    file_size_bytes: Option<i64>,
    sha256: Option<String>,
    status: String,
    iso_639_1: Option<String>,
}
impl From<ImageRow> for CatalogImageAsset {
    fn from(row: ImageRow) -> Self {
        Self {
            id: row.id,
            image_kind: row.image_kind,
            source: row.source,
            source_key: row.source_key,
            source_url: row.source_url,
            storage_path: row.storage_path,
            mime_type: row.mime_type,
            width: row.width,
            height: row.height,
            file_size_bytes: row.file_size_bytes,
            sha256: row.sha256,
            status: row.status,
            iso_639_1: row.iso_639_1,
        }
    }
}

#[derive(Debug, FromRow)]
struct DetailRow {
    id: i64,
    media_type: String,
    tmdb_id: i64,
    display_title: Option<String>,
    original_title: Option<String>,
    overview: Option<String>,
    popularity: Option<f64>,
    vote_average: Option<f64>,
    vote_count: Option<i64>,
    release_date: Option<NaiveDate>,
    is_anime: bool,
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
    movie_title_id: Option<i64>,
    movie_budget: Option<i64>,
    movie_revenue: Option<i64>,
    movie_runtime_minutes: Option<i32>,
    movie_imdb_id: Option<String>,
    movie_collection_id: Option<i64>,
    tv_title_id: Option<i64>,
    tv_in_production: Option<bool>,
    tv_number_of_episodes: Option<i32>,
    tv_number_of_seasons: Option<i32>,
    tv_series_type: Option<String>,
    genres: Json<Vec<CatalogGenre>>,
    keywords: Json<Vec<CatalogKeyword>>,
    tags: Json<Vec<CatalogTag>>,
    languages: Json<Vec<CatalogLanguage>>,
    companies: Json<Vec<CatalogCompany>>,
    networks: Json<Vec<CatalogNetwork>>,
}

impl DetailRow {
    fn try_into_detail(self) -> Result<CatalogDetail, CatalogError> {
        let title = TitleRow {
            id: self.id,
            media_type: self.media_type,
            tmdb_id: self.tmdb_id,
            display_title: self.display_title,
            original_title: self.original_title,
            overview: self.overview,
            popularity: self.popularity,
            vote_average: self.vote_average,
            vote_count: self.vote_count,
            release_date: self.release_date,
            is_anime: self.is_anime,
        }
        .try_into_title()?;
        let movie = self.movie_title_id.map(|_| CatalogMovieDetails {
            budget: self.movie_budget,
            revenue: self.movie_revenue,
            runtime_minutes: self.movie_runtime_minutes,
            imdb_id: self.movie_imdb_id,
            collection_id: self.movie_collection_id,
        });
        let tv = self.tv_title_id.map(|_| CatalogTvDetails {
            in_production: self.tv_in_production,
            number_of_episodes: self.tv_number_of_episodes,
            number_of_seasons: self.tv_number_of_seasons,
            series_type: self.tv_series_type,
        });
        Ok(CatalogDetail {
            title,
            movie,
            tv,
            tagline: self.tagline,
            status: self.status,
            original_language: self.original_language,
            last_air_date: self.last_air_date,
            runtime_minutes: self.runtime_minutes,
            adult: self.adult,
            video: self.video,
            homepage: self.homepage,
            poster_path: self.poster_path,
            backdrop_path: self.backdrop_path,
            source_updated_at: self.source_updated_at,
            facets: CatalogFacets {
                genres: self.genres.0,
                keywords: self.keywords.0,
                tags: self.tags.0,
                languages: self.languages.0,
                companies: self.companies.0,
                networks: self.networks.0,
            },
        })
    }
}

#[derive(Debug, FromRow)]
struct TitleRow {
    id: i64,
    media_type: String,
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

impl TitleRow {
    fn try_into_title(self) -> Result<CatalogTitle, CatalogError> {
        let media_type = MediaType::from_str(&self.media_type).map_err(|_| CatalogError::Query)?;
        Ok(CatalogTitle {
            id: self.id,
            media_type,
            tmdb_id: self.tmdb_id,
            display_title: self.display_title,
            original_title: self.original_title,
            overview: self.overview,
            popularity: self.popularity,
            vote_average: self.vote_average,
            vote_count: self.vote_count,
            release_date: self.release_date,
            is_anime: self.is_anime,
        })
    }
}
