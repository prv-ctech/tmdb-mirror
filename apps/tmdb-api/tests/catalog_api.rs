use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::Value;
use tmdb_api::{ApiState, CatalogApiStore, ReadinessProbe, build_catalog_router, build_router};
use tmdb_db::{
    AnimeScope, CatalogDetail, CatalogError, CatalogImageAsset, CatalogMovieDetails, CatalogPage,
    CatalogRecentPage, CatalogTitle, CatalogTopPage, PopularCursor, RecentCursor, TopCursor,
};
use tmdb_domain::{MediaType, TitleKey};
use tower::ServiceExt;

type ScopeCall = (Option<MediaType>, AnimeScope, u16);
type ScopeCalls = Arc<Mutex<Vec<ScopeCall>>>;
type SearchCall = (String, Option<MediaType>, AnimeScope, u16);
type SearchCalls = Arc<Mutex<Vec<SearchCall>>>;

#[derive(Clone, Debug, Default)]
struct FakeStore {
    popular: ScopeCalls,
    recent: ScopeCalls,
    top: ScopeCalls,
    searches: SearchCalls,
    detail: Arc<Mutex<Option<CatalogDetail>>>,
    images: Arc<Mutex<Option<Vec<CatalogImageAsset>>>>,
}

#[async_trait]
impl CatalogApiStore for FakeStore {
    async fn get_title(&self, _key: TitleKey) -> Result<Option<CatalogTitle>, CatalogError> {
        Ok(None)
    }

    async fn get_detail(
        &self,
        _key: TitleKey,
        _anime_scope: AnimeScope,
    ) -> Result<Option<CatalogDetail>, CatalogError> {
        self.detail
            .lock()
            .map_err(|_| CatalogError::Query)
            .map(|detail| detail.clone())
    }

    async fn list_popular(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        _after: Option<PopularCursor>,
    ) -> Result<CatalogPage, CatalogError> {
        self.popular.lock().map_err(|_| CatalogError::Query)?.push((
            media_type,
            anime_scope,
            limit,
        ));
        Ok(CatalogPage {
            items: Vec::new(),
            next: None,
        })
    }

    async fn list_recent(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        _after: Option<RecentCursor>,
    ) -> Result<CatalogRecentPage, CatalogError> {
        self.recent
            .lock()
            .map_err(|_| CatalogError::Query)?
            .push((media_type, anime_scope, limit));
        Ok(CatalogRecentPage {
            items: Vec::new(),
            next: None,
        })
    }

    async fn list_top(
        &self,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
        _after: Option<TopCursor>,
    ) -> Result<CatalogTopPage, CatalogError> {
        self.top
            .lock()
            .map_err(|_| CatalogError::Query)?
            .push((media_type, anime_scope, limit));
        Ok(CatalogTopPage {
            items: Vec::new(),
            next: None,
        })
    }

    async fn search(
        &self,
        term: &str,
        media_type: Option<MediaType>,
        anime_scope: AnimeScope,
        limit: u16,
    ) -> Result<Vec<CatalogTitle>, CatalogError> {
        self.searches
            .lock()
            .map_err(|_| CatalogError::Query)?
            .push((term.to_owned(), media_type, anime_scope, limit));
        Ok(Vec::new())
    }

    async fn list_images(
        &self,
        _key: TitleKey,
        anime_scope: AnimeScope,
    ) -> Result<Option<Vec<CatalogImageAsset>>, CatalogError> {
        match anime_scope {
            AnimeScope::OnlyAnime => self
                .images
                .lock()
                .map_err(|_| CatalogError::Query)
                .map(|images| images.clone()),
            AnimeScope::OnlyNonAnime => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
struct ReadyProbe;

#[async_trait]
impl ReadinessProbe for ReadyProbe {
    async fn check(&self) -> Result<tmdb_db::ReadinessReport, tmdb_api::ProbeError> {
        Ok(tmdb_db::ReadinessReport {
            postgres_major: 18,
            schema_revision: "0015".to_owned(),
            extensions: vec![
                "pg_stat_statements".to_owned(),
                "pg_trgm".to_owned(),
                "unaccent".to_owned(),
            ],
        })
    }
}

fn app(store: &FakeStore) -> Router {
    build_router(ApiState::from_probe(ReadyProbe))
        .merge(build_catalog_router(Arc::new(store.clone())))
}

async fn get(
    app: Router,
    uri: &str,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(app.oneshot(Request::get(uri).body(Body::empty())?).await?)
}

#[tokio::test]
async fn movie_and_tv_lists_always_exclude_anime() -> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    let response = get(app(&store), "/movies?limit=7").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response = get(app(&store), "/tv?limit=8").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let calls = store
        .popular
        .lock()
        .map_err(|_| "store lock poisoned")?
        .clone();
    assert_eq!(
        calls,
        vec![
            (Some(MediaType::Movie), AnimeScope::OnlyNonAnime, 7),
            (Some(MediaType::Tv), AnimeScope::OnlyNonAnime, 8),
        ]
    );
    Ok(())
}

#[tokio::test]
async fn anime_route_reads_both_media_namespaces_with_anime_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    let response = get(app(&store), "/anime?limit=9").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let calls = store
        .popular
        .lock()
        .map_err(|_| "store lock poisoned")?
        .clone();
    assert_eq!(calls, vec![(None, AnimeScope::OnlyAnime, 9)]);
    Ok(())
}

#[tokio::test]
async fn ordinary_search_cannot_broaden_to_anime() -> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    let response = get(app(&store), "/search?q=one+piece&anime=true&limit=11").await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/problem+json")
    );
    assert!(
        store
            .searches
            .lock()
            .map_err(|_| "store lock poisoned")?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn anime_search_uses_only_anime_scope_and_plus_decodes()
-> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    let response = get(app(&store), "/anime?q=one+piece&limit=11").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let calls = store
        .searches
        .lock()
        .map_err(|_| "store lock poisoned")?
        .clone();
    assert_eq!(
        calls,
        vec![("one piece".to_owned(), None, AnimeScope::OnlyAnime, 11)]
    );
    Ok(())
}

#[tokio::test]
async fn anime_image_route_returns_anime_assets_and_ordinary_route_stays_isolated()
-> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    store
        .images
        .lock()
        .map_err(|_| "store lock poisoned")?
        .replace(vec![CatalogImageAsset {
            id: 7,
            image_kind: "poster".to_owned(),
            source: "tmdb".to_owned(),
            source_key: "anime-poster".to_owned(),
            source_url: Some("https://image.tmdb.org/t/p/w500/anime-poster.jpg".to_owned()),
            storage_path: Some("anime/movie/123/cover.jpg".to_owned()),
            mime_type: Some("image/jpeg".to_owned()),
            width: Some(500),
            height: Some(750),
            file_size_bytes: Some(12_345),
            sha256: Some("a".repeat(64)),
            status: "ready".to_owned(),
            iso_639_1: None,
        }]);

    let response = get(app(&store), "/anime/movie/123/images").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1_000_000).await?;
    let json: Value = serde_json::from_slice(&body)?;
    assert_eq!(
        json["data"][0]["url"],
        "https://image.tmdb.org/t/p/w500/anime-poster.jpg"
    );

    let response = get(app(&store), "/movies/123/images").await?;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[tokio::test]
async fn recent_and_top_routes_keep_their_explicit_anime_scope()
-> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    let response = get(app(&store), "/movies/recent?limit=4").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response = get(app(&store), "/anime/recent?limit=5").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response = get(app(&store), "/tv/top-rated?limit=6").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let calls = store
        .recent
        .lock()
        .map_err(|_| "store lock poisoned")?
        .clone();
    assert_eq!(
        calls,
        vec![
            (Some(MediaType::Movie), AnimeScope::OnlyNonAnime, 4),
            (None, AnimeScope::OnlyAnime, 5),
        ]
    );
    let calls = store.top.lock().map_err(|_| "store lock poisoned")?.clone();
    assert_eq!(
        calls,
        vec![(Some(MediaType::Tv), AnimeScope::OnlyNonAnime, 6)]
    );
    Ok(())
}

#[tokio::test]
async fn detail_response_uses_camel_case_api_fields() -> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    store
        .detail
        .lock()
        .map_err(|_| "store lock poisoned")?
        .replace(CatalogDetail {
            title: CatalogTitle {
                id: 41,
                media_type: MediaType::Movie,
                tmdb_id: 123,
                display_title: Some("Example".to_owned()),
                original_title: None,
                overview: None,
                popularity: Some(4.2),
                vote_average: Some(8.1),
                vote_count: Some(42),
                release_date: None,
                is_anime: false,
            },
            movie: Some(CatalogMovieDetails {
                budget: None,
                revenue: None,
                runtime_minutes: Some(120),
                imdb_id: None,
                collection_id: None,
            }),
            tv: None,
            tagline: None,
            status: None,
            original_language: None,
            last_air_date: None,
            runtime_minutes: Some(120),
            adult: false,
            video: false,
            homepage: None,
            poster_path: None,
            backdrop_path: None,
            source_updated_at: Some(chrono::Utc::now()),
            facets: tmdb_db::CatalogFacets::default(),
        });
    let response = get(app(&store), "/movies/123").await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1_000_000).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let data = json.get("data").ok_or("missing detail data")?;
    assert_eq!(data["runtimeMinutes"], 120);
    assert!(data.get("runtime_minutes").is_none());
    assert!(data.get("sourceUpdatedAt").is_some());
    assert!(data.get("source_updated_at").is_none());
    assert_eq!(data["databaseId"], 41);
    Ok(())
}

#[tokio::test]
async fn invalid_limits_and_media_route_anime_flags_are_problem_details()
-> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    for uri in ["/movies?limit=101", "/movies?anime=true", "/movies/0"] {
        let response = get(app(&store), uri).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("application/problem+json")
        );
    }
    Ok(())
}

#[tokio::test]
async fn episode_detail_route_is_registered_and_validates_episode_number()
-> Result<(), Box<dyn std::error::Error>> {
    let store = FakeStore::default();
    let response = get(app(&store), "/tv/1399/seasons/1/episodes/not-a-number").await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/problem+json")
    );
    Ok(())
}
