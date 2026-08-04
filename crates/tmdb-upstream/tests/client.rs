#![allow(clippy::expect_used)]

use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use axum::Router;
use http::{header::AUTHORIZATION, HeaderValue, StatusCode};
use reqwest::Url;
use secrecy::SecretString;
use tempfile::tempdir;
use tmdb_domain::MediaType;
use tmdb_upstream::{
    classify_response, trawl_decision, RateLimitPolicy, ResponseClass, RetryPolicy, TmdbClient,
    TmdbClientError, TrawlDecision,
};
use tokio::sync::Mutex;

#[derive(Clone, Debug)]
struct MockResponse {
    status: StatusCode,
    body: &'static str,
    retry_after: Option<&'static str>,
}

#[derive(Clone, Debug)]
struct MockState {
    responses: Arc<Mutex<VecDeque<MockResponse>>>,
    calls: Arc<AtomicUsize>,
    saw_bearer: Arc<AtomicUsize>,
    uris: Arc<Mutex<Vec<String>>>,
}

async fn mock_handler(State(state): State<MockState>, request: http::Request<Body>) -> Response {
    state.calls.fetch_add(1, Ordering::Relaxed);
    state.uris.lock().await.push(request.uri().to_string());
    if request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        == Some("Bearer unit-token")
    {
        state.saw_bearer.fetch_add(1, Ordering::Relaxed);
    }
    let spec = state
        .responses
        .lock()
        .await
        .pop_front()
        .unwrap_or(MockResponse {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: "no queued response",
            retry_after: None,
        });
    let mut response = Response::new(Body::from(spec.body));
    *response.status_mut() = spec.status;
    if let Some(retry_after) = spec.retry_after {
        response.headers_mut().insert(
            http::header::RETRY_AFTER,
            HeaderValue::from_static(retry_after),
        );
    }
    response
}

async fn mock_server(
    responses: Vec<MockResponse>,
) -> (String, MockState, tokio::task::JoinHandle<()>) {
    let state = MockState {
        responses: Arc::new(Mutex::new(responses.into_iter().collect())),
        calls: Arc::new(AtomicUsize::new(0)),
        saw_bearer: Arc::new(AtomicUsize::new(0)),
        uris: Arc::new(Mutex::new(Vec::new())),
    };
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("test listener address");
    let app = Router::new()
        .fallback(mock_handler)
        .with_state(state.clone());
    let task = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{address}/"), state, task)
}

fn test_policy() -> RetryPolicy {
    RetryPolicy::try_new(
        3,
        RateLimitPolicy::try_new(40, 4).expect("valid test rate policy"),
        Duration::from_secs(2),
        Duration::from_millis(1),
        Duration::from_millis(10),
        1024,
    )
    .expect("valid test retry policy")
}

fn client(base_url: &str) -> TmdbClient {
    TmdbClient::new(base_url, SecretString::from("unit-token"), test_policy())
        .expect("valid test client")
}

#[tokio::test]
async fn successful_details_are_typed_and_authorized() {
    let (base_url, state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: r#"{"id":42,"title":"One Piece","vote_count":100,"keywords":{"keywords":[{"id":210024,"name":"anime"}]},"genres":[{"id":16,"name":"Animation"}]}"#,
        retry_after: None,
    }])
    .await;
    let movie = client(&base_url).fetch_movie(42).await.expect("movie JSON");
    assert_eq!(movie.id, 42);
    assert_eq!(movie.title.as_deref(), Some("One Piece"));
    assert_eq!(
        movie.keywords.first().map(|keyword| keyword.id),
        Some(210_024)
    );
    assert_eq!(movie.genres.first().map(|genre| genre.id), Some(16));
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    assert_eq!(state.saw_bearer.load(Ordering::Relaxed), 1);
    task.abort();
}

#[tokio::test]
async fn television_keyword_results_are_unwrapped() {
    let (base_url, _state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: r#"{"id":42,"name":"One Piece","keywords":{"results":[{"id":210024,"name":"anime"}]},"genres":[{"id":16,"name":"Animation"}]}"#,
        retry_after: None,
    }])
    .await;
    let tv = client(&base_url).fetch_tv(42).await.expect("tv JSON");
    assert_eq!(tv.keywords.first().map(|keyword| keyword.id), Some(210_024));
    assert_eq!(tv.genres.first().map(|genre| genre.id), Some(16));
    task.abort();
}

#[tokio::test]
async fn television_numeric_tvdb_id_is_normalized() {
    let (base_url, _state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: r#"{"id":42,"name":"One Piece","external_ids":{"tvdb_id":309164}}"#,
        retry_after: None,
    }])
    .await;
    let tv = client(&base_url).fetch_tv(42).await.expect("tv JSON");
    assert_eq!(tv.external_ids.tvdb_id.as_deref(), Some("309164"));
    task.abort();
}

#[tokio::test]
async fn gallery_and_video_endpoints_parse_metadata_and_use_bounded_languages() {
    let (base_url, state, task) = mock_server(vec![
        MockResponse {
            status: StatusCode::OK,
            body: r#"{"backdrops":[{"file_path":"/backdrop.jpg","width":1920,"height":1080,"aspect_ratio":1.777,"iso_639_1":null,"vote_average":7.5,"vote_count":4}],"posters":[{"file_path":"/poster.jpg","width":1000,"height":1500,"iso_639_1":"en","vote_average":8.0,"vote_count":5}],"logos":[{"file_path":"/logo.png","width":500,"height":200,"file_type":".svg"}]}"#,
            retry_after: None,
        },
        MockResponse {
            status: StatusCode::OK,
            body: r#"{"results":[{"key":"youtube-key","site":"YouTube","type":"Opening Credits","name":"Intro","official":false,"iso_639_1":"en","iso_3166_1":"US","published_at":"2024-01-01T00:00:00.000Z","size":1080}]}"#,
            retry_after: None,
        },
    ])
    .await;

    let images = client(&base_url)
        .fetch_tv_images(119_495)
        .await
        .expect("typed gallery");
    assert_eq!(images.posters.len(), 1);
    assert_eq!(images.backdrops[0].file_path, "/backdrop.jpg");
    assert_eq!(images.logos[0].file_type.as_deref(), Some(".svg"));

    let videos = client(&base_url)
        .fetch_tv_videos(4_586)
        .await
        .expect("typed videos");
    assert_eq!(
        videos.results[0].video_type.as_deref(),
        Some("Opening Credits")
    );
    assert_eq!(videos.results[0].site.as_deref(), Some("YouTube"));

    let uris = state.uris.lock().await.clone();
    assert!(uris[0].contains("language=en-US"));
    assert!(uris[0].contains("include_image_language=en%2Cnull"));
    assert!(uris[1].contains("include_video_language=en%2Cnull"));
    task.abort();
}

#[tokio::test]
async fn episode_images_allow_specials_season_and_reject_zero_episode() {
    let (base_url, _state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: r#"{"posters":[],"backdrops":[{"file_path":"/still.jpg","width":1280,"height":720}]}"#,
        retry_after: None,
    }])
    .await;
    let images = client(&base_url)
        .fetch_episode_images(119_495, 0, 1)
        .await
        .expect("specials episode gallery");
    assert_eq!(images.backdrops[0].width, 1280);
    assert!(matches!(
        client(&base_url).fetch_episode_images(119_495, 0, 0).await,
        Err(TmdbClientError::InvalidPath)
    ));
    task.abort();
}

#[tokio::test]
async fn season_and_episode_details_use_exact_tmdb_routes_without_appends() {
    let (base_url, state, task) = mock_server(vec![
        MockResponse {
            status: StatusCode::OK,
            body: r#"{"id":900,"season_number":1,"episodes":[{"id":901,"episode_number":1,"name":"Pilot"}]}"#,
            retry_after: None,
        },
        MockResponse {
            status: StatusCode::OK,
            body: r#"{"id":901,"season_number":1,"episode_number":1,"name":"Pilot"}"#,
            retry_after: None,
        },
    ])
    .await;

    let (season_raw, season) = client(&base_url)
        .fetch_season_with_raw(119_495, 1)
        .await
        .expect("season detail");
    assert_eq!(season_raw["id"], 900);
    assert_eq!(season.episodes[0].id, 901);

    let (episode_raw, episode) = client(&base_url)
        .fetch_episode_with_raw(119_495, 1, 1)
        .await
        .expect("episode detail");
    assert_eq!(episode_raw["id"], 901);
    assert_eq!(episode.name.as_deref(), Some("Pilot"));

    let uris = state.uris.lock().await.clone();
    assert_eq!(uris[0], "/tv/119495/season/1");
    assert_eq!(uris[1], "/tv/119495/season/1/episode/1");
    task.abort();
}

#[tokio::test]
async fn retry_after_is_honored_before_success() {
    let (base_url, state, task) = mock_server(vec![
        MockResponse {
            status: StatusCode::TOO_MANY_REQUESTS,
            body: "slow down",
            retry_after: Some("0"),
        },
        MockResponse {
            status: StatusCode::OK,
            body: r#"{"id":42,"name":"One Piece"}"#,
            retry_after: None,
        },
    ])
    .await;
    let tv = client(&base_url)
        .fetch_tv(42)
        .await
        .expect("retry succeeds");
    assert_eq!(tv.id, 42);
    assert_eq!(state.calls.load(Ordering::Relaxed), 2);
    task.abort();
}

#[tokio::test]
async fn server_errors_retry_with_bounded_backoff() {
    let (base_url, state, task) = mock_server(vec![
        MockResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: "temporarily unavailable",
            retry_after: None,
        },
        MockResponse {
            status: StatusCode::OK,
            body: r#"{"id":7,"title":"Retry"}"#,
            retry_after: None,
        },
    ])
    .await;
    let movie = client(&base_url)
        .fetch_movie(7)
        .await
        .expect("retry succeeds");
    assert_eq!(movie.id, 7);
    assert_eq!(state.calls.load(Ordering::Relaxed), 2);
    task.abort();
}

#[tokio::test]
async fn auth_and_not_found_are_permanent() {
    let (base_url, state, task) = mock_server(vec![MockResponse {
        status: StatusCode::UNAUTHORIZED,
        body: "unauthorized",
        retry_after: None,
    }])
    .await;
    assert!(matches!(
        client(&base_url).fetch_movie(1).await,
        Err(TmdbClientError::Unauthorized)
    ));
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    task.abort();

    let (base_url, state, task) = mock_server(vec![MockResponse {
        status: StatusCode::NOT_FOUND,
        body: "missing",
        retry_after: None,
    }])
    .await;
    assert!(matches!(
        client(&base_url).fetch_movie(1).await,
        Err(TmdbClientError::NotFound)
    ));
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    task.abort();
}

#[tokio::test]
async fn arbitrary_document_paths_reject_traversal_and_empty_segments() {
    let base_url = "http://127.0.0.1/3/";
    let upstream = client(base_url);
    assert!(matches!(
        upstream
            .fetch_json::<serde_json::Value>("../configuration", &[], true)
            .await,
        Err(TmdbClientError::InvalidPath)
    ));
    assert!(matches!(
        upstream
            .fetch_json::<serde_json::Value>("movie//42", &[], true)
            .await,
        Err(TmdbClientError::InvalidPath)
    ));
}

#[tokio::test]
async fn malformed_json_is_not_retried() {
    let (base_url, state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: "not-json",
        retry_after: None,
    }])
    .await;
    assert!(matches!(
        client(&base_url).fetch_movie(1).await,
        Err(TmdbClientError::MalformedJson { .. })
    ));
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    task.abort();
}

#[tokio::test]
async fn oversized_response_is_rejected_before_json_decode() {
    let (base_url, state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        retry_after: None,
    }])
    .await;
    let error = TmdbClient::new(
        &base_url,
        SecretString::from("unit-token"),
        RetryPolicy::try_new(
            1,
            RateLimitPolicy::try_new(40, 4).expect("valid test rate policy"),
            Duration::from_secs(2),
            Duration::from_millis(1),
            Duration::from_millis(10),
            8,
        )
        .expect("valid small-body policy"),
    )
    .expect("valid test client")
    .fetch_movie(1)
    .await;
    assert!(matches!(error, Err(TmdbClientError::ResponseTooLarge)));
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    task.abort();
}

#[tokio::test]
async fn changes_endpoint_returns_typed_page() {
    let (base_url, state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: r#"{"results":[{"id":10,"adult":null,"video":null,"popularity":1.0}],"page":1,"total_pages":1,"total_results":1}"#,
        retry_after: None,
    }])
    .await;
    let page = client(&base_url)
        .fetch_changes(MediaType::Movie, 1)
        .await
        .expect("typed change page");
    assert_eq!(page.results[0].id, 10);
    assert!(!page.results[0].adult);
    assert!(!page.results[0].video);
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    task.abort();
}

#[tokio::test]
async fn daily_export_stream_is_atomically_published_without_bearer() {
    let (base_url, state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: "{\"id\":42,\"adult\":false}\n",
        retry_after: None,
    }])
    .await;
    let directory = tempdir().expect("temporary export directory");
    let destination = directory.path().join("movie_ids.ndjson.gz");
    let bytes = client(&base_url)
        .fetch_daily_export_to_file(&base_url, &destination, 1024)
        .await
        .expect("streamed export");
    assert_eq!(bytes.bytes, 24);
    assert_eq!(
        std::fs::read_to_string(&destination).expect("published export"),
        "{\"id\":42,\"adult\":false}\n"
    );
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    assert_eq!(state.saw_bearer.load(Ordering::Relaxed), 0);
    task.abort();
}

#[tokio::test]
async fn daily_export_stream_retries_server_failure_and_keeps_destination_bounded() {
    let (base_url, state, task) = mock_server(vec![
        MockResponse {
            status: StatusCode::SERVICE_UNAVAILABLE,
            body: "try again",
            retry_after: None,
        },
        MockResponse {
            status: StatusCode::OK,
            body: "{\"id\":7}\n",
            retry_after: None,
        },
    ])
    .await;
    let directory = tempdir().expect("temporary export directory");
    let destination = directory.path().join("tv_ids.ndjson");
    let bytes = client(&base_url)
        .fetch_daily_export_to_file(&base_url, &destination, 1024)
        .await
        .expect("retried streamed export");
    assert_eq!(bytes.bytes, 9);
    assert_eq!(
        std::fs::read_to_string(&destination).expect("published export"),
        "{\"id\":7}\n"
    );
    assert_eq!(state.calls.load(Ordering::Relaxed), 2);
    task.abort();
}

#[tokio::test]
async fn daily_export_stream_rejects_limit_without_overwriting_destination() {
    let (base_url, state, task) = mock_server(vec![MockResponse {
        status: StatusCode::OK,
        body: "{\"id\":123456789}\n",
        retry_after: None,
    }])
    .await;
    let directory = tempdir().expect("temporary export directory");
    let destination = directory.path().join("bounded.ndjson");
    std::fs::write(&destination, "previous\n").expect("seed destination");
    let error = client(&base_url)
        .fetch_daily_export_to_file(&base_url, &destination, 8)
        .await;
    assert!(matches!(error, Err(TmdbClientError::ExportSizeLimit)));
    assert_eq!(
        std::fs::read_to_string(&destination).expect("existing destination"),
        "previous\n"
    );
    assert_eq!(state.calls.load(Ordering::Relaxed), 1);
    task.abort();
}

#[test]
fn challenge_detection_is_allowlisted_and_never_used_for_rate_limits() {
    let url = Url::parse("https://api.themoviedb.org/3/movie/1").expect("valid URL");
    let headers = http::HeaderMap::new();
    let challenge = trawl_decision(
        &url,
        StatusCode::FORBIDDEN,
        &headers,
        b"<html><title>Just a moment...</title><div class='cf-chl-abc'>",
    );
    assert_eq!(challenge, TrawlDecision::ChallengeDetected);
    assert_eq!(
        trawl_decision(&url, StatusCode::TOO_MANY_REQUESTS, &headers, b"captcha"),
        TrawlDecision::NotEligible
    );
    let other = Url::parse("https://example.invalid/blocked").expect("valid URL");
    assert_eq!(
        trawl_decision(&other, StatusCode::FORBIDDEN, &headers, b"captcha"),
        TrawlDecision::NotEligible
    );
}

#[test]
fn retry_after_and_status_classification_are_deterministic() {
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::RETRY_AFTER, HeaderValue::from_static("2"));
    assert_eq!(
        classify_response(StatusCode::TOO_MANY_REQUESTS, &headers, b""),
        ResponseClass::RateLimited {
            retry_after: Some(Duration::from_secs(2))
        }
    );
    assert_eq!(
        classify_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &http::HeaderMap::new(),
            b""
        ),
        ResponseClass::TransientServer { status: 500 }
    );
}
