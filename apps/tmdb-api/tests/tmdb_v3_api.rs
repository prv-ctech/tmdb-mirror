use axum::{body::Body, http::Request};
use serde_json::json;
use tmdb_api::build_tmdb_v3_router;
use tmdb_db::TmdbDocumentRepository;
use tower::ServiceExt;

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn v3_routes_return_captured_tmdb_json_and_tmdb_not_found_shape(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let repository = TmdbDocumentRepository::new(pool.clone());
    repository
        .upsert(
            "configuration",
            "",
            &json!({"images": {"base_url": "https://image.tmdb.org/t/p/"}}),
        )
        .await?;
    repository
        .upsert(
            "movie/42",
            "append_to_response=credits",
            &json!({
                "id": 42,
                "title": "Movie",
                "poster_path": "/poster.jpg",
                "backdrop_path": "/missing.jpg",
                "images": {
                    "posters": [
                        {"file_path": "/poster.jpg"},
                        {"file_path": "/missing-gallery.jpg"}
                    ]
                },
                "media_field_fixture": {
                    "logo_path": "/missing-logo.png",
                    "profile_path": "/missing-profile.jpg",
                    "still_path": "/missing-still.jpg"
                }
            }),
        )
        .await?;
    repository
        .upsert("movie/42", "", &json!({"id": 42, "title": "Movie base"}))
        .await?;
    repository
        .upsert(
            "tv/119495/season/1",
            "",
            &json!({"id": 9001, "season_number": 1, "name": "Season 1"}),
        )
        .await?;
    repository
        .upsert(
            "tv/119495/season/1/episode/1",
            "",
            &json!({"id": 9002, "episode_number": 1, "name": "Episode 1"}),
        )
        .await?;
    for (path, response) in [
        ("credit/credit-1", json!({"id": "credit-1"})),
        ("review/review-1", json!({"id": "review-1"})),
        ("keyword/20", json!({"id": 20, "name": "anime"})),
        ("keyword/20/movies", json!({"id": 20, "results": []})),
        (
            "tv/episode_group/group-1",
            json!({"id": "group-1", "name": "Broadcast"}),
        ),
    ] {
        repository.upsert(path, "", &response).await?;
    }
    sqlx::query(
        "INSERT INTO catalog.titles
            (media_type, tmdb_id, display_title, original_title, overview, original_language)
         VALUES ('movie', 42, 'Movie', 'Movie', 'A local movie', 'en')
         ",
    )
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO catalog.genres (id, name) VALUES (16, 'Animation')")
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO catalog.title_genres (title_id, genre_id)
         SELECT id, 16 FROM catalog.titles WHERE media_type = 'movie' AND tmdb_id = 42",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.title_external_ids (title_id, imdb_id)
         SELECT id, 'tt0000042' FROM catalog.titles
          WHERE media_type = 'movie' AND tmdb_id = 42",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO assets.image_assets
            (title_id, image_kind, source, source_key, storage_path, mime_type,
             width, height, file_size_bytes, sha256, status, downloaded_at)
         SELECT id, 'poster', 'tmdb', '/poster.jpg', 'movies/42/posters/poster.jpg',
                'image/jpeg', 640, 960, 1024,
                repeat('a', 64), 'ready', clock_timestamp()
           FROM catalog.titles
          WHERE media_type = 'movie' AND tmdb_id = 42",
    )
    .execute(&pool)
    .await?;

    let app = build_tmdb_v3_router(pool.clone(), pool, Some("http://media.test/media".parse()?));
    let configuration = app
        .clone()
        .oneshot(Request::get("/3/configuration").body(Body::empty())?)
        .await?;
    assert_eq!(configuration.status(), 200);

    let movie = app
        .clone()
        .oneshot(Request::get("/3/movie/42?append_to_response=credits").body(Body::empty())?)
        .await?;
    assert_eq!(movie.status(), 200);
    let movie_body = body_json(movie).await?;
    assert_eq!(movie_body["poster_path"], "/poster.jpg");
    assert_eq!(
        movie_body["local_poster_path"],
        "http://media.test/media/movies/42/posters/poster.jpg"
    );
    assert_eq!(movie_body["backdrop_path"], "/missing.jpg");
    assert!(movie_body["local_backdrop_path"].is_null());
    assert_eq!(
        movie_body["images"]["posters"][0]["file_path"],
        "/poster.jpg"
    );
    assert_eq!(
        movie_body["images"]["posters"][0]["local_file_path"],
        "http://media.test/media/movies/42/posters/poster.jpg"
    );
    assert_eq!(
        movie_body["images"]["posters"][1]["file_path"],
        "/missing-gallery.jpg"
    );
    assert!(movie_body["images"]["posters"][1]["local_file_path"].is_null());
    assert_eq!(
        movie_body["media_field_fixture"]["logo_path"],
        "/missing-logo.png"
    );
    assert!(movie_body["media_field_fixture"]["local_logo_path"].is_null());
    assert_eq!(
        movie_body["media_field_fixture"]["profile_path"],
        "/missing-profile.jpg"
    );
    assert!(movie_body["media_field_fixture"]["local_profile_path"].is_null());
    assert_eq!(
        movie_body["media_field_fixture"]["still_path"],
        "/missing-still.jpg"
    );
    assert!(movie_body["media_field_fixture"]["local_still_path"].is_null());

    let season = app
        .clone()
        .oneshot(Request::get("/3/tv/119495/season/1").body(Body::empty())?)
        .await?;
    assert_eq!(season.status(), 200);
    assert_eq!(body_json(season).await?["season_number"], 1);

    let episode = app
        .clone()
        .oneshot(Request::get("/3/tv/119495/season/1/episode/1").body(Body::empty())?)
        .await?;
    assert_eq!(episode.status(), 200);
    assert_eq!(body_json(episode).await?["episode_number"], 1);

    for path in [
        "/3/credit/credit-1",
        "/3/review/review-1",
        "/3/keyword/20",
        "/3/keyword/20/movies",
        "/3/tv/episode_group/group-1",
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), 200, "{path}");
    }

    let movie_with_unmatched_query = app
        .clone()
        .oneshot(Request::get("/3/movie/42?api_key=local-only&language=en-US").body(Body::empty())?)
        .await?;
    assert_eq!(movie_with_unmatched_query.status(), 200);

    repository
        .upsert("movie/42/reviews", "", &json!({"id": 42, "results": []}))
        .await?;
    let reviews_with_default_query = app
        .clone()
        .oneshot(Request::get("/3/movie/42/reviews?language=en-US&page=1").body(Body::empty())?)
        .await?;
    assert_eq!(reviews_with_default_query.status(), 200);

    let search = app
        .clone()
        .oneshot(Request::get("/3/search/movie?query=Movie").body(Body::empty())?)
        .await?;
    assert_eq!(search.status(), 200);
    let search_body = body_json(search).await?;
    assert_eq!(search_body["total_results"], 1);
    assert_eq!(search_body["results"][0]["id"], 42);

    let discover = app
        .clone()
        .oneshot(Request::get("/3/discover/movie?with_genres=16").body(Body::empty())?)
        .await?;
    assert_eq!(discover.status(), 200);
    let discover_body = body_json(discover).await?;
    assert_eq!(discover_body["total_results"], 1);
    assert_eq!(discover_body["results"][0]["id"], 42);

    let found = app
        .clone()
        .oneshot(Request::get("/3/find/tt0000042").body(Body::empty())?)
        .await?;
    assert_eq!(found.status(), 200);
    let found_body = body_json(found).await?;
    assert_eq!(found_body["movie_results"][0]["id"], 42);

    let missing = app
        .oneshot(Request::get("/3/movie/999").body(Body::empty())?)
        .await?;
    assert_eq!(missing.status(), 404);
    let bytes = axum::body::to_bytes(missing.into_body(), 4096).await?;
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(
        body,
        json!({
            "status_code": 34,
            "status_message": "The resource you requested could not be found.",
            "success": false
        })
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn v3_sessions_lists_account_items_and_ratings_use_tmdb_shapes(
    pool: sqlx::PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = build_tmdb_v3_router(pool.clone(), pool, None);

    let token = app
        .clone()
        .oneshot(Request::get("/3/authentication/token/new").body(Body::empty())?)
        .await?;
    assert_eq!(token.status(), 200);
    let token_body = body_json(token).await?;
    assert_eq!(token_body["success"], true);
    let request_token = token_body["request_token"]
        .as_str()
        .ok_or("request token missing")?;

    let session = app
        .clone()
        .oneshot(
            Request::post("/3/authentication/session/new")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"request_token": request_token}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(session.status(), 200);
    let session_body = body_json(session).await?;
    assert_eq!(session_body["success"], true);
    let session_id = session_body["session_id"]
        .as_str()
        .ok_or("session id missing")?;

    let list = app
        .clone()
        .oneshot(
            Request::post("/3/list?session_id=".to_owned() + session_id)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "name": "Local list",
                        "description": "A test list",
                        "language": "en-US"
                    })
                    .to_string(),
                ))?,
        )
        .await?;
    assert_eq!(list.status(), 200);
    let list_body = body_json(list).await?;
    assert_eq!(list_body["success"], true);
    let list_id = list_body["list_id"].as_i64().ok_or("list id missing")?;

    let add = app
        .clone()
        .oneshot(
            Request::post(format!(
                "/3/list/{list_id}/add_item?session_id={session_id}"
            ))
            .header("content-type", "application/json")
            .body(Body::from(json!({"media_id": 550}).to_string()))?,
        )
        .await?;
    assert_eq!(add.status(), 200);
    assert_eq!(body_json(add).await?["status_code"], 12);

    let favorite = app
        .clone()
        .oneshot(
            Request::post("/3/account/1/favorite?session_id=".to_owned() + session_id)
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({"media_type": "movie", "media_id": 550, "favorite": true}).to_string(),
                ))?,
        )
        .await?;
    assert_eq!(favorite.status(), 200);
    assert_eq!(body_json(favorite).await?["status_code"], 1);

    let favorites = app
        .clone()
        .oneshot(
            Request::get("/3/account/1/favorite/movies?session_id=".to_owned() + session_id)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(favorites.status(), 200);
    let favorites_body = body_json(favorites).await?;
    assert_eq!(favorites_body["total_results"], 1);
    assert_eq!(favorites_body["results"][0]["id"], 550);

    let rating = app
        .clone()
        .oneshot(
            Request::post("/3/movie/550/rating?session_id=".to_owned() + session_id)
                .header("content-type", "application/json")
                .body(Body::from(json!({"value": 8.5}).to_string()))?,
        )
        .await?;
    assert_eq!(rating.status(), 200);
    assert_eq!(body_json(rating).await?["status_code"], 1);

    let rated = app
        .clone()
        .oneshot(
            Request::get("/3/account/1/rated/movies?session_id=".to_owned() + session_id)
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(rated.status(), 200);
    let rated_body = body_json(rated).await?;
    assert_eq!(rated_body["total_results"], 1);
    assert_eq!(rated_body["results"][0]["id"], 550);

    let unsupported = app
        .oneshot(
            Request::post("/3/search/movie")
                .header("content-type", "application/json")
                .body(Body::from("{}"))?,
        )
        .await?;
    assert_eq!(unsupported.status(), 405);
    assert_eq!(body_json(unsupported).await?["status_code"], 36);
    Ok(())
}

async fn body_json(
    response: axum::response::Response,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
