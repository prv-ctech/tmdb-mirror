//! Local TMDB v3 routes.
//!
//! Read-only resources are returned from the exact JSON documents captured by
//! the ingest worker. The small set of official v3 write routes uses local
//! session/list/favorite/watchlist/rating state so clients can use the same
//! contract without sending writes to TMDB.

// Invalid TMDB-compatible requests carry a ready-to-send Axum response. Keeping
// that response inline avoids an allocation on every rejected request.
#![allow(clippy::result_large_err)]

use std::collections::{HashMap, HashSet};

use axum::{
    Router,
    body::Bytes,
    extract::{OriginalUri, Path, State},
    response::{IntoResponse, Response},
    routing::any,
};
use chrono::{DateTime, Duration, Utc};
use http::{Method, StatusCode, Uri};
use serde_json::{Map, Value, json};
use sqlx::{FromRow, PgPool};
use tmdb_db::TmdbDocumentRepository;
use uuid::Uuid;

const PAGE_SIZE: i64 = 20;
const REQUEST_TOKEN_LIFETIME: Duration = Duration::hours(1);
const SESSION_LIFETIME: Duration = Duration::hours(24);

#[derive(Clone, Debug)]
struct TmdbV3State {
    documents: TmdbDocumentRepository,
    read_pool: PgPool,
    write_pool: PgPool,
    media_base_url: Option<String>,
}

#[derive(Debug, FromRow)]
struct SessionRow {
    account_id: Option<i64>,
    is_guest: bool,
}

#[derive(Debug, FromRow)]
struct ListRow {
    id: i64,
    name: String,
    description: String,
    language_code: String,
}

#[derive(Debug, FromRow)]
struct ListSummaryRow {
    id: i64,
    name: String,
    description: String,
    language_code: String,
    item_count: i64,
}

#[derive(Debug, FromRow)]
struct RatedRow {
    media_id: i64,
    rating: f64,
}

#[derive(Debug, FromRow)]
struct TitleSearchRow {
    tmdb_id: i64,
    display_title: Option<String>,
    original_title: Option<String>,
    overview: Option<String>,
    original_language: Option<String>,
    release_date: Option<String>,
    first_air_date: Option<String>,
    popularity: Option<f64>,
    vote_average: Option<f64>,
    vote_count: Option<i64>,
    adult: bool,
    video: bool,
    poster_path: Option<String>,
    backdrop_path: Option<String>,
    genre_ids: Vec<i64>,
    total_results: i64,
}

enum GeneratedGet {
    NotHandled,
    Response(Value),
    NotFound,
}

enum ApiError {
    Response(Response),
    Database,
}

impl From<Response> for ApiError {
    fn from(value: Response) -> Self {
        Self::Response(value)
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(_: sqlx::Error) -> Self {
        Self::Database
    }
}

/// Builds the local TMDB v3 route surface.
pub fn build_tmdb_v3_router(
    read_pool: PgPool,
    write_pool: PgPool,
    media_base_url: Option<Uri>,
) -> Router {
    let state = TmdbV3State {
        documents: TmdbDocumentRepository::new(read_pool.clone()),
        read_pool,
        write_pool,
        media_base_url: media_base_url.map(|uri| uri.to_string().trim_end_matches('/').to_owned()),
    };
    Router::new()
        .route("/3/{*endpoint_path}", any(dispatch))
        .with_state(state)
}

async fn dispatch(
    State(state): State<TmdbV3State>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Path(endpoint_path): Path<String>,
    body: Bytes,
) -> Response {
    if !valid_endpoint_path(&endpoint_path) {
        return tmdb_not_found();
    }
    let query = canonical_query(query_string(&uri));
    match method {
        Method::GET => get_operation(&state, &endpoint_path, &query).await,
        Method::POST => write_operation(&state, &endpoint_path, &query, &body, false).await,
        Method::DELETE => write_operation(&state, &endpoint_path, &query, &body, true).await,
        _ => method_not_allowed(),
    }
}

async fn get_operation(state: &TmdbV3State, endpoint_path: &str, query: &str) -> Response {
    match generated_get(state, endpoint_path, query).await {
        Ok(GeneratedGet::Response(value)) => match add_local_media_paths(state, value).await {
            Ok(value) => json_response(value),
            Err(_) => database_unavailable(),
        },
        Ok(GeneratedGet::NotFound) => tmdb_not_found(),
        Ok(GeneratedGet::NotHandled) => document(state, endpoint_path, query).await,
        Err(ApiError::Response(response)) => response,
        Err(ApiError::Database) => database_unavailable(),
    }
}

async fn document(state: &TmdbV3State, endpoint_path: &str, query: &str) -> Response {
    for candidate in document_query_candidates(endpoint_path, query) {
        match state.documents.get(endpoint_path, &candidate).await {
            Ok(Some(value)) => {
                return match add_local_media_paths(state, value).await {
                    Ok(value) => json_response(value),
                    Err(_) => database_unavailable(),
                };
            }
            Ok(None) => {}
            Err(_) => return database_unavailable(),
        }
    }
    tmdb_not_found()
}

fn document_query_candidates(endpoint_path: &str, query: &str) -> Vec<String> {
    let mut candidates = vec![query.to_owned()];
    let fallback = query
        .split('&')
        .filter(|part| !part.is_empty())
        .filter(|part| {
            let (name, value) = part.split_once('=').unwrap_or((part, ""));
            !((name == "page" && value == "1")
                || (name == "language" && default_language(endpoint_path, value)))
        })
        .collect::<Vec<_>>()
        .join("&");
    if fallback != query {
        candidates.push(fallback);
    }
    candidates
}

fn default_language(endpoint_path: &str, value: &str) -> bool {
    if endpoint_path.ends_with("/images") {
        return false;
    }
    if matches!(endpoint_path, "genre/movie/list" | "genre/tv/list") {
        value == "en"
    } else {
        value == "en-US"
    }
}

const LOCAL_MEDIA_FIELDS: &[(&str, &str)] = &[
    ("backdrop_path", "local_backdrop_path"),
    ("file_path", "local_file_path"),
    ("logo_path", "local_logo_path"),
    ("poster_path", "local_poster_path"),
    ("profile_path", "local_profile_path"),
    ("still_path", "local_still_path"),
];

#[derive(Debug, FromRow)]
struct LocalMediaPathRow {
    source_key: String,
    storage_path: String,
    sha256: String,
}

#[derive(Debug)]
struct LocalMediaPath {
    storage_path: String,
    sha256: String,
}

async fn add_local_media_paths(
    state: &TmdbV3State,
    mut value: Value,
) -> Result<Value, sqlx::Error> {
    let mut source_keys = HashSet::new();
    collect_media_source_keys(&value, &mut source_keys);
    let source_keys = source_keys.into_iter().collect::<Vec<_>>();
    let mut local_paths = HashMap::new();
    if !source_keys.is_empty() {
        let rows = sqlx::query_as::<_, LocalMediaPathRow>(
            "SELECT source_key, storage_path, sha256
               FROM assets.image_assets
              WHERE status = 'ready'
                AND storage_path IS NOT NULL
                AND sha256 IS NOT NULL
                AND source_key = ANY($1::text[])
              ORDER BY id",
        )
        .bind(&source_keys)
        .fetch_all(&state.read_pool)
        .await?;
        for row in rows {
            local_paths.entry(row.source_key).or_insert(LocalMediaPath {
                storage_path: row.storage_path,
                sha256: row.sha256,
            });
        }
    }
    insert_local_media_paths(&mut value, state.media_base_url.as_deref(), &local_paths);
    Ok(value)
}

fn collect_media_source_keys(value: &Value, source_keys: &mut HashSet<String>) {
    match value {
        Value::Array(values) => {
            for value in values {
                collect_media_source_keys(value, source_keys);
            }
        }
        Value::Object(object) => {
            for (key, value) in object {
                if local_media_field(key).is_some() {
                    if let Some(source_key) = value.as_str()
                        && is_tmdb_source_key(source_key)
                    {
                        source_keys.insert(source_key.to_owned());
                    }
                } else {
                    collect_media_source_keys(value, source_keys);
                }
            }
        }
        _ => {}
    }
}

fn insert_local_media_paths(
    value: &mut Value,
    media_base_url: Option<&str>,
    local_paths: &HashMap<String, LocalMediaPath>,
) {
    match value {
        Value::Array(values) => {
            for value in values {
                insert_local_media_paths(value, media_base_url, local_paths);
            }
        }
        Value::Object(object) => {
            let mut additions = Vec::new();
            for (key, value) in object.iter_mut() {
                if let Some(local_field) = local_media_field(key) {
                    let local_url = value
                        .as_str()
                        .filter(|source_key| is_tmdb_source_key(source_key))
                        .and_then(|source_key| local_paths.get(source_key))
                        .filter(|local| {
                            is_safe_storage_path(&local.storage_path)
                                && local.sha256.len() == 64
                                && local.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
                        })
                        .and_then(|local| {
                            media_base_url.map(|base| {
                                format!("{base}/{}?v={}", local.storage_path, &local.sha256[..16])
                            })
                        });
                    additions.push((
                        local_field.to_owned(),
                        local_url.map_or(Value::Null, Value::String),
                    ));
                } else {
                    insert_local_media_paths(value, media_base_url, local_paths);
                }
            }
            for (key, value) in additions {
                object.insert(key, value);
            }
        }
        _ => {}
    }
}

fn local_media_field(upstream_field: &str) -> Option<&'static str> {
    LOCAL_MEDIA_FIELDS
        .iter()
        .find_map(|(field, local_field)| (*field == upstream_field).then_some(*local_field))
}

fn is_tmdb_source_key(value: &str) -> bool {
    value.starts_with('/')
        && value.len() <= 512
        && !value.chars().any(char::is_control)
        && !value.contains(['?', '#', '\\'])
        && !value.contains("..")
}

fn is_safe_storage_path(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !value.starts_with('/')
        && !value.chars().any(char::is_control)
        && !value.contains(['?', '#', '\\'])
        && !value
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
}

#[allow(clippy::too_many_lines)]
async fn generated_get(
    state: &TmdbV3State,
    endpoint_path: &str,
    query: &str,
) -> Result<GeneratedGet, ApiError> {
    let parts = endpoint_path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        ["authentication"] => Ok(GeneratedGet::Response(json!({
            "status_code": 1,
            "status_message": "Success."
        }))),
        ["authentication", "token", "new"] => {
            let token = new_token();
            let expires_at = Utc::now() + REQUEST_TOKEN_LIFETIME;
            sqlx::query(
                "INSERT INTO source.tmdb_v3_request_tokens (token, expires_at)
                 VALUES ($1, $2)",
            )
            .bind(&token)
            .bind(expires_at)
            .execute(&state.write_pool)
            .await?;
            Ok(GeneratedGet::Response(json!({
                "success": true,
                "expires_at": tmdb_timestamp(expires_at),
                "request_token": token
            })))
        }
        ["authentication", "guest_session", "new"] => {
            let guest_session_id = new_token();
            let expires_at = Utc::now() + SESSION_LIFETIME;
            sqlx::query(
                "INSERT INTO source.tmdb_v3_sessions
                    (session_id, account_id, is_guest, expires_at)
                 VALUES ($1, NULL, true, $2)",
            )
            .bind(&guest_session_id)
            .bind(expires_at)
            .execute(&state.write_pool)
            .await?;
            Ok(GeneratedGet::Response(json!({
                "success": true,
                "guest_session_id": guest_session_id,
                "expires_at": tmdb_timestamp(expires_at)
            })))
        }
        ["search", "movie"] => search_titles(state, "movie", query, false).await,
        ["search", "tv"] => search_titles(state, "tv", query, false).await,
        ["search", "multi"] => search_titles(state, "multi", query, true).await,
        ["search", "person"] => search_people(state, query, false).await,
        ["search", "collection"] => search_collections(state, query).await,
        ["search", "company"] => search_companies(state, query).await,
        ["search", "keyword"] => search_keywords(state, query).await,
        ["discover", "movie"] => discover_titles(state, "movie", query).await,
        ["discover", "tv"] => discover_titles(state, "tv", query).await,
        ["find", external_id] => find_external_id(state, external_id, query).await,
        ["account", account_id] => {
            let account_id = parse_id(account_id).ok_or_else(invalid_id_error)?;
            let session = require_account_session(&state.write_pool, query, account_id).await?;
            let _ = session;
            Ok(GeneratedGet::Response(json!({"id": account_id})))
        }
        ["account", account_id, "lists"] => {
            let account_id = parse_id(account_id).ok_or_else(invalid_id_error)?;
            let session = require_account_session(&state.write_pool, query, account_id).await?;
            let _ = session;
            account_lists(&state.write_pool, account_id, query).await
        }
        ["account", account_id, relation, media_type]
            if matches!(*relation, "favorite" | "watchlist")
                && matches!(*media_type, "movies" | "tv") =>
        {
            let account_id = parse_id(account_id).ok_or_else(invalid_id_error)?;
            let _session = require_account_session(&state.write_pool, query, account_id).await?;
            let media_type = if *media_type == "movies" {
                "movie"
            } else {
                "tv"
            };
            account_media(state, account_id, relation, media_type, query).await
        }
        ["account", account_id, "rated", media_type] if matches!(*media_type, "movies" | "tv") => {
            let account_id = parse_id(account_id).ok_or_else(invalid_id_error)?;
            let session = require_account_session(&state.write_pool, query, account_id).await?;
            let media_type = if *media_type == "movies" {
                "movie"
            } else {
                "tv"
            };
            rated_media(state, "session", &session.session_id, media_type, query).await
        }
        ["account", account_id, "rated", "tv", "episodes"] => {
            let account_id = parse_id(account_id).ok_or_else(invalid_id_error)?;
            let session = require_account_session(&state.write_pool, query, account_id).await?;
            rated_media(state, "session", &session.session_id, "tv_episode", query).await
        }
        ["guest_session", guest_session_id, "rated", media_type]
            if matches!(*media_type, "movies" | "tv") =>
        {
            let _session = require_guest_session(&state.write_pool, guest_session_id).await?;
            let media_type = if *media_type == "movies" {
                "movie"
            } else {
                "tv"
            };
            rated_media(state, "guest", guest_session_id, media_type, query).await
        }
        ["guest_session", guest_session_id, "rated", "tv", "episodes"] => {
            let _ = require_guest_session(&state.write_pool, guest_session_id).await?;
            rated_media(state, "guest", guest_session_id, "tv_episode", query).await
        }
        ["list", list_id] => {
            let list_id = parse_id(list_id).ok_or_else(invalid_id_error)?;
            list_detail(state, list_id, query).await
        }
        ["list", list_id, "item_status"] => {
            let list_id = parse_id(list_id).ok_or_else(invalid_id_error)?;
            let movie_id = query_parameter(query, "movie_id")
                .and_then(|value| parse_id(&value))
                .ok_or_else(invalid_id_error)?;
            let present: bool = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM source.tmdb_v3_list_items
                      WHERE list_id = $1 AND media_id = $2
                 )",
            )
            .bind(list_id)
            .bind(movie_id)
            .fetch_one(&state.write_pool)
            .await?;
            Ok(GeneratedGet::Response(json!({
                "id": list_id,
                "item_present": present
            })))
        }
        _ => Ok(GeneratedGet::NotHandled),
    }
}

async fn write_operation(
    state: &TmdbV3State,
    endpoint_path: &str,
    query: &str,
    body: &Bytes,
    deleting: bool,
) -> Response {
    let parts = endpoint_path.split('/').collect::<Vec<_>>();
    let result = match (deleting, parts.as_slice()) {
        (false, ["authentication", "session", "new"]) => create_session(state, body).await,
        (false, ["authentication", "session", "convert", "4"]) => {
            create_session_from_value(state, body, "access_token").await
        }
        (false, ["authentication", "token", "validate_with_login"]) => {
            create_session_from_value(state, body, "request_token").await
        }
        (true, ["authentication", "session"]) => delete_session(state, body).await,
        (false, ["account", account_id, relation])
            if matches!(*relation, "favorite" | "watchlist") =>
        {
            account_relation(state, account_id, relation, query, body).await
        }
        (false, ["list"]) => create_list(state, query, body).await,
        (false, ["list", list_id, "add_item"]) => {
            list_item(state, list_id, query, body, true).await
        }
        (false, ["list", list_id, "remove_item"]) => {
            list_item(state, list_id, query, body, false).await
        }
        (false, ["list", list_id, "clear"]) => clear_list(state, list_id, query).await,
        (true, ["list", list_id]) => delete_list(state, list_id, query).await,
        (false, ["movie", media_id, "rating"]) => {
            rating_operation(state, "movie", media_id, "0", "0", query, body, false).await
        }
        (true, ["movie", media_id, "rating"]) => {
            rating_operation(state, "movie", media_id, "0", "0", query, body, true).await
        }
        (false, ["tv", media_id, "rating"]) => {
            rating_operation(state, "tv", media_id, "0", "0", query, body, false).await
        }
        (true, ["tv", media_id, "rating"]) => {
            rating_operation(state, "tv", media_id, "0", "0", query, body, true).await
        }
        (
            false,
            [
                "tv",
                media_id,
                "season",
                season_number,
                "episode",
                episode_number,
                "rating",
            ],
        ) => {
            rating_operation(
                state,
                "tv_episode",
                media_id,
                season_number,
                episode_number,
                query,
                body,
                false,
            )
            .await
        }
        (
            true,
            [
                "tv",
                media_id,
                "season",
                season_number,
                "episode",
                episode_number,
                "rating",
            ],
        ) => {
            rating_operation(
                state,
                "tv_episode",
                media_id,
                season_number,
                episode_number,
                query,
                body,
                true,
            )
            .await
        }
        _ => Err(Response::from(method_not_allowed())),
    };
    match result {
        Ok(response) | Err(response) => response,
    }
}

async fn create_session(state: &TmdbV3State, body: &Bytes) -> Result<Response, Response> {
    create_session_from_value(state, body, "request_token").await
}

async fn create_session_from_value(
    state: &TmdbV3State,
    body: &Bytes,
    token_field: &str,
) -> Result<Response, Response> {
    let object = body_object(body)?;
    let token = required_string(&object, token_field)?;
    let session_id = new_token();
    let expires_at = Utc::now() + SESSION_LIFETIME;
    let mut transaction = state
        .write_pool
        .begin()
        .await
        .map_err(|_| database_unavailable())?;
    let consumed = sqlx::query(
        "UPDATE source.tmdb_v3_request_tokens
            SET consumed_at = pg_catalog.clock_timestamp()
          WHERE token = $1
            AND consumed_at IS NULL
            AND expires_at > pg_catalog.clock_timestamp()",
    )
    .bind(token)
    .execute(&mut *transaction)
    .await
    .map_err(|_| database_unavailable())?
    .rows_affected();
    if consumed != 1 && token_field == "request_token" {
        return Err(authentication_error());
    }
    sqlx::query(
        "INSERT INTO source.tmdb_v3_sessions
            (session_id, account_id, is_guest, expires_at)
         VALUES ($1, 1, false, $2)",
    )
    .bind(&session_id)
    .bind(expires_at)
    .execute(&mut *transaction)
    .await
    .map_err(|_| database_unavailable())?;
    transaction
        .commit()
        .await
        .map_err(|_| database_unavailable())?;
    Ok(json_response(json!({
        "success": true,
        "session_id": session_id
    })))
}

async fn delete_session(state: &TmdbV3State, body: &Bytes) -> Result<Response, Response> {
    let object = body_object(body)?;
    let session_id = required_string(&object, "session_id")?;
    sqlx::query("DELETE FROM source.tmdb_v3_sessions WHERE session_id = $1")
        .bind(session_id)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
    Ok(json_response(json!({"success": true})))
}

async fn account_relation(
    state: &TmdbV3State,
    account_id: &str,
    relation: &str,
    query: &str,
    body: &Bytes,
) -> Result<Response, Response> {
    let account_id = parse_id(account_id).ok_or_else(invalid_id_error)?;
    let session = require_account_session(&state.write_pool, query, account_id).await?;
    let object = body_object(body)?;
    let media_type = required_string(&object, "media_type")?;
    if !matches!(media_type.as_str(), "movie" | "tv") {
        return Err(invalid_parameter_error());
    }
    let media_id = object
        .get("media_id")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(invalid_id_error)?;
    let enabled = object
        .get(relation)
        .and_then(Value::as_bool)
        .ok_or_else(invalid_parameter_error)?;
    if enabled {
        sqlx::query(
            "INSERT INTO source.tmdb_v3_account_items
                (account_id, relation, media_type, media_id)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT DO NOTHING",
        )
        .bind(account_id)
        .bind(relation)
        .bind(&media_type)
        .bind(media_id)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
    } else {
        sqlx::query(
            "DELETE FROM source.tmdb_v3_account_items
              WHERE account_id = $1 AND relation = $2
                AND media_type = $3 AND media_id = $4",
        )
        .bind(account_id)
        .bind(relation)
        .bind(&media_type)
        .bind(media_id)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
    }
    let _ = session;
    Ok(status_response(1, "Success."))
}

async fn create_list(state: &TmdbV3State, query: &str, body: &Bytes) -> Result<Response, Response> {
    let session = require_account_session(&state.write_pool, query, 1).await?;
    let object = body_object(body)?;
    let name = required_string(&object, "name")?;
    let description = optional_string(&object, "description").unwrap_or_default();
    let language = optional_string(&object, "language").unwrap_or_else(|| "en-US".to_owned());
    let list_id: i64 = sqlx::query_scalar(
        "INSERT INTO source.tmdb_v3_lists
            (account_id, name, description, language_code)
         VALUES ($1, $2, $3, $4)
         RETURNING id",
    )
    .bind(session.row.account_id.ok_or_else(authentication_error)?)
    .bind(name)
    .bind(description)
    .bind(language)
    .fetch_one(&state.write_pool)
    .await
    .map_err(|_| database_unavailable())?;
    Ok(json_response(json!({
        "status_message": "The item/record was created successfully.",
        "success": true,
        "status_code": 1,
        "list_id": list_id
    })))
}

async fn list_item(
    state: &TmdbV3State,
    list_id: &str,
    query: &str,
    body: &Bytes,
    adding: bool,
) -> Result<Response, Response> {
    let list_id = parse_id(list_id).ok_or_else(invalid_id_error)?;
    let owner = list_owner(&state.write_pool, list_id, query).await?;
    let object = body_object(body)?;
    let media_id = object
        .get("media_id")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(invalid_id_error)?;
    if adding {
        sqlx::query(
            "INSERT INTO source.tmdb_v3_list_items (list_id, media_id)
             VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(list_id)
        .bind(media_id)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
        let _ = owner;
        Ok(status_response(
            12,
            "The item/record was updated successfully.",
        ))
    } else {
        sqlx::query(
            "DELETE FROM source.tmdb_v3_list_items
              WHERE list_id = $1 AND media_id = $2",
        )
        .bind(list_id)
        .bind(media_id)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
        let _ = owner;
        Ok(status_response(
            13,
            "The item/record was deleted successfully.",
        ))
    }
}

async fn clear_list(state: &TmdbV3State, list_id: &str, query: &str) -> Result<Response, Response> {
    let list_id = parse_id(list_id).ok_or_else(invalid_id_error)?;
    let _ = list_owner(&state.write_pool, list_id, query).await?;
    if query_parameter(query, "confirm").as_deref() != Some("true") {
        return Err(invalid_parameter_error());
    }
    sqlx::query("DELETE FROM source.tmdb_v3_list_items WHERE list_id = $1")
        .bind(list_id)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
    Ok(status_response(
        12,
        "The item/record was updated successfully.",
    ))
}

async fn delete_list(
    state: &TmdbV3State,
    list_id: &str,
    query: &str,
) -> Result<Response, Response> {
    let list_id = parse_id(list_id).ok_or_else(invalid_id_error)?;
    let _ = list_owner(&state.write_pool, list_id, query).await?;
    sqlx::query("DELETE FROM source.tmdb_v3_lists WHERE id = $1")
        .bind(list_id)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
    Ok(status_response(
        12,
        "The item/record was updated successfully.",
    ))
}

#[allow(clippy::too_many_arguments)]
async fn rating_operation(
    state: &TmdbV3State,
    media_type: &str,
    media_id: &str,
    season_number: &str,
    episode_number: &str,
    query: &str,
    body: &Bytes,
    deleting: bool,
) -> Result<Response, Response> {
    let media_id = parse_id(media_id).ok_or_else(invalid_id_error)?;
    let season_number = if media_type == "tv_episode" {
        parse_positive_component(season_number).ok_or_else(invalid_id_error)?
    } else {
        0
    };
    let episode_number = if media_type == "tv_episode" {
        parse_positive_component(episode_number).ok_or_else(invalid_id_error)?
    } else {
        0
    };
    let (owner_kind, owner_id) = rating_owner(state, query).await?;
    if deleting {
        sqlx::query(
            "DELETE FROM source.tmdb_v3_ratings
              WHERE owner_kind = $1 AND owner_id = $2 AND media_type = $3
                AND media_id = $4 AND season_number = $5 AND episode_number = $6",
        )
        .bind(owner_kind)
        .bind(owner_id)
        .bind(media_type)
        .bind(media_id)
        .bind(season_number)
        .bind(episode_number)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
        Ok(status_response(
            13,
            "The item/record was deleted successfully.",
        ))
    } else {
        let object = body_object(body)?;
        let rating = object
            .get("value")
            .and_then(Value::as_f64)
            .filter(|value| (0.5..=10.0).contains(value) && (value * 2.0).fract() == 0.0)
            .ok_or_else(invalid_parameter_error)?;
        sqlx::query(
            "INSERT INTO source.tmdb_v3_ratings
                (owner_kind, owner_id, media_type, media_id,
                 season_number, episode_number, rating)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (
                 owner_kind, owner_id, media_type, media_id, season_number, episode_number
             ) DO UPDATE SET rating = EXCLUDED.rating,
                             updated_at = pg_catalog.clock_timestamp()",
        )
        .bind(owner_kind)
        .bind(owner_id)
        .bind(media_type)
        .bind(media_id)
        .bind(season_number)
        .bind(episode_number)
        .bind(rating)
        .execute(&state.write_pool)
        .await
        .map_err(|_| database_unavailable())?;
        Ok(status_response(1, "Success."))
    }
}

async fn account_lists(
    pool: &PgPool,
    account_id: i64,
    query: &str,
) -> Result<GeneratedGet, ApiError> {
    let page = requested_page(query);
    let rows = sqlx::query_as::<_, ListSummaryRow>(
        "SELECT list.id, list.name, list.description, list.language_code,
                pg_catalog.count(item.media_id)::bigint AS item_count
           FROM source.tmdb_v3_lists AS list
           LEFT JOIN source.tmdb_v3_list_items AS item ON item.list_id = list.id
          WHERE list.account_id = $1
          GROUP BY list.id
          ORDER BY list.id
          LIMIT $2 OFFSET $3",
    )
    .bind(account_id)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(pool)
    .await?;
    let total: i64 =
        sqlx::query_scalar("SELECT count(*) FROM source.tmdb_v3_lists WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(pool)
            .await?;
    let results = rows.into_iter().map(list_summary).collect::<Vec<_>>();
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

async fn account_media(
    state: &TmdbV3State,
    account_id: i64,
    relation: &str,
    media_type: &str,
    query: &str,
) -> Result<GeneratedGet, ApiError> {
    let page = requested_page(query);
    let total: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM source.tmdb_v3_account_items
          WHERE account_id = $1 AND relation = $2 AND media_type = $3",
    )
    .bind(account_id)
    .bind(relation)
    .bind(media_type)
    .fetch_one(&state.write_pool)
    .await?;
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT media_id
           FROM source.tmdb_v3_account_items
          WHERE account_id = $1 AND relation = $2 AND media_type = $3
          ORDER BY created_at DESC, media_id DESC
          LIMIT $4 OFFSET $5",
    )
    .bind(account_id)
    .bind(relation)
    .bind(media_type)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(&state.write_pool)
    .await?;
    let results = media_results(&state.documents, media_type, ids, None).await?;
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

async fn rated_media(
    state: &TmdbV3State,
    owner_kind: &str,
    owner_id: &str,
    media_type: &str,
    query: &str,
) -> Result<GeneratedGet, ApiError> {
    let page = requested_page(query);
    let total: i64 = sqlx::query_scalar(
        "SELECT count(*)
           FROM source.tmdb_v3_ratings
          WHERE owner_kind = $1 AND owner_id = $2 AND media_type = $3",
    )
    .bind(owner_kind)
    .bind(owner_id)
    .bind(media_type)
    .fetch_one(&state.write_pool)
    .await?;
    let rows = sqlx::query_as::<_, RatedRow>(
        "SELECT media_id, rating
           FROM source.tmdb_v3_ratings
          WHERE owner_kind = $1 AND owner_id = $2 AND media_type = $3
          ORDER BY updated_at DESC, media_id DESC
          LIMIT $4 OFFSET $5",
    )
    .bind(owner_kind)
    .bind(owner_id)
    .bind(media_type)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(&state.write_pool)
    .await?;
    let results = media_results_with_ratings(&state.documents, media_type, rows).await?;
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

#[allow(clippy::struct_excessive_bools)]
#[derive(Default)]
struct DiscoverFilters {
    include_adult: bool,
    include_video: bool,
    year: Option<i32>,
    date_gte: Option<chrono::NaiveDate>,
    date_lte: Option<chrono::NaiveDate>,
    original_language: Option<String>,
    without_original_language: Option<String>,
    with_genres: Vec<i64>,
    with_genres_all: bool,
    without_genres: Vec<i64>,
    with_keywords: Vec<i64>,
    with_keywords_all: bool,
    without_keywords: Vec<i64>,
    vote_average_gte: Option<f64>,
    vote_average_lte: Option<f64>,
    vote_count_gte: Option<i64>,
    vote_count_lte: Option<i64>,
    runtime_gte: Option<i32>,
    runtime_lte: Option<i32>,
    sort_by: String,
}

async fn discover_titles(
    state: &TmdbV3State,
    media_type: &str,
    query: &str,
) -> Result<GeneratedGet, ApiError> {
    let filters = parse_discover_filters(query, media_type)?;
    let page = requested_page(query);
    let rows = fetch_discover_rows(&state.read_pool, media_type, &filters, page).await?;
    let total = rows.first().map_or(0, |row| row.total_results);
    let results = rows
        .into_iter()
        .map(|row| title_search_result(row, media_type, false))
        .collect::<Vec<_>>();
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

fn parse_discover_filters(query: &str, media_type: &str) -> Result<DiscoverFilters, ApiError> {
    let include_adult = parse_bool_parameter(query, "include_adult", false)?;
    let include_video = parse_bool_parameter(query, "include_video", false)?;
    let year_name = if media_type == "movie" {
        "primary_release_year"
    } else {
        "first_air_date_year"
    };
    let year = parse_i32_parameter(query, year_name)?;
    let start_date = parse_date_parameter(
        query,
        if media_type == "movie" {
            "primary_release_date.gte"
        } else {
            "first_air_date.gte"
        },
    )?;
    let end_date = parse_date_parameter(
        query,
        if media_type == "movie" {
            "primary_release_date.lte"
        } else {
            "first_air_date.lte"
        },
    )?;
    let with_genres = parse_id_filter(query_parameter(query, "with_genres"))?;
    let with_keywords = parse_id_filter(query_parameter(query, "with_keywords"))?;
    Ok(DiscoverFilters {
        include_adult,
        include_video,
        year,
        date_gte: start_date,
        date_lte: end_date,
        original_language: bounded_filter_string(query_parameter(query, "with_original_language"))?,
        without_original_language: bounded_filter_string(query_parameter(
            query,
            "without_original_language",
        ))?,
        with_genres_all: with_genres.1,
        with_genres: with_genres.0,
        without_genres: parse_id_list(query_parameter(query, "without_genres"))?,
        with_keywords_all: with_keywords.1,
        with_keywords: with_keywords.0,
        without_keywords: parse_id_list(query_parameter(query, "without_keywords"))?,
        vote_average_gte: parse_f64_parameter(query, "vote_average.gte")?,
        vote_average_lte: parse_f64_parameter(query, "vote_average.lte")?,
        vote_count_gte: parse_i64_parameter(query, "vote_count.gte")?,
        vote_count_lte: parse_i64_parameter(query, "vote_count.lte")?,
        runtime_gte: parse_i32_parameter(query, "with_runtime.gte")?,
        runtime_lte: parse_i32_parameter(query, "with_runtime.lte")?,
        sort_by: discover_sort(query_parameter(query, "sort_by"), media_type)?,
    })
}

#[allow(clippy::too_many_lines)]
async fn fetch_discover_rows(
    pool: &PgPool,
    media_type: &str,
    filters: &DiscoverFilters,
    page: i64,
) -> Result<Vec<TitleSearchRow>, ApiError> {
    let order_by = filters.sort_by.as_str();
    let sql = format!(
        "SELECT title.tmdb_id,
                title.display_title,
                title.original_title,
                title.overview,
                title.original_language,
                title.release_date::text,
                title.first_air_date::text,
                title.popularity,
                title.vote_average,
                title.vote_count,
                title.adult,
                title.video,
                title.poster_path,
                title.backdrop_path,
                COALESCE(
                    pg_catalog.array_agg(genre.id ORDER BY genre.id)
                        FILTER (WHERE genre.id IS NOT NULL),
                    ARRAY[]::bigint[]
                ) AS genre_ids,
                pg_catalog.count(*) OVER ()::bigint AS total_results
           FROM catalog.titles AS title
           LEFT JOIN catalog.title_genres AS title_genre
             ON title_genre.title_id = title.id
           LEFT JOIN catalog.genres AS genre
             ON genre.id = title_genre.genre_id
          WHERE title.media_type = $1
            AND title.active
            AND ($2 OR NOT title.adult)
            AND ($3 OR NOT title.video)
            AND ($4::text IS NULL OR title.original_language = $4)
            AND ($5::text IS NULL OR title.original_language <> $5)
            AND ($6::integer IS NULL OR EXTRACT(YEAR FROM COALESCE(title.release_date, title.first_air_date)) = $6)
            AND ($7::date IS NULL OR COALESCE(title.release_date, title.first_air_date) >= $7)
            AND ($8::date IS NULL OR COALESCE(title.release_date, title.first_air_date) <= $8)
            AND (pg_catalog.cardinality($9::bigint[]) = 0 OR EXISTS (
                SELECT 1 FROM catalog.title_genres AS filter_genre
                 WHERE filter_genre.title_id = title.id
                   AND filter_genre.genre_id = ANY($9::bigint[])
            ))
            AND (NOT $10 OR (
                SELECT pg_catalog.count(DISTINCT filter_genre.genre_id)
                  FROM catalog.title_genres AS filter_genre
                 WHERE filter_genre.title_id = title.id
                   AND filter_genre.genre_id = ANY($9::bigint[])
            ) = pg_catalog.cardinality($9::bigint[]))
            AND (pg_catalog.cardinality($11::bigint[]) = 0 OR NOT EXISTS (
                SELECT 1 FROM catalog.title_genres AS filter_genre
                 WHERE filter_genre.title_id = title.id
                   AND filter_genre.genre_id = ANY($11::bigint[])
            ))
            AND (pg_catalog.cardinality($12::bigint[]) = 0 OR EXISTS (
                SELECT 1 FROM catalog.title_keywords AS filter_keyword
                 WHERE filter_keyword.title_id = title.id
                   AND filter_keyword.keyword_id = ANY($12::bigint[])
            ))
            AND (NOT $13 OR (
                SELECT pg_catalog.count(DISTINCT filter_keyword.keyword_id)
                  FROM catalog.title_keywords AS filter_keyword
                 WHERE filter_keyword.title_id = title.id
                   AND filter_keyword.keyword_id = ANY($12::bigint[])
            ) = pg_catalog.cardinality($12::bigint[]))
            AND (pg_catalog.cardinality($14::bigint[]) = 0 OR NOT EXISTS (
                SELECT 1 FROM catalog.title_keywords AS filter_keyword
                 WHERE filter_keyword.title_id = title.id
                   AND filter_keyword.keyword_id = ANY($14::bigint[])
            ))
            AND ($15::double precision IS NULL OR title.vote_average >= $15)
            AND ($16::double precision IS NULL OR title.vote_average <= $16)
            AND ($17::bigint IS NULL OR title.vote_count >= $17)
            AND ($18::bigint IS NULL OR title.vote_count <= $18)
            AND ($19::integer IS NULL OR title.runtime_minutes >= $19)
            AND ($20::integer IS NULL OR title.runtime_minutes <= $20)
          GROUP BY title.id
          ORDER BY {order_by}
          LIMIT $21 OFFSET $22"
    );
    sqlx::query_as::<_, TitleSearchRow>(sqlx::AssertSqlSafe(sql.as_str()))
        .bind(media_type)
        .bind(filters.include_adult)
        .bind(filters.include_video)
        .bind(&filters.original_language)
        .bind(&filters.without_original_language)
        .bind(filters.year)
        .bind(filters.date_gte)
        .bind(filters.date_lte)
        .bind(&filters.with_genres)
        .bind(filters.with_genres_all)
        .bind(&filters.without_genres)
        .bind(&filters.with_keywords)
        .bind(filters.with_keywords_all)
        .bind(&filters.without_keywords)
        .bind(filters.vote_average_gte)
        .bind(filters.vote_average_lte)
        .bind(filters.vote_count_gte)
        .bind(filters.vote_count_lte)
        .bind(filters.runtime_gte)
        .bind(filters.runtime_lte)
        .bind(PAGE_SIZE)
        .bind((page - 1) * PAGE_SIZE)
        .fetch_all(pool)
        .await
        .map_err(ApiError::from)
}

async fn search_titles(
    state: &TmdbV3State,
    media_type: &str,
    query: &str,
    include_people: bool,
) -> Result<GeneratedGet, ApiError> {
    let search_term = query_parameter(query, "query")
        .filter(|value| !value.trim().is_empty() && value.chars().count() <= 256)
        .ok_or_else(|| ApiError::Response(invalid_parameter_error()))?;
    let page = requested_page(query);
    let include_adult = query_parameter(query, "include_adult")
        .map(|value| value.parse::<bool>())
        .transpose()
        .map_err(|_| ApiError::Response(invalid_parameter_error()))?
        .unwrap_or(false);
    let year_name = if media_type == "tv" {
        "first_air_date_year"
    } else {
        "primary_release_year"
    };
    let year = query_parameter(query, year_name)
        .map(|value| value.parse::<i32>())
        .transpose()
        .map_err(|_| ApiError::Response(invalid_parameter_error()))?;

    let (mut results, total) = if media_type == "multi" {
        let (movies, movie_total) = fetch_title_search_rows(
            &state.read_pool,
            "movie",
            &search_term,
            include_adult,
            year,
            page,
        )
        .await?;
        let (tv, tv_total) = fetch_title_search_rows(
            &state.read_pool,
            "tv",
            &search_term,
            include_adult,
            year,
            page,
        )
        .await?;
        let mut rows = movies
            .into_iter()
            .map(|row| title_search_result(row, "movie", true))
            .chain(
                tv.into_iter()
                    .map(|row| title_search_result(row, "tv", true)),
            )
            .collect::<Vec<_>>();
        if include_people {
            let (people, people_total) =
                fetch_person_search_rows(&state.read_pool, &search_term, include_adult, page)
                    .await?;
            let total = movie_total
                .saturating_add(tv_total)
                .saturating_add(people_total);
            rows.extend(
                people
                    .into_iter()
                    .map(|row| person_search_result(row, true)),
            );
            (rows, total)
        } else {
            (rows, movie_total.saturating_add(tv_total))
        }
    } else {
        let (rows, title_total) = fetch_title_search_rows(
            &state.read_pool,
            media_type,
            &search_term,
            include_adult,
            year,
            page,
        )
        .await?;
        (
            rows.into_iter()
                .map(|row| title_search_result(row, media_type, false))
                .collect::<Vec<_>>(),
            title_total,
        )
    };
    if results.len() > usize::try_from(PAGE_SIZE).unwrap_or(usize::MAX) {
        results.truncate(usize::try_from(PAGE_SIZE).unwrap_or(usize::MAX));
    }
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

async fn fetch_title_search_rows(
    pool: &PgPool,
    media_type: &str,
    search_term: &str,
    include_adult: bool,
    year: Option<i32>,
    page: i64,
) -> Result<(Vec<TitleSearchRow>, i64), ApiError> {
    let rows = sqlx::query_as::<_, TitleSearchRow>(
        "SELECT title.tmdb_id,
                title.display_title,
                title.original_title,
                title.overview,
                title.original_language,
                title.release_date::text,
                title.first_air_date::text,
                title.popularity,
                title.vote_average,
                title.vote_count,
                title.adult,
                title.video,
                title.poster_path,
                title.backdrop_path,
                COALESCE(
                    pg_catalog.array_agg(genre.id ORDER BY genre.id)
                        FILTER (WHERE genre.id IS NOT NULL),
                    ARRAY[]::bigint[]
                ) AS genre_ids,
                pg_catalog.count(*) OVER ()::bigint AS total_results
           FROM catalog.titles AS title
           LEFT JOIN search.search_documents AS search
             ON search.title_id = title.id AND search.locale = ''
           LEFT JOIN catalog.title_genres AS title_genre
             ON title_genre.title_id = title.id
           LEFT JOIN catalog.genres AS genre
             ON genre.id = title_genre.genre_id
          WHERE title.media_type = $1
            AND title.active
            AND ($2 = '' OR search.normalized_title LIKE '%' || lower(public.unaccent($2)) || '%'
                 OR search.normalized_original_title LIKE '%' || lower(public.unaccent($2)) || '%'
                 OR search.normalized_aliases LIKE '%' || lower(public.unaccent($2)) || '%')
            AND ($3 OR NOT title.adult)
            AND ($4::integer IS NULL OR (
                ($1 = 'movie' AND EXTRACT(YEAR FROM title.release_date) = $4)
                OR ($1 = 'tv' AND EXTRACT(YEAR FROM title.first_air_date) = $4)
            ))
          GROUP BY title.id
          ORDER BY title.popularity DESC NULLS LAST, title.tmdb_id DESC
          LIMIT $5 OFFSET $6",
    )
    .bind(media_type)
    .bind(search_term)
    .bind(include_adult)
    .bind(year)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(pool)
    .await?;
    let total = rows.first().map_or(0, |row| row.total_results);
    Ok((rows, total))
}

#[derive(Debug, FromRow)]
struct PersonSearchRow {
    id: i64,
    name: Option<String>,
    original_name: Option<String>,
    known_for_department: Option<String>,
    popularity: Option<f64>,
    profile_path: Option<String>,
    adult: bool,
    total_results: i64,
}

async fn fetch_person_search_rows(
    pool: &PgPool,
    search_term: &str,
    include_adult: bool,
    page: i64,
) -> Result<(Vec<PersonSearchRow>, i64), ApiError> {
    let rows = sqlx::query_as::<_, PersonSearchRow>(
        "SELECT person.id,
                person.name,
                person.original_name,
                person.known_for_department,
                person.popularity,
                person.profile_path,
                person.adult,
                pg_catalog.count(*) OVER ()::bigint AS total_results
           FROM catalog.people AS person
          WHERE ($1 = '' OR person.normalized_name LIKE '%' || lower(public.unaccent($1)) || '%')
            AND ($2 OR NOT person.adult)
          ORDER BY person.popularity DESC NULLS LAST, person.id DESC
          LIMIT $3 OFFSET $4",
    )
    .bind(search_term)
    .bind(include_adult)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(pool)
    .await?;
    let total = rows.first().map_or(0, |row| row.total_results);
    Ok((rows, total))
}

async fn search_people(
    state: &TmdbV3State,
    query: &str,
    include_media_type: bool,
) -> Result<GeneratedGet, ApiError> {
    let search_term = query_parameter(query, "query")
        .filter(|value| !value.trim().is_empty() && value.chars().count() <= 256)
        .ok_or_else(|| ApiError::Response(invalid_parameter_error()))?;
    let include_adult = parse_bool_parameter(query, "include_adult", false)?;
    let page = requested_page(query);
    let (rows, total) =
        fetch_person_search_rows(&state.read_pool, &search_term, include_adult, page).await?;
    let results = rows
        .into_iter()
        .map(|row| person_search_result(row, include_media_type))
        .collect::<Vec<_>>();
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

#[derive(Debug, FromRow)]
struct CollectionSearchRow {
    id: i64,
    name: Option<String>,
    poster_path: Option<String>,
    backdrop_path: Option<String>,
    total_results: i64,
}

#[derive(Debug, FromRow)]
struct CompanySearchRow {
    id: i64,
    name: Option<String>,
    logo_path: Option<String>,
    origin_country: Option<String>,
    total_results: i64,
}

#[derive(Debug, FromRow)]
struct KeywordSearchRow {
    id: i64,
    name: Option<String>,
    total_results: i64,
}

async fn search_collections(state: &TmdbV3State, query: &str) -> Result<GeneratedGet, ApiError> {
    let search_term = required_search_term(query)?;
    let page = requested_page(query);
    let rows = sqlx::query_as::<_, CollectionSearchRow>(
        "SELECT collection.id,
                collection.name,
                collection.poster_path,
                collection.backdrop_path,
                pg_catalog.count(*) OVER ()::bigint AS total_results
           FROM catalog.collections AS collection
          WHERE lower(public.unaccent(coalesce(collection.name, '')))
                LIKE '%' || lower(public.unaccent($1)) || '%'
          ORDER BY collection.id DESC
          LIMIT $2 OFFSET $3",
    )
    .bind(&search_term)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(&state.read_pool)
    .await?;
    let total = rows.first().map_or(0, |row| row.total_results);
    let results = rows
        .into_iter()
        .map(|row| {
            json!({
                "backdrop_path": row.backdrop_path,
                "id": row.id,
                "name": row.name,
                "poster_path": row.poster_path
            })
        })
        .collect::<Vec<_>>();
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

async fn search_companies(state: &TmdbV3State, query: &str) -> Result<GeneratedGet, ApiError> {
    let search_term = required_search_term(query)?;
    let page = requested_page(query);
    let rows = sqlx::query_as::<_, CompanySearchRow>(
        "SELECT company.id,
                company.name,
                company.logo_path,
                company.origin_country,
                pg_catalog.count(*) OVER ()::bigint AS total_results
           FROM catalog.companies AS company
          WHERE lower(public.unaccent(coalesce(company.name, '')))
                LIKE '%' || lower(public.unaccent($1)) || '%'
          ORDER BY company.id DESC
          LIMIT $2 OFFSET $3",
    )
    .bind(&search_term)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(&state.read_pool)
    .await?;
    let total = rows.first().map_or(0, |row| row.total_results);
    let results = rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.id,
                "logo_path": row.logo_path,
                "name": row.name,
                "origin_country": row.origin_country
            })
        })
        .collect::<Vec<_>>();
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

async fn search_keywords(state: &TmdbV3State, query: &str) -> Result<GeneratedGet, ApiError> {
    let search_term = required_search_term(query)?;
    let page = requested_page(query);
    let rows = sqlx::query_as::<_, KeywordSearchRow>(
        "SELECT keyword.id,
                keyword.name,
                pg_catalog.count(*) OVER ()::bigint AS total_results
           FROM catalog.keywords AS keyword
          WHERE lower(public.unaccent(coalesce(keyword.name, '')))
                LIKE '%' || lower(public.unaccent($1)) || '%'
          ORDER BY keyword.id DESC
          LIMIT $2 OFFSET $3",
    )
    .bind(&search_term)
    .bind(PAGE_SIZE)
    .bind((page - 1) * PAGE_SIZE)
    .fetch_all(&state.read_pool)
    .await?;
    let total = rows.first().map_or(0, |row| row.total_results);
    let results = rows
        .into_iter()
        .map(|row| json!({"id": row.id, "name": row.name}))
        .collect::<Vec<_>>();
    Ok(GeneratedGet::Response(page_response(results, total, page)))
}

fn required_search_term(query: &str) -> Result<String, ApiError> {
    query_parameter(query, "query")
        .filter(|value| !value.trim().is_empty() && value.chars().count() <= 256)
        .ok_or_else(|| ApiError::Response(invalid_parameter_error()))
}

#[derive(Debug, FromRow)]
struct FindTitleRow {
    media_type: String,
    tmdb_id: i64,
}

#[derive(Debug, FromRow)]
struct FindPersonRow {
    id: i64,
    name: Option<String>,
    known_for_department: Option<String>,
    popularity: Option<f64>,
    profile_path: Option<String>,
    adult: bool,
}

async fn find_external_id(
    state: &TmdbV3State,
    external_id: &str,
    query: &str,
) -> Result<GeneratedGet, ApiError> {
    if external_id.is_empty()
        || external_id.chars().count() > 128
        || external_id.chars().any(char::is_control)
    {
        return Err(ApiError::Response(invalid_parameter_error()));
    }
    let source = query_parameter(query, "external_source");
    let predicate = match source.as_deref() {
        None => "external.imdb_id = $1 OR external.tvdb_id = $1 OR external.wikidata_id = $1 OR external.facebook_id = $1 OR external.instagram_id = $1 OR external.twitter_id = $1".to_owned(),
        Some("imdb_id" | "tvdb_id" | "wikidata_id" | "facebook_id" | "instagram_id" | "twitter_id") => {
            format!("external.{} = $1", source.as_deref().unwrap_or_default())
        }
        Some(_) => return Err(ApiError::Response(invalid_parameter_error())),
    };
    let title_sql = format!(
        "SELECT title.media_type, title.tmdb_id
           FROM catalog.title_external_ids AS external
           JOIN catalog.titles AS title ON title.id = external.title_id
          WHERE title.active AND ({predicate})
          ORDER BY title.media_type, title.tmdb_id"
    );
    let title_rows = sqlx::query_as::<_, FindTitleRow>(sqlx::AssertSqlSafe(title_sql.as_str()))
        .bind(external_id)
        .fetch_all(&state.read_pool)
        .await?;
    let mut movie_ids = Vec::new();
    let mut tv_ids = Vec::new();
    for row in title_rows {
        match row.media_type.as_str() {
            "movie" => movie_ids.push(row.tmdb_id),
            "tv" => tv_ids.push(row.tmdb_id),
            _ => {}
        }
    }
    let person_rows = sqlx::query_as::<_, FindPersonRow>(
        "SELECT id, name, known_for_department, popularity, profile_path, adult
           FROM catalog.people
          WHERE imdb_id = $1
          ORDER BY id",
    )
    .bind(external_id)
    .fetch_all(&state.read_pool)
    .await?;
    let movie_results = media_results(&state.documents, "movie", movie_ids, None).await?;
    let tv_results = media_results(&state.documents, "tv", tv_ids, None).await?;
    let person_results = person_rows
        .into_iter()
        .map(|person| {
            json!({
                "adult": person.adult,
                "gender": 0,
                "id": person.id,
                "known_for": [],
                "known_for_department": person.known_for_department,
                "name": person.name,
                "popularity": person.popularity,
                "profile_path": person.profile_path
            })
        })
        .collect::<Vec<_>>();
    Ok(GeneratedGet::Response(json!({
        "movie_results": movie_results,
        "person_results": person_results,
        "tv_results": tv_results,
        "tv_episode_results": [],
        "tv_season_results": []
    })))
}

fn title_search_result(row: TitleSearchRow, media_type: &str, include_media_type: bool) -> Value {
    let mut result = if media_type == "movie" {
        json!({
            "adult": row.adult,
            "backdrop_path": row.backdrop_path,
            "genre_ids": row.genre_ids,
            "id": row.tmdb_id,
            "original_language": row.original_language,
            "original_title": row.original_title,
            "overview": row.overview,
            "popularity": row.popularity,
            "poster_path": row.poster_path,
            "release_date": row.release_date.unwrap_or_default(),
            "title": row.display_title,
            "video": row.video,
            "vote_average": row.vote_average,
            "vote_count": row.vote_count
        })
    } else {
        json!({
            "adult": row.adult,
            "backdrop_path": row.backdrop_path,
            "first_air_date": row.first_air_date.unwrap_or_default(),
            "genre_ids": row.genre_ids,
            "id": row.tmdb_id,
            "name": row.display_title,
            "origin_country": Vec::<String>::new(),
            "original_language": row.original_language,
            "original_name": row.original_title,
            "overview": row.overview,
            "popularity": row.popularity,
            "poster_path": row.poster_path,
            "vote_average": row.vote_average,
            "vote_count": row.vote_count
        })
    };
    if include_media_type {
        result["media_type"] = json!(media_type);
    }
    result
}

#[allow(clippy::needless_pass_by_value)]
fn person_search_result(row: PersonSearchRow, include_media_type: bool) -> Value {
    let mut result = json!({
        "adult": row.adult,
        "gender": 0,
        "id": row.id,
        "known_for": [],
        "known_for_department": row.known_for_department,
        "name": row.name,
        "original_name": row.original_name,
        "popularity": row.popularity,
        "profile_path": row.profile_path
    });
    if include_media_type {
        result["media_type"] = json!("person");
    }
    result
}

async fn list_detail(
    state: &TmdbV3State,
    list_id: i64,
    query: &str,
) -> Result<GeneratedGet, ApiError> {
    let Some(owner) = list_owner_optional(&state.write_pool, list_id, query).await? else {
        return Ok(GeneratedGet::NotFound);
    };
    let row = sqlx::query_as::<_, ListRow>(
        "SELECT id, name, description, language_code
           FROM source.tmdb_v3_lists WHERE id = $1",
    )
    .bind(list_id)
    .fetch_one(&state.write_pool)
    .await?;
    let ids: Vec<i64> = sqlx::query_scalar(
        "SELECT media_id FROM source.tmdb_v3_list_items
          WHERE list_id = $1 ORDER BY created_at, media_id",
    )
    .bind(list_id)
    .fetch_all(&state.write_pool)
    .await?;
    let item_count = i64::try_from(ids.len()).map_err(|_| ApiError::Database)?;
    let items = media_results(&state.documents, "movie", ids, None).await?;
    let _ = owner;
    Ok(GeneratedGet::Response(json!({
        "created_by": "local",
        "description": row.description,
        "favorite_count": 0,
        "id": row.id,
        "iso_639_1": row.language_code.split('-').next().unwrap_or("en"),
        "item_count": item_count,
        "items": items,
        "name": row.name,
        "poster_path": Value::Null
    })))
}

async fn list_owner(pool: &PgPool, list_id: i64, query: &str) -> Result<i64, Response> {
    let row = list_owner_optional(pool, list_id, query)
        .await?
        .ok_or_else(tmdb_not_found)?;
    Ok(row)
}

async fn list_owner_optional(
    pool: &PgPool,
    list_id: i64,
    query: &str,
) -> Result<Option<i64>, Response> {
    let session_id = query_parameter(query, "session_id").ok_or_else(authentication_error)?;
    let session = load_session(pool, &session_id)
        .await
        .map_err(|_| database_unavailable())?
        .ok_or_else(authentication_error)?;
    let account_id = session.account_id.ok_or_else(authentication_error)?;
    let owner: Option<i64> = sqlx::query_scalar(
        "SELECT account_id FROM source.tmdb_v3_lists WHERE id = $1 AND account_id = $2",
    )
    .bind(list_id)
    .bind(account_id)
    .fetch_optional(pool)
    .await
    .map_err(|_| database_unavailable())?;
    Ok(owner)
}

async fn require_account_session(
    pool: &PgPool,
    query: &str,
    account_id: i64,
) -> Result<SessionRowWithId, Response> {
    let session_id = query_parameter(query, "session_id").ok_or_else(authentication_error)?;
    let session = load_session(pool, &session_id)
        .await
        .map_err(|_| database_unavailable())?
        .ok_or_else(authentication_error)?;
    if session.is_guest || session.account_id != Some(account_id) {
        return Err(authentication_error());
    }
    Ok(SessionRowWithId {
        session_id,
        row: session,
    })
}

async fn require_guest_session(pool: &PgPool, session_id: &str) -> Result<SessionRow, Response> {
    let session = load_session(pool, session_id)
        .await
        .map_err(|_| database_unavailable())?
        .ok_or_else(authentication_error)?;
    if !session.is_guest {
        return Err(authentication_error());
    }
    Ok(session)
}

async fn rating_owner(
    state: &TmdbV3State,
    query: &str,
) -> Result<(&'static str, String), Response> {
    if let Some(session_id) = query_parameter(query, "session_id") {
        let session = load_session(&state.write_pool, &session_id)
            .await
            .map_err(|_| database_unavailable())?
            .ok_or_else(authentication_error)?;
        if session.is_guest {
            return Err(authentication_error());
        }
        return Ok(("session", session_id));
    }
    if let Some(guest_session_id) = query_parameter(query, "guest_session_id") {
        let session = require_guest_session(&state.write_pool, &guest_session_id).await?;
        let _ = session;
        return Ok(("guest", guest_session_id));
    }
    Err(authentication_error())
}

async fn load_session(pool: &PgPool, session_id: &str) -> Result<Option<SessionRow>, sqlx::Error> {
    sqlx::query_as::<_, SessionRow>(
        "SELECT account_id, is_guest
           FROM source.tmdb_v3_sessions
          WHERE session_id = $1 AND expires_at > pg_catalog.clock_timestamp()",
    )
    .bind(session_id)
    .fetch_optional(pool)
    .await
}

async fn media_results(
    documents: &TmdbDocumentRepository,
    media_type: &str,
    ids: Vec<i64>,
    ratings: Option<&[f64]>,
) -> Result<Vec<Value>, sqlx::Error> {
    let mut results = Vec::with_capacity(ids.len());
    for (index, id) in ids.into_iter().enumerate() {
        let path = format!("{media_type}/{id}");
        let mut value = documents
            .get(&path, "")
            .await?
            .unwrap_or_else(|| json!({"id": id}));
        if let Some(ratings) = ratings
            && let Some(object) = value.as_object_mut()
            && let Some(rating) = ratings.get(index)
        {
            object.insert("rating".to_owned(), json!(rating));
        }
        results.push(value);
    }
    Ok(results)
}

async fn media_results_with_ratings(
    documents: &TmdbDocumentRepository,
    media_type: &str,
    rows: Vec<RatedRow>,
) -> Result<Vec<Value>, sqlx::Error> {
    let ids = rows.iter().map(|row| row.media_id).collect::<Vec<_>>();
    let ratings = rows.iter().map(|row| row.rating).collect::<Vec<_>>();
    media_results(documents, media_type, ids, Some(&ratings)).await
}

#[allow(clippy::needless_pass_by_value)]
fn list_summary(row: ListSummaryRow) -> Value {
    json!({
        "description": row.description,
        "favorite_count": 0,
        "id": row.id,
        "iso_639_1": row.language_code.split('-').next().unwrap_or("en"),
        "item_count": row.item_count,
        "name": row.name,
        "poster_path": Value::Null
    })
}

#[allow(clippy::needless_pass_by_value)]
fn page_response(results: Vec<Value>, total: i64, page: i64) -> Value {
    let total_pages = if total == 0 {
        1
    } else {
        (total + PAGE_SIZE - 1) / PAGE_SIZE
    };
    json!({
        "page": page,
        "results": results,
        "total_pages": total_pages,
        "total_results": total
    })
}

fn requested_page(query: &str) -> i64 {
    query_parameter(query, "page")
        .and_then(|value| value.parse::<i64>().ok())
        .filter(|page| *page > 0)
        .unwrap_or(1)
}

fn parse_bool_parameter(query: &str, name: &str, default: bool) -> Result<bool, ApiError> {
    query_parameter(query, name)
        .map(|value| value.parse::<bool>())
        .transpose()
        .map(|value| value.unwrap_or(default))
        .map_err(|_| ApiError::Response(invalid_parameter_error()))
}

fn parse_i32_parameter(query: &str, name: &str) -> Result<Option<i32>, ApiError> {
    query_parameter(query, name)
        .map(|value| value.parse::<i32>())
        .transpose()
        .map_err(|_| ApiError::Response(invalid_parameter_error()))
}

fn parse_i64_parameter(query: &str, name: &str) -> Result<Option<i64>, ApiError> {
    query_parameter(query, name)
        .map(|value| value.parse::<i64>())
        .transpose()
        .map_err(|_| ApiError::Response(invalid_parameter_error()))
}

fn parse_f64_parameter(query: &str, name: &str) -> Result<Option<f64>, ApiError> {
    let value = query_parameter(query, name)
        .map(|value| value.parse::<f64>())
        .transpose()
        .map_err(|_| ApiError::Response(invalid_parameter_error()))?;
    if value.is_some_and(|value| !value.is_finite()) {
        return Err(ApiError::Response(invalid_parameter_error()));
    }
    Ok(value)
}

fn parse_date_parameter(query: &str, name: &str) -> Result<Option<chrono::NaiveDate>, ApiError> {
    query_parameter(query, name)
        .map(|value| chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d"))
        .transpose()
        .map_err(|_| ApiError::Response(invalid_parameter_error()))
}

fn bounded_filter_string(value: Option<String>) -> Result<Option<String>, ApiError> {
    if value.as_deref().is_some_and(|value| {
        value.is_empty() || value.chars().count() > 32 || value.chars().any(char::is_control)
    }) {
        return Err(ApiError::Response(invalid_parameter_error()));
    }
    Ok(value)
}

fn parse_id_filter(value: Option<String>) -> Result<(Vec<i64>, bool), ApiError> {
    let Some(value) = value else {
        return Ok((Vec::new(), false));
    };
    let require_all = value.contains('|');
    let ids = value
        .split(['|', ','])
        .map(|value| {
            value
                .parse::<i64>()
                .ok()
                .filter(|id| *id > 0)
                .ok_or_else(|| ApiError::Response(invalid_parameter_error()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if ids.is_empty() || ids.len() > 100 {
        return Err(ApiError::Response(invalid_parameter_error()));
    }
    Ok((ids, require_all))
}

fn parse_id_list(value: Option<String>) -> Result<Vec<i64>, ApiError> {
    parse_id_filter(value).map(|(ids, _)| ids)
}

fn discover_sort(value: Option<String>, media_type: &str) -> Result<String, ApiError> {
    let value = value.unwrap_or_else(|| "popularity.desc".to_owned());
    let order = match value.as_str() {
        "popularity.asc" => "title.popularity ASC NULLS LAST, title.tmdb_id ASC",
        "popularity.desc" => "title.popularity DESC NULLS LAST, title.tmdb_id DESC",
        "vote_average.asc" => "title.vote_average ASC NULLS LAST, title.tmdb_id ASC",
        "vote_average.desc" => "title.vote_average DESC NULLS LAST, title.tmdb_id DESC",
        "vote_count.asc" => "title.vote_count ASC NULLS LAST, title.tmdb_id ASC",
        "vote_count.desc" => "title.vote_count DESC NULLS LAST, title.tmdb_id DESC",
        "release_date.asc" | "primary_release_date.asc" if media_type == "movie" => {
            "title.release_date ASC NULLS LAST, title.tmdb_id ASC"
        }
        "release_date.desc" | "primary_release_date.desc" if media_type == "movie" => {
            "title.release_date DESC NULLS LAST, title.tmdb_id DESC"
        }
        "first_air_date.asc" if media_type == "tv" => {
            "title.first_air_date ASC NULLS LAST, title.tmdb_id ASC"
        }
        "first_air_date.desc" if media_type == "tv" => {
            "title.first_air_date DESC NULLS LAST, title.tmdb_id DESC"
        }
        "original_title.asc" | "name.asc" => {
            "title.display_title ASC NULLS LAST, title.tmdb_id ASC"
        }
        "original_title.desc" | "name.desc" => {
            "title.display_title DESC NULLS LAST, title.tmdb_id DESC"
        }
        _ => return Err(ApiError::Response(invalid_parameter_error())),
    };
    Ok(order.to_owned())
}

fn body_object(body: &Bytes) -> Result<Map<String, Value>, Response> {
    if body.len() > 64 * 1024 {
        return Err(invalid_parameter_error());
    }
    let value: Value = serde_json::from_slice(body).map_err(|_| invalid_parameter_error())?;
    value
        .as_object()
        .cloned()
        .ok_or_else(invalid_parameter_error)
}

fn required_string(object: &Map<String, Value>, name: &str) -> Result<String, Response> {
    optional_string(object, name).ok_or_else(invalid_parameter_error)
}

fn optional_string(object: &Map<String, Value>, name: &str) -> Option<String> {
    object
        .get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_id(value: &str) -> Option<i64> {
    value.parse::<i64>().ok().filter(|value| *value > 0)
}

fn parse_positive_component(value: &str) -> Option<i32> {
    value.parse::<i32>().ok().filter(|value| *value > 0)
}

fn new_token() -> String {
    Uuid::now_v7().simple().to_string()
}

fn tmdb_timestamp(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

fn json_response(value: Value) -> Response {
    (StatusCode::OK, axum::Json(value)).into_response()
}

fn status_response(status_code: u16, status_message: &str) -> Response {
    json_response(json!({
        "status_code": status_code,
        "status_message": status_message
    }))
}

fn query_string(uri: &Uri) -> &str {
    uri.query().unwrap_or_default()
}

fn query_parameter(query: &str, name: &str) -> Option<String> {
    url::form_urlencoded::parse(query.as_bytes())
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.into_owned())
}

fn canonical_query(raw: &str) -> String {
    url::form_urlencoded::parse(raw.as_bytes())
        .filter(|(name, _)| name != "api_key")
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn valid_endpoint_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 512
        && !path.chars().any(char::is_control)
        && !path.contains("..")
        && !path.contains(['?', '#', '\\'])
        && !path.starts_with('/')
        && !path.ends_with('/')
}

fn tmdb_not_found() -> Response {
    problem(
        StatusCode::NOT_FOUND,
        34,
        "The resource you requested could not be found.",
    )
}

fn method_not_allowed() -> Response {
    problem(
        StatusCode::METHOD_NOT_ALLOWED,
        36,
        "The requested method is not allowed.",
    )
}

fn authentication_error() -> Response {
    problem(StatusCode::UNAUTHORIZED, 3, "Authentication failed.")
}

fn invalid_id_error() -> Response {
    problem(StatusCode::BAD_REQUEST, 6, "Invalid id.")
}

fn invalid_parameter_error() -> Response {
    problem(StatusCode::BAD_REQUEST, 7, "Invalid parameter.")
}

fn database_unavailable() -> Response {
    problem(
        StatusCode::SERVICE_UNAVAILABLE,
        41,
        "The database is unavailable.",
    )
}

fn problem(status: StatusCode, status_code: u16, status_message: &str) -> Response {
    (
        status,
        axum::Json(json!({
            "status_code": status_code,
            "status_message": status_message,
            "success": false
        })),
    )
        .into_response()
}

struct SessionRowWithId {
    session_id: String,
    row: SessionRow,
}
