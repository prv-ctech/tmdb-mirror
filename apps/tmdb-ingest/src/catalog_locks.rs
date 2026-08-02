use std::collections::BTreeSet;
use std::time::Duration;

use sqlx::{Postgres, Transaction};
use tmdb_domain::MediaType;
use tmdb_jobs::JobExecutionError;
use tmdb_upstream::{
    ChangePage, TmdbCollection, TmdbCompany, TmdbCredit, TmdbCredits, TmdbEpisode, TmdbGenre,
    TmdbKeyword, TmdbMovie, TmdbNetwork, TmdbSeason, TmdbSeasonSummary, TmdbTv,
};

use super::{normalize_language, source_id};

/// Builds the complete shared-resource set a movie-detail write can mutate.
///
/// The set is validated before the transaction begins so malformed upstream
/// identifiers cannot leave a partially written catalog transaction behind.
pub(crate) fn movie_write_resources(
    movie: &TmdbMovie,
    tmdb_id: i64,
) -> Result<BTreeSet<String>, JobExecutionError> {
    let mut resources = BTreeSet::new();
    insert_title_resource(&mut resources, "movie", tmdb_id);
    insert_genre_resources(&mut resources, &movie.genres)?;
    insert_keyword_resources(&mut resources, &movie.keywords)?;
    insert_credit_resources(&mut resources, &movie.credits)?;
    insert_company_resources(&mut resources, &movie.production_companies)?;
    insert_language_resource(&mut resources, movie.original_language.as_deref())?;
    insert_collection_resource(&mut resources, movie.belongs_to_collection.as_ref())?;
    Ok(resources)
}

/// Builds the complete shared-resource set a TV-detail write can mutate.
pub(crate) fn tv_write_resources(
    series: &TmdbTv,
    tmdb_id: i64,
) -> Result<BTreeSet<String>, JobExecutionError> {
    let mut resources = BTreeSet::new();
    insert_title_resource(&mut resources, "tv", tmdb_id);
    insert_genre_resources(&mut resources, &series.genres)?;
    insert_keyword_resources(&mut resources, &series.keywords)?;
    insert_credit_resources(&mut resources, &series.credits)?;
    insert_season_resources(&mut resources, &series.seasons)?;
    insert_company_resources(&mut resources, &series.production_companies)?;
    insert_network_resources(&mut resources, &series.networks)?;
    insert_language_resource(&mut resources, series.original_language.as_deref())?;
    Ok(resources)
}

/// Builds the complete shared-resource set a season-detail write can mutate.
pub(crate) fn season_write_resources(
    tv_id: i64,
    season: &TmdbSeason,
    season_id: i64,
) -> Result<BTreeSet<String>, JobExecutionError> {
    let mut resources = BTreeSet::new();
    insert_title_resource(&mut resources, "tv", tv_id);
    insert_resource(&mut resources, "season", season_id);
    for episode in &season.episodes {
        insert_episode_resource(&mut resources, episode)?;
    }
    Ok(resources)
}

/// Builds title locks for a page of change-list rows without changing the
/// source-page write order.
pub(crate) fn changes_write_resources(
    media_type: MediaType,
    page: &ChangePage,
) -> Result<BTreeSet<String>, JobExecutionError> {
    let mut resources = BTreeSet::new();
    let media_type = media_type.to_string();
    for changed in &page.results {
        let tmdb_id = source_id(changed.id)?;
        insert_title_resource(&mut resources, &media_type, tmdb_id);
    }
    Ok(resources)
}

/// Acquires transaction-scoped advisory locks for every resource the caller
/// will mutate. This must run before any catalog write in the transaction.
///
/// Both the Rust `BTreeSet` and the database function sort resource names.
/// The duplicate ordering guard ensures that every ingestion path queues on
/// the same first lock, removing circular row-lock waits without changing the
/// order of upstream catalog data or image-job submission.
pub(crate) async fn prelock_catalog_write_resources(
    transaction: &mut Transaction<'_, Postgres>,
    resources: BTreeSet<String>,
) -> Result<(), JobExecutionError> {
    if resources.is_empty() {
        return Ok(());
    }

    let (statement_timeout, lock_timeout): (String, String) = sqlx::query_as(
        "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')",
    )
    .fetch_one(&mut **transaction)
    .await
    .map_err(database_error)?;

    // This is a queue, not a conflicting catalog operation. The normal 2 s
    // lock timeout and 5 s statement timeout are restored before any write;
    // disabling them only here prevents a healthy ordered queue from being
    // retried just because another full-detail transaction is still running.
    set_local_setting(transaction, "lock_timeout", "0").await?;
    set_local_setting(transaction, "statement_timeout", "0").await?;
    sqlx::query("SELECT ops.lock_catalog_write_resources($1::text[])")
        .bind(resources.into_iter().collect::<Vec<_>>())
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    set_local_setting(transaction, "statement_timeout", &statement_timeout).await?;
    set_local_setting(transaction, "lock_timeout", &lock_timeout).await
}

async fn set_local_setting(
    transaction: &mut Transaction<'_, Postgres>,
    setting: &str,
    value: &str,
) -> Result<(), JobExecutionError> {
    sqlx::query("SELECT pg_catalog.set_config($1, $2, true)")
        .bind(setting)
        .bind(value)
        .execute(&mut **transaction)
        .await
        .map_err(database_error)?;
    Ok(())
}

fn insert_title_resource(resources: &mut BTreeSet<String>, media_type: &str, tmdb_id: i64) {
    resources.insert(format!("catalog:title:{media_type}:{tmdb_id}"));
}

fn insert_genre_resources(
    resources: &mut BTreeSet<String>,
    genres: &[TmdbGenre],
) -> Result<(), JobExecutionError> {
    for genre in genres {
        insert_source_resource(resources, "genre", genre.id)?;
    }
    Ok(())
}

fn insert_keyword_resources(
    resources: &mut BTreeSet<String>,
    keywords: &[TmdbKeyword],
) -> Result<(), JobExecutionError> {
    for keyword in keywords {
        insert_source_resource(resources, "keyword", keyword.id)?;
    }
    Ok(())
}

fn insert_credit_resources(
    resources: &mut BTreeSet<String>,
    credits: &TmdbCredits,
) -> Result<(), JobExecutionError> {
    for credit in credits.cast.iter().chain(credits.crew.iter()) {
        insert_credit_resource(resources, credit)?;
    }
    Ok(())
}

fn insert_credit_resource(
    resources: &mut BTreeSet<String>,
    credit: &TmdbCredit,
) -> Result<(), JobExecutionError> {
    insert_source_resource(resources, "person", credit.id)?;
    Ok(())
}

fn insert_season_resources(
    resources: &mut BTreeSet<String>,
    seasons: &[TmdbSeasonSummary],
) -> Result<(), JobExecutionError> {
    for season in seasons {
        insert_source_resource(resources, "season", season.id)?;
    }
    Ok(())
}

fn insert_episode_resource(
    resources: &mut BTreeSet<String>,
    episode: &TmdbEpisode,
) -> Result<(), JobExecutionError> {
    insert_source_resource(resources, "episode", episode.id)?;
    insert_credit_resources(resources, &episode.credits)
}

fn insert_company_resources(
    resources: &mut BTreeSet<String>,
    companies: &[TmdbCompany],
) -> Result<(), JobExecutionError> {
    for company in companies {
        insert_source_resource(resources, "company", company.id)?;
    }
    Ok(())
}

fn insert_network_resources(
    resources: &mut BTreeSet<String>,
    networks: &[TmdbNetwork],
) -> Result<(), JobExecutionError> {
    for network in networks {
        insert_source_resource(resources, "network", network.id)?;
    }
    Ok(())
}

fn insert_language_resource(
    resources: &mut BTreeSet<String>,
    language: Option<&str>,
) -> Result<(), JobExecutionError> {
    let Some(language) = language.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(());
    };
    resources.insert(format!(
        "catalog:language:{}",
        normalize_language(language)?
    ));
    Ok(())
}

fn insert_collection_resource(
    resources: &mut BTreeSet<String>,
    collection: Option<&TmdbCollection>,
) -> Result<(), JobExecutionError> {
    let Some(collection) = collection else {
        return Ok(());
    };
    insert_source_resource(resources, "collection", collection.id)?;
    Ok(())
}

fn insert_source_resource(
    resources: &mut BTreeSet<String>,
    resource_type: &str,
    raw_id: u64,
) -> Result<i64, JobExecutionError> {
    let id = source_id(raw_id)?;
    insert_resource(resources, resource_type, id);
    Ok(id)
}

fn insert_resource(resources: &mut BTreeSet<String>, resource_type: &str, id: i64) {
    resources.insert(format!("catalog:{resource_type}:{id}"));
}

fn database_error(_: sqlx::Error) -> JobExecutionError {
    JobExecutionError::retry("database_unavailable", Duration::from_secs(5))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use sqlx::PgPool;
    use tmdb_upstream::{
        TmdbCollection, TmdbCompany, TmdbCredit, TmdbCredits, TmdbEpisode, TmdbGenre, TmdbKeyword,
        TmdbMovie, TmdbSeason,
    };

    use super::*;

    #[test]
    fn movie_resource_set_is_complete_deduplicated_and_sorted()
    -> Result<(), Box<dyn std::error::Error>> {
        let movie = TmdbMovie {
            id: 42,
            original_language: Some("EN".to_owned()),
            genres: vec![
                TmdbGenre { id: 2, name: None },
                TmdbGenre { id: 1, name: None },
            ],
            keywords: vec![TmdbKeyword { id: 3, name: None }],
            credits: TmdbCredits {
                cast: vec![TmdbCredit {
                    id: 4,
                    ..TmdbCredit::default()
                }],
                crew: vec![TmdbCredit {
                    id: 4,
                    ..TmdbCredit::default()
                }],
            },
            production_companies: vec![TmdbCompany {
                id: 5,
                ..TmdbCompany::default()
            }],
            belongs_to_collection: Some(TmdbCollection {
                id: 6,
                ..TmdbCollection::default()
            }),
            ..TmdbMovie::default()
        };

        assert_eq!(
            movie_write_resources(&movie, 42)?
                .into_iter()
                .collect::<Vec<_>>(),
            [
                "catalog:collection:6",
                "catalog:company:5",
                "catalog:genre:1",
                "catalog:genre:2",
                "catalog:keyword:3",
                "catalog:language:en",
                "catalog:person:4",
                "catalog:title:movie:42",
            ]
        );
        Ok(())
    }

    #[test]
    fn season_resource_set_covers_parent_season_episode_and_people()
    -> Result<(), Box<dyn std::error::Error>> {
        let season = TmdbSeason {
            id: 20,
            episodes: vec![TmdbEpisode {
                id: 30,
                credits: TmdbCredits {
                    cast: vec![TmdbCredit {
                        id: 40,
                        ..TmdbCredit::default()
                    }],
                    ..TmdbCredits::default()
                },
                ..TmdbEpisode::default()
            }],
            ..TmdbSeason::default()
        };

        assert_eq!(
            season_write_resources(10, &season, 20)?
                .into_iter()
                .collect::<Vec<_>>(),
            [
                "catalog:episode:30",
                "catalog:person:40",
                "catalog:season:20",
                "catalog:title:tv:10",
            ]
        );
        Ok(())
    }

    #[test]
    fn nested_invalid_source_id_is_rejected_before_any_transaction() {
        let movie = TmdbMovie {
            genres: vec![TmdbGenre { id: 0, name: None }],
            ..TmdbMovie::default()
        };
        assert!(movie_write_resources(&movie, 42).is_err());
    }

    #[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
    async fn prelocking_restores_regular_transaction_timeouts(pool: PgPool) -> sqlx::Result<()> {
        let mut transaction = pool.begin().await?;
        let expected: (String, String) = sqlx::query_as(
            "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')",
        )
        .fetch_one(&mut *transaction)
        .await?;

        prelock_catalog_write_resources(
            &mut transaction,
            BTreeSet::from(["catalog:genre:2".to_owned(), "catalog:genre:1".to_owned()]),
        )
        .await
        .map_err(|error| sqlx::Error::Protocol(error.to_string()))?;

        let actual: (String, String) = sqlx::query_as(
            "SELECT current_setting('statement_timeout'), current_setting('lock_timeout')",
        )
        .fetch_one(&mut *transaction)
        .await?;
        assert_eq!(actual, expected);
        transaction.rollback().await?;
        Ok(())
    }
}
