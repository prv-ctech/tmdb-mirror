use std::num::NonZeroU32;

use chrono::NaiveDate;
use sqlx::PgPool;
use tmdb_db::{
    AnimeScope, CatalogCompany, CatalogError, CatalogFilters, CatalogGenre, CatalogKeyword,
    CatalogLanguage, CatalogNetwork, CatalogRepository, CatalogTag, RecentCursor,
};
use tmdb_domain::{MediaType, TitleKey};

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn catalog_migration_exposes_shared_titles_dimensions_and_search_projection(
    pool: PgPool,
) -> sqlx::Result<()> {
    let versions: Vec<i64> = sqlx::query_scalar(
        "SELECT version FROM ops._sqlx_migrations WHERE success ORDER BY version",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        versions,
        [
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ]
    );

    let revision: String = sqlx::query_scalar("SELECT schema_revision FROM ops.readiness")
        .fetch_one(&pool)
        .await?;
    assert_eq!(revision, "0015");

    let objects: Vec<String> = sqlx::query_scalar(
        "SELECT format('%I.%I', n.nspname, c.relname)
           FROM pg_class AS c
           JOIN pg_namespace AS n ON n.oid = c.relnamespace
          WHERE n.nspname IN ('catalog', 'search')
            AND c.relkind IN ('r', 'p', 'v', 'm')
          ORDER BY 1",
    )
    .fetch_all(&pool)
    .await?;
    for expected in [
        "catalog.titles",
        "catalog.movie_details",
        "catalog.tv_details",
        "catalog.genres",
        "catalog.keywords",
        "catalog.tags",
        "catalog.companies",
        "catalog.networks",
        "catalog.languages",
        "catalog.countries",
        "catalog.collections",
        "catalog.title_genres",
        "catalog.title_keywords",
        "catalog.title_tags",
        "catalog.title_companies",
        "catalog.title_networks",
        "catalog.title_languages",
        "catalog.title_countries",
        "catalog.title_collections",
        "search.search_documents",
    ] {
        assert!(
            objects.iter().any(|object| object == expected),
            "missing {expected}"
        );
    }

    for extension in ["pg_trgm", "unaccent"] {
        let installed: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)")
                .bind(extension)
                .fetch_one(&pool)
                .await?;
        assert!(installed, "required extension {extension} is missing");
    }

    let search_write_privileges: (bool, bool, bool) = sqlx::query_as(
        "SELECT has_table_privilege('ingest_writer', 'search.search_documents', 'INSERT'),
                has_table_privilege('ingest_writer', 'search.search_documents', 'UPDATE'),
                has_table_privilege('ingest_writer', 'search.search_documents', 'DELETE')",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(search_write_privileges, (false, false, false));

    for index_name in [
        "titles_non_anime_popularity_idx",
        "titles_anime_popularity_idx",
        "titles_non_anime_popularity_global_idx",
        "titles_anime_popularity_global_idx",
        "titles_non_anime_top_rating_global_idx",
        "titles_anime_top_rating_global_idx",
        "titles_non_anime_release_idx",
        "titles_non_anime_first_air_idx",
        "titles_anime_release_idx",
        "titles_anime_first_air_idx",
        "title_genres_genre_idx",
        "title_keywords_keyword_idx",
        "title_tags_tag_idx",
        "title_companies_company_idx",
        "title_networks_network_idx",
        "title_languages_language_idx",
        "title_countries_country_idx",
        "title_collections_collection_idx",
        "search_documents_search_vector_gin_idx",
        "search_documents_normalized_title_trgm_idx",
        "search_documents_normalized_original_title_trgm_idx",
        "search_documents_normalized_aliases_trgm_idx",
        "titles_non_anime_runtime_idx",
        "titles_anime_runtime_idx",
        "titles_non_anime_status_idx",
        "titles_anime_status_idx",
    ] {
        let installed: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
            .bind(format!("catalog.{index_name}"))
            .fetch_one(&pool)
            .await?
            || sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(format!("search.{index_name}"))
                .fetch_one(&pool)
                .await?;
        assert!(installed, "required index {index_name} is missing");
    }
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn catalog_constraints_reject_invalid_identity_and_cross_media_details(
    pool: PgPool,
) -> sqlx::Result<()> {
    assert_sqlstate(
        &pool,
        "INSERT INTO catalog.titles(media_type, tmdb_id) VALUES ('book', 1)",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO catalog.titles(media_type, tmdb_id) VALUES ('movie', 0)",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO catalog.titles(media_type, tmdb_id, popularity)
         VALUES ('movie', 2, 'NaN'::double precision)",
        "23514",
    )
    .await?;
    assert_sqlstate(
        &pool,
        "INSERT INTO catalog.titles(media_type, tmdb_id, popularity)
         VALUES ('movie', 3, 'Infinity'::double precision)",
        "23514",
    )
    .await?;

    let title_id = insert_title(&pool, "tv", 9_001, "Cross-media fixture", 1.0, false).await?;
    assert_title_sqlstate(
        &pool,
        "INSERT INTO catalog.movie_details(title_id) VALUES ($1)",
        title_id,
        "23503",
    )
    .await?;

    sqlx::query("INSERT INTO catalog.genres(id, name) VALUES (18, 'Drama')")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.title_genres(title_id, genre_id) VALUES ($1, 18)")
        .bind(title_id)
        .execute(&pool)
        .await?;
    assert_title_sqlstate(
        &pool,
        "INSERT INTO catalog.title_genres(title_id, genre_id) VALUES ($1, 18)",
        title_id,
        "23505",
    )
    .await?;
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn repository_isolates_anime_and_media_type_with_keyset_ordering(
    pool: PgPool,
) -> sqlx::Result<()> {
    let anime_movie = insert_title(&pool, "movie", 10_001, "One Piece Film", 100.0, true).await?;
    let anime_tv = insert_title(&pool, "tv", 10_002, "One Piece", 90.0, true).await?;
    let live_action =
        insert_title(&pool, "tv", 10_003, "One Piece Live Action", 95.0, false).await?;
    let ordinary_movie =
        insert_title(&pool, "movie", 10_004, "Ordinary Movie", 80.0, false).await?;

    let repository = CatalogRepository::new(pool.clone());
    let first = repository
        .list_popular(None, AnimeScope::OnlyNonAnime, 1, None)
        .await
        .map_err(db_error)?;
    assert_eq!(first.items.len(), 1);
    assert_eq!(first.items[0].id, live_action);
    assert!(!first.items[0].is_anime);
    let cursor = first
        .next
        .ok_or_else(|| test_error("full page has no cursor"))?;

    let second = repository
        .list_popular(None, AnimeScope::OnlyNonAnime, 1, Some(cursor))
        .await
        .map_err(db_error)?;
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].id, ordinary_movie);
    assert!(!second.items[0].is_anime);
    assert!(second.next.is_none());

    let movie_page = repository
        .list_popular(Some(MediaType::Movie), AnimeScope::OnlyAnime, 10, None)
        .await
        .map_err(db_error)?;
    assert_eq!(movie_page.items.len(), 1);
    assert_eq!(movie_page.items[0].id, anime_movie);
    assert!(movie_page.items[0].is_anime);

    let anime_page = repository
        .list_popular(None, AnimeScope::OnlyAnime, 10, None)
        .await
        .map_err(db_error)?;
    assert_eq!(
        anime_page
            .items
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [anime_movie, anime_tv]
    );
    assert!(anime_page.items.iter().all(|item| item.is_anime));

    let key = TitleKey::new(
        MediaType::Tv,
        NonZeroU32::new(10_002).ok_or_else(|| test_error("fixture ID must be nonzero"))?,
    );
    let fetched = repository.get_title(key).await.map_err(db_error)?;
    assert_eq!(fetched.as_ref().map(|title| title.id), Some(anime_tv));
    assert!(matches!(
        repository
            .list_popular(None, AnimeScope::OnlyNonAnime, 0, None)
            .await,
        Err(CatalogError::InvalidInput)
    ));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn search_projection_normalizes_accents_and_uses_fts_and_trigram_indexes(
    pool: PgPool,
) -> sqlx::Result<()> {
    let accented = insert_title(&pool, "movie", 20_001, "Été One Piece", 20.0, true).await?;
    let anime_tv = insert_title(&pool, "tv", 20_002, "One Piece Treasure", 19.0, true).await?;
    let live_action =
        insert_title(&pool, "tv", 20_003, "One Piece Live Action", 18.0, false).await?;
    let ordinary = insert_title(
        &pool,
        "movie",
        20_004,
        "Ordinary Search Result",
        17.0,
        false,
    )
    .await?;

    sqlx::query(
        "INSERT INTO catalog.titles(media_type, tmdb_id, display_title, popularity)
         SELECT 'movie', 30_000 + serial, 'Noise Title ' || serial, 1.0
           FROM generate_series(1, 100_000) AS serial",
    )
    .execute(&pool)
    .await?;
    sqlx::raw_sql("ANALYZE catalog.titles; ANALYZE search.search_documents;")
        .execute(&pool)
        .await?;

    let normalized: String = sqlx::query_scalar(
        "SELECT normalized_title FROM search.search_documents WHERE title_id = $1 AND locale = ''",
    )
    .bind(accented)
    .fetch_one(&pool)
    .await?;
    assert_eq!(normalized, "ete one piece");

    let repository = CatalogRepository::new(pool.clone());
    let unaccented_query_results = repository
        .search("Et\u{00e9}", None, AnimeScope::OnlyAnime, 10)
        .await
        .map_err(db_error)?;
    assert!(
        unaccented_query_results
            .iter()
            .any(|item| item.id == accented),
        "accented query should match the normalized search projection"
    );
    let anime_results = repository
        .search("one piece", None, AnimeScope::OnlyAnime, 10)
        .await
        .map_err(db_error)?;
    assert_eq!(
        anime_results.iter().map(|item| item.id).collect::<Vec<_>>(),
        [anime_tv, accented]
    );
    assert!(anime_results.iter().all(|item| item.is_anime));

    let ordinary_results = repository
        .search("one piece", None, AnimeScope::OnlyNonAnime, 10)
        .await
        .map_err(db_error)?;
    assert!(ordinary_results.iter().all(|item| !item.is_anime));
    assert!(!ordinary_results.iter().any(|item| item.id == anime_tv));
    assert!(ordinary_results.iter().any(|item| item.id == live_action));
    assert!(ordinary_results.iter().all(|item| item.id != ordinary));

    let fts_plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN (COSTS OFF)
         SELECT title_id FROM search.search_documents
          WHERE search_vector @@ websearch_to_tsquery('simple', 'one piece')",
    )
    .fetch_all(&pool)
    .await?;
    let fts_plan = fts_plan.join("\n");
    assert!(
        fts_plan.contains("search_documents_search_vector_gin_idx"),
        "FTS plan did not use GIN index: {fts_plan}"
    );

    let trigram_plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN (COSTS OFF)
         SELECT title_id FROM search.search_documents
          WHERE normalized_title % 'Noise Ttle 9999'
          ORDER BY normalized_title <-> 'Noise Ttle 9999'
          LIMIT 5",
    )
    .fetch_all(&pool)
    .await?;
    let trigram_plan = trigram_plan.join("\n");
    assert!(
        trigram_plan.contains("search_documents_normalized_title_trgm_idx"),
        "trigram plan did not use GiST index: {trigram_plan}"
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn repository_reads_scope_isolated_detail_and_all_committed_facets(
    pool: PgPool,
) -> sqlx::Result<()> {
    let title_id = insert_title(&pool, "movie", 40_001, "Anime Detail Fixture", 50.0, true).await?;
    sqlx::query(
        "UPDATE catalog.titles
            SET original_title = 'Anime Detail Original',
                overview = 'A detail fixture',
                tagline = 'The detail tagline',
                status = 'Released',
                original_language = 'ja',
                release_date = '2024-01-02',
                runtime_minutes = 115,
                adult = true,
                video = true,
                homepage = 'https://example.invalid/detail',
                poster_path = '/detail-poster.jpg',
                backdrop_path = '/detail-backdrop.jpg'
          WHERE id = $1",
    )
    .bind(title_id)
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.movie_details(
             title_id, budget, revenue, runtime_minutes, imdb_id
         ) VALUES ($1, 100, 200, 115, 'tt40001')",
    )
    .bind(title_id)
    .execute(&pool)
    .await?;

    sqlx::query("INSERT INTO catalog.genres(id, name) VALUES (18, 'Drama')")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.keywords(id, name) VALUES (210024, 'anime')")
        .execute(&pool)
        .await?;
    let tag_id: i64 =
        sqlx::query_scalar("INSERT INTO catalog.tags(name) VALUES ('editorial') RETURNING id")
            .fetch_one(&pool)
            .await?;
    sqlx::query(
        "INSERT INTO catalog.companies(id, name, origin_country, logo_path)
         VALUES (101, 'Detail Studio', 'JP', '/studio.svg')",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.networks(id, name, origin_country, logo_path)
         VALUES (201, 'Detail Network', 'JP', '/network.svg')",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.languages(iso_639_1, english_name, name)
         VALUES ('ja', 'Japanese', '日本語')",
    )
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO catalog.title_genres(title_id, genre_id) VALUES ($1, 18)")
        .bind(title_id)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.title_keywords(title_id, keyword_id) VALUES ($1, 210024)")
        .bind(title_id)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.title_tags(title_id, tag_id) VALUES ($1, $2)")
        .bind(title_id)
        .bind(tag_id)
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO catalog.title_companies(title_id, company_id, company_role)
         VALUES ($1, 101, 'production')",
    )
    .bind(title_id)
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO catalog.title_networks(title_id, network_id) VALUES ($1, 201)")
        .bind(title_id)
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO catalog.title_languages(title_id, language_id, is_original)
         VALUES ($1, 'ja', true)",
    )
    .bind(title_id)
    .execute(&pool)
    .await?;

    let key = TitleKey::new(
        MediaType::Movie,
        NonZeroU32::new(40_001).ok_or_else(|| test_error("fixture ID must be nonzero"))?,
    );
    let repository = CatalogRepository::new(pool.clone());
    let detail = repository
        .get_detail(key, AnimeScope::OnlyAnime)
        .await
        .map_err(db_error)?
        .ok_or_else(|| test_error("scoped detail row is missing"))?;
    assert_eq!(detail.title.id, title_id);
    assert!(detail.title.is_anime);
    assert_eq!(
        detail.title.release_date,
        NaiveDate::from_ymd_opt(2024, 1, 2)
    );
    assert_eq!(detail.tagline.as_deref(), Some("The detail tagline"));
    assert_eq!(detail.runtime_minutes, Some(115));
    assert!(detail.adult);
    assert!(detail.video);
    assert_eq!(
        detail.movie,
        Some(tmdb_db::CatalogMovieDetails {
            budget: Some(100),
            revenue: Some(200),
            runtime_minutes: Some(115),
            imdb_id: Some("tt40001".to_owned()),
            collection_id: None,
        })
    );
    assert_eq!(
        detail.facets.genres,
        vec![CatalogGenre {
            id: 18,
            name: Some("Drama".to_owned()),
        }]
    );
    assert_eq!(
        detail.facets.keywords,
        vec![CatalogKeyword {
            id: 210_024,
            name: Some("anime".to_owned()),
        }]
    );
    assert_eq!(
        detail.facets.tags,
        vec![CatalogTag {
            id: tag_id,
            name: "editorial".to_owned(),
        }]
    );
    assert_eq!(
        detail.facets.languages,
        vec![CatalogLanguage {
            iso_639_1: "ja".to_owned(),
            english_name: Some("Japanese".to_owned()),
            name: Some("日本語".to_owned()),
            is_original: true,
        }]
    );
    assert_eq!(
        detail.facets.companies,
        vec![CatalogCompany {
            id: 101,
            name: Some("Detail Studio".to_owned()),
            origin_country: Some("JP".to_owned()),
            logo_path: Some("/studio.svg".to_owned()),
            company_role: Some("production".to_owned()),
        }]
    );
    assert_eq!(
        detail.facets.networks,
        vec![CatalogNetwork {
            id: 201,
            name: Some("Detail Network".to_owned()),
            origin_country: Some("JP".to_owned()),
            logo_path: Some("/network.svg".to_owned()),
        }]
    );

    let facets = repository
        .get_facets(key, AnimeScope::OnlyAnime)
        .await
        .map_err(db_error)?
        .ok_or_else(|| test_error("scoped facets are missing"))?;
    assert_eq!(facets, detail.facets);
    assert_eq!(
        repository
            .list_genres(key, AnimeScope::OnlyAnime)
            .await
            .map_err(db_error)?,
        Some(detail.facets.genres.clone())
    );
    assert_eq!(
        repository
            .list_keywords(key, AnimeScope::OnlyAnime)
            .await
            .map_err(db_error)?,
        Some(detail.facets.keywords.clone())
    );
    assert_eq!(
        repository
            .list_tags(key, AnimeScope::OnlyAnime)
            .await
            .map_err(db_error)?,
        Some(detail.facets.tags.clone())
    );
    assert_eq!(
        repository
            .list_languages(key, AnimeScope::OnlyAnime)
            .await
            .map_err(db_error)?,
        Some(detail.facets.languages.clone())
    );
    assert_eq!(
        repository
            .list_companies(key, AnimeScope::OnlyAnime)
            .await
            .map_err(db_error)?,
        Some(detail.facets.companies.clone())
    );
    assert_eq!(
        repository
            .list_networks(key, AnimeScope::OnlyAnime)
            .await
            .map_err(db_error)?,
        Some(detail.facets.networks.clone())
    );
    assert!(
        repository
            .get_detail(key, AnimeScope::OnlyNonAnime)
            .await
            .map_err(db_error)?
            .is_none()
    );
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn repository_recent_and_top_lists_share_bounded_keyset_and_scope_rules(
    pool: PgPool,
) -> sqlx::Result<()> {
    let recent_movie = insert_title(&pool, "movie", 41_001, "Recent Movie", 10.0, false).await?;
    let recent_tv = insert_title(&pool, "tv", 41_002, "Recent TV", 12.0, false).await?;
    let older_movie = insert_title(&pool, "movie", 41_003, "Older Movie", 11.0, false).await?;
    let anime_movie = insert_title(&pool, "movie", 41_004, "Anime Recent", 99.0, true).await?;
    sqlx::query(
        "UPDATE catalog.titles
            SET release_date = CASE id WHEN $1 THEN '2024-01-02'::date
                                       WHEN $3 THEN '2023-01-01'::date
                                       WHEN $4 THEN '2025-01-01'::date END,
                first_air_date = CASE id WHEN $2 THEN '2024-06-01'::date END,
                vote_average = CASE id WHEN $1 THEN 8.5::double precision
                                       WHEN $2 THEN 9.2::double precision
                                       WHEN $3 THEN 9.2::double precision
                                       WHEN $4 THEN 10.0::double precision END,
                vote_count = CASE id WHEN $1 THEN 200
                                     WHEN $2 THEN 100
                                     WHEN $3 THEN 50
                                     WHEN $4 THEN 1000 END
          WHERE id IN ($1, $2, $3, $4)",
    )
    .bind(recent_movie)
    .bind(recent_tv)
    .bind(older_movie)
    .bind(anime_movie)
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.titles(
             media_type, tmdb_id, display_title, popularity, vote_average
         )
         SELECT 'movie', 50_000 + serial, 'Top noise ' || serial, 1.0, 1.0
           FROM generate_series(1, 20_000) AS serial",
    )
    .execute(&pool)
    .await?;
    sqlx::raw_sql("ANALYZE catalog.titles;")
        .execute(&pool)
        .await?;

    let repository = CatalogRepository::new(pool.clone());
    let first = repository
        .list_recent(None, AnimeScope::OnlyNonAnime, 1, None)
        .await
        .map_err(db_error)?;
    assert_eq!(
        first.items.iter().map(|item| item.id).collect::<Vec<_>>(),
        [recent_tv]
    );
    let cursor = first
        .next
        .ok_or_else(|| test_error("full recent page has no cursor"))?;
    assert_eq!(
        cursor.date(),
        NaiveDate::from_ymd_opt(2024, 6, 1).ok_or_else(|| test_error("date"))?
    );
    assert_eq!(cursor.title_id(), recent_tv);

    let second = repository
        .list_recent(None, AnimeScope::OnlyNonAnime, 2, Some(cursor))
        .await
        .map_err(db_error)?;
    assert_eq!(
        second.items.iter().map(|item| item.id).collect::<Vec<_>>(),
        [recent_movie, older_movie]
    );
    assert!(second.next.is_none());
    assert!(second.items.iter().all(|item| !item.is_anime));

    let anime_page = repository
        .list_recent(Some(MediaType::Movie), AnimeScope::OnlyAnime, 10, None)
        .await
        .map_err(db_error)?;
    assert_eq!(
        anime_page
            .items
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [anime_movie]
    );
    assert!(anime_page.items[0].is_anime);

    let top = repository
        .list_top(None, AnimeScope::OnlyNonAnime, 2, None)
        .await
        .map_err(db_error)?;
    assert_eq!(
        top.items.iter().map(|item| item.id).collect::<Vec<_>>(),
        [recent_tv, older_movie]
    );
    assert!(top.next.is_some());
    let top_cursor = top
        .next
        .ok_or_else(|| test_error("full top page has no cursor"))?;
    assert!((top_cursor.vote_average() - 9.2).abs() < f64::EPSILON);
    assert_eq!(top_cursor.vote_count(), 50);
    assert_eq!(top_cursor.title_id(), older_movie);
    let top_tail = repository
        .list_top(None, AnimeScope::OnlyNonAnime, 2, Some(top_cursor))
        .await
        .map_err(db_error)?;
    assert_eq!(
        top_tail
            .items
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [recent_movie]
    );
    assert!(top_tail.next.is_none());
    let anime_top = repository
        .list_top(Some(MediaType::Movie), AnimeScope::OnlyAnime, 10, None)
        .await
        .map_err(db_error)?;
    assert_eq!(
        anime_top
            .items
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [anime_movie]
    );
    assert!(anime_top.items[0].is_anime);
    let top_plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN (COSTS OFF)
         SELECT id
           FROM catalog.titles
          WHERE active AND NOT is_anime
            AND vote_average IS NOT NULL
            AND vote_count IS NOT NULL
          ORDER BY vote_average DESC, vote_count DESC, id DESC
          LIMIT 2",
    )
    .fetch_all(&pool)
    .await?;
    let top_plan = top_plan.join("\n");
    assert!(
        top_plan.contains("titles_non_anime_top_rating_global_idx"),
        "top-rated plan did not use global index: {top_plan}"
    );
    assert!(matches!(
        tmdb_db::TopCursor::try_new(10.0, 0, 0),
        Err(CatalogError::InvalidInput)
    ));
    assert!(matches!(
        RecentCursor::try_new(
            NaiveDate::from_ymd_opt(2024, 1, 1).ok_or_else(|| test_error("date"))?,
            0
        ),
        Err(CatalogError::InvalidInput)
    ));
    assert!(matches!(
        repository
            .list_recent(None, AnimeScope::OnlyNonAnime, 0, None)
            .await,
        Err(CatalogError::InvalidInput)
    ));
    Ok(())
}

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn repository_discovery_filters_are_bounded_and_anime_scoped(
    pool: PgPool,
) -> sqlx::Result<()> {
    let matching = insert_title(&pool, "movie", 60_001, "One Piece Filtered", 40.0, false).await?;
    let non_matching = insert_title(&pool, "movie", 60_002, "Other Film", 50.0, false).await?;
    let anime = insert_title(&pool, "movie", 60_003, "One Piece Anime", 60.0, true).await?;
    sqlx::query("UPDATE catalog.titles SET runtime_minutes = CASE id WHEN $1 THEN 120 WHEN $2 THEN 80 WHEN $3 THEN 120 END, vote_average = 8, vote_count = 100")
        .bind(matching)
        .bind(non_matching)
        .bind(anime)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.genres(id, name) VALUES (28, 'Action'), (16, 'Animation')")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.keywords(id, name) VALUES (210024, 'anime')")
        .execute(&pool)
        .await?;
    let tag_id: i64 =
        sqlx::query_scalar("INSERT INTO catalog.tags(name) VALUES ('curated') RETURNING id")
            .fetch_one(&pool)
            .await?;
    sqlx::query(
        "INSERT INTO catalog.companies(id, name)
         VALUES (601, 'Filtered Studio'), (602, 'Anime Studio')",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.languages(iso_639_1, english_name)
         VALUES ('en', 'English'), ('ja', 'Japanese')",
    )
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.networks(id, name)
         VALUES (801, 'Filtered Network'), (802, 'Anime Network')",
    )
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO catalog.people(id, name) VALUES (701, 'Filtered Actor')")
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.title_genres(title_id, genre_id) VALUES ($1, 28), ($2, 16)")
        .bind(matching)
        .bind(anime)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.title_tags(title_id, tag_id) VALUES ($1, $2)")
        .bind(matching)
        .bind(tag_id)
        .execute(&pool)
        .await?;
    sqlx::query(
        "INSERT INTO catalog.title_companies(title_id, company_id)
         VALUES ($1, 601), ($2, 601), ($3, 602)",
    )
    .bind(matching)
    .bind(non_matching)
    .bind(anime)
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.title_languages(title_id, language_id)
         VALUES ($1, 'en'), ($2, 'en'), ($3, 'ja')",
    )
    .bind(matching)
    .bind(non_matching)
    .bind(anime)
    .execute(&pool)
    .await?;
    sqlx::query(
        "INSERT INTO catalog.title_networks(title_id, network_id)
         VALUES ($1, 801), ($2, 801), ($3, 802)",
    )
    .bind(matching)
    .bind(non_matching)
    .bind(anime)
    .execute(&pool)
    .await?;
    sqlx::query("INSERT INTO catalog.title_credits(title_id, person_id, credit_id) VALUES ($1, 701, 'credit-701')")
        .bind(matching)
        .execute(&pool)
        .await?;
    sqlx::query("INSERT INTO catalog.title_keywords(title_id, keyword_id) VALUES ($1, 210024)")
        .bind(anime)
        .execute(&pool)
        .await?;

    let repository = CatalogRepository::new(pool.clone());
    let filters = CatalogFilters {
        genre_id: Some(28),
        language: Some("en".to_owned()),
        runtime_min: Some(100),
        runtime_max: Some(130),
        person_id: Some(701),
        company_id: Some(601),
        tag_id: Some(tag_id),
        ..CatalogFilters::default()
    };
    let page = repository
        .list_popular_filtered(
            Some(MediaType::Movie),
            AnimeScope::OnlyNonAnime,
            &filters,
            10,
            None,
        )
        .await
        .map_err(|_| test_error("filtered popular"))?;
    assert_eq!(
        page.items.iter().map(|item| item.id).collect::<Vec<_>>(),
        [matching]
    );
    let search = repository
        .search_filtered(
            "one piece",
            Some(MediaType::Movie),
            AnimeScope::OnlyNonAnime,
            &CatalogFilters {
                genre_id: Some(28),
                ..CatalogFilters::default()
            },
            10,
        )
        .await
        .map_err(|_| test_error("filtered search"))?;
    assert_eq!(
        search.iter().map(|item| item.id).collect::<Vec<_>>(),
        [matching]
    );
    let anime_filters = CatalogFilters {
        keyword_id: Some(210_024),
        ..CatalogFilters::default()
    };
    let anime_page = repository
        .list_popular_filtered(None, AnimeScope::OnlyAnime, &anime_filters, 10, None)
        .await
        .map_err(|_| test_error("filtered anime popular"))?;
    assert_eq!(
        anime_page
            .items
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        [anime]
    );
    assert_eq!(
        repository
            .list_genre_entities(Some("action"), AnimeScope::OnlyNonAnime, 10)
            .await
            .map_err(|_| test_error("genre entities"))?
            .iter()
            .map(|genre| genre.id)
            .collect::<Vec<_>>(),
        [28]
    );
    assert_eq!(
        repository
            .list_company_entities(None, AnimeScope::OnlyNonAnime, 10)
            .await
            .map_err(|_| test_error("company entities"))?
            .iter()
            .map(|company| company.id)
            .collect::<Vec<_>>(),
        [601]
    );
    assert_eq!(
        repository
            .list_company_entities(Some("filtered"), AnimeScope::OnlyNonAnime, 10)
            .await
            .map_err(|_| test_error("filtered company entities"))?
            .iter()
            .map(|company| company.id)
            .collect::<Vec<_>>(),
        [601]
    );
    assert_eq!(
        repository
            .list_network_entities(None, AnimeScope::OnlyNonAnime, 10)
            .await
            .map_err(|_| test_error("network entities"))?
            .iter()
            .map(|network| network.id)
            .collect::<Vec<_>>(),
        [801]
    );
    assert_eq!(
        repository
            .list_network_entities(Some("filtered"), AnimeScope::OnlyNonAnime, 10)
            .await
            .map_err(|_| test_error("filtered network entities"))?
            .iter()
            .map(|network| network.id)
            .collect::<Vec<_>>(),
        [801]
    );
    assert_eq!(
        repository
            .list_language_entities(None, AnimeScope::OnlyNonAnime, 10)
            .await
            .map_err(|_| test_error("language entities"))?
            .iter()
            .map(|language| language.iso_639_1.as_str())
            .collect::<Vec<_>>(),
        ["en"]
    );
    assert_eq!(
        repository
            .list_language_entities(Some("english"), AnimeScope::OnlyNonAnime, 10)
            .await
            .map_err(|_| test_error("filtered language entities"))?
            .iter()
            .map(|language| language.iso_639_1.as_str())
            .collect::<Vec<_>>(),
        ["en"]
    );
    assert!(matches!(
        repository
            .list_popular_filtered(
                None,
                AnimeScope::OnlyNonAnime,
                &CatalogFilters {
                    runtime_min: Some(200),
                    runtime_max: Some(100),
                    ..CatalogFilters::default()
                },
                10,
                None,
            )
            .await,
        Err(CatalogError::InvalidInput)
    ));
    Ok(())
}

async fn insert_title(
    pool: &PgPool,
    media_type: &str,
    tmdb_id: i64,
    display_title: &str,
    popularity: f64,
    is_anime: bool,
) -> sqlx::Result<i64> {
    sqlx::query_scalar(
        "INSERT INTO catalog.titles(
             media_type, tmdb_id, display_title, popularity, is_anime
         ) VALUES ($1, $2, $3, $4, $5)
         RETURNING id",
    )
    .bind(media_type)
    .bind(tmdb_id)
    .bind(display_title)
    .bind(popularity)
    .bind(is_anime)
    .fetch_one(pool)
    .await
}

async fn assert_sqlstate(
    pool: &PgPool,
    statement: &'static str,
    expected: &'static str,
) -> sqlx::Result<()> {
    let Err(error) = sqlx::query(statement).execute(pool).await else {
        return Err(test_error("invalid fixture was accepted"));
    };
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some(expected)
    );
    Ok(())
}

async fn assert_title_sqlstate(
    pool: &PgPool,
    statement: &'static str,
    title_id: i64,
    expected: &'static str,
) -> sqlx::Result<()> {
    let Err(error) = sqlx::query(statement).bind(title_id).execute(pool).await else {
        return Err(test_error("invalid fixture was accepted"));
    };
    assert_eq!(
        error
            .as_database_error()
            .and_then(sqlx::error::DatabaseError::code)
            .as_deref(),
        Some(expected)
    );
    Ok(())
}

fn db_error(error: CatalogError) -> sqlx::Error {
    sqlx::Error::Protocol(error.to_string())
}

fn test_error(message: &str) -> sqlx::Error {
    sqlx::Error::Protocol(message.to_owned())
}
