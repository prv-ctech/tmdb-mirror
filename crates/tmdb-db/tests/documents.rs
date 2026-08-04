use serde_json::json;
use tmdb_db::TmdbDocumentRepository;

#[sqlx::test(migrator = "tmdb_db::MIGRATOR")]
async fn tmdb_documents_replace_exact_endpoint_and_query(pool: sqlx::PgPool) -> sqlx::Result<()> {
    let repository = TmdbDocumentRepository::new(pool);
    let first = json!({"id": 42, "title": "First"});
    let second = json!({"id": 42, "title": "Second"});

    repository
        .upsert("movie/42", "append_to_response=credits", &first)
        .await?;
    assert_eq!(
        repository
            .get("movie/42", "append_to_response=credits")
            .await?,
        Some(first.clone())
    );
    assert_eq!(repository.get("movie/42", "").await?, None);
    repository
        .upsert("movie/42", "", &json!({"id": 42, "title": "Base"}))
        .await?;
    assert_eq!(
        repository.get("movie/42", "language=en-US").await?,
        None
    );

    repository
        .upsert(
            "movie/43/images",
            "language=en-US&include_image_language=en,null",
            &json!({"id": 43, "backdrops": []}),
        )
        .await?;
    assert_eq!(repository.get("movie/43/images", "").await?, None);

    repository
        .upsert("movie/42", "append_to_response=credits", &second)
        .await?;
    assert_eq!(
        repository
            .get("movie/42", "append_to_response=credits")
            .await?,
        Some(second)
    );
    Ok(())
}
