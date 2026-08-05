//! Exact TMDB response documents persisted for local API reads.

use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder};

const DOCUMENT_BATCH_SIZE: usize = 500;

/// Repository for the source JSON captured from TMDB.
#[derive(Clone, Debug)]
pub struct TmdbDocumentRepository {
    pool: PgPool,
}

impl TmdbDocumentRepository {
    /// Creates a document repository over an existing database pool.
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Reads one exact endpoint/query response.
    ///
    /// # Errors
    ///
    /// Returns the database error if the document query cannot be executed.
    pub async fn get(
        &self,
        endpoint_path: &str,
        query_string: &str,
    ) -> Result<Option<Value>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT response
               FROM source.tmdb_documents
              WHERE endpoint_path = $1
                AND query_string = $2",
        )
        .bind(endpoint_path)
        .bind(query_string)
        .fetch_optional(&self.pool)
        .await
    }

    /// Inserts or replaces one exact endpoint/query response.
    ///
    /// # Errors
    ///
    /// Returns the database error if the document cannot be written.
    pub async fn upsert(
        &self,
        endpoint_path: &str,
        query_string: &str,
        response: &Value,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO source.tmdb_documents (endpoint_path, query_string, response)
             VALUES ($1, $2, $3)
             ON CONFLICT (endpoint_path, query_string)
             DO UPDATE SET response = EXCLUDED.response,
                           fetched_at = pg_catalog.clock_timestamp(),
                           updated_at = pg_catalog.clock_timestamp()",
        )
        .bind(endpoint_path)
        .bind(query_string)
        .bind(response)
        .execute(&self.pool)
        .await
        .map(|_| ())
    }

    /// Inserts or replaces a bounded batch of exact upstream documents.
    ///
    /// The input order is not significant. Batches are split internally so a
    /// caller cannot accidentally create an unbounded SQL statement.
    ///
    /// # Errors
    ///
    /// Returns the database error if any batch cannot be written.
    pub async fn upsert_many(
        &self,
        documents: &[(String, String, Value)],
    ) -> Result<(), sqlx::Error> {
        for batch in documents.chunks(DOCUMENT_BATCH_SIZE) {
            let mut query = QueryBuilder::<Postgres>::new(
                "INSERT INTO source.tmdb_documents (
                     endpoint_path, query_string, response
                 ) ",
            );
            query.push_values(batch, |mut values, document| {
                values
                    .push_bind(&document.0)
                    .push_bind(&document.1)
                    .push_bind(&document.2);
            });
            query.push(
                " ON CONFLICT (endpoint_path, query_string)
                  DO UPDATE SET response = EXCLUDED.response,
                                fetched_at = pg_catalog.clock_timestamp(),
                                updated_at = pg_catalog.clock_timestamp()",
            );
            query.build().execute(&self.pool).await?;
        }
        Ok(())
    }
}
