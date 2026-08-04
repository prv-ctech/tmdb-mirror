//! Consolidated migration and ingestion worker entry point.

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tmdb_ingest::runtime::run_worker().await
}
