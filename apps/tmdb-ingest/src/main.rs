#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tmdb_ingest::runtime::run().await
}
