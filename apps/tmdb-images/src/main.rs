pub mod image;
mod media_server;
mod persistence;
mod requests;
mod runtime;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    runtime::run().await
}
