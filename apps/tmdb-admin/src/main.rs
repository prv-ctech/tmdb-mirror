use std::path::PathBuf;

use clap::{Parser, Subcommand};
use serde::Serialize;
use tmdb_config::{EnvSource, Environment, load_shared_database};
use tmdb_db::{PoolPolicy, ReadinessReport, connect_direct, migrate, readiness};
use tmdb_jobs::{JobId, JobRepository, NewJob};
use tmdb_upstream::DailyExportParser;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(version, about = "TMDB database administration")]
struct Cli {
    #[arg(long, env = "TMDB_ENVIRONMENT", default_value = "development")]
    environment: Environment,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Apply embedded migrations with the configured database account.
    Migrate,
    /// Check `PostgreSQL` compatibility, migrations, extensions, and read-only access.
    Doctor {
        /// Emit stable machine-readable output.
        #[arg(long)]
        json: bool,
    },
    /// Submit one registered no-op job through the submission function.
    SubmitNoop {
        /// Active deduplication key for the no-op job.
        #[arg(long)]
        dedup_key: String,
    },
    /// Submit one movie or TV detail refresh through the job boundary.
    SubmitRefresh {
        /// `movie` or `tv`.
        #[arg(long)]
        media_type: String,
        /// Positive TMDB identifier.
        #[arg(long)]
        tmdb_id: u32,
    },
    /// Submit one official daily export download/count job.
    SubmitDailyExport {
        /// `movie` or `tv`.
        #[arg(long)]
        media_type: String,
        /// An allowlisted files.tmdb.org export URL.
        #[arg(long)]
        url: String,
    },
    /// Fully validate an NDJSON export and optionally queue a bounded prefix.
    ScanExport {
        /// Local plain or gzip NDJSON path.
        #[arg(long)]
        path: PathBuf,
        /// `movie` or `tv`.
        #[arg(long)]
        media_type: String,
        /// Queue at most this many IDs after the full validation pass.
        #[arg(long)]
        queue_limit: Option<usize>,
    },
    /// Read one job's sanitized status projection.
    JobStatus {
        /// Durable job UUID.
        #[arg(long)]
        job_id: String,
    },
}

#[tokio::main]
#[allow(clippy::too_many_lines)] // Keep the small CLI command dispatcher in one audit surface.
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = load_shared_database(&EnvSource, cli.environment)?;

    match cli.command {
        Command::Migrate => {
            let pool = connect_direct(&config, PoolPolicy::Migrator).await?;
            let report = migrate(&pool).await?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Command::Doctor { json: true } => {
            let pool = connect_direct(&config, PoolPolicy::ReadOnly).await?;
            let report = doctor(&pool).await?;
            println!("{}", serde_json::to_string(&report)?);
        }
        Command::Doctor { json: false } => {
            anyhow::bail!("doctor requires --json");
        }
        Command::SubmitNoop { dedup_key } => {
            let pool = connect_direct(&config, PoolPolicy::ReadWrite).await?;
            let outcome = JobRepository::new(pool)
                .submit(NewJob::noop(&dedup_key)?)
                .await?;
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "job_id": outcome.job_id().as_uuid(),
                    "duplicate": outcome.was_duplicate(),
                }))?
            );
        }
        Command::SubmitRefresh {
            media_type,
            tmdb_id,
        } => {
            let (job_type, payload) = refresh_job(&media_type, tmdb_id)?;
            let pool = connect_direct(&config, PoolPolicy::ReadWrite).await?;
            let outcome = JobRepository::new(pool)
                .submit(NewJob::new(
                    job_type,
                    1,
                    payload,
                    &format!("{job_type}:{tmdb_id}"),
                )?)
                .await?;
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "job_id": outcome.job_id().as_uuid(),
                    "duplicate": outcome.was_duplicate(),
                    "job_type": job_type,
                    "tmdb_id": tmdb_id,
                }))?
            );
        }
        Command::SubmitDailyExport { media_type, url } => {
            let media_type = validate_media_type(&media_type)?;
            let job_type = "ingest.daily_export";
            let payload = serde_json::json!({"media_type": media_type, "url": url});
            let dedup_key = format!("{job_type}:{media_type}:{url}");
            let pool = connect_direct(&config, PoolPolicy::ReadWrite).await?;
            let outcome = JobRepository::new(pool)
                .submit(NewJob::new(job_type, 1, payload, &dedup_key)?)
                .await?;
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "job_id": outcome.job_id().as_uuid(),
                    "duplicate": outcome.was_duplicate(),
                    "job_type": job_type,
                    "media_type": media_type,
                    "url": url,
                }))?
            );
        }
        Command::ScanExport {
            path,
            media_type,
            queue_limit,
        } => {
            let (job_type, media_type) = refresh_job_type(&media_type)?;
            let parser = DailyExportParser::default();
            let full_records = parser.count_file(&path)?;
            let mut queued = 0_usize;
            let mut duplicates = 0_usize;
            if let Some(queue_limit) = queue_limit {
                if queue_limit == 0 {
                    anyhow::bail!("queue-limit must be positive");
                }
                let mut records = Vec::with_capacity(queue_limit.min(100_000));
                parser.scan_file_limited(&path, queue_limit, |record| records.push(record))?;
                let pool = connect_direct(&config, PoolPolicy::ReadWrite).await?;
                let repository = JobRepository::new(pool);
                for record in records {
                    let tmdb_id = u32::try_from(record.id)
                        .map_err(|_| anyhow::anyhow!("export contains an unsupported TMDB ID"))?;
                    let outcome = repository
                        .submit(NewJob::new(
                            job_type,
                            1,
                            serde_json::json!({"tmdb_id": tmdb_id}),
                            &format!("{job_type}:{tmdb_id}"),
                        )?)
                        .await?;
                    if outcome.was_duplicate() {
                        duplicates = duplicates.saturating_add(1);
                    } else {
                        queued = queued.saturating_add(1);
                    }
                }
            }
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "path": path,
                    "media_type": media_type,
                    "full_records": full_records,
                    "queue_limit": queue_limit,
                    "queued": queued,
                    "duplicates": duplicates,
                }))?
            );
        }
        Command::JobStatus { job_id } => {
            let job_id =
                Uuid::parse_str(&job_id).map_err(|_| anyhow::anyhow!("invalid job UUID"))?;
            let pool = connect_direct(&config, PoolPolicy::ReadOnly).await?;
            let job = JobRepository::new(pool).get(JobId::from(job_id)).await?;
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "job_id": job.job_id().as_uuid(),
                    "status": job.status(),
                    "attempts": job.attempts(),
                    "max_attempts": job.max_attempts(),
                    "available_at": job.available_at(),
                    "cancellation_requested": job.cancellation_requested(),
                    "error_message": job.error_message(),
                    "created_at": job.created_at(),
                    "updated_at": job.updated_at(),
                    "finished_at": job.finished_at(),
                }))?
            );
        }
    }

    Ok(())
}

#[derive(Debug, Serialize)]
struct DoctorReport {
    #[serde(flatten)]
    readiness: ReadinessReport,
    identity: DoctorIdentity,
}

#[derive(Debug, Serialize)]
struct DoctorIdentity {
    current_user: String,
    transaction_read_only: String,
    statement_timeout: String,
    lock_timeout: String,
    catalog_usage: bool,
    search_usage: bool,
    readiness_select: bool,
}

async fn doctor(pool: &sqlx::PgPool) -> anyhow::Result<DoctorReport> {
    let readiness_report = readiness(pool).await?;
    let current_user: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(pool)
        .await
        .map_err(|_| anyhow::anyhow!("doctor identity check failed"))?;
    let settings: (String, String, String) = sqlx::query_as(
        "SELECT current_setting('transaction_read_only'),
                current_setting('statement_timeout'),
                current_setting('lock_timeout')",
    )
    .fetch_one(pool)
    .await
    .map_err(|_| anyhow::anyhow!("doctor session check failed"))?;
    let grants: (bool, bool, bool) = sqlx::query_as(
        "SELECT has_schema_privilege(current_user, 'catalog', 'USAGE'),
                has_schema_privilege(current_user, 'search', 'USAGE'),
                has_table_privilege(current_user, 'ops.readiness', 'SELECT')",
    )
    .fetch_one(pool)
    .await
    .map_err(|_| anyhow::anyhow!("doctor grant check failed"))?;
    if settings.0 != "on"
        || settings.1 != "5s"
        || settings.2 != "2s"
        || !grants.0
        || !grants.1
        || !grants.2
    {
        anyhow::bail!("doctor safety checks failed");
    }
    Ok(DoctorReport {
        readiness: readiness_report,
        identity: DoctorIdentity {
            current_user,
            transaction_read_only: settings.0,
            statement_timeout: settings.1,
            lock_timeout: settings.2,
            catalog_usage: grants.0,
            search_usage: grants.1,
            readiness_select: grants.2,
        },
    })
}

fn validate_media_type(value: &str) -> anyhow::Result<&str> {
    match value {
        "movie" | "tv" => Ok(value),
        _ => anyhow::bail!("media-type must be movie or tv"),
    }
}

fn refresh_job_type(value: &str) -> anyhow::Result<(&'static str, &str)> {
    match value {
        "movie" => Ok(("ingest.refresh_movie", "movie")),
        "tv" => Ok(("ingest.refresh_tv", "tv")),
        _ => anyhow::bail!("media-type must be movie or tv"),
    }
}

fn refresh_job(
    media_type: &str,
    tmdb_id: u32,
) -> anyhow::Result<(&'static str, serde_json::Value)> {
    if tmdb_id == 0 {
        anyhow::bail!("tmdb-id must be positive");
    }
    let (job_type, _) = refresh_job_type(media_type)?;
    Ok((job_type, serde_json::json!({"tmdb_id": tmdb_id})))
}
