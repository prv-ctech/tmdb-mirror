use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Timelike, Utc};
use chrono_tz::Tz;
use croner::Cron;
use sqlx::{FromRow, PgPool};
use tokio_util::sync::CancellationToken;

const SCHEDULER_POLL_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Clone, Debug)]
pub(crate) struct CatalogSchedulerConfig {
    timezone: Tz,
    schedules: Vec<CatalogSchedule>,
}

#[derive(Clone, Debug)]
struct CatalogSchedule {
    mode: &'static str,
    expression: String,
    cron: Cron,
}

#[derive(Debug, FromRow)]
struct ScheduledSubmission {
    job_id: Option<uuid::Uuid>,
    outcome: String,
    full_sweep_required: bool,
}

#[derive(Debug, sqlx::FromRow)]
struct PendingScheduleSlot {
    mode: String,
    scheduled_for: DateTime<Utc>,
    window_start: Option<NaiveDate>,
    window_end: Option<NaiveDate>,
}

impl CatalogSchedulerConfig {
    pub(crate) fn new(
        timezone: &str,
        daily_sync: &str,
        missing_only: &str,
        reconcile: &str,
    ) -> anyhow::Result<Self> {
        let timezone = timezone
            .parse::<Tz>()
            .map_err(|_| anyhow::anyhow!("configuration field TZ is invalid"))?;
        let mut schedules = Vec::with_capacity(3);
        for (mode, expression) in [
            ("daily_sync", daily_sync),
            ("missing_only", missing_only),
            ("reconcile", reconcile),
        ] {
            let expression = expression.trim();
            if expression.is_empty() {
                continue;
            }
            if expression.split_ascii_whitespace().count() != 5 {
                return Err(anyhow::anyhow!(
                    "configuration field for {mode} must be a five-field cron expression"
                ));
            }
            let cron = Cron::from_str(expression)
                .map_err(|_| anyhow::anyhow!("configuration field for {mode} is invalid"))?;
            schedules.push(CatalogSchedule {
                mode,
                expression: expression.to_owned(),
                cron,
            });
        }
        Ok(Self {
            timezone,
            schedules,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.schedules.is_empty()
    }
}

pub(crate) async fn run(
    pool: PgPool,
    config: CatalogSchedulerConfig,
    cancellation: CancellationToken,
) {
    let mut interval = tokio::time::interval(SCHEDULER_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = cancellation.cancelled() => return,
            _ = interval.tick() => {
                retry_pending_slots(&pool).await;
                let now = Utc::now().with_timezone(&config.timezone);
                let slot = minute_slot(now);
                for schedule in &config.schedules {
                    match schedule.cron.is_time_matching(&slot) {
                        Ok(true) => submit_slot(&pool, schedule, slot).await,
                        Ok(false) => {}
                        Err(_) => tracing::error!(
                            event = "catalog_schedule_evaluation_failed",
                            mode = schedule.mode,
                            expression = schedule.expression,
                        ),
                    }
                }
            }
        }
    }
}

fn minute_slot(value: DateTime<Tz>) -> DateTime<Tz> {
    value
        .with_second(0)
        .and_then(|value| value.with_nanosecond(0))
        .unwrap_or(value)
}

async fn submit_slot(pool: &PgPool, schedule: &CatalogSchedule, slot: DateTime<Tz>) {
    let (window_start, window_end) = if schedule.mode == "daily_sync" {
        let Ok(window) = daily_window(pool, slot.date_naive()).await else {
            tracing::warn!(
                event = "catalog_schedule_submission_failed",
                mode = schedule.mode,
                error_code = "database_unavailable",
            );
            return;
        };
        (Some(window.0), Some(window.1))
    } else {
        (None, None)
    };
    submit_database_slot(
        pool,
        schedule.mode,
        slot.with_timezone(&Utc),
        window_start,
        window_end,
    )
    .await;
}

async fn retry_pending_slots(pool: &PgPool) {
    let pending = sqlx::query_as::<_, PendingScheduleSlot>(
        "SELECT mode, scheduled_for, window_start, window_end
           FROM ops.catalog_schedule_slots
          WHERE outcome = 'pending'
          ORDER BY scheduled_for, mode
          LIMIT 10",
    )
    .fetch_all(pool)
    .await;
    let Ok(pending) = pending else {
        tracing::warn!(
            event = "catalog_schedule_pending_read_failed",
            error_code = "database_unavailable",
        );
        return;
    };
    for slot in pending {
        submit_database_slot(
            pool,
            &slot.mode,
            slot.scheduled_for,
            slot.window_start,
            slot.window_end,
        )
        .await;
    }
}

async fn submit_database_slot(
    pool: &PgPool,
    mode: &str,
    scheduled_for: DateTime<Utc>,
    window_start: Option<NaiveDate>,
    window_end: Option<NaiveDate>,
) {
    let submission = sqlx::query_as::<_, ScheduledSubmission>(
        "SELECT job_id, outcome, full_sweep_required
           FROM ops.submit_scheduled_catalog_scan($1, $2, $3, $4)",
    )
    .bind(mode)
    .bind(scheduled_for)
    .bind(window_start)
    .bind(window_end)
    .fetch_one(pool)
    .await;
    if let Ok(submission) = submission {
        tracing::info!(
            event = "catalog_schedule_slot",
            mode,
            slot = %scheduled_for,
            outcome = submission.outcome,
            job_id = submission.job_id.map(|id| id.to_string()),
            full_sweep_required = submission.full_sweep_required,
        );
    } else {
        tracing::warn!(
            event = "catalog_schedule_submission_failed",
            mode,
            error_code = "database_unavailable",
        );
    }
}

async fn daily_window(pool: &PgPool, today: NaiveDate) -> Result<(NaiveDate, NaiveDate), ()> {
    let state: (Option<NaiveDate>, bool) = sqlx::query_as(
        "SELECT last_successful_window_end, full_sweep_required
           FROM ops.catalog_sync_state
          WHERE mode = 'daily_sync'",
    )
    .fetch_one(pool)
    .await
    .map_err(|_| ())?;
    if state.1 {
        return Ok((today, today));
    }
    let Some(start) = state.0 else {
        mark_full_sweep_required(pool).await?;
        return Ok((today, today));
    };
    if (today - start).num_days() > 13 {
        mark_full_sweep_required(pool).await?;
        return Ok((today, today));
    }
    Ok((start, today))
}

async fn mark_full_sweep_required(pool: &PgPool) -> Result<(), ()> {
    sqlx::query_scalar::<_, bool>("SELECT ops.mark_catalog_full_sweep_required()")
        .fetch_one(pool)
        .await
        .map(|_| ())
        .map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn configured_cron_is_five_field_and_timezone_aware() -> anyhow::Result<()> {
        let config = CatalogSchedulerConfig::new(
            "America/New_York",
            "0 * * * *",
            "0 3 * * *",
            "0 4 1,15 * *",
        )?;
        assert_eq!(config.schedules.len(), 3);
        let instant = config
            .timezone
            .with_ymd_and_hms(2026, 8, 15, 4, 0, 0)
            .single()
            .ok_or_else(|| anyhow::anyhow!("fixture time is invalid"))?;
        assert!(config.schedules[2].cron.is_time_matching(&instant)?);
        Ok(())
    }

    #[test]
    fn empty_schedule_disables_only_that_mode() -> anyhow::Result<()> {
        let config = CatalogSchedulerConfig::new("UTC", "", "0 3 * * *", "")?;
        assert_eq!(config.schedules.len(), 1);
        assert_eq!(config.schedules[0].mode, "missing_only");
        Ok(())
    }

    #[test]
    fn six_field_cron_is_rejected() {
        assert!(CatalogSchedulerConfig::new("UTC", "0 0 * * * *", "", "").is_err());
    }
}
