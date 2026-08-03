use secrecy::ExposeSecret;
use sqlx::postgres::PgConnectOptions;
use tmdb_config::DatabaseConfig;

use crate::PoolPolicy;

pub(crate) fn connect_options(config: &DatabaseConfig, policy: PoolPolicy) -> PgConnectOptions {
    PgConnectOptions::new_without_pgpass()
        .host(&config.host)
        .port(config.port)
        .database(&config.database)
        .username(&config.username)
        .password(config.password.expose_secret())
        .application_name(policy.application_name())
        .options([
            ("TimeZone", "UTC"),
            ("statement_timeout", "5s"),
            ("lock_timeout", "2s"),
            (
                "default_transaction_read_only",
                if policy.is_read_only() { "on" } else { "off" },
            ),
        ])
}
