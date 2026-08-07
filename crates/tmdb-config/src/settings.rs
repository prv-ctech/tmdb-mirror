use std::net::SocketAddr;
use std::str::FromStr;

use http::Uri;
use secrecy::{ExposeSecret, SecretString};

use crate::secret::load_secret_with_origin;
use crate::{ConfigError, ConfigSource, StorageRoots};

/// Deployment environment controlling secret policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Environment {
    /// Developer workstation or development container.
    Development,
    /// Automated test environment.
    Test,
    /// Production deployment.
    Production,
}

impl FromStr for Environment {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "development" => Ok(Self::Development),
            "test" => Ok(Self::Test),
            "production" => Ok(Self::Production),
            _ => Err(ConfigError::InvalidValue("TMDB_ENVIRONMENT".to_owned())),
        }
    }
}

/// One bounded `PostgreSQL` connection identity.
#[derive(Clone, Debug)]
pub struct DatabaseConfig {
    /// `PostgreSQL` host name.
    pub host: String,
    /// TCP port.
    pub port: u16,
    /// Database name.
    pub database: String,
    /// Role name.
    pub username: String,
    /// Redacted role password.
    pub password: SecretString,
}

/// Complete validated process configuration.
#[derive(Debug)]
pub struct AppConfig {
    /// Deployment environment.
    pub environment: Environment,
    /// Public API listener.
    pub api_bind: SocketAddr,
    /// Administrative listener.
    pub admin_bind: SocketAddr,
    /// Base `PostgreSQL` identity used for initialization and compatibility.
    pub database: DatabaseConfig,
    /// Pairwise-disjoint storage trees.
    pub storage_roots: StorageRoots,
    /// Optional private Trawl service endpoint.
    pub trawl_base_url: Option<Uri>,
    /// Optional public base URL used for local media fields in v3 responses.
    pub media_base_url: Option<Uri>,
    /// Optional API key used to protect the private administrative listener.
    /// Production configuration must provide this through `TMDB_ADMIN_API_KEY`.
    pub admin_api_key: Option<SecretString>,
}

impl AppConfig {
    /// Loads and validates all application configuration from one source.
    ///
    /// The required keys are `TMDB_ENVIRONMENT`, `POSTGRES_DB`,
    /// `POSTGRES_USER`, and `POSTGRES_PASSWORD`. Listener overrides are
    /// optional; the four-container deployment defaults them to the fixed
    /// internal ports. The database host and port are fixed to
    /// `tmdb-mirror-postgres:5432`; filesystem roots are fixed to `/media` and
    /// `/config`.
    ///
    /// # Errors
    ///
    /// Returns a field-naming error when a required value is missing, typed
    /// parsing or validation fails, secret policy is violated, or storage
    /// roots overlap. Errors never include configured values.
    pub fn load(source: &impl ConfigSource) -> Result<Self, ConfigError> {
        let environment = parse_required(source, "TMDB_ENVIRONMENT")?;
        let api_bind = parse_or(
            source,
            "TMDB_API_BIND",
            SocketAddr::from(([0, 0, 0, 0], 9000)),
        )?;
        let admin_bind = parse_or(
            source,
            "TMDB_ADMIN_BIND",
            SocketAddr::from(([0, 0, 0, 0], 9001)),
        )?;
        let database = load_shared_database(source, environment)?;
        let storage_roots = StorageRoots::fixed();
        let trawl_base_url = optional_uri(source, "TMDB_TRAWL_BASE_URL")?;
        let media_base_url = optional_uri(source, "TMDB_MEDIA_BASE_URL")?;
        let admin_key_name = if has_secret_source(source, "TMDB_ADMIN_API_KEY") {
            "TMDB_ADMIN_API_KEY"
        } else {
            "TMDB_API_KEY"
        };
        let admin_api_key = optional_secret(source, environment, admin_key_name)?;
        if environment == Environment::Production && admin_api_key.is_none() {
            return Err(ConfigError::Missing("TMDB_ADMIN_API_KEY".to_owned()));
        }

        Ok(Self {
            environment,
            api_bind,
            admin_bind,
            database,
            storage_roots,
            trawl_base_url,
            media_base_url,
            admin_api_key,
        })
    }
}

fn optional_secret(
    source: &impl ConfigSource,
    environment: Environment,
    name: &str,
) -> Result<Option<SecretString>, ConfigError> {
    if source.get(name).is_none() && source.get(&format!("{name}_FILE")).is_none() {
        return Ok(None);
    }
    let (secret, _origin) = load_secret_with_origin(source, name)?;
    if matches!(name, "TMDB_API_KEY" | "TMDB_ADMIN_API_KEY") && secret.expose_secret().len() < 32 {
        return Err(ConfigError::InvalidValue(name.to_owned()));
    }
    if environment != Environment::Production && is_known_example(&secret) {
        return Err(ConfigError::ExampleSecretForbidden(name.to_owned()));
    }
    Ok(Some(secret))
}

/// Loads the base database identity used for initialization and compatibility.
///
/// The four-container deployment always connects to the product-unique
/// Compose service at `tmdb-mirror-postgres:5432`. The standard `POSTGRES_*`
/// values initialize `PostgreSQL`.
///
/// # Errors
///
/// Returns [`ConfigError`] when a selected setting is invalid, required
/// database credentials are absent, or a development/example secret is used
/// outside production.
pub fn load_shared_database(
    source: &impl ConfigSource,
    environment: Environment,
) -> Result<DatabaseConfig, ConfigError> {
    reject_legacy_database_settings(source)?;
    let database = required_string(source, "POSTGRES_DB")?;
    let username = required_string(source, "POSTGRES_USER")?;
    let (password, _origin) = load_secret_with_origin(source, "POSTGRES_PASSWORD")?;
    if environment != Environment::Production && is_known_example(&password) {
        return Err(ConfigError::ExampleSecretForbidden(
            "POSTGRES_PASSWORD".to_owned(),
        ));
    }

    Ok(DatabaseConfig {
        host: "tmdb-mirror-postgres".to_owned(),
        port: 5432,
        database,
        username,
        password,
    })
}

/// Loads one fixed least-privilege database identity for an application role.
///
/// Role names are defined by the `PostgreSQL` bootstrap. All internal roles use
/// the shared `POSTGRES_PASSWORD`; the database host, name, and port remain
/// fixed to the internal Compose service.
///
/// # Errors
///
/// Returns [`ConfigError`] when the shared database settings are missing,
/// invalid, or use a forbidden example secret.
pub fn load_database_for_role(
    source: &impl ConfigSource,
    environment: Environment,
    role_name: &str,
) -> Result<DatabaseConfig, ConfigError> {
    let shared = load_shared_database(source, environment)?;

    Ok(DatabaseConfig {
        host: shared.host,
        port: shared.port,
        database: shared.database,
        username: role_name.to_owned(),
        password: shared.password,
    })
}

fn reject_legacy_database_settings(source: &impl ConfigSource) -> Result<(), ConfigError> {
    const LEGACY_DATABASE_SETTINGS: [&str; 20] = [
        "DATABASE_HOST",
        "DATABASE_PORT",
        "DATABASE_NAME",
        "DATABASE_USER",
        "DATABASE_PASSWORD",
        "TMDB_DB_HOST",
        "TMDB_DB_PORT",
        "TMDB_DB_NAME",
        "TMDB_DB_USER",
        "TMDB_DB_PASSWORD",
        "TMDB_DIRECT_DB_HOST",
        "TMDB_DIRECT_DB_PORT",
        "TMDB_DIRECT_DB_NAME",
        "TMDB_DIRECT_DB_USER",
        "TMDB_DIRECT_DB_PASSWORD",
        "TMDB_POOLED_DB_HOST",
        "TMDB_POOLED_DB_PORT",
        "TMDB_POOLED_DB_NAME",
        "TMDB_POOLED_DB_USER",
        "TMDB_POOLED_DB_PASSWORD",
    ];

    for name in LEGACY_DATABASE_SETTINGS {
        if source.get(name).is_some() {
            return Err(ConfigError::UnsupportedSetting(name.to_owned()));
        }
        let file_name = format!("{name}_FILE");
        if source.get(&file_name).is_some() {
            return Err(ConfigError::UnsupportedSetting(file_name));
        }
    }
    Ok(())
}

fn has_secret_source(source: &impl ConfigSource, name: &str) -> bool {
    source.get(name).is_some() || source.get(&format!("{name}_FILE")).is_some()
}

fn is_known_example(secret: &SecretString) -> bool {
    ["password", "changeme", "example", "secret", "pw"]
        .iter()
        .any(|example| secret.expose_secret().eq_ignore_ascii_case(example))
}

fn required_string(source: &impl ConfigSource, name: &str) -> Result<String, ConfigError> {
    let value = source
        .get(name)
        .ok_or_else(|| ConfigError::Missing(name.to_owned()))?
        .into_string()
        .map_err(|_| ConfigError::InvalidUnicode(name.to_owned()))?;
    if value.is_empty() || value.contains('\0') {
        return Err(ConfigError::InvalidValue(name.to_owned()));
    }
    Ok(value)
}

fn parse_required<T>(source: &impl ConfigSource, name: &str) -> Result<T, ConfigError>
where
    T: FromStr,
{
    required_string(source, name)?
        .parse()
        .map_err(|_| ConfigError::InvalidValue(name.to_owned()))
}

fn parse_or<T>(source: &impl ConfigSource, name: &str, default: T) -> Result<T, ConfigError>
where
    T: FromStr,
{
    if source.get(name).is_some() {
        parse_required(source, name)
    } else {
        Ok(default)
    }
}

fn optional_uri(source: &impl ConfigSource, name: &str) -> Result<Option<Uri>, ConfigError> {
    let Some(value) = source.get(name) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| ConfigError::InvalidUnicode(name.to_owned()))?;
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    if value.contains('\0') {
        return Err(ConfigError::InvalidValue(name.to_owned()));
    }
    let uri: Uri = value
        .parse()
        .map_err(|_| ConfigError::InvalidValue(name.to_owned()))?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.authority().is_none()
        || uri.query().is_some()
        || uri
            .authority()
            .is_some_and(|authority| authority.as_str().contains('@'))
    {
        return Err(ConfigError::InvalidValue(name.to_owned()));
    }
    Ok(Some(uri))
}
