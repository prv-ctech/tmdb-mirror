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
    /// Direct `PostgreSQL` settings used by workers.
    pub direct_database: DatabaseConfig,
    /// Direct `PostgreSQL` settings used by public reads.
    pub pooled_database: DatabaseConfig,
    /// Pairwise-disjoint storage trees.
    pub storage_roots: StorageRoots,
    /// Optional private Trawl service endpoint.
    pub trawl_base_url: Option<Uri>,
    /// Optional API key used to protect the private administrative listener.
    /// Production configuration must provide this through `TMDB_ADMIN_API_KEY`.
    pub admin_api_key: Option<SecretString>,
}

impl AppConfig {
    /// Loads and validates all application configuration from one source.
    ///
    /// The environment keys are `TMDB_ENVIRONMENT`, `TMDB_API_BIND`,
    /// `TMDB_ADMIN_BIND`, and the shared `TMDB_DB_*` database settings. The
    /// legacy `TMDB_DIRECT_DB_*` and `TMDB_POOLED_DB_*` names remain accepted
    /// for existing callers. Database and API credentials may be supplied
    /// directly in the environment; optional `_FILE` indirection is retained
    /// only for compatibility. Filesystem roots are fixed to `/media` and
    /// `/config` and are selected only by deployment volume mappings.
    ///
    /// # Errors
    ///
    /// Returns a field-naming error when a required value is missing, typed
    /// parsing or validation fails, secret policy is violated, or storage
    /// roots overlap. Errors never include configured values.
    pub fn load(source: &impl ConfigSource) -> Result<Self, ConfigError> {
        let environment = parse_required(source, "TMDB_ENVIRONMENT")?;
        let api_bind = parse_required(source, "TMDB_API_BIND")?;
        let admin_bind = parse_required(source, "TMDB_ADMIN_BIND")?;
        let direct_database = load_database_or_shared(source, environment, "TMDB_DIRECT_DB")?;
        let pooled_database = load_database_or_shared(source, environment, "TMDB_POOLED_DB")?;
        let storage_roots = StorageRoots::fixed();
        let trawl_base_url = optional_uri(source, "TMDB_TRAWL_BASE_URL")?;
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
            direct_database,
            pooled_database,
            storage_roots,
            trawl_base_url,
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

fn load_database_or_shared(
    source: &impl ConfigSource,
    environment: Environment,
    prefix: &str,
) -> Result<DatabaseConfig, ConfigError> {
    if source.get(&format!("{prefix}_HOST")).is_none()
        && source.get(&format!("{prefix}_PORT")).is_none()
        && source.get(&format!("{prefix}_NAME")).is_none()
        && source.get(&format!("{prefix}_USER")).is_none()
        && !has_secret_source(source, &format!("{prefix}_PASSWORD"))
    {
        return load_shared_database(source, environment);
    }

    load_prefixed_database(source, environment, prefix)
}

fn load_prefixed_database(
    source: &impl ConfigSource,
    environment: Environment,
    prefix: &str,
) -> Result<DatabaseConfig, ConfigError> {
    let host_name = format!("{prefix}_HOST");
    let port_name = format!("{prefix}_PORT");
    let database_name = format!("{prefix}_NAME");
    let user_name = format!("{prefix}_USER");
    let password_name = format!("{prefix}_PASSWORD");
    let (password, _origin) = load_secret_with_origin(source, &password_name)?;
    if environment != Environment::Production && is_known_example(&password) {
        return Err(ConfigError::ExampleSecretForbidden(password_name));
    }

    Ok(DatabaseConfig {
        host: required_string(source, &host_name)?,
        port: parse_required(source, &port_name)?,
        database: required_string(source, &database_name)?,
        username: required_string(source, &user_name)?,
        password,
    })
}

/// Loads one shared database identity for the API and both workers.
///
/// `TMDB_DB_*` values take precedence. `DATABASE_*` aliases and the standard
/// `POSTGRES_*` names are accepted so a single Compose `env_file` can be used
/// without repeating the same settings per service.
pub fn load_shared_database(
    source: &impl ConfigSource,
    environment: Environment,
) -> Result<DatabaseConfig, ConfigError> {
    let host = configured_string(source, &["TMDB_DB_HOST", "DATABASE_HOST"])?
        .unwrap_or_else(|| "postgres".to_owned());
    let port_text = configured_string(source, &["TMDB_DB_PORT", "DATABASE_PORT"])?
        .unwrap_or_else(|| "5432".to_owned());
    let port = port_text
        .parse()
        .map_err(|_| ConfigError::InvalidValue("TMDB_DB_PORT".to_owned()))?;
    let database = configured_string(source, &["TMDB_DB_NAME", "DATABASE_NAME", "POSTGRES_DB"])?
        .unwrap_or_else(|| "tmdb".to_owned());
    let username = configured_string(source, &["TMDB_DB_USER", "DATABASE_USER", "POSTGRES_USER"])?
        .unwrap_or_else(|| "tmdb_owner".to_owned());
    let password_name = [
        "TMDB_DB_PASSWORD",
        "DATABASE_PASSWORD",
        "POSTGRES_PASSWORD",
        "TMDB_DIRECT_DB_PASSWORD",
    ]
    .into_iter()
    .find(|name| has_secret_source(source, name))
    .ok_or_else(|| ConfigError::Missing("POSTGRES_PASSWORD".to_owned()))?;
    let (password, _origin) = load_secret_with_origin(source, password_name)?;
    if environment != Environment::Production && is_known_example(&password) {
        return Err(ConfigError::ExampleSecretForbidden(
            password_name.to_owned(),
        ));
    }

    Ok(DatabaseConfig {
        host,
        port,
        database,
        username,
        password,
    })
}

fn configured_string(
    source: &impl ConfigSource,
    names: &[&str],
) -> Result<Option<String>, ConfigError> {
    for name in names {
        let Some(value) = source.get(name) else {
            continue;
        };
        let value = value
            .into_string()
            .map_err(|_| ConfigError::InvalidUnicode((*name).to_owned()))?;
        if value.is_empty() || value.contains('\0') {
            return Err(ConfigError::InvalidValue((*name).to_owned()));
        }
        return Ok(Some(value));
    }
    Ok(None)
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

fn optional_uri(source: &impl ConfigSource, name: &str) -> Result<Option<Uri>, ConfigError> {
    let Some(value) = source.get(name) else {
        return Ok(None);
    };
    let value = value
        .into_string()
        .map_err(|_| ConfigError::InvalidUnicode(name.to_owned()))?;
    if value.is_empty() || value.contains('\0') {
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
