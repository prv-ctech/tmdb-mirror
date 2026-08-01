use std::net::SocketAddr;
use std::str::FromStr;

use http::Uri;
use secrecy::{ExposeSecret, SecretString};

use crate::secret::{SecretOrigin, load_secret_with_origin};
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
#[derive(Debug)]
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
    /// Production configuration must provide this through `TMDB_API_KEY_FILE`.
    pub admin_api_key: Option<SecretString>,
}

impl AppConfig {
    /// Loads and validates all application configuration from one source.
    ///
    /// The environment keys are `TMDB_ENVIRONMENT`, `TMDB_API_BIND`,
    /// `TMDB_ADMIN_BIND`, `TMDB_{DIRECT,POOLED}_DB_{HOST,PORT,NAME,USER}`,
    /// password fields with optional `_FILE` indirection, and optional
    /// `TMDB_TRAWL_BASE_URL`.  Filesystem roots are fixed to `/media` and
    /// `/config` (with their application subdirectories); host paths are
    /// selected only by deployment volume mappings and are never read here.
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
        let direct_database = load_database(source, environment, "TMDB_DIRECT_DB")?;
        let pooled_database = load_database(source, environment, "TMDB_POOLED_DB")?;
        let storage_roots = StorageRoots::fixed();
        let trawl_base_url = optional_uri(source, "TMDB_TRAWL_BASE_URL")?;
        let admin_api_key = optional_secret(source, environment, "TMDB_API_KEY")?;
        if environment == Environment::Production && admin_api_key.is_none() {
            return Err(ConfigError::Missing("TMDB_API_KEY".to_owned()));
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
    let (secret, origin) = load_secret_with_origin(source, name)?;
    if environment == Environment::Production && origin == SecretOrigin::Direct {
        return Err(ConfigError::InlineSecretForbidden(name.to_owned()));
    }
    if name == "TMDB_API_KEY" && secret.expose_secret().len() < 32 {
        return Err(ConfigError::InvalidValue(name.to_owned()));
    }
    if environment != Environment::Production && is_known_example(&secret) {
        return Err(ConfigError::ExampleSecretForbidden(name.to_owned()));
    }
    Ok(Some(secret))
}

fn load_database(
    source: &impl ConfigSource,
    environment: Environment,
    prefix: &str,
) -> Result<DatabaseConfig, ConfigError> {
    let host_name = format!("{prefix}_HOST");
    let port_name = format!("{prefix}_PORT");
    let database_name = format!("{prefix}_NAME");
    let user_name = format!("{prefix}_USER");
    let password_name = format!("{prefix}_PASSWORD");
    let (password, origin) = load_secret_with_origin(source, &password_name)?;

    if environment == Environment::Production && origin == SecretOrigin::Direct {
        return Err(ConfigError::InlineSecretForbidden(password_name));
    }
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
