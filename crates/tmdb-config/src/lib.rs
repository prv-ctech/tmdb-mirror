//! Configuration primitives for the TMDB mirror.

mod path;
mod secret;
mod settings;
mod source;

pub use http::Uri;
pub use path::StorageRoots;
pub use secrecy::SecretString;
pub use secret::{load_secret, load_secret_for_environment};
pub use settings::{AppConfig, DatabaseConfig, Environment, load_shared_database};
pub use source::{ConfigSource, EnvSource, MapSource};

/// A configuration value could not be loaded or validated.
#[derive(Clone, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ConfigError {
    /// Neither the direct value nor its file indirection was configured.
    #[error("missing configuration field {0}")]
    Missing(String),
    /// Both a direct secret and its file indirection were configured.
    #[error("conflicting secret sources for {0}")]
    ConflictingSecretSources(String),
    /// A configured value was not valid Unicode.
    #[error("configuration field {0} is not valid Unicode")]
    InvalidUnicode(String),
    /// A configured non-secret value failed validation or typed parsing.
    #[error("configuration field {0} is invalid")]
    InvalidValue(String),
    /// A legacy configuration name is deliberately not supported.
    #[error("configuration field {0} is not supported")]
    UnsupportedSetting(String),
    /// A secret file could not be read.
    #[error("could not read the secret configured by {0}")]
    SecretFileRead(String),
    /// A secret file exceeded the fixed input limit.
    #[error("secret configured by {0} exceeds the size limit")]
    SecretTooLarge(String),
    /// A secret was empty, contained NUL, or was not valid UTF-8.
    #[error("secret configured by {0} is invalid")]
    InvalidSecret(String),
    /// Production configuration attempted to use an inline secret.
    #[error("production requires a secret file for {0}")]
    InlineSecretForbidden(String),
    /// Development or test configuration used a known example password.
    #[error("configuration field {0} uses a forbidden example secret")]
    ExampleSecretForbidden(String),
    /// A storage root was relative, non-normalized, a filesystem root, or otherwise invalid.
    #[error("storage root {0} is invalid")]
    InvalidStorageRoot(&'static str),
    /// Two storage roots were equal or had an ancestor relationship.
    #[error("storage roots {0} and {1} overlap")]
    OverlappingStorageRoots(&'static str, &'static str),
}
