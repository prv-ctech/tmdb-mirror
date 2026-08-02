use std::ffi::OsString;
use std::fs;
use std::path::Path;

use secrecy::ExposeSecret;
use tempfile::NamedTempFile;
use tmdb_config::{
    AppConfig, ConfigError, Environment, MapSource, StorageRoots, load_secret, load_shared_database,
};

const DATABASE_PASSWORD: &str = "unit-test-database-credential";
const ADMIN_API_KEY: &str = "unit-test-admin-key-012345678901234567890123456789";

fn file_source(name: &str, path: &Path) -> MapSource {
    MapSource::from([(
        OsString::from(format!("{name}_FILE")),
        path.as_os_str().to_os_string(),
    )])
}

fn valid_entries(environment: &str) -> Vec<(OsString, OsString)> {
    [
        ("TMDB_ENVIRONMENT", environment),
        ("TMDB_API_BIND", "127.0.0.1:8080"),
        ("TMDB_ADMIN_BIND", "127.0.0.1:8081"),
        ("POSTGRES_DB", "example_catalog"),
        ("POSTGRES_USER", "example_owner"),
        ("POSTGRES_PASSWORD", DATABASE_PASSWORD),
        ("TMDB_TRAWL_BASE_URL", "http://trawl.internal:8080"),
        ("TMDB_API_KEY", ADMIN_API_KEY),
    ]
    .into_iter()
    .map(|(key, value)| (OsString::from(key), OsString::from(value)))
    .collect()
}

fn source_from_entries(entries: Vec<(OsString, OsString)>) -> MapSource {
    entries.into_iter().collect()
}

fn shared_database_entries(environment: &str) -> MapSource {
    MapSource::from([
        ("TMDB_ENVIRONMENT", environment),
        ("POSTGRES_DB", "example_catalog"),
        ("POSTGRES_USER", "example_owner"),
        ("POSTGRES_PASSWORD", "shared-database-password"),
    ])
}

fn postgres_database_entries(environment: &str) -> Vec<(OsString, OsString)> {
    [
        ("TMDB_ENVIRONMENT", environment),
        ("POSTGRES_DB", "example_catalog"),
        ("POSTGRES_USER", "example_owner"),
        ("POSTGRES_PASSWORD", "shared-database-password"),
    ]
    .into_iter()
    .map(|(key, value)| (OsString::from(key), OsString::from(value)))
    .collect()
}

fn replace_entry(entries: &mut [(OsString, OsString)], key: &str, value: &str) -> bool {
    if let Some(entry) = entries.iter_mut().find(|(candidate, _)| candidate == key) {
        entry.1 = OsString::from(value);
        true
    } else {
        false
    }
}

#[test]
fn secret_requires_exactly_one_source() -> Result<(), Box<dyn std::error::Error>> {
    let conflicting = MapSource::from([
        ("TEST_SECRET", "never-log-this"),
        ("TEST_SECRET_FILE", "/private/secret/location"),
    ]);
    let error = load_secret(&conflicting, "TEST_SECRET")
        .err()
        .ok_or("conflicting secret sources must fail")?;
    assert!(matches!(
        error,
        ConfigError::ConflictingSecretSources(ref name) if name == "TEST_SECRET"
    ));
    let rendered = format!("{error:?} {error}");
    assert!(rendered.contains("TEST_SECRET"));
    assert!(!rendered.contains("never-log-this"));
    assert!(!rendered.contains("/private/secret/location"));

    let missing = MapSource::default();
    assert!(matches!(
        load_secret(&missing, "TEST_SECRET"),
        Err(ConfigError::Missing(ref name)) if name == "TEST_SECRET"
    ));
    Ok(())
}

#[test]
fn map_source_debug_never_exposes_values() {
    let source = MapSource::from([
        ("TEST_SECRET", "map-secret-must-be-hidden"),
        ("TEST_SECRET_FILE", "/private/map-secret-path"),
    ]);

    let rendered = format!("{source:?}");
    assert!(rendered.contains("TEST_SECRET"));
    assert!(!rendered.contains("map-secret-must-be-hidden"));
    assert!(!rendered.contains("/private/map-secret-path"));
}

#[test]
fn direct_secret_rejects_empty_and_nul_and_redacts_debug() -> Result<(), Box<dyn std::error::Error>>
{
    let source = MapSource::from([("TEST_SECRET", "visible-only-in-test")]);
    let secret = load_secret(&source, "TEST_SECRET")?;
    assert_eq!(secret.expose_secret(), "visible-only-in-test");
    let rendered = format!("{secret:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains("visible-only-in-test"));

    for invalid in ["", "has\0nul"] {
        let source = MapSource::from([("TEST_SECRET", invalid)]);
        assert!(matches!(
            load_secret(&source, "TEST_SECRET"),
            Err(ConfigError::InvalidSecret(ref name)) if name == "TEST_SECRET"
        ));
    }
    Ok(())
}

#[test]
fn secret_file_removes_exactly_one_crlf() -> Result<(), Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    fs::write(file.path(), b"file-secret\r\n")?;
    let source = file_source("TEST_SECRET", file.path());

    let secret = load_secret(&source, "TEST_SECRET")?;
    assert_eq!(secret.expose_secret(), "file-secret");

    let file = NamedTempFile::new()?;
    fs::write(file.path(), b"file-secret\n\n")?;
    let source = file_source("TEST_SECRET", file.path());
    let secret = load_secret(&source, "TEST_SECRET")?;
    assert_eq!(secret.expose_secret(), "file-secret\n");
    Ok(())
}

#[test]
fn secret_file_rejects_empty_and_nul_values() -> Result<(), Box<dyn std::error::Error>> {
    for bytes in [b"\n".as_slice(), b"has\0nul".as_slice()] {
        let file = NamedTempFile::new()?;
        fs::write(file.path(), bytes)?;
        let source = file_source("TEST_SECRET", file.path());
        assert!(matches!(
            load_secret(&source, "TEST_SECRET"),
            Err(ConfigError::InvalidSecret(ref name)) if name == "TEST_SECRET"
        ));
    }
    Ok(())
}

#[test]
fn secret_file_enforces_the_64_kib_limit() -> Result<(), Box<dyn std::error::Error>> {
    let at_limit = NamedTempFile::new()?;
    fs::write(at_limit.path(), vec![b'x'; 64 * 1024])?;
    let source = file_source("TEST_SECRET", at_limit.path());
    let secret = load_secret(&source, "TEST_SECRET")?;
    assert_eq!(secret.expose_secret().len(), 64 * 1024);

    let over_limit = NamedTempFile::new()?;
    fs::write(over_limit.path(), vec![b'x'; 64 * 1024 + 1])?;
    let source = file_source("TEST_SECRET", over_limit.path());
    assert!(matches!(
        load_secret(&source, "TEST_SECRET"),
        Err(ConfigError::SecretTooLarge(ref name)) if name == "TEST_SECRET"
    ));

    for trailing_line_ending in [b"\n".as_slice(), b"\r\n".as_slice()] {
        let over_limit = NamedTempFile::new()?;
        let mut bytes = vec![b'x'; 64 * 1024];
        bytes.extend_from_slice(trailing_line_ending);
        fs::write(over_limit.path(), bytes)?;
        let source = file_source("TEST_SECRET", over_limit.path());
        assert!(matches!(
            load_secret(&source, "TEST_SECRET"),
            Err(ConfigError::SecretTooLarge(ref name)) if name == "TEST_SECRET"
        ));
    }
    Ok(())
}

#[test]
fn storage_roots_must_be_absolute_normalized_distinct_and_non_root() {
    assert!(StorageRoots::try_new("/", "/images", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("work", "/images", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("/work/../escape", "/images", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("/work/./cache", "/images", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("/work/", "/images", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("/work", "/work", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("/work", "/work/images", "/raw", "/backups").is_err());
    assert!(StorageRoots::try_new("/work", "/workbench", "/raw", "/backups").is_ok());
}

#[test]
fn app_config_parses_typed_settings_and_redacts_secrets() -> Result<(), Box<dyn std::error::Error>>
{
    let source = source_from_entries(valid_entries("development"));
    let config = AppConfig::load(&source)?;

    assert_eq!(config.environment, Environment::Development);
    assert_eq!(config.api_bind.to_string(), "127.0.0.1:8080");
    assert_eq!(config.admin_bind.to_string(), "127.0.0.1:8081");
    assert_eq!(config.database.host, "postgres");
    assert_eq!(config.database.port, 5432);
    assert_eq!(
        config.storage_roots.work,
        std::path::Path::new("/config/work")
    );
    assert_eq!(config.storage_roots.images, std::path::Path::new("/media"));
    assert_eq!(
        config.storage_roots.raw_archive,
        std::path::Path::new("/config/raw")
    );
    assert_eq!(
        config.storage_roots.backups,
        std::path::Path::new("/config/backups")
    );
    assert!(config.admin_api_key.is_some());
    assert_eq!(
        config
            .trawl_base_url
            .as_ref()
            .and_then(|uri| uri.scheme_str()),
        Some("http")
    );

    let rendered = format!("{config:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains(DATABASE_PASSWORD));
    Ok(())
}

#[test]
fn postgres_settings_accept_custom_identity() -> Result<(), Box<dyn std::error::Error>> {
    let source = shared_database_entries("production");
    let config = load_shared_database(&source, Environment::Production)?;

    assert_eq!(config.host, "postgres");
    assert_eq!(config.port, 5432);
    assert_eq!(config.database, "example_catalog");
    assert_eq!(config.username, "example_owner");
    assert_eq!(config.password.expose_secret(), "shared-database-password");
    Ok(())
}

#[test]
fn redundant_database_aliases_are_rejected() {
    for alias in [
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
    ] {
        for setting in [alias.to_owned(), format!("{alias}_FILE")] {
            let mut entries = postgres_database_entries("production");
            entries.push((OsString::from(&setting), OsString::from("legacy-value")));
            assert!(matches!(
                load_shared_database(&source_from_entries(entries), Environment::Production),
                Err(ConfigError::UnsupportedSetting(ref name)) if name == &setting
            ));
        }
    }
}

#[test]
fn app_config_errors_name_fields_without_exposing_values() -> Result<(), Box<dyn std::error::Error>>
{
    let mut entries = valid_entries("test");
    assert!(replace_entry(
        &mut entries,
        "TMDB_API_BIND",
        "private-invalid-port"
    ));
    let error = AppConfig::load(&source_from_entries(entries))
        .err()
        .ok_or("invalid port must fail")?;
    let rendered = format!("{error:?} {error}");
    assert!(rendered.contains("TMDB_API_BIND"));
    assert!(!rendered.contains("private-invalid-port"));
    assert!(!rendered.contains(DATABASE_PASSWORD));

    let mut entries = valid_entries("test");
    assert!(replace_entry(
        &mut entries,
        "TMDB_TRAWL_BASE_URL",
        "ftp://trawl.internal"
    ));
    assert!(matches!(
        AppConfig::load(&source_from_entries(entries)),
        Err(ConfigError::InvalidValue(ref name)) if name == "TMDB_TRAWL_BASE_URL"
    ));
    Ok(())
}

#[test]
fn app_config_rejects_known_example_passwords_outside_production() {
    let mut entries = valid_entries("development");
    assert!(replace_entry(&mut entries, "POSTGRES_PASSWORD", "changeme"));
    assert!(matches!(
        AppConfig::load(&source_from_entries(entries)),
        Err(ConfigError::ExampleSecretForbidden(ref name))
            if name == "POSTGRES_PASSWORD"
    ));
}

#[test]
fn trawl_base_url_rejects_embedded_query_credentials() {
    let mut entries = valid_entries("test");
    assert!(replace_entry(
        &mut entries,
        "TMDB_TRAWL_BASE_URL",
        "http://trawl.internal/?token=must-not-appear-in-debug"
    ));

    assert!(matches!(
        AppConfig::load(&source_from_entries(entries)),
        Err(ConfigError::InvalidValue(ref name)) if name == "TMDB_TRAWL_BASE_URL"
    ));
}

#[test]
fn production_accepts_postgres_password() -> Result<(), Box<dyn std::error::Error>> {
    let source = source_from_entries(valid_entries("production"));
    let config = AppConfig::load(&source)?;
    assert_eq!(config.environment, Environment::Production);
    Ok(())
}

#[test]
fn admin_api_key_requires_at_least_256_bits() {
    let mut entries = valid_entries("test");
    assert!(replace_entry(&mut entries, "TMDB_API_KEY", "too-short"));
    assert!(matches!(
        AppConfig::load(&source_from_entries(entries)),
        Err(ConfigError::InvalidValue(ref name)) if name == "TMDB_API_KEY"
    ));
}
