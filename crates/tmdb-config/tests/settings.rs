use std::ffi::OsString;
use std::fs;
use std::path::Path;

use secrecy::ExposeSecret;
use tempfile::NamedTempFile;
use tmdb_config::{AppConfig, ConfigError, Environment, MapSource, StorageRoots, load_secret};

const DIRECT_PASSWORD: &str = "unit-test-direct-credential";
const POOLED_PASSWORD: &str = "unit-test-pooled-credential";
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
        ("TMDB_DIRECT_DB_HOST", "postgres.internal"),
        ("TMDB_DIRECT_DB_PORT", "5432"),
        ("TMDB_DIRECT_DB_NAME", "tmdb"),
        ("TMDB_DIRECT_DB_USER", "ingest_writer"),
        ("TMDB_DIRECT_DB_PASSWORD", DIRECT_PASSWORD),
        ("TMDB_POOLED_DB_HOST", "pgbouncer.internal"),
        ("TMDB_POOLED_DB_PORT", "6432"),
        ("TMDB_POOLED_DB_NAME", "tmdb"),
        ("TMDB_POOLED_DB_USER", "api_reader"),
        ("TMDB_POOLED_DB_PASSWORD", POOLED_PASSWORD),
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
        ("TMDB_DB_PASSWORD", "never-log-this"),
        ("TMDB_DB_PASSWORD_FILE", "/private/secret/location"),
    ]);
    let error = load_secret(&conflicting, "TMDB_DB_PASSWORD")
        .err()
        .ok_or("conflicting secret sources must fail")?;
    assert!(matches!(
        error,
        ConfigError::ConflictingSecretSources(ref name) if name == "TMDB_DB_PASSWORD"
    ));
    let rendered = format!("{error:?} {error}");
    assert!(rendered.contains("TMDB_DB_PASSWORD"));
    assert!(!rendered.contains("never-log-this"));
    assert!(!rendered.contains("/private/secret/location"));

    let missing = MapSource::default();
    assert!(matches!(
        load_secret(&missing, "TMDB_DB_PASSWORD"),
        Err(ConfigError::Missing(ref name)) if name == "TMDB_DB_PASSWORD"
    ));
    Ok(())
}

#[test]
fn map_source_debug_never_exposes_values() {
    let source = MapSource::from([
        ("TMDB_DB_PASSWORD", "map-secret-must-be-hidden"),
        ("TMDB_DB_PASSWORD_FILE", "/private/map-secret-path"),
    ]);

    let rendered = format!("{source:?}");
    assert!(rendered.contains("TMDB_DB_PASSWORD"));
    assert!(!rendered.contains("map-secret-must-be-hidden"));
    assert!(!rendered.contains("/private/map-secret-path"));
}

#[test]
fn direct_secret_rejects_empty_and_nul_and_redacts_debug() -> Result<(), Box<dyn std::error::Error>>
{
    let source = MapSource::from([("TMDB_DB_PASSWORD", "visible-only-in-test")]);
    let secret = load_secret(&source, "TMDB_DB_PASSWORD")?;
    assert_eq!(secret.expose_secret(), "visible-only-in-test");
    let rendered = format!("{secret:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains("visible-only-in-test"));

    for invalid in ["", "has\0nul"] {
        let source = MapSource::from([("TMDB_DB_PASSWORD", invalid)]);
        assert!(matches!(
            load_secret(&source, "TMDB_DB_PASSWORD"),
            Err(ConfigError::InvalidSecret(ref name)) if name == "TMDB_DB_PASSWORD"
        ));
    }
    Ok(())
}

#[test]
fn secret_file_removes_exactly_one_crlf() -> Result<(), Box<dyn std::error::Error>> {
    let file = NamedTempFile::new()?;
    fs::write(file.path(), b"file-secret\r\n")?;
    let source = file_source("TMDB_DB_PASSWORD", file.path());

    let secret = load_secret(&source, "TMDB_DB_PASSWORD")?;
    assert_eq!(secret.expose_secret(), "file-secret");

    let file = NamedTempFile::new()?;
    fs::write(file.path(), b"file-secret\n\n")?;
    let source = file_source("TMDB_DB_PASSWORD", file.path());
    let secret = load_secret(&source, "TMDB_DB_PASSWORD")?;
    assert_eq!(secret.expose_secret(), "file-secret\n");
    Ok(())
}

#[test]
fn secret_file_rejects_empty_and_nul_values() -> Result<(), Box<dyn std::error::Error>> {
    for bytes in [b"\n".as_slice(), b"has\0nul".as_slice()] {
        let file = NamedTempFile::new()?;
        fs::write(file.path(), bytes)?;
        let source = file_source("TMDB_DB_PASSWORD", file.path());
        assert!(matches!(
            load_secret(&source, "TMDB_DB_PASSWORD"),
            Err(ConfigError::InvalidSecret(ref name)) if name == "TMDB_DB_PASSWORD"
        ));
    }
    Ok(())
}

#[test]
fn secret_file_enforces_the_64_kib_limit() -> Result<(), Box<dyn std::error::Error>> {
    let at_limit = NamedTempFile::new()?;
    fs::write(at_limit.path(), vec![b'x'; 64 * 1024])?;
    let source = file_source("TMDB_DB_PASSWORD", at_limit.path());
    let secret = load_secret(&source, "TMDB_DB_PASSWORD")?;
    assert_eq!(secret.expose_secret().len(), 64 * 1024);

    let over_limit = NamedTempFile::new()?;
    fs::write(over_limit.path(), vec![b'x'; 64 * 1024 + 1])?;
    let source = file_source("TMDB_DB_PASSWORD", over_limit.path());
    assert!(matches!(
        load_secret(&source, "TMDB_DB_PASSWORD"),
        Err(ConfigError::SecretTooLarge(ref name)) if name == "TMDB_DB_PASSWORD"
    ));

    for trailing_line_ending in [b"\n".as_slice(), b"\r\n".as_slice()] {
        let over_limit = NamedTempFile::new()?;
        let mut bytes = vec![b'x'; 64 * 1024];
        bytes.extend_from_slice(trailing_line_ending);
        fs::write(over_limit.path(), bytes)?;
        let source = file_source("TMDB_DB_PASSWORD", over_limit.path());
        assert!(matches!(
            load_secret(&source, "TMDB_DB_PASSWORD"),
            Err(ConfigError::SecretTooLarge(ref name)) if name == "TMDB_DB_PASSWORD"
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
    assert_eq!(config.direct_database.port, 5432);
    assert_eq!(config.pooled_database.port, 6432);
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
    assert!(!rendered.contains(DIRECT_PASSWORD));
    assert!(!rendered.contains(POOLED_PASSWORD));
    Ok(())
}

#[test]
fn app_config_errors_name_fields_without_exposing_values() -> Result<(), Box<dyn std::error::Error>>
{
    let mut entries = valid_entries("test");
    assert!(replace_entry(
        &mut entries,
        "TMDB_DIRECT_DB_PORT",
        "private-invalid-port"
    ));
    let error = AppConfig::load(&source_from_entries(entries))
        .err()
        .ok_or("invalid port must fail")?;
    let rendered = format!("{error:?} {error}");
    assert!(rendered.contains("TMDB_DIRECT_DB_PORT"));
    assert!(!rendered.contains("private-invalid-port"));
    assert!(!rendered.contains(DIRECT_PASSWORD));

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
    assert!(replace_entry(
        &mut entries,
        "TMDB_DIRECT_DB_PASSWORD",
        "changeme"
    ));
    assert!(matches!(
        AppConfig::load(&source_from_entries(entries)),
        Err(ConfigError::ExampleSecretForbidden(ref name))
            if name == "TMDB_DIRECT_DB_PASSWORD"
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
fn production_rejects_inline_secrets_and_accepts_secret_files()
-> Result<(), Box<dyn std::error::Error>> {
    let source = source_from_entries(valid_entries("production"));
    assert!(matches!(
        AppConfig::load(&source),
        Err(ConfigError::InlineSecretForbidden(ref name))
            if name == "TMDB_DIRECT_DB_PASSWORD"
    ));

    let direct_file = NamedTempFile::new()?;
    fs::write(direct_file.path(), DIRECT_PASSWORD)?;
    let pooled_file = NamedTempFile::new()?;
    fs::write(pooled_file.path(), POOLED_PASSWORD)?;
    let api_key_file = NamedTempFile::new()?;
    fs::write(api_key_file.path(), ADMIN_API_KEY)?;

    let mut entries = valid_entries("production");
    entries.retain(|(key, _)| {
        key != "TMDB_DIRECT_DB_PASSWORD"
            && key != "TMDB_POOLED_DB_PASSWORD"
            && key != "TMDB_API_KEY"
    });
    entries.push((
        OsString::from("TMDB_DIRECT_DB_PASSWORD_FILE"),
        direct_file.path().as_os_str().to_os_string(),
    ));
    entries.push((
        OsString::from("TMDB_POOLED_DB_PASSWORD_FILE"),
        pooled_file.path().as_os_str().to_os_string(),
    ));
    entries.push((
        OsString::from("TMDB_API_KEY_FILE"),
        api_key_file.path().as_os_str().to_os_string(),
    ));

    let config = AppConfig::load(&source_from_entries(entries))?;
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
