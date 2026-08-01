use std::fs::File;
use std::io::{Read, Take};
use std::path::PathBuf;

use secrecy::SecretString;

use crate::{ConfigError, ConfigSource};

const MAX_SECRET_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SecretOrigin {
    Direct,
    File,
}

/// Loads a secret from exactly one of `NAME` or `NAME_FILE`.
///
/// File-backed secrets are bounded at 64 KiB and have exactly one trailing
/// line ending removed. Secret contents are never included in errors.
///
/// # Errors
///
/// Returns an error when the source selection is ambiguous or missing, the
/// file cannot be read, the size limit is exceeded, or the value is empty,
/// contains NUL, or is not UTF-8.
pub fn load_secret(source: &impl ConfigSource, name: &str) -> Result<SecretString, ConfigError> {
    load_secret_with_origin(source, name).map(|(secret, _)| secret)
}

/// Loads a secret while enforcing the deployment environment's source policy.
///
/// Production processes must use a file-backed secret. Development and test
/// processes may use either source, subject to the normal one-source and value
/// validation rules.
///
/// # Errors
///
/// Returns the same validation errors as [`load_secret`], plus
/// [`ConfigError::InlineSecretForbidden`] for production inline values.
pub fn load_secret_for_environment(
    source: &impl ConfigSource,
    name: &str,
    environment: crate::Environment,
) -> Result<SecretString, ConfigError> {
    let (secret, origin) = load_secret_with_origin(source, name)?;
    if environment == crate::Environment::Production && origin == SecretOrigin::Direct {
        return Err(ConfigError::InlineSecretForbidden(name.to_owned()));
    }
    Ok(secret)
}

pub(crate) fn load_secret_with_origin(
    source: &impl ConfigSource,
    name: &str,
) -> Result<(SecretString, SecretOrigin), ConfigError> {
    let direct = source.get(name);
    let file_name = format!("{name}_FILE");
    let file = source.get(&file_name);

    match (direct, file) {
        (Some(_), Some(_)) => Err(ConfigError::ConflictingSecretSources(name.to_owned())),
        (None, None) => Err(ConfigError::Missing(name.to_owned())),
        (Some(value), None) => {
            let value = value
                .into_string()
                .map_err(|_| ConfigError::InvalidUnicode(name.to_owned()))?;
            validate_secret(value.into_bytes(), name).map(|secret| (secret, SecretOrigin::Direct))
        }
        (None, Some(path)) => {
            let file = File::open(PathBuf::from(path))
                .map_err(|_| ConfigError::SecretFileRead(name.to_owned()))?;
            let bytes = read_bounded(file.take((MAX_SECRET_BYTES + 1) as u64), name)?;
            validate_secret(trim_one_line_ending(bytes), name)
                .map(|secret| (secret, SecretOrigin::File))
        }
    }
}

fn read_bounded(mut reader: Take<File>, name: &str) -> Result<Vec<u8>, ConfigError> {
    let mut bytes = Vec::with_capacity(MAX_SECRET_BYTES + 1);
    reader
        .read_to_end(&mut bytes)
        .map_err(|_| ConfigError::SecretFileRead(name.to_owned()))?;
    if bytes.len() > MAX_SECRET_BYTES {
        return Err(ConfigError::SecretTooLarge(name.to_owned()));
    }
    Ok(bytes)
}

fn trim_one_line_ending(mut bytes: Vec<u8>) -> Vec<u8> {
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
    }
    bytes
}

fn validate_secret(bytes: Vec<u8>, name: &str) -> Result<SecretString, ConfigError> {
    if bytes.is_empty() || bytes.contains(&0) {
        return Err(ConfigError::InvalidSecret(name.to_owned()));
    }
    let value =
        String::from_utf8(bytes).map_err(|_| ConfigError::InvalidSecret(name.to_owned()))?;
    Ok(SecretString::from(value))
}
