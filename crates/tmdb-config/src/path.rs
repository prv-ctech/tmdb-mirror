use std::path::{Component, Path, PathBuf};

use crate::ConfigError;

/// Validated, pairwise-disjoint storage trees.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageRoots {
    /// Permanent image library storage.
    pub images: PathBuf,
    /// Compressed source archive storage.
    pub raw_archive: PathBuf,
    /// Backup repository storage.
    pub backups: PathBuf,
}

impl StorageRoots {
    /// Returns the fixed storage layout used inside application containers.
    /// Host directories are selected by the deployment volume mappings.
    #[must_use]
    pub fn fixed() -> Self {
        // These literals are validated by the unit tests and are part of the
        // container contract, so construction cannot fail at runtime.
        Self {
            images: PathBuf::from("/media"),
            raw_archive: PathBuf::from("/config/raw"),
            backups: PathBuf::from("/config/backups"),
        }
    }

    /// Validates three lexical storage roots without accessing the filesystem.
    ///
    /// # Errors
    ///
    /// Returns an error if a path is relative, non-normalized, a filesystem
    /// root, contains NUL or parent traversal, or overlaps any other root.
    pub fn try_new(
        images: impl Into<PathBuf>,
        raw_archive: impl Into<PathBuf>,
        backups: impl Into<PathBuf>,
    ) -> Result<Self, ConfigError> {
        let roots = [
            ("images", validate("images", images.into())?),
            ("raw_archive", validate("raw_archive", raw_archive.into())?),
            ("backups", validate("backups", backups.into())?),
        ];

        for left in 0..roots.len() {
            for right in (left + 1)..roots.len() {
                let (left_name, left_path) = &roots[left];
                let (right_name, right_path) = &roots[right];
                if left_path.starts_with(right_path) || right_path.starts_with(left_path) {
                    return Err(ConfigError::OverlappingStorageRoots(left_name, right_name));
                }
            }
        }

        let [(_, images), (_, raw_archive), (_, backups)] = roots;
        Ok(Self {
            images,
            raw_archive,
            backups,
        })
    }
}

fn validate(name: &'static str, value: PathBuf) -> Result<PathBuf, ConfigError> {
    if !value.is_absolute() || value.as_os_str().as_encoded_bytes().contains(&0) {
        return Err(ConfigError::InvalidStorageRoot(name));
    }

    let mut has_normal_component = false;
    for component in value.components() {
        match component {
            Component::Normal(_) => has_normal_component = true,
            Component::ParentDir | Component::CurDir => {
                return Err(ConfigError::InvalidStorageRoot(name));
            }
            Component::Prefix(_) | Component::RootDir => {}
        }
    }
    if !has_normal_component {
        return Err(ConfigError::InvalidStorageRoot(name));
    }

    let normalized: PathBuf = value.components().collect();
    if !same_spelling(&value, &normalized) {
        return Err(ConfigError::InvalidStorageRoot(name));
    }
    Ok(value)
}

fn same_spelling(left: &Path, right: &Path) -> bool {
    left.as_os_str().as_encoded_bytes() == right.as_os_str().as_encoded_bytes()
}
