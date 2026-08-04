//! Fixed-container-path media layout shared by the API and media worker.
//!
//! Host paths deliberately do not appear in this crate.  Deployments mount
//! their chosen host directories at [`MEDIA_ROOT`] and [`CONFIG_ROOT`].

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use thiserror::Error;

/// Permanent public media mount inside every application container.
pub const MEDIA_ROOT: &str = "/media";
/// NVMe-backed application-data mount inside every worker container.
pub const CONFIG_ROOT: &str = "/config";
/// Media worker scratch directory below [`CONFIG_ROOT`].
pub const MEDIA_WORK_ROOT: &str = "/config/media";
/// Raw exports and checkpoints below [`CONFIG_ROOT`].
pub const RAW_ROOT: &str = "/config/raw";
/// Durable worker logs below [`CONFIG_ROOT`].
pub const LOG_ROOT: &str = "/config/logs";
/// General worker scratch directory below [`CONFIG_ROOT`].
pub const WORK_ROOT: &str = "/config/work";
/// PostgreSQL-owned pgBackRest parent directory below [`CONFIG_ROOT`].
///
/// Application workers deliberately do not prepare or write this path. The
/// `PostgreSQL` entrypoint creates `/config/backups/pgbackrest` as the
/// only backup repository and keeps that child owned by the `PostgreSQL` OS
/// user.
pub const BACKUP_ROOT: &str = "/config/backups";

/// Fixed service role whose writable directories must be ready before startup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeStorageRole {
    /// The main worker, which owns migrations, metadata ingest, and exports.
    Worker,
    /// The media worker, which owns image scratch data and final media publication.
    Media,
}

impl RuntimeStorageRole {
    /// Returns the stable role name used in safe operational events.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Media => "media",
        }
    }
}

/// A fixed, application-owned writable path. These labels deliberately never
/// contain a deployment's host-side mount path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeStoragePath {
    /// `/config/work`
    ConfigWork,
    /// `/config/raw`
    ConfigRaw,
    /// `/config/backups` (PostgreSQL-owned; not included in worker preflight)
    ConfigBackups,
    /// `/config/logs`
    ConfigLogs,
    /// `/config/media`
    ConfigMedia,
    /// `/media/movies`
    MediaMovies,
    /// `/media/tv`
    MediaTv,
    /// `/media/people`
    MediaPeople,
    /// `/media/networks`
    MediaNetworks,
    /// `/media/companies`
    MediaCompanies,
    /// `/media/collections`
    MediaCollections,
}

impl RuntimeStoragePath {
    /// Returns the fixed in-container path shown in logs and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConfigWork => WORK_ROOT,
            Self::ConfigRaw => RAW_ROOT,
            Self::ConfigBackups => BACKUP_ROOT,
            Self::ConfigLogs => LOG_ROOT,
            Self::ConfigMedia => MEDIA_WORK_ROOT,
            Self::MediaMovies => "/media/movies",
            Self::MediaTv => "/media/tv",
            Self::MediaPeople => "/media/people",
            Self::MediaNetworks => "/media/networks",
            Self::MediaCompanies => "/media/companies",
            Self::MediaCollections => "/media/collections",
        }
    }

    fn resolve(self, config_root: &Path, media_root: &Path) -> PathBuf {
        match self {
            Self::ConfigWork => config_root.join("work"),
            Self::ConfigRaw => config_root.join("raw"),
            Self::ConfigBackups => config_root.join("backups"),
            Self::ConfigLogs => config_root.join("logs"),
            Self::ConfigMedia => config_root.join("media"),
            Self::MediaMovies => media_root.join("movies"),
            Self::MediaTv => media_root.join("tv"),
            Self::MediaPeople => media_root.join("people"),
            Self::MediaNetworks => media_root.join("networks"),
            Self::MediaCompanies => media_root.join("companies"),
            Self::MediaCollections => media_root.join("collections"),
        }
    }
}

/// Failure while creating or verifying an application-owned runtime directory.
#[derive(Debug, Error)]
pub enum RuntimeStorageError {
    /// A path required to be a directory is a regular file or another entry.
    #[error("required storage path is not a directory")]
    NotDirectory { path: RuntimeStoragePath },
    /// A required path is a symlink, which would let startup modify a target
    /// outside the fixed storage layout.
    #[error("required storage path must not be a symlink")]
    Symlink { path: RuntimeStoragePath },
    /// The directory could not be created.
    #[error("could not create required storage path")]
    Create {
        path: RuntimeStoragePath,
        #[source]
        source: std::io::Error,
    },
    /// The service user cannot create and remove a small probe file in the path.
    #[error("required storage path is not writable")]
    Write {
        path: RuntimeStoragePath,
        #[source]
        source: std::io::Error,
    },
    /// A successful probe could not be removed, so retaining it would leave
    /// unwanted state behind.
    #[error("could not remove storage write probe")]
    Cleanup {
        path: RuntimeStoragePath,
        #[source]
        source: std::io::Error,
    },
}

impl RuntimeStorageError {
    /// Returns the fixed in-container path that failed, never a host mount path.
    #[must_use]
    pub const fn path(&self) -> RuntimeStoragePath {
        match self {
            Self::NotDirectory { path }
            | Self::Symlink { path }
            | Self::Create { path, .. }
            | Self::Write { path, .. }
            | Self::Cleanup { path, .. } => *path,
        }
    }

    /// Returns the bounded operation name suitable for a log field.
    #[must_use]
    pub const fn operation(&self) -> &'static str {
        match self {
            Self::NotDirectory { .. } => "not_directory",
            Self::Symlink { .. } => "symlink",
            Self::Create { .. } => "create",
            Self::Write { .. } => "write_probe",
            Self::Cleanup { .. } => "cleanup_probe",
        }
    }

    /// Returns a bounded I/O classification when the failure came from the
    /// filesystem. It intentionally omits operating-system error text.
    #[must_use]
    pub fn io_kind(&self) -> Option<&'static str> {
        let source = match self {
            Self::Create { source, .. }
            | Self::Write { source, .. }
            | Self::Cleanup { source, .. } => source,
            Self::NotDirectory { .. } | Self::Symlink { .. } => return None,
        };
        Some(match source.kind() {
            std::io::ErrorKind::PermissionDenied => "permission_denied",
            std::io::ErrorKind::ReadOnlyFilesystem => "read_only_filesystem",
            std::io::ErrorKind::StorageFull => "storage_full",
            std::io::ErrorKind::QuotaExceeded => "quota_exceeded",
            std::io::ErrorKind::NotFound => "not_found",
            std::io::ErrorKind::AlreadyExists => "already_exists",
            _ => "io_error",
        })
    }
}

const WORKER_RUNTIME_PATHS: &[RuntimeStoragePath] = &[
    RuntimeStoragePath::ConfigWork,
    RuntimeStoragePath::ConfigRaw,
    RuntimeStoragePath::ConfigLogs,
];
const MEDIA_RUNTIME_PATHS: &[RuntimeStoragePath] = &[
    RuntimeStoragePath::ConfigMedia,
    RuntimeStoragePath::ConfigLogs,
    RuntimeStoragePath::MediaMovies,
    RuntimeStoragePath::MediaTv,
    RuntimeStoragePath::MediaPeople,
    RuntimeStoragePath::MediaNetworks,
    RuntimeStoragePath::MediaCompanies,
    RuntimeStoragePath::MediaCollections,
];
static WRITE_PROBE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Creates and verifies the fixed writable paths for a worker role.
///
/// This uses only the fixed container roots and is run after the entrypoint has
/// dropped to the unprivileged service identity. It therefore proves that the
/// actual service process—not only the root startup helper—can use every path.
///
/// # Errors
///
/// Returns a bounded error naming the failing in-container path and operation.
pub fn prepare_runtime_storage(role: RuntimeStorageRole) -> Result<(), RuntimeStorageError> {
    prepare_runtime_storage_at(role, Path::new(CONFIG_ROOT), Path::new(MEDIA_ROOT))
}

/// Creates and verifies runtime storage at supplied roots for isolated tests.
/// Production callers should use [`prepare_runtime_storage`].
///
/// # Errors
///
/// Returns a bounded error naming the corresponding fixed in-container path.
pub fn prepare_runtime_storage_at(
    role: RuntimeStorageRole,
    config_root: &Path,
    media_root: &Path,
) -> Result<(), RuntimeStorageError> {
    let paths = match role {
        RuntimeStorageRole::Worker => WORKER_RUNTIME_PATHS,
        RuntimeStorageRole::Media => MEDIA_RUNTIME_PATHS,
    };
    for storage_path in paths {
        let path = storage_path.resolve(config_root, media_root);
        prepare_directory(*storage_path, &path, role)?;
    }
    Ok(())
}

fn prepare_directory(
    storage_path: RuntimeStoragePath,
    path: &Path,
    role: RuntimeStorageRole,
) -> Result<(), RuntimeStorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(RuntimeStorageError::Symlink { path: storage_path });
        }
        Ok(metadata) if !metadata.is_dir() => {
            return Err(RuntimeStorageError::NotDirectory { path: storage_path });
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => fs::create_dir_all(path)
            .map_err(|source| RuntimeStorageError::Create {
                path: storage_path,
                source,
            })?,
        Err(source) => {
            return Err(RuntimeStorageError::Create {
                path: storage_path,
                source,
            });
        }
    }

    // The timestamp prevents a stale probe from a hard-killed previous
    // container (whose PID may be reused) from blocking a healthy restart.
    let timestamp_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let probe_number = WRITE_PROBE_COUNTER.fetch_add(1, Ordering::Relaxed);
    let probe = path.join(format!(
        ".tmdb-{}-write-probe-{}-{timestamp_nanos}-{probe_number}",
        role.as_str(),
        std::process::id()
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
        .map_err(|source| RuntimeStorageError::Write {
            path: storage_path,
            source,
        })?;
    file.write_all(b"tmdb-runtime-probe")
        .and_then(|()| file.sync_all())
        .map_err(|source| RuntimeStorageError::Write {
            path: storage_path,
            source,
        })?;
    drop(file);
    fs::remove_file(probe).map_err(|source| RuntimeStorageError::Cleanup {
        path: storage_path,
        source,
    })
}

/// A title's catalog scope determines its public directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TitleScope {
    /// A regular movie.
    Movie,
    /// A regular TV show.
    Tv,
}

/// Reusable catalog entity whose image is not copied per title.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReusableEntity {
    /// A cast/crew person.
    Person,
    /// A production or broadcast network.
    Network,
    /// A production company.
    Company,
    /// A TMDB collection.
    Collection,
}

/// Deterministic public image variant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssetVariant {
    /// A title or collection poster. Index one is `poster`, later indexes are padded.
    Poster { index: u16 },
    /// A title or collection backdrop. Numbering always starts at one.
    Backdrop { index: u16 },
    /// A title or reusable entity logo.  Index one is `logo`, later indexes are padded.
    Logo { index: u16 },
    /// A reusable person profile image.  Index one is `profile`.
    Profile { index: u16 },
    /// A TV season poster.
    SeasonPoster { season: u16, index: u16 },
    /// An episode thumbnail, with season zero represented as specials.
    EpisodeThumbnail {
        season: u16,
        episode: u16,
        index: u16,
    },
}

/// Output encoding for a public derivative.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ImageFormat {
    /// Baseline JPEG derivative.
    Jpeg,
    /// Source WebP, retained only when TMDB supplies WebP bytes.
    Webp,
    /// PNG source or optimized logo.
    Png,
    /// GIF source. No GIF derivative is generated.
    Gif,
}

impl ImageFormat {
    /// Returns the stable filename extension.
    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::Jpeg => "jpg",
            Self::Webp => "webp",
            Self::Png => "png",
            Self::Gif => "gif",
        }
    }
}

/// A safe media-layout error.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum MediaPathError {
    /// The catalog identifier was not a positive integer.
    #[error("media identifier must be positive")]
    InvalidId,
    /// A variant index or season/episode number was invalid.
    #[error("media variant number is invalid")]
    InvalidNumber,
    /// The supplied digest was not a lowercase SHA-256 hex string.
    #[error("media digest is invalid")]
    InvalidDigest,
}

/// Returns the public title directory below [`MEDIA_ROOT`].
///
/// # Errors
///
/// Returns [`MediaPathError::InvalidId`] for a non-positive TMDB identifier.
pub fn title_dir(scope: TitleScope, tmdb_id: i64) -> Result<PathBuf, MediaPathError> {
    let id = positive_id(tmdb_id)?;
    let path = match scope {
        TitleScope::Movie => format!("movies/{id}"),
        TitleScope::Tv => format!("tv/{id}"),
    };
    Ok(PathBuf::from(path))
}

/// Returns the public title derivative path below [`MEDIA_ROOT`].
///
/// # Errors
///
/// Returns [`MediaPathError`] when the identifier or variant number is invalid.
pub fn title_asset(
    scope: TitleScope,
    tmdb_id: i64,
    variant: AssetVariant,
    format: ImageFormat,
) -> Result<PathBuf, MediaPathError> {
    if matches!(variant, AssetVariant::EpisodeThumbnail { .. }) {
        return optimized_title_asset(scope, tmdb_id, variant, format, 640);
    }
    let mut path = title_dir(scope, tmdb_id)?;
    path.push(title_subdirectory(variant));
    path.push(variant_filename(variant, format)?);
    Ok(path)
}

/// Returns an optimized title path below `optimized/`.
///
/// # Errors
///
/// Returns [`MediaPathError`] when the identifier, variant number, or width is invalid.
pub fn optimized_title_asset(
    scope: TitleScope,
    tmdb_id: i64,
    variant: AssetVariant,
    format: ImageFormat,
    width: u32,
) -> Result<PathBuf, MediaPathError> {
    let mut path = title_dir(scope, tmdb_id)?;
    path.push("optimized");
    path.push(optimized_subdirectory(variant));
    path.push(optimized_filename(variant, format, width)?);
    Ok(path)
}

/// Returns a stable path for a reusable person, network, company, or collection asset.
///
/// # Errors
///
/// Returns [`MediaPathError`] when the TMDB identifier or variant number is invalid.
pub fn reusable_asset(
    entity: ReusableEntity,
    tmdb_id: i64,
    variant: AssetVariant,
    format: ImageFormat,
) -> Result<PathBuf, MediaPathError> {
    let id = positive_id(tmdb_id)?;
    let directory = match entity {
        ReusableEntity::Person => "people",
        ReusableEntity::Network => "networks",
        ReusableEntity::Company => "companies",
        ReusableEntity::Collection => "collections",
    };
    let mut path = PathBuf::from(directory).join(id.to_string());
    let subdirectory = reusable_subdirectory(entity, variant);
    if !subdirectory.is_empty() {
        path.push(subdirectory);
    }
    path.push(reusable_filename(entity, variant, format)?);
    Ok(path)
}

/// Returns an optimized path for a reusable entity.
///
/// # Errors
///
/// Returns [`MediaPathError`] when the TMDB identifier, variant number, or width is invalid.
pub fn optimized_reusable_asset(
    entity: ReusableEntity,
    tmdb_id: i64,
    variant: AssetVariant,
    format: ImageFormat,
    width: u32,
) -> Result<PathBuf, MediaPathError> {
    let id = positive_id(tmdb_id)?;
    let directory = match entity {
        ReusableEntity::Person => "people",
        ReusableEntity::Network => "networks",
        ReusableEntity::Company => "companies",
        ReusableEntity::Collection => "collections",
    };
    let mut path = PathBuf::from(directory)
        .join(id.to_string())
        .join("optimized");
    let subdirectory = match (entity, variant) {
        (ReusableEntity::Person, AssetVariant::Profile { .. }) => "",
        _ => optimized_subdirectory(variant),
    };
    if !subdirectory.is_empty() {
        path.push(subdirectory);
    }
    path.push(optimized_filename(variant, format, width)?);
    Ok(path)
}

/// Returns whether a relative path is safe to expose through the embedded
/// media server. Hidden and traversal paths are never public.
#[must_use]
pub fn is_public_relative(path: &str) -> bool {
    let candidate = std::path::Path::new(path);
    !path.is_empty()
        && !path.starts_with('.')
        && !candidate.is_absolute()
        && candidate.components().all(|component| match component {
            std::path::Component::Normal(value) => !value.to_string_lossy().starts_with('.'),
            _ => false,
        })
}

fn positive_id(id: i64) -> Result<i64, MediaPathError> {
    (id > 0).then_some(id).ok_or(MediaPathError::InvalidId)
}

fn positive_number(number: u16) -> Result<u16, MediaPathError> {
    (number > 0)
        .then_some(number)
        .ok_or(MediaPathError::InvalidNumber)
}

fn numbered_name(prefix: &str, index: u16, extension: &str) -> Result<String, MediaPathError> {
    let index = positive_number(index)?;
    if index == 1 {
        Ok(format!("{prefix}.{extension}"))
    } else {
        Ok(format!("{prefix}-{index:02}.{extension}"))
    }
}

fn title_subdirectory(variant: AssetVariant) -> &'static str {
    match variant {
        AssetVariant::Poster { .. } | AssetVariant::SeasonPoster { .. } => "posters",
        AssetVariant::Backdrop { .. } => "backdrops",
        AssetVariant::Logo { .. } => "logos",
        AssetVariant::Profile { .. } => "profiles",
        AssetVariant::EpisodeThumbnail { .. } => "optimized",
    }
}

fn optimized_subdirectory(variant: AssetVariant) -> &'static str {
    match variant {
        AssetVariant::Poster { .. } | AssetVariant::SeasonPoster { .. } => "posters",
        AssetVariant::Backdrop { .. } => "backdrops",
        AssetVariant::Logo { .. } => "logos",
        AssetVariant::Profile { .. } => "profiles",
        AssetVariant::EpisodeThumbnail { .. } => "thumbnails",
    }
}

fn variant_filename(variant: AssetVariant, format: ImageFormat) -> Result<String, MediaPathError> {
    let extension = format.extension();
    match variant {
        AssetVariant::Poster { index } => numbered_name("poster", index, extension),
        AssetVariant::Backdrop { index } => backdrop_name(index, extension),
        AssetVariant::Logo { index } => numbered_name("logo", index, extension),
        AssetVariant::Profile { index } => numbered_name("profile", index, extension),
        AssetVariant::SeasonPoster { season, index } => {
            let base = if season == 0 {
                "season-specials-poster".to_owned()
            } else {
                format!("season{season:02}-poster")
            };
            numbered_name(&base, index, extension)
        }
        AssetVariant::EpisodeThumbnail {
            season,
            episode,
            index,
        } => {
            let episode = positive_number(episode)?;
            let base = if season == 0 {
                "season-specials".to_owned()
            } else {
                format!("season{season:02}")
            };
            numbered_name(
                &format!("{base}-episode{episode:02}-thumbnails"),
                index,
                extension,
            )
        }
    }
}

fn reusable_subdirectory(entity: ReusableEntity, variant: AssetVariant) -> &'static str {
    match (entity, variant) {
        (ReusableEntity::Collection, AssetVariant::Poster { .. }) => "posters",
        (ReusableEntity::Collection, AssetVariant::Backdrop { .. }) => "backdrops",
        (_, AssetVariant::Logo { .. }) => "logos",
        _ => "",
    }
}

fn backdrop_name(index: u16, extension: &str) -> Result<String, MediaPathError> {
    let index = positive_number(index)?;
    Ok(format!("backdrop-{index:02}.{extension}"))
}

fn reusable_filename(
    entity: ReusableEntity,
    variant: AssetVariant,
    format: ImageFormat,
) -> Result<String, MediaPathError> {
    match (entity, variant) {
        (ReusableEntity::Person, AssetVariant::Profile { index }) => {
            numbered_name("profile", index, format.extension())
        }
        _ => variant_filename(variant, format),
    }
}

fn optimized_filename(
    variant: AssetVariant,
    format: ImageFormat,
    width: u32,
) -> Result<String, MediaPathError> {
    if width == 0 {
        return Err(MediaPathError::InvalidNumber);
    }
    let original = variant_filename(variant, format)?;
    let stem = Path::new(&original)
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or(MediaPathError::InvalidNumber)?;
    Ok(format!("{stem}-w{width}.{}", format.extension()))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn fixed_root_contract_is_container_only() {
        assert_eq!(MEDIA_ROOT, "/media");
        assert_eq!(CONFIG_ROOT, "/config");
        assert_eq!(MEDIA_WORK_ROOT, "/config/media");
        assert_eq!(RAW_ROOT, "/config/raw");
    }

    #[test]
    fn worker_storage_preflight_creates_only_worker_config_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let sandbox = tempdir()?;
        let config = sandbox.path().join("config");
        let media = sandbox.path().join("media");
        fs::create_dir(&config)?;

        prepare_runtime_storage_at(RuntimeStorageRole::Worker, &config, &media)?;

        for child in ["work", "raw", "logs"] {
            assert!(config.join(child).is_dir(), "missing {child}");
        }
        assert!(
            !config.join("backups").exists(),
            "the main worker must not prepare PostgreSQL's backup parent"
        );
        assert!(!media.exists());
        Ok(())
    }

    #[test]
    fn media_storage_preflight_creates_fixed_scratch_and_public_paths()
    -> Result<(), Box<dyn std::error::Error>> {
        let sandbox = tempdir()?;
        let config = sandbox.path().join("config");
        let media = sandbox.path().join("media");
        fs::create_dir(&config)?;
        fs::create_dir(&media)?;

        prepare_runtime_storage_at(RuntimeStorageRole::Media, &config, &media)?;

        for child in ["media", "logs"] {
            assert!(config.join(child).is_dir(), "missing /config/{child}");
        }
        for child in [
            "movies",
            "tv",
            "people",
            "networks",
            "companies",
            "collections",
        ] {
            assert!(media.join(child).is_dir(), "missing /media/{child}");
        }
        Ok(())
    }

    #[test]
    fn storage_preflight_reports_a_file_that_blocks_a_required_directory()
    -> Result<(), Box<dyn std::error::Error>> {
        let sandbox = tempdir()?;
        let config = sandbox.path().join("config");
        let media = sandbox.path().join("media");
        fs::create_dir(&config)?;
        fs::write(config.join("media"), b"not a directory")?;
        fs::create_dir(&media)?;

        let Err(error) = prepare_runtime_storage_at(RuntimeStorageRole::Media, &config, &media)
        else {
            return Err(std::io::Error::other(
                "a regular file must not satisfy the media scratch directory",
            )
            .into());
        };
        assert_eq!(error.path(), RuntimeStoragePath::ConfigMedia,);
        assert_eq!(error.operation(), "not_directory");
        Ok(())
    }

    #[test]
    fn title_paths_cover_movie_tv_and_specials() {
        assert_eq!(
            title_asset(
                TitleScope::Movie,
                11,
                AssetVariant::Poster { index: 1 },
                ImageFormat::Jpeg
            )
            .ok(),
            Some(PathBuf::from("movies/11/posters/poster.jpg"))
        );
        assert_eq!(
            optimized_title_asset(
                TitleScope::Tv,
                12,
                AssetVariant::EpisodeThumbnail {
                    season: 0,
                    episode: 1,
                    index: 1
                },
                ImageFormat::Jpeg,
                640
            )
            .ok(),
            Some(PathBuf::from(
                "tv/12/optimized/thumbnails/season-specials-episode01-thumbnails-w640.jpg",
            ))
        );
        assert_eq!(
            title_asset(
                TitleScope::Tv,
                12,
                AssetVariant::EpisodeThumbnail {
                    season: 0,
                    episode: 1,
                    index: 1
                },
                ImageFormat::Jpeg
            )
            .ok(),
            Some(PathBuf::from(
                "tv/12/optimized/thumbnails/season-specials-episode01-thumbnails-w640.jpg",
            ))
        );
        assert_eq!(
            title_asset(
                TitleScope::Tv,
                13,
                AssetVariant::SeasonPoster {
                    season: 1,
                    index: 2
                },
                ImageFormat::Jpeg
            )
            .ok(),
            Some(PathBuf::from("tv/13/posters/season01-poster-02.jpg"))
        );
    }

    #[test]
    fn reusable_paths_are_stable_and_numbered() {
        assert_eq!(
            reusable_asset(
                ReusableEntity::Person,
                44,
                AssetVariant::Profile { index: 1 },
                ImageFormat::Jpeg
            )
            .ok(),
            Some(PathBuf::from("people/44/profile.jpg"))
        );
        assert_eq!(
            reusable_asset(
                ReusableEntity::Network,
                55,
                AssetVariant::Logo { index: 2 },
                ImageFormat::Png
            )
            .ok(),
            Some(PathBuf::from("networks/55/logos/logo-02.png"))
        );
        assert_eq!(
            optimized_reusable_asset(
                ReusableEntity::Person,
                44,
                AssetVariant::Profile { index: 1 },
                ImageFormat::Jpeg,
                640
            )
            .ok(),
            Some(PathBuf::from("people/44/optimized/profile-w640.jpg"))
        );
    }

    #[test]
    fn optimized_paths_are_public_and_dot_paths_are_private() {
        assert!(!is_public_relative(".private/original"));
        assert!(is_public_relative("movies/1/posters/poster.jpg"));
        assert!(!is_public_relative("../movies/1/posters/poster.jpg"));
        assert!(is_public_relative("tv/1/optimized/posters/poster-w640.jpg"));
    }

    #[test]
    fn invalid_numbers_are_rejected() {
        assert!(title_dir(TitleScope::Movie, 0).is_err());
        assert!(
            title_asset(
                TitleScope::Tv,
                1,
                AssetVariant::EpisodeThumbnail {
                    season: 1,
                    episode: 0,
                    index: 1
                },
                ImageFormat::Jpeg
            )
            .is_err()
        );
    }
}
