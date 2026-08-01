//! Fixed-container-path media layout shared by the API and media worker.
//!
//! Host paths deliberately do not appear in this crate.  Deployments mount
//! their chosen host directories at [`MEDIA_ROOT`] and [`CONFIG_ROOT`].

use std::path::PathBuf;

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

/// A title's catalog scope determines its public directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TitleScope {
    /// A regular movie.
    Movie,
    /// A regular TV show.
    Tv,
    /// A movie explicitly classified as anime.
    AnimeMovie,
    /// A TV show explicitly classified as anime.
    AnimeTv,
}

/// Reusable catalog entity whose image is not copied per title.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReusableEntity {
    /// A cast/crew person.
    Cast,
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
    /// A title poster/cover.  Index one is `cover`, later indexes are padded.
    Cover { index: u16 },
    /// A title backdrop/banner.  Index one is `banner`, later indexes are padded.
    Banner { index: u16 },
    /// A title or reusable entity logo.  Index one is `logo`, later indexes are padded.
    Logo { index: u16 },
    /// A reusable person profile image.  Index one is `profile`.
    Profile { index: u16 },
    /// A TV season still.
    Season { season: u16, index: u16 },
    /// An episode still, with season zero represented as specials.
    Episode {
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
    /// WebP derivative for clients that negotiate it.
    Webp,
    /// Source-preserving PNG fallback used before a derivative is available.
    Png,
    /// Source-preserving GIF fallback used before a derivative is available.
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
        TitleScope::AnimeMovie => format!("anime/movie/{id}"),
        TitleScope::AnimeTv => format!("anime/tv/{id}"),
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
    let mut path = title_dir(scope, tmdb_id)?;
    path.push(variant_filename(variant, format)?);
    Ok(path)
}

/// Returns a stable path for a reusable cast, network, company, or collection asset.
///
/// # Errors
///
/// Returns [`MediaPathError`] when the local identifier or variant number is invalid.
pub fn reusable_asset(
    entity: ReusableEntity,
    local_id: i64,
    variant: AssetVariant,
    format: ImageFormat,
) -> Result<PathBuf, MediaPathError> {
    let id = positive_id(local_id)?;
    let directory = match entity {
        ReusableEntity::Cast => "casting",
        ReusableEntity::Network => "networks",
        ReusableEntity::Company => "companies",
        ReusableEntity::Collection => "collections",
    };
    let filename = match entity {
        ReusableEntity::Cast => profile_filename(variant, format)?,
        ReusableEntity::Network | ReusableEntity::Company | ReusableEntity::Collection => {
            logo_or_cover_filename(entity, variant, format)?
        }
    };
    Ok(PathBuf::from(directory).join(id.to_string()).join(filename))
}

/// Returns a private, content-addressed original-master path below [`MEDIA_ROOT`].
///
/// # Errors
///
/// Returns [`MediaPathError::InvalidDigest`] when `sha256` is not 64 hex characters.
pub fn master_path(sha256: &str) -> Result<PathBuf, MediaPathError> {
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(MediaPathError::InvalidDigest);
    }
    let digest = sha256.to_ascii_lowercase();
    Ok(PathBuf::from(".masters")
        .join("sha256")
        .join(&digest[..2])
        .join(&digest[2..4])
        .join(digest))
}

/// Returns whether a relative public path is safe to expose through the
/// embedded media server.  Private original masters are intentionally hidden.
#[must_use]
pub fn is_public_relative(path: &str) -> bool {
    let candidate = std::path::Path::new(path);
    !path.is_empty()
        && !path.starts_with('.')
        && !candidate.is_absolute()
        && candidate
            .components()
            .all(|component| matches!(component, std::path::Component::Normal(_)))
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

fn variant_filename(variant: AssetVariant, format: ImageFormat) -> Result<String, MediaPathError> {
    let extension = format.extension();
    match variant {
        AssetVariant::Cover { index } => numbered_name("cover", index, extension),
        AssetVariant::Banner { index } => numbered_name("banner", index, extension),
        AssetVariant::Logo { index } => numbered_name("logo", index, extension),
        AssetVariant::Profile { index } => numbered_name("profile", index, extension),
        AssetVariant::Season { season, index } => {
            let season = positive_number(season)?;
            let base = format!("season{season}");
            numbered_name(&base, index, extension)
        }
        AssetVariant::Episode {
            season,
            episode,
            index,
        } => {
            let episode = positive_number(episode)?;
            let base = if season == 0 {
                "specials".to_owned()
            } else {
                format!("season{season}")
            };
            let base = format!("{base}-episode{episode}");
            numbered_name(&base, index, extension)
        }
    }
}

fn profile_filename(variant: AssetVariant, format: ImageFormat) -> Result<String, MediaPathError> {
    match variant {
        AssetVariant::Profile { index } => numbered_name("profile", index, format.extension()),
        other => variant_filename(other, format),
    }
}

fn logo_or_cover_filename(
    entity: ReusableEntity,
    variant: AssetVariant,
    format: ImageFormat,
) -> Result<String, MediaPathError> {
    match (entity, variant) {
        (ReusableEntity::Collection, AssetVariant::Cover { index }) => {
            numbered_name("cover", index, format.extension())
        }
        (_, AssetVariant::Logo { index }) => numbered_name("logo", index, format.extension()),
        (_, AssetVariant::Cover { index }) => numbered_name("cover", index, format.extension()),
        _ => variant_filename(variant, format),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_root_contract_is_container_only() {
        assert_eq!(MEDIA_ROOT, "/media");
        assert_eq!(CONFIG_ROOT, "/config");
        assert_eq!(MEDIA_WORK_ROOT, "/config/media");
        assert_eq!(RAW_ROOT, "/config/raw");
    }

    #[test]
    fn title_paths_cover_regular_and_anime_scopes() {
        assert_eq!(
            title_asset(
                TitleScope::Movie,
                11,
                AssetVariant::Cover { index: 1 },
                ImageFormat::Jpeg
            )
            .ok(),
            Some(PathBuf::from("movies/11/cover.jpg"))
        );
        assert_eq!(
            title_asset(
                TitleScope::AnimeTv,
                12,
                AssetVariant::Episode {
                    season: 0,
                    episode: 1,
                    index: 1
                },
                ImageFormat::Webp
            )
            .ok(),
            Some(PathBuf::from("anime/tv/12/specials-episode1.webp"))
        );
        assert_eq!(
            title_asset(
                TitleScope::Tv,
                13,
                AssetVariant::Season {
                    season: 1,
                    index: 2
                },
                ImageFormat::Jpeg
            )
            .ok(),
            Some(PathBuf::from("tv/13/season1-02.jpg"))
        );
    }

    #[test]
    fn reusable_paths_are_stable_and_numbered() {
        assert_eq!(
            reusable_asset(
                ReusableEntity::Cast,
                44,
                AssetVariant::Profile { index: 1 },
                ImageFormat::Jpeg
            )
            .ok(),
            Some(PathBuf::from("casting/44/profile.jpg"))
        );
        assert_eq!(
            reusable_asset(
                ReusableEntity::Network,
                55,
                AssetVariant::Logo { index: 2 },
                ImageFormat::Webp
            )
            .ok(),
            Some(PathBuf::from("networks/55/logo-02.webp"))
        );
    }

    #[test]
    fn masters_are_private_and_digests_are_validated() {
        let digest = "ab".repeat(32);
        assert_eq!(
            master_path(&digest).ok(),
            Some(PathBuf::from(format!(".masters/sha256/ab/ab/{digest}")))
        );
        assert!(!is_public_relative(".masters/sha256/ab/ab/file"));
        assert!(is_public_relative("movies/1/cover.jpg"));
        assert!(!is_public_relative("../movies/1/cover.jpg"));
        assert!(master_path("not-a-digest").is_err());
    }

    #[test]
    fn invalid_numbers_are_rejected() {
        assert!(title_dir(TitleScope::Movie, 0).is_err());
        assert!(
            title_asset(
                TitleScope::Tv,
                1,
                AssetVariant::Episode {
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
