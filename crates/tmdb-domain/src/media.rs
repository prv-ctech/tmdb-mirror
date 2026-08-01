use std::{fmt, num::NonZeroU32, str::FromStr};

/// The TMDB media namespace for a title.
#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MediaType {
    Movie,
    Tv,
}

impl fmt::Display for MediaType {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Movie => "movie",
            Self::Tv => "tv",
        };
        formatter.write_str(value)
    }
}

/// The error returned when a media type is not one of TMDB's supported namespaces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParseMediaTypeError;

impl fmt::Display for ParseMediaTypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("media type must be `movie` or `tv`")
    }
}

impl std::error::Error for ParseMediaTypeError {}

impl FromStr for MediaType {
    type Err = ParseMediaTypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "movie" => Ok(Self::Movie),
            "tv" => Ok(Self::Tv),
            _ => Err(ParseMediaTypeError),
        }
    }
}

/// A globally distinct TMDB title identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TitleKey {
    media_type: MediaType,
    tmdb_id: NonZeroU32,
}

impl TitleKey {
    #[must_use]
    pub const fn new(media_type: MediaType, tmdb_id: NonZeroU32) -> Self {
        Self {
            media_type,
            tmdb_id,
        }
    }

    #[must_use]
    pub const fn media_type(&self) -> MediaType {
        self.media_type
    }

    #[must_use]
    pub const fn tmdb_id(&self) -> NonZeroU32 {
        self.tmdb_id
    }
}
