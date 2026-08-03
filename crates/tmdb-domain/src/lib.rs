//! Domain types and invariants for the TMDB mirror.

mod anime;
mod media;

pub use anime::{
    ANIME_KEYWORD_ID, ANIME_RULE_VERSION, AnimeDecision, AnimeOverride, AnimeOverrideError,
    AnimeSource, classify_anime,
};
pub use media::{MediaType, ParseMediaTypeError, TitleKey};
