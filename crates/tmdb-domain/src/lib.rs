//! Domain types and invariants for the TMDB mirror.

mod anime;
mod media;

pub use anime::{ANIMATION_GENRE_ID, ANIME_KEYWORD_ID, classify_anime};
pub use media::{MediaType, ParseMediaTypeError, TitleKey};
