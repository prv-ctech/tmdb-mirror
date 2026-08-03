/// The exact TMDB keyword that identifies anime titles.
pub const ANIME_KEYWORD_ID: u64 = 210_024;
/// The TMDB genre that identifies animated titles.
pub const ANIMATION_GENRE_ID: u64 = 16;

/// Classifies a title as anime only when TMDB supplies both required signals.
#[must_use]
pub fn classify_anime(
    keyword_ids: impl IntoIterator<Item = u64>,
    genre_ids: impl IntoIterator<Item = u64>,
) -> bool {
    let has_anime_keyword = keyword_ids.into_iter().any(|id| id == ANIME_KEYWORD_ID);
    let has_animation_genre = genre_ids.into_iter().any(|id| id == ANIMATION_GENRE_ID);
    has_anime_keyword && has_animation_genre
}
