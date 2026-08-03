use std::num::NonZeroU32;

use tmdb_domain::{ANIMATION_GENRE_ID, ANIME_KEYWORD_ID, MediaType, TitleKey, classify_anime};

#[test]
fn anime_requires_both_tmdb_signals() {
    let cases = [
        (
            [ANIME_KEYWORD_ID].as_slice(),
            [ANIMATION_GENRE_ID].as_slice(),
            true,
        ),
        ([ANIME_KEYWORD_ID].as_slice(), [].as_slice(), false),
        ([].as_slice(), [ANIMATION_GENRE_ID].as_slice(), false),
        ([].as_slice(), [].as_slice(), false),
    ];

    for (keyword_ids, genre_ids, expected) in cases {
        assert_eq!(
            classify_anime(keyword_ids.iter().copied(), genre_ids.iter().copied()),
            expected
        );
    }
}

#[test]
fn unrelated_ids_do_not_match_by_name_or_position() {
    assert!(!classify_anime([42, 210_025], [15, 17]));
}

#[test]
fn media_type_uses_exact_tmdb_wire_values() {
    assert_eq!(MediaType::Movie.to_string(), "movie");
    assert_eq!(MediaType::Tv.to_string(), "tv");
    assert_eq!("movie".parse(), Ok(MediaType::Movie));
    assert_eq!("tv".parse(), Ok(MediaType::Tv));

    for value in ["Movie", "TV", " tv", "tv ", "series"] {
        assert!(
            value.parse::<MediaType>().is_err(),
            "{value} must be rejected"
        );
    }
}

#[test]
fn title_key_keeps_media_namespaces_distinct() -> Result<(), Box<dyn std::error::Error>> {
    let tmdb_id = NonZeroU32::new(7).ok_or("fixture ID must be non-zero")?;
    let movie = TitleKey::new(MediaType::Movie, tmdb_id);
    let tv = TitleKey::new(MediaType::Tv, tmdb_id);

    assert_ne!(movie, tv);
    assert_eq!(movie.media_type(), MediaType::Movie);
    assert_eq!(tv.media_type(), MediaType::Tv);
    assert_eq!(movie.tmdb_id(), tmdb_id);
    assert_eq!(tv.tmdb_id(), tmdb_id);
    Ok(())
}
