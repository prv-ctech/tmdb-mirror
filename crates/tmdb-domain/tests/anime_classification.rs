use std::{collections::BTreeSet, num::NonZeroU32};

use tmdb_domain::{
    ANIME_KEYWORD_ID, ANIME_RULE_VERSION, AnimeOverride, AnimeSource, MediaType, TitleKey,
    classify_anime,
};

#[test]
fn keyword_classifies_movie_and_tv_as_anime() -> Result<(), Box<dyn std::error::Error>> {
    let tmdb_id = NonZeroU32::new(1).ok_or("fixture ID must be non-zero")?;
    for media_type in [MediaType::Movie, MediaType::Tv] {
        let key = TitleKey::new(media_type, tmdb_id);
        let decision = classify_anime(&BTreeSet::from([ANIME_KEYWORD_ID]), None);
        assert!(decision.is_anime);
        assert_eq!(decision.source, AnimeSource::TmdbKeyword);
        assert_eq!(decision.rule_version, ANIME_RULE_VERSION);
        assert_eq!(
            decision.evidence_keyword_ids,
            BTreeSet::from([ANIME_KEYWORD_ID])
        );
        assert_eq!(decision.reason, None);
        assert_eq!(key.tmdb_id().get(), 1);
    }
    Ok(())
}

#[test]
fn administrator_override_has_precedence_and_keeps_reason() -> Result<(), Box<dyn std::error::Error>>
{
    let decision = classify_anime(
        &BTreeSet::from([ANIME_KEYWORD_ID]),
        Some(AnimeOverride::try_new(false, "  live action  ")?),
    );
    assert!(!decision.is_anime);
    assert_eq!(decision.source, AnimeSource::AdministratorOverride);
    assert_eq!(decision.reason.as_deref(), Some("live action"));
    assert_eq!(
        decision.evidence_keyword_ids,
        BTreeSet::from([ANIME_KEYWORD_ID])
    );
    Ok(())
}

#[test]
fn empty_override_reason_is_rejected() {
    assert!(AnimeOverride::try_new(true, "   ").is_err());
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

#[test]
fn missing_anime_keyword_produces_a_no_match_decision() {
    let decision = classify_anime(&BTreeSet::from([1, 2]), None);

    assert!(!decision.is_anime);
    assert_eq!(decision.source, AnimeSource::NoMatch);
    assert_eq!(decision.rule_version, ANIME_RULE_VERSION);
    assert!(decision.evidence_keyword_ids.is_empty());
    assert_eq!(decision.reason, None);
}
