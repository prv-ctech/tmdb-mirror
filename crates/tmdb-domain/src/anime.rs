use std::{collections::BTreeSet, fmt};

/// The exact TMDB keyword that identifies anime titles.
pub const ANIME_KEYWORD_ID: u32 = 210_024;
/// The version of the deterministic anime-classification rule.
pub const ANIME_RULE_VERSION: &str = "anime-keyword-210024-v1";

/// The origin of an anime-classification decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnimeSource {
    AdministratorOverride,
    TmdbKeyword,
    NoMatch,
}

/// An administrator's explicit anime-classification decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnimeOverride {
    is_anime: bool,
    reason: String,
}

impl AnimeOverride {
    /// Creates an override with a non-empty, normalized explanation.
    ///
    /// # Errors
    ///
    /// Returns [`AnimeOverrideError::EmptyReason`] when the trimmed reason is empty.
    pub fn try_new(is_anime: bool, reason: impl Into<String>) -> Result<Self, AnimeOverrideError> {
        let reason = reason.into();
        let reason = reason.trim();
        if reason.is_empty() {
            return Err(AnimeOverrideError::EmptyReason);
        }

        Ok(Self {
            is_anime,
            reason: reason.to_owned(),
        })
    }
}

/// The error returned when an administrator override violates its invariants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnimeOverrideError {
    EmptyReason,
}

impl fmt::Display for AnimeOverrideError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyReason => formatter.write_str("anime override reason must not be empty"),
        }
    }
}

impl std::error::Error for AnimeOverrideError {}

/// A deterministic anime-classification result and its evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnimeDecision {
    pub is_anime: bool,
    pub source: AnimeSource,
    pub rule_version: &'static str,
    pub evidence_keyword_ids: BTreeSet<u32>,
    pub reason: Option<String>,
}

/// Classifies a title using the TMDB anime keyword unless an administrator overrides it.
#[must_use]
pub fn classify_anime(
    keyword_ids: &BTreeSet<u32>,
    administrator_override: Option<AnimeOverride>,
) -> AnimeDecision {
    let evidence_keyword_ids = keyword_ids
        .contains(&ANIME_KEYWORD_ID)
        .then_some(ANIME_KEYWORD_ID)
        .into_iter()
        .collect();

    if let Some(administrator_override) = administrator_override {
        return AnimeDecision {
            is_anime: administrator_override.is_anime,
            source: AnimeSource::AdministratorOverride,
            rule_version: ANIME_RULE_VERSION,
            evidence_keyword_ids,
            reason: Some(administrator_override.reason),
        };
    }

    if evidence_keyword_ids.is_empty() {
        AnimeDecision {
            is_anime: false,
            source: AnimeSource::NoMatch,
            rule_version: ANIME_RULE_VERSION,
            evidence_keyword_ids,
            reason: None,
        }
    } else {
        AnimeDecision {
            is_anime: true,
            source: AnimeSource::TmdbKeyword,
            rule_version: ANIME_RULE_VERSION,
            evidence_keyword_ids,
            reason: None,
        }
    }
}
