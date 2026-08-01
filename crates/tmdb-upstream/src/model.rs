use serde::{Deserialize, Serialize};

/// A row in one of TMDB's newline-delimited daily ID exports.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DailyExportRecord {
    /// The globally stable TMDB identifier in the selected export namespace.
    pub id: u64,
    /// Whether TMDB marks the entity as adult content.
    #[serde(default)]
    pub adult: bool,
    /// Whether TMDB marks the entity as a video.
    #[serde(default)]
    pub video: bool,
    /// TMDB's current popularity value, when supplied by the export.
    #[serde(default)]
    pub popularity: Option<f64>,
}

/// A movie response returned by the TMDB details endpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbMovie {
    /// TMDB identifier.
    pub id: u64,
    /// Display title.
    #[serde(default)]
    pub title: Option<String>,
    /// Original title.
    #[serde(default)]
    pub original_title: Option<String>,
    /// Summary text.
    #[serde(default)]
    pub overview: Option<String>,
    /// Original language code.
    #[serde(default)]
    pub original_language: Option<String>,
    /// Release date in TMDB's ISO date representation.
    #[serde(default)]
    pub release_date: Option<String>,
    /// Poster image path, when supplied.
    #[serde(default)]
    pub poster_path: Option<String>,
    /// Backdrop image path, when supplied.
    #[serde(default)]
    pub backdrop_path: Option<String>,
    /// Runtime in minutes, when TMDB has one.
    #[serde(default)]
    pub runtime: Option<u16>,
    /// Popularity score.
    #[serde(default)]
    pub popularity: Option<f64>,
    /// Average vote score.
    #[serde(default)]
    pub vote_average: Option<f64>,
    /// Vote count.
    #[serde(default)]
    pub vote_count: Option<u64>,
    /// Keyword IDs included by the append-to-response request.
    #[serde(default, deserialize_with = "deserialize_keywords")]
    pub keywords: Vec<TmdbKeyword>,
    /// Genre IDs included by the append-to-response request.
    #[serde(default)]
    pub genres: Vec<TmdbGenre>,
    /// Production companies.
    #[serde(default)]
    pub production_companies: Vec<TmdbCompany>,
    /// Collection, if the movie belongs to one.
    #[serde(default)]
    pub belongs_to_collection: Option<TmdbCollection>,
    /// Cast and crew returned by the `credits` append.
    #[serde(default)]
    pub credits: TmdbCredits,
}

/// A television-series response returned by the TMDB details endpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbTv {
    /// TMDB identifier.
    pub id: u64,
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Original name.
    #[serde(default)]
    pub original_name: Option<String>,
    /// Summary text.
    #[serde(default)]
    pub overview: Option<String>,
    /// Original language code.
    #[serde(default)]
    pub original_language: Option<String>,
    /// First air date in TMDB's ISO date representation.
    #[serde(default)]
    pub first_air_date: Option<String>,
    /// Poster image path, when supplied.
    #[serde(default)]
    pub poster_path: Option<String>,
    /// Backdrop image path, when supplied.
    #[serde(default)]
    pub backdrop_path: Option<String>,
    /// Number of episodes, when supplied.
    #[serde(default)]
    pub number_of_episodes: Option<u32>,
    /// Number of seasons, when supplied.
    #[serde(default)]
    pub number_of_seasons: Option<u16>,
    /// Popularity score.
    #[serde(default)]
    pub popularity: Option<f64>,
    /// Average vote score.
    #[serde(default)]
    pub vote_average: Option<f64>,
    /// Vote count.
    #[serde(default)]
    pub vote_count: Option<u64>,
    /// Genres.
    #[serde(default)]
    pub genres: Vec<TmdbGenre>,
    /// Keyword IDs included by the append-to-response request.
    #[serde(default, deserialize_with = "deserialize_keywords")]
    pub keywords: Vec<TmdbKeyword>,
    /// Production companies.
    #[serde(default)]
    pub production_companies: Vec<TmdbCompany>,
    /// Networks.
    #[serde(default)]
    pub networks: Vec<TmdbNetwork>,
    /// Season summaries returned by the TV details endpoint.
    #[serde(default)]
    pub seasons: Vec<TmdbSeasonSummary>,
    /// Cast and crew returned by the `credits` append.
    #[serde(default)]
    pub credits: TmdbCredits,
}

/// Cast and crew lists from a TMDB details response.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbCredits {
    /// Acting and guest-star credits.
    #[serde(default)]
    pub cast: Vec<TmdbCredit>,
    /// Writing, directing, production, and other crew credits.
    #[serde(default)]
    pub crew: Vec<TmdbCredit>,
}

/// A normalized cast or crew credit returned by TMDB.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbCredit {
    /// TMDB person identifier.
    pub id: u64,
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Original name.
    #[serde(default)]
    pub original_name: Option<String>,
    /// Department, such as Acting or Directing.
    #[serde(default)]
    pub department: Option<String>,
    /// Crew job, when this is a crew credit.
    #[serde(default)]
    pub job: Option<String>,
    /// Character name, when this is a cast credit.
    #[serde(default)]
    pub character: Option<String>,
    /// Stable TMDB credit identifier.
    #[serde(default)]
    pub credit_id: Option<String>,
    /// Cast ordering.
    #[serde(default)]
    pub order: Option<i32>,
    /// Episode count for a TV aggregate credit.
    #[serde(default)]
    pub total_episode_count: Option<i32>,
    /// Profile image path.
    #[serde(default)]
    pub profile_path: Option<String>,
    /// Adult flag.
    #[serde(default)]
    pub adult: bool,
}

/// A season summary embedded in a TV details response.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbSeasonSummary {
    /// TMDB season identifier.
    pub id: u64,
    /// Season number; zero is the specials season.
    pub season_number: u16,
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Summary text.
    #[serde(default)]
    pub overview: Option<String>,
    /// Air date in ISO format.
    #[serde(default)]
    pub air_date: Option<String>,
    /// Number of episodes reported by TMDB.
    #[serde(default)]
    pub episode_count: Option<u16>,
    /// Season poster path.
    #[serde(default)]
    pub poster_path: Option<String>,
}

/// A full season response, including its episode list.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbSeason {
    /// TMDB season identifier.
    pub id: u64,
    /// Parent TV identifier, when supplied.
    #[serde(default)]
    pub show_id: Option<u64>,
    /// Season number; zero is the specials season.
    pub season_number: u16,
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Summary text.
    #[serde(default)]
    pub overview: Option<String>,
    /// Air date in ISO format.
    #[serde(default)]
    pub air_date: Option<String>,
    /// Season poster path.
    #[serde(default)]
    pub poster_path: Option<String>,
    /// Episodes in this season.
    #[serde(default)]
    pub episodes: Vec<TmdbEpisode>,
}

/// An episode returned by a TMDB season endpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbEpisode {
    /// TMDB episode identifier.
    pub id: u64,
    /// Episode number within its season.
    pub episode_number: u16,
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Summary text.
    #[serde(default)]
    pub overview: Option<String>,
    /// Air date in ISO format.
    #[serde(default)]
    pub air_date: Option<String>,
    /// Runtime in minutes.
    #[serde(default)]
    pub runtime: Option<u16>,
    /// Still image path.
    #[serde(default)]
    pub still_path: Option<String>,
    /// Average vote score.
    #[serde(default)]
    pub vote_average: Option<f64>,
    /// Vote count.
    #[serde(default)]
    pub vote_count: Option<u64>,
    /// Episode cast and crew.
    #[serde(default)]
    pub credits: TmdbCredits,
}

/// A TMDB movie or television genre.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbGenre {
    /// TMDB identifier.
    pub id: u64,
    /// Localized name.
    #[serde(default)]
    pub name: Option<String>,
}

/// A TMDB keyword.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbKeyword {
    /// TMDB identifier.
    pub id: u64,
    /// Keyword text.
    #[serde(default)]
    pub name: Option<String>,
}

fn deserialize_keywords<'de, D>(deserializer: D) -> Result<Vec<TmdbKeyword>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    if value.is_null() {
        return Ok(Vec::new());
    }
    let value = match value {
        serde_json::Value::Object(mut object) => object
            .remove("keywords")
            .or_else(|| object.remove("results"))
            .unwrap_or_else(|| serde_json::Value::Array(Vec::new())),
        value => value,
    };
    serde_json::from_value(value).map_err(serde::de::Error::custom)
}

/// A TMDB production company or studio.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbCompany {
    /// TMDB identifier.
    pub id: u64,
    /// Company name.
    #[serde(default)]
    pub name: Option<String>,
    /// Logo path, if available.
    #[serde(default)]
    pub logo_path: Option<String>,
    /// Country of registration.
    #[serde(default)]
    pub origin_country: Option<String>,
}

/// A TMDB television network.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbNetwork {
    /// TMDB identifier.
    pub id: u64,
    /// Network name.
    #[serde(default)]
    pub name: Option<String>,
    /// Logo path, if available.
    #[serde(default)]
    pub logo_path: Option<String>,
    /// Country of origin.
    #[serde(default)]
    pub origin_country: Option<String>,
}

/// A TMDB collection summary.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbCollection {
    /// TMDB identifier.
    pub id: u64,
    /// Collection name.
    #[serde(default)]
    pub name: Option<String>,
    /// Poster path, if available.
    #[serde(default)]
    pub poster_path: Option<String>,
    /// Backdrop path, if available.
    #[serde(default)]
    pub backdrop_path: Option<String>,
}

/// A TMDB person summary.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct TmdbPerson {
    /// TMDB identifier.
    pub id: u64,
    /// Display name.
    #[serde(default)]
    pub name: Option<String>,
    /// Known-for department.
    #[serde(default)]
    pub known_for_department: Option<String>,
    /// Profile image path.
    #[serde(default)]
    pub profile_path: Option<String>,
}

/// One ID returned by a TMDB change-list endpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ChangedId {
    /// TMDB identifier.
    pub id: u64,
    /// Adult flag from the change list.
    #[serde(default, deserialize_with = "deserialize_bool_or_false")]
    pub adult: bool,
    /// Video flag from the change list.
    #[serde(default, deserialize_with = "deserialize_bool_or_false")]
    pub video: bool,
    /// Popularity value from the change list.
    #[serde(default)]
    pub popularity: Option<f64>,
}

fn deserialize_bool_or_false<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or(false))
}

/// One field-level change group returned by a TMDB entity change endpoint.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ChangeGroup {
    /// Name of the changed field.
    pub key: String,
    /// Changes for that field.
    #[serde(default)]
    pub items: Vec<ChangeItem>,
}

/// One item in a TMDB field-level change group.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ChangeItem {
    /// Action, usually `created`, `updated`, or `deleted`.
    pub action: String,
    /// Change timestamp as supplied by TMDB.
    #[serde(default)]
    pub time: Option<String>,
    /// Value before the change when supplied.
    #[serde(default)]
    pub original_value: Option<serde_json::Value>,
    /// Value after the change when supplied.
    #[serde(default)]
    pub value: Option<serde_json::Value>,
}

/// A paginated TMDB change-list response.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ChangePage {
    /// Changed entity IDs.
    #[serde(default)]
    pub results: Vec<ChangedId>,
    /// One-based result page.
    pub page: u32,
    /// Number of pages in this result.
    #[serde(default)]
    pub total_pages: u32,
    /// Number of changed groups, when supplied.
    #[serde(default)]
    pub total_results: Option<u32>,
}

/// A paginated field-level change response for one entity.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct ChangeHistory {
    /// Changed field groups.
    #[serde(default)]
    pub changes: Vec<ChangeGroup>,
    /// One-based result page.
    pub page: u32,
    /// Number of pages in this result.
    #[serde(default)]
    pub total_pages: u32,
}
