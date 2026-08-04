use std::str::FromStr;

use tmdb_domain::{MediaType, ParseMediaTypeError};

#[test]
fn parses_only_tmdb_title_media_types() {
    assert_eq!(MediaType::from_str("movie"), Ok(MediaType::Movie));
    assert_eq!(MediaType::from_str("tv"), Ok(MediaType::Tv));
    assert_eq!(MediaType::Movie.to_string(), "movie");
    assert_eq!(MediaType::Tv.to_string(), "tv");
    assert_eq!(MediaType::from_str("anime"), Err(ParseMediaTypeError));
}
