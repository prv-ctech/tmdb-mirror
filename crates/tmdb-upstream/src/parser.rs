use std::fs::File;
use std::io::{self, BufRead, BufReader, Cursor, Read};
use std::path::Path;

use flate2::read::GzDecoder;

use crate::{ChangeHistory, ChangePage, DailyExportRecord};

const DEFAULT_MAX_LINE_BYTES: usize = 1 << 20;
const DEFAULT_MAX_RECORDS: usize = 5_000_000;
const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = 512 << 20;

/// Bounds for newline-delimited daily export parsing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DailyExportParser {
    line_bytes: usize,
    records: usize,
    decompressed_bytes: usize,
}

impl Default for DailyExportParser {
    fn default() -> Self {
        Self {
            line_bytes: DEFAULT_MAX_LINE_BYTES,
            records: DEFAULT_MAX_RECORDS,
            decompressed_bytes: DEFAULT_MAX_DECOMPRESSED_BYTES,
        }
    }
}

impl DailyExportParser {
    /// Creates a parser with explicit input bounds.
    ///
    /// # Errors
    ///
    /// Returns [`ExportParseError::InvalidBounds`] when any bound is zero.
    pub fn try_new(
        max_line_bytes: usize,
        max_records: usize,
        max_decompressed_bytes: usize,
    ) -> Result<Self, ExportParseError> {
        if max_line_bytes == 0 || max_records == 0 || max_decompressed_bytes == 0 {
            return Err(ExportParseError::InvalidBounds);
        }
        Ok(Self {
            line_bytes: max_line_bytes,
            records: max_records,
            decompressed_bytes: max_decompressed_bytes,
        })
    }

    /// Parses either plain NDJSON or gzip-compressed NDJSON.
    ///
    /// TMDB documents each daily export as one valid JSON object per line, not
    /// one JSON array. The parser therefore never requires the complete export
    /// to be a single in-memory JSON value.
    ///
    /// # Errors
    ///
    /// Returns a sanitized parser error when a line, record count, or
    /// decompressed-byte bound is exceeded, or when JSON/gzip input is invalid.
    pub fn parse_bytes(&self, bytes: &[u8]) -> Result<Vec<DailyExportRecord>, ExportParseError> {
        if bytes.starts_with(&[0x1f, 0x8b]) {
            self.parse_reader(BufReader::new(GzDecoder::new(Cursor::new(bytes))))
        } else {
            self.parse_reader(BufReader::new(Cursor::new(bytes)))
        }
    }

    /// Parses a reader containing plain NDJSON.
    ///
    /// # Errors
    ///
    /// Returns a sanitized parser error when a line, record count, or
    /// decompressed-byte bound is exceeded, or when JSON input is invalid.
    pub fn parse_reader<R>(&self, mut reader: R) -> Result<Vec<DailyExportRecord>, ExportParseError>
    where
        R: BufRead,
    {
        let mut records = Vec::new();
        self.scan_reader(&mut reader, |record| records.push(record))?;
        Ok(records)
    }

    /// Validates a reader while discarding records, returning only its count.
    ///
    /// This is the bounded path used after a streamed export has been written
    /// to `NVMe`; it avoids retaining millions of export rows in the worker heap.
    ///
    /// # Errors
    ///
    /// Returns the same bounded parser failures as [`Self::parse_reader`].
    pub fn count_reader<R>(&self, mut reader: R) -> Result<usize, ExportParseError>
    where
        R: BufRead,
    {
        self.scan_reader(&mut reader, |_| {})
    }

    /// Validates and counts a plain or gzip daily export file.
    ///
    /// # Errors
    ///
    /// Returns a sanitized file, gzip, JSON, or bound error.
    pub fn count_file(&self, path: impl AsRef<Path>) -> Result<usize, ExportParseError> {
        self.scan_file(path, |_| {})
    }

    /// Validates and visits every record in a plain or gzip daily export file without retaining
    /// the complete export in memory.
    ///
    /// # Errors
    ///
    /// Returns a sanitized file, gzip, JSON, or bound error.
    pub fn scan_file<F>(
        &self,
        path: impl AsRef<Path>,
        mut on_record: F,
    ) -> Result<usize, ExportParseError>
    where
        F: FnMut(DailyExportRecord),
    {
        self.scan_file_until(path, None, &mut on_record)
    }

    /// Validates and visits at most `limit` records from a plain or gzip daily
    /// export without retaining the complete export in memory. The visitor is
    /// called in file order and the parser stops cleanly once the limit is
    /// reached; line and decompressed-byte safety bounds remain active.
    ///
    /// # Errors
    ///
    /// Returns a sanitized file, gzip, JSON, or bound error.
    pub fn scan_file_limited<F>(
        &self,
        path: impl AsRef<Path>,
        limit: usize,
        mut on_record: F,
    ) -> Result<usize, ExportParseError>
    where
        F: FnMut(DailyExportRecord),
    {
        if limit == 0 {
            return Err(ExportParseError::InvalidBounds);
        }
        self.scan_file_until(path, Some(limit), &mut on_record)
    }

    fn scan_file_until<F>(
        &self,
        path: impl AsRef<Path>,
        limit: Option<usize>,
        on_record: &mut F,
    ) -> Result<usize, ExportParseError>
    where
        F: FnMut(DailyExportRecord),
    {
        let path = path.as_ref();
        let mut probe = File::open(path)?;
        let mut header = [0_u8; 2];
        let gzip = match probe.read_exact(&mut header) {
            Ok(()) => header == [0x1f, 0x8b],
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => false,
            Err(_) => return Err(ExportParseError::Io),
        };
        drop(probe);
        if gzip {
            self.scan_reader_until(
                &mut BufReader::new(GzDecoder::new(File::open(path)?)),
                limit,
                on_record,
            )
        } else {
            self.scan_reader_until(&mut BufReader::new(File::open(path)?), limit, on_record)
        }
    }

    fn scan_reader<R, F>(&self, reader: &mut R, mut on_record: F) -> Result<usize, ExportParseError>
    where
        R: BufRead,
        F: FnMut(DailyExportRecord),
    {
        self.scan_reader_until(reader, None, &mut on_record)
    }

    fn scan_reader_until<R, F>(
        &self,
        reader: &mut R,
        limit: Option<usize>,
        mut on_record: F,
    ) -> Result<usize, ExportParseError>
    where
        R: BufRead,
        F: FnMut(DailyExportRecord),
    {
        let mut line = Vec::new();
        let mut line_number = 0_usize;
        let mut decompressed_bytes = 0_usize;
        let mut record_count = 0_usize;

        loop {
            line.clear();
            let bytes_read = read_line_bounded(reader, &mut line, self.line_bytes).map_err(
                |error| match error {
                    LineReadError::Io => ExportParseError::Io,
                    LineReadError::TooLong => ExportParseError::LineTooLong {
                        line: line_number.saturating_add(1),
                    },
                },
            )?;
            if bytes_read == 0 {
                break;
            }
            line_number = line_number.saturating_add(1);
            decompressed_bytes = decompressed_bytes
                .checked_add(bytes_read)
                .ok_or(ExportParseError::InputTooLarge)?;
            if decompressed_bytes > self.decompressed_bytes {
                return Err(ExportParseError::InputTooLarge);
            }
            if line.len() > self.line_bytes {
                return Err(ExportParseError::LineTooLong { line: line_number });
            }
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let record: DailyExportRecord = serde_json::from_slice(&line)
                .map_err(|_| ExportParseError::InvalidJson { line: line_number })?;
            if record.id == 0 {
                return Err(ExportParseError::InvalidId { line: line_number });
            }
            on_record(record);
            record_count = record_count.saturating_add(1);
            if limit.is_some_and(|limit| record_count >= limit) {
                return Ok(record_count);
            }
            if record_count > self.records {
                return Err(ExportParseError::TooManyRecords);
            }
        }
        Ok(record_count)
    }
}

fn read_line_bounded<R>(
    reader: &mut R,
    line: &mut Vec<u8>,
    max_line_bytes: usize,
) -> Result<usize, LineReadError>
where
    R: BufRead,
{
    loop {
        let buffered = reader.fill_buf().map_err(|_| LineReadError::Io)?;
        if buffered.is_empty() {
            return Ok(line.len());
        }
        let newline = buffered.iter().position(|byte| *byte == b'\n');
        let bytes_to_take = newline.map_or(buffered.len(), |position| position + 1);
        if line.len().saturating_add(bytes_to_take) > max_line_bytes {
            return Err(LineReadError::TooLong);
        }
        line.extend_from_slice(&buffered[..bytes_to_take]);
        reader.consume(bytes_to_take);
        if newline.is_some() {
            return Ok(line.len());
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LineReadError {
    Io,
    TooLong,
}

/// Parses a daily ID export using conservative default bounds.
///
/// # Errors
///
/// Returns a sanitized parser error when the input is malformed or exceeds a
/// conservative line, record-count, or decompressed-byte bound.
pub fn parse_daily_export(bytes: &[u8]) -> Result<Vec<DailyExportRecord>, ExportParseError> {
    DailyExportParser::default().parse_bytes(bytes)
}

/// Parses a typed change-list page returned by `/3/{media_type}/changes`.
///
/// # Errors
///
/// Returns a sanitized parser error when the JSON is malformed or its page
/// range is invalid.
pub fn parse_change_page(bytes: &[u8]) -> Result<ChangePage, ExportParseError> {
    let page: ChangePage =
        serde_json::from_slice(bytes).map_err(|_| ExportParseError::InvalidJson { line: 0 })?;
    if page.page == 0 || page.total_pages < page.page {
        return Err(ExportParseError::InvalidPage);
    }
    Ok(page)
}

/// Parses a typed field-level history returned by
/// `/3/{media_type}/{id}/changes`.
///
/// # Errors
///
/// Returns a sanitized parser error when the JSON is malformed or its page
/// range is invalid.
pub fn parse_change_history(bytes: &[u8]) -> Result<ChangeHistory, ExportParseError> {
    let history: ChangeHistory =
        serde_json::from_slice(bytes).map_err(|_| ExportParseError::InvalidJson { line: 0 })?;
    if history.page == 0 || history.total_pages < history.page {
        return Err(ExportParseError::InvalidPage);
    }
    Ok(history)
}

/// A bounded parser failure that does not retain upstream response bodies.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ExportParseError {
    /// A configured parser bound was zero.
    #[error("invalid export parser bounds")]
    InvalidBounds,
    /// The decompressed input exceeded its configured byte limit.
    #[error("export input is too large")]
    InputTooLarge,
    /// A line exceeded its configured byte limit.
    #[error("export line {line} is too long")]
    LineTooLong { line: usize },
    /// A line was not a valid export object, or a change document was malformed.
    #[error("invalid export JSON at line {line}")]
    InvalidJson { line: usize },
    /// An export object did not carry a positive TMDB ID.
    #[error("export line {line} has an invalid TMDB ID")]
    InvalidId { line: usize },
    /// A change page had an invalid page range.
    #[error("invalid change page")]
    InvalidPage,
    /// The export contained more records than allowed.
    #[error("export contains too many records")]
    TooManyRecords,
    /// The input reader failed without exposing the upstream error text.
    #[error("could not read export input")]
    Io,
}

impl From<io::Error> for ExportParseError {
    fn from(_: io::Error) -> Self {
        Self::Io
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flate2::{Compression, write::GzEncoder};

    use super::*;

    #[test]
    fn parses_plain_and_gzip_daily_exports() -> Result<(), ExportParseError> {
        let plain = br#"{"id":1,"adult":false,"video":true,"popularity":2.5}
{"id":2,"adult":true,"video":false,"popularity":null}
"#;
        let expected = parse_daily_export(plain)?;
        assert_eq!(expected.len(), 2);
        assert_eq!(expected[0].id, 1);
        assert!(expected[0].video);

        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(plain).map_err(|_| ExportParseError::Io)?;
        let compressed = encoder.finish().map_err(|_| ExportParseError::Io)?;
        assert_eq!(parse_daily_export(&compressed)?, expected);
        Ok(())
    }

    #[test]
    fn malformed_and_oversized_export_lines_fail_without_retaining_body()
    -> Result<(), ExportParseError> {
        assert_eq!(
            parse_daily_export(
                br#"{"id":1}
not-json
"#
            ),
            Err(ExportParseError::InvalidJson { line: 2 })
        );
        let parser =
            DailyExportParser::try_new(8, 10, 100).map_err(|_| ExportParseError::InvalidBounds)?;
        assert_eq!(
            parser.parse_bytes(
                br#"{"id":123456}
"#
            ),
            Err(ExportParseError::LineTooLong { line: 1 })
        );
        assert_eq!(
            parse_daily_export(
                br#"{"id":0}
"#
            ),
            Err(ExportParseError::InvalidId { line: 1 })
        );
        Ok(())
    }

    #[test]
    fn change_page_is_typed_and_page_bounds_are_checked() -> Result<(), ExportParseError> {
        let history = parse_change_history(
            br#"{"changes":[{"key":"title","items":[{"action":"updated","time":"2026-07-31T00:00:00.000Z"}]}],"page":1,"total_pages":1}"#,
        )?;
        assert_eq!(history.changes[0].key, "title");
        assert_eq!(history.changes[0].items[0].action, "updated");
        assert_eq!(
            parse_change_history(br#"{"changes":[],"page":2,"total_pages":1}"#),
            Err(ExportParseError::InvalidPage)
        );
        Ok(())
    }

    #[test]
    fn count_reader_validates_without_retaining_records() -> Result<(), ExportParseError> {
        let parser = DailyExportParser::try_new(1024, 10, 1024)?;
        let count = parser.count_reader(BufReader::new(Cursor::new(
            br#"{"id":1}
{"id":2}
"#,
        )))?;
        assert_eq!(count, 2);
        Ok(())
    }

    #[test]
    fn limited_file_scan_stops_after_requested_records() -> Result<(), ExportParseError> {
        let file = tempfile::NamedTempFile::new().map_err(|_| ExportParseError::Io)?;
        let path = file.path().to_owned();
        std::fs::write(
            &path,
            br#"{"id":1}
{"id":2}
{"id":3}
"#,
        )
        .map_err(|_| ExportParseError::Io)?;
        let mut ids = Vec::new();
        let count = DailyExportParser::default().scan_file_limited(&path, 2, |record| {
            ids.push(record.id);
        })?;
        assert_eq!(count, 2);
        assert_eq!(ids, vec![1, 2]);
        Ok(())
    }
}
