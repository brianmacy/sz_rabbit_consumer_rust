//! Record parsing and Senzing error classification.

use std::fmt;

use sz_rust_sdk::prelude::{ErrorCategory, SzError};

/// The DATA_SOURCE / RECORD_ID extracted from a message body, used both as
/// `add_record` arguments and for reject logging (mirrors the Python).
#[derive(Debug, Clone)]
pub struct RecordInfo {
    pub data_source: String,
    pub record_id: String,
}

/// Why a message body could not be turned into a well-formed `RecordInfo`.
///
/// Each variant is DEAD-LETTERED by the consumer, NOT treated as fatal. A single
/// malformed message (unparseable JSON, non-object, or a missing/non-string
/// `DATA_SOURCE` / `RECORD_ID`) is a poison message: making it fatal leaves it
/// unacked, the broker requeues it, and the consumer crash-loops on it forever.
/// Instead the caller `basic_reject`s it to the dead-letter queue with no requeue
/// (exactly like the engine `SzBadInputError` / `SENZ0082` path) and keeps
/// processing. Dead-lettering is NOT a silent failure: the DLQ is a visible,
/// inspectable destination, so this satisfies the no-silent-failures rule while
/// avoiding the crash-loop (reverses the earlier "Python parity = fatal" choice
/// per explicit user direction).
#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    /// Body was not valid JSON (Python: `orjson.loads` raises).
    InvalidJson,
    /// JSON was valid but not an object, so keys cannot be looked up.
    NotAnObject,
    /// A required key (`DATA_SOURCE` / `RECORD_ID`) is absent or not a string
    /// (Python: `record["KEY"]` raises `KeyError`).
    MissingField(&'static str),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::InvalidJson => write!(f, "record body is not valid JSON"),
            ParseError::NotAnObject => write!(f, "record body is not a JSON object"),
            ParseError::MissingField(key) => {
                write!(f, "record is missing required string field '{key}'")
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// Parses a message body to extract DATA_SOURCE and RECORD_ID via serde_json.
///
/// Returns `Err(ParseError)` for unparseable JSON, a non-object, or a
/// missing/non-string `DATA_SOURCE` / `RECORD_ID`. The caller DEAD-LETTERS this
/// (`basic_reject`, no requeue) and keeps processing — the same handling as the
/// engine's `SzBadInputError` / `SzRetryTimeoutExceeded` / `SENZ0082` cases — so
/// one poison message cannot crash-loop the consumer.
pub fn parse_record(body: &[u8]) -> Result<RecordInfo, ParseError> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| ParseError::InvalidJson)?;
    let obj = value.as_object().ok_or(ParseError::NotAnObject)?;
    let get = |key: &'static str| -> Result<String, ParseError> {
        obj.get(key)
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .ok_or(ParseError::MissingField(key))
    };
    Ok(RecordInfo {
        data_source: get("DATA_SOURCE")?,
        record_id: get("RECORD_ID")?,
    })
}

/// How an engine error should be handled.
#[derive(Debug, PartialEq, Eq)]
pub enum ErrorClass {
    /// Bad data, retry timeout, or an unmapped code we treat as bad input
    /// (e.g. SENZ0082) -> reject to the dead-letter queue.
    BadInputOrTimeout,
    /// Any other error -> propagate and trigger graceful shutdown.
    Fatal,
}

/// Classifies a Senzing engine error the same way the Python consumer does,
/// using the v4.3.1 SDK's STRUCTURED error API (`SzError::is_bad_input` /
/// `SzError::is` + `ErrorCategory` / `SzError::error_code`) rather than matching
/// on the Display string. Under the hood the SDK derives every engine error from
/// `getLastExceptionCode()` -> generated code table -> typed variant, so the code
/// and category are authoritative.
///
/// * Bad-input errors — `ErrorCategory::BadInput` and its subtypes `NotFound` /
///   `UnknownDataSource` (`is_bad_input()`) -> dead-letter. This mirrors the
///   Python `SzBadInputError` catch: in the Senzing C# hierarchy
///   `SzUnknownDataSourceException` is itself an `SzBadInputException`, so an
///   unregistered data source is bad input, not a fatal.
/// * Retry-timeout — `SENZ0010`, which v4.3.1 maps to
///   `SzError::RetryTimeoutExceeded` (`ErrorCategory::RetryTimeoutExceeded`) ->
///   dead-letter. The engine exhausted its internal retry budget for THIS record
///   (e.g. a lock/deadlock it could not clear); the record was not added, so we
///   reject it and keep serving the queue instead of treating it as fatal. The
///   pre-v4.3.1 coarse bucketing could not express retry-timeout distinctly
///   (it fell through to a non-retryable class); the structured category fixes
///   that — this is the bug this bump exists to fix.
/// * `SENZ0082` (DQM plugin bad-input, e.g. an invalid name like `**`) maps to
///   `SzError::Unknown` in the generated code table, so `is_bad_input()` does not
///   catch it; we match it by its structured error CODE (82), not a Display
///   substring.
/// * Everything else -> fatal (graceful shutdown).
pub fn classify_error(err: &SzError) -> ErrorClass {
    if err.is_bad_input()
        || err.is(ErrorCategory::RetryTimeoutExceeded)
        || err.error_code() == Some(82)
    {
        return ErrorClass::BadInputOrTimeout;
    }
    ErrorClass::Fatal
}

#[cfg(test)]
mod tests {
    use super::*;
    // Test-only: build an engine error carrying a specific numeric code, exactly
    // as the SDK does internally (`ErrorContext::with_code`), without needing the
    // native library loaded.
    use sz_rust_sdk::prelude::{ErrorContext, SzComponent};

    #[test]
    fn parses_data_source_and_record_id() {
        let body = br#"{"DATA_SOURCE":"TEST","RECORD_ID":"R1","NAME_FULL":"A B"}"#;
        let info = parse_record(body).expect("well-formed record should parse");
        assert_eq!(info.data_source, "TEST");
        assert_eq!(info.record_id, "R1");
    }

    // The following four tests document that a malformed record parses to the
    // right `ParseError` variant. The consumer (see `consumer.rs`) dead-letters
    // every one of these via `basic_reject(requeue=false)` and KEEPS PROCESSING
    // — it does NOT shut down or exit non-zero. A parse failure is a poison
    // message, so dead-lettering it (not crashing) is the correct, non-silent
    // handling (FIX 1).

    #[test]
    fn missing_data_source_is_dead_lettered() {
        // A missing DATA_SOURCE -> ParseError -> consumer dead-letters it.
        let err = parse_record(br#"{"RECORD_ID":"R1","NAME_FULL":"A B"}"#).unwrap_err();
        assert_eq!(err, ParseError::MissingField("DATA_SOURCE"));
    }

    #[test]
    fn missing_record_id_is_dead_lettered() {
        let err = parse_record(br#"{"DATA_SOURCE":"TEST"}"#).unwrap_err();
        assert_eq!(err, ParseError::MissingField("RECORD_ID"));
    }

    #[test]
    fn non_string_field_is_dead_lettered() {
        // A numeric DATA_SOURCE is not a string -> missing-field -> dead-lettered.
        let err = parse_record(br#"{"DATA_SOURCE":123,"RECORD_ID":"R1"}"#).unwrap_err();
        assert_eq!(err, ParseError::MissingField("DATA_SOURCE"));
    }

    #[test]
    fn invalid_json_is_dead_lettered() {
        // Unparseable JSON -> ParseError::InvalidJson -> consumer dead-letters it.
        let err = parse_record(b"not json").unwrap_err();
        assert_eq!(err, ParseError::InvalidJson);
    }

    #[test]
    fn json_scalar_is_not_an_object() {
        // A non-object JSON scalar -> ParseError::NotAnObject -> dead-lettered.
        let err = parse_record(b"42").unwrap_err();
        assert_eq!(err, ParseError::NotAnObject);
    }

    #[test]
    fn bad_input_is_dead_lettered() {
        let e = SzError::bad_input("bad");
        assert_eq!(classify_error(&e), ErrorClass::BadInputOrTimeout);
    }

    #[test]
    fn retry_timeout_is_dead_lettered() {
        let e = SzError::retry_timeout_exceeded("timeout");
        assert_eq!(classify_error(&e), ErrorClass::BadInputOrTimeout);
    }

    #[test]
    fn senz0082_is_dead_lettered() {
        // SENZ0082 (DQM plugin bad input) maps to SzError::Unknown in the SDK
        // code table, so is_bad_input() does NOT catch it. The consumer matches
        // it by its STRUCTURED error code (82) — build the error the same way the
        // SDK does (Unknown variant carrying code 82), not via a Display string.
        let e = SzError::Unknown(ErrorContext::with_code(
            "Error: SENZ0082 invalid name",
            82,
            SzComponent::Engine,
        ));
        assert_eq!(e.error_code(), Some(82));
        assert_eq!(classify_error(&e), ErrorClass::BadInputOrTimeout);
    }

    #[test]
    fn other_errors_are_fatal() {
        let e = SzError::database("connection lost");
        assert_eq!(classify_error(&e), ErrorClass::Fatal);
    }
}
