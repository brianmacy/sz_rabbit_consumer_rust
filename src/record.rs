//! Record parsing and Senzing error classification.

use std::fmt;

use sz_rust_sdk::prelude::SzError;

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

/// Classifies a Senzing engine error the same way the Python consumer does:
///
/// * `SzBadInputError` / `SzRetryTimeoutExceededError` -> dead-letter.
/// * Any error whose message contains `SENZ0082` (DQM plugin error, e.g. an
///   invalid name like `**`) -> dead-letter, even though the SDK may not map it
///   to `BadInput`.
/// * Everything else -> fatal (graceful shutdown).
pub fn classify_error(err: &SzError) -> ErrorClass {
    if matches!(
        err,
        SzError::BadInput { .. } | SzError::RetryTimeoutExceeded { .. }
    ) {
        return ErrorClass::BadInputOrTimeout;
    }
    if err.to_string().contains("SENZ0082") {
        return ErrorClass::BadInputOrTimeout;
    }
    ErrorClass::Fatal
}

#[cfg(test)]
mod tests {
    use super::*;

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
        // An unmapped code surfaced as Unknown but containing SENZ0082.
        let e = SzError::unknown("Error: SENZ0082 invalid name");
        assert_eq!(classify_error(&e), ErrorClass::BadInputOrTimeout);
    }

    #[test]
    fn other_errors_are_fatal() {
        let e = SzError::database("connection lost");
        assert_eq!(classify_error(&e), ErrorClass::Fatal);
    }
}
