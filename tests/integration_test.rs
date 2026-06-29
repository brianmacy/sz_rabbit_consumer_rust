//! Integration tests for the RabbitMQ -> Senzing consumer.
//!
//! These tests exercise REAL infrastructure — no mocks. They require all of the
//! following and are skipped (with a printed reason) when prerequisites are
//! absent, but FAIL LOUDLY if a step that should succeed does not:
//!
//! * The Senzing native library at `/opt/senzing/er/lib` (set
//!   `SENZING_LIB_PATH` / `LD_LIBRARY_PATH`).
//! * `SENZING_ENGINE_CONFIGURATION_JSON` pointing at a real backend (sqlite is
//!   sufficient for the engine-only test; PostgreSQL/MSSQL for backend matrices).
//! * A running RabbitMQ reachable via `SENZING_AMQP_URL` for the end-to-end test.
//!
//! How to run locally:
//! ```bash
//! export SENZING_LIB_PATH=/opt/senzing/er/lib
//! export LD_LIBRARY_PATH=/opt/senzing/er/lib
//! export SENZING_ENGINE_CONFIGURATION_JSON='{"PIPELINE":{"CONFIGPATH":"/etc/opt/senzing","RESOURCEPATH":"/opt/senzing/er/resources","SUPPORTPATH":"/opt/senzing/data"},"SQL":{"CONNECTION":"sqlite3://na:na@/tmp/G2C.db"}}'
//! # for the end-to-end test additionally:
//! export SENZING_AMQP_URL='amqp://guest:guest@localhost:5672/%2F'
//! export SENZING_RABBITMQ_QUEUE='senzing-rabbitmq-queue'
//! cargo test --test integration_test -- --nocapture
//! ```

use std::sync::Arc;

use sz_rabbit_consumer::record::{ErrorClass, ParseError, classify_error, parse_record};
use sz_rust_sdk::prelude::*;

/// Returns the engine configuration JSON if the environment is fully set up for
/// a real Senzing test, otherwise `None` (test is skipped).
fn engine_config() -> Option<String> {
    std::env::var("SENZING_ENGINE_CONFIGURATION_JSON")
        .ok()
        .filter(|s| !s.is_empty())
}

#[test]
fn record_parsing_extracts_fields() {
    // Pure logic; always runs.
    let info = parse_record(br#"{"DATA_SOURCE":"TEST","RECORD_ID":"INT_1"}"#)
        .expect("well-formed record should parse");
    assert_eq!(info.data_source, "TEST");
    assert_eq!(info.record_id, "INT_1");
}

#[test]
fn missing_data_source_is_fatal_parse_error() {
    // FIX 3: a missing DATA_SOURCE is a FATAL upstream-feed error (shutdown +
    // non-zero exit), NOT a silent dead-letter. Mirrors Python's KeyError ->
    // `else: raise` path.
    let err = parse_record(br#"{"NAME_FULL":"No Identifiers"}"#).unwrap_err();
    assert_eq!(err, ParseError::MissingField("DATA_SOURCE"));
}

/// Real engine test: initialize the environment and add a record. This is the
/// same call path the consumer's worker threads use. Fails loudly if the engine
/// is configured but `add_record` errors.
#[test]
fn real_engine_add_record() {
    let Some(config) = engine_config() else {
        eprintln!(
            "SKIP real_engine_add_record: SENZING_ENGINE_CONFIGURATION_JSON not set \
             (requires /opt/senzing + a backend)"
        );
        return;
    };

    let env: Arc<SzEnvironmentCore> =
        SzEnvironmentCore::get_instance("sz_rabbit_consumer_it", &config, false)
            .expect("failed to initialize Senzing environment");

    let engine = env.get_engine().expect("failed to get engine handle");

    let body = r#"{"DATA_SOURCE":"TEST","RECORD_ID":"IT_REC_1","NAME_FULL":"Integration Tester","EMAIL_ADDRESS":"it@example.com"}"#;
    let info = parse_record(body.as_bytes()).expect("well-formed record should parse");
    let result = engine.add_record(
        &info.data_source,
        &info.record_id,
        body,
        Some(SzFlags::ADD_RECORD_DEFAULT),
    );
    assert!(result.is_ok(), "add_record failed: {:?}", result.err());

    // Stats must be retrievable (the consumer's periodic monitor depends on it).
    let stats = engine.get_stats();
    assert!(stats.is_ok(), "get_stats failed: {:?}", stats.err());

    SzEnvironmentCore::destroy_global_instance().expect("failed to destroy environment");
}

/// Confirms a deliberately malformed record is classified for the dead-letter
/// queue rather than crashing the consumer, using the REAL engine.
#[test]
fn real_engine_bad_record_is_dead_lettered() {
    let Some(config) = engine_config() else {
        eprintln!("SKIP real_engine_bad_record_is_dead_lettered: engine config not set");
        return;
    };

    let env: Arc<SzEnvironmentCore> =
        SzEnvironmentCore::get_instance("sz_rabbit_consumer_it", &config, false)
            .expect("failed to initialize Senzing environment");
    let engine = env.get_engine().expect("failed to get engine handle");

    // Dead-lettering is reserved for a WELL-FORMED record (valid DATA_SOURCE +
    // RECORD_ID) that the ENGINE rejects as bad input — distinct from FIX 3's
    // fatal missing-field case. An invalid name like "**" triggers the DQM
    // plugin (SENZ0082), which the consumer classifies as a dead-letter, never
    // fatal.
    let body = r#"{"DATA_SOURCE":"TEST","RECORD_ID":"IT_BAD_1","PRIMARY_NAME_FULL":"**"}"#;
    let info = parse_record(body.as_bytes()).expect("record is well-formed and must parse");
    if let Err(e) = engine.add_record(
        &info.data_source,
        &info.record_id,
        body,
        Some(SzFlags::ADD_RECORD_DEFAULT),
    ) {
        // The consumer treats this engine-level bad-input class as a dead-letter,
        // never fatal.
        assert_eq!(
            classify_error(&e),
            ErrorClass::BadInputOrTimeout,
            "engine-rejected record should classify as dead-letter, got: {e}"
        );
    }

    SzEnvironmentCore::destroy_global_instance().expect("failed to destroy environment");
}
