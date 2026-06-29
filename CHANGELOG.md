# Changelog

## [Unreleased]

### Added
- Initial Rust port of the Python `sz_rabbit_consumer-v4`.
- RabbitMQ consumer that submits records to Senzing via `add_record`, with the
  AMQP I/O layer on `lapin`/tokio and the Senzing engine work on a pool of
  `std::thread` workers (one engine handle per OS thread, per the SDK).
- Dead-letter handling for `SzBadInputError`, `SzRetryTimeoutExceededError`, and
  errors containing `SENZ0082`; other engine errors trigger graceful shutdown.
- Periodic `get_stats()` and long-record monitoring (reject records running
  longer than `2 * LONG_RECORD` to the dead-letter queue).
- Graceful shutdown on SIGINT/SIGTERM.
- Configuration via clap (CLI) with environment fallbacks matching the Python.
- Distroless multi-stage Dockerfile with `WITH_POSTGRES` / `WITH_MSSQL` backend
  selection.
- CI (fmt, clippy, release build, optional Docker matrix) and Dependabot for
  cargo / github-actions / docker.
