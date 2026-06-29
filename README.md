# sz_rabbit_consumer (Rust)

Rust port of the Senzing RabbitMQ consumer. This is the Rust sibling of the
Python [`sz_rabbit_consumer-v4`](../sz_rabbit_consumer-v4) and exists so the
container can be **distroless** (no Python interpreter, no pip, no apt, no shell)
and so glue-layer errors surface at compile time.

## Overview

A simple, scalable parallel JSON data processor built on the Senzing SDK. It
consumes JSON records from a RabbitMQ queue and submits each to the Senzing
engine via `add_record`. It is intended as a starting point for a scalable
`add_record` processor.

## API demonstrated

### Core
* `add_record` — adds the Senzing JSON record.

### Supporting
* `SzEnvironmentCore::get_instance` — initialize the Senzing environment.
* `get_stats` — retrieve internal engine diagnostics (printed periodically).

For more on the Senzing SDK see https://docs.senzing.com.

## Concurrency model

The Senzing SDK requires synchronous engine calls on real OS threads (each
thread owns its own engine handle). RabbitMQ I/O is async (`lapin`). The two are
bridged:

* A tokio runtime owns the lapin connection + channel, runs the consume loop and
  the periodic stats/long-record monitor, and is the only place that issues
  `basic_ack` / `basic_reject`.
* `SENZING_THREADS_PER_PROCESS` `std::thread` workers each own an engine handle
  and perform the blocking `add_record` calls.
* A bounded work channel (capacity = worker count) carries deliveries to the
  workers; this mirrors the Python `basic_qos(prefetch_count = max_workers)` so
  there is exactly one prefetched record per thread. A result channel carries the
  ack/reject decision back to the async task.

## Configuration

Priority: **CLI argument > environment variable > default.**

### CLI flags
```
-u, --url <URL>        RabbitMQ server URL          (env: SENZING_AMQP_URL)
-q, --queue <QUEUE>    Source queue name            (env: SENZING_RABBITMQ_QUEUE)
-i, --info             Print the WithInfo response for each added record
-t, --debugTrace       Output Senzing engine debug trace (verbose logging)
```

### Required (environment)
```
SENZING_ENGINE_CONFIGURATION_JSON   Engine configuration JSON. If unset, the
                                    process prints an error and exits non-zero.
SENZING_RABBITMQ_QUEUE              Source queue (or --queue).
SENZING_AMQP_URL                   RabbitMQ URL (or --url).
```

### Optional (environment)
```
SENZING_LOG_LEVEL            Default: info. One of notset/debug/info/warning/
                             error/fatal/critical. (RUST_LOG overrides for finer
                             control via env-filter.)
SENZING_THREADS_PER_PROCESS  Default: number of CPUs. Worker thread count and the
                             AMQP prefetch count.
LONG_RECORD                  Default: 300 (seconds). See dead-letter behavior.
```

## Building and running

### Native (requires the Senzing SDK at /opt/senzing)
```bash
export SENZING_LIB_PATH=/opt/senzing/er/lib       # for the build (links libSz)
export LD_LIBRARY_PATH=/opt/senzing/er/lib        # for running

cargo build --release

export SENZING_ENGINE_CONFIGURATION_JSON='{...}'
export SENZING_AMQP_URL='amqp://guest:guest@localhost:5672/%2F'
export SENZING_RABBITMQ_QUEUE='senzing-rabbitmq-queue'
./target/release/sz_rabbit_consumer
```

### Docker (distroless, backend-selected)

Build args `WITH_POSTGRES` (default 1) and `WITH_MSSQL` (default 1) select which
database backend plugins are bundled. At least one must be enabled (the build
errors out if both are 0).

```bash
# Both backends (default):
docker build -t brian/sz_rabbit_consumer .

# PostgreSQL only:
docker build --build-arg WITH_MSSQL=0 -t brian/sz_rabbit_consumer:pg .

# MSSQL only:
docker build --build-arg WITH_POSTGRES=0 -t brian/sz_rabbit_consumer:mssql .

docker run --rm \
  -e SENZING_ENGINE_CONFIGURATION_JSON \
  -e SENZING_AMQP_URL \
  -e SENZING_RABBITMQ_QUEUE \
  brian/sz_rabbit_consumer
```

## Dead-letter behavior

* **Success** → `basic_ack`.
* **Bad data / retry timeout** (`SzBadInputError`, `SzRetryTimeoutExceededError`,
  or an error whose message contains `SENZ0082` — a DQM plugin error such as an
  invalid name) → `basic_reject(requeue=false)`, sending the record to the
  dead-letter queue. The reason is logged:
  `REJECTING due to bad data or timeout: {DATA_SOURCE} : {RECORD_ID}`.
* **Any other engine error** → propagated; the process drains in-flight work and
  exits non-zero.

### Long-record monitoring

Every `LONG_RECORD / 2` seconds the consumer prints `get_stats()` and inspects
in-flight records:

* A record running longer than `2 * LONG_RECORD` is rejected to the dead-letter
  queue (`REJECTING: {DATA_SOURCE} : {RECORD_ID}`).
* A record running longer than `LONG_RECORD` is logged as still-processing.

**Deviation from Python (documented):** Senzing engine calls are uninterruptible
in both Python and Rust. When a record is dead-lettered for running too long, the
broker-side delivery is rejected and marked so its eventual (now ignored) worker
result does not also ack it — but the worker thread continues running the engine
call until the C library returns. Neither language can cancel an in-flight engine
call; only the broker-side delivery is acted upon.

## Graceful shutdown

On SIGINT or SIGTERM the consumer stops consuming new messages, drains and joins
the worker threads (in-flight engine calls complete), acks/rejects what remains,
and closes the AMQP connection.

## Testing

Tests use the **real** Senzing SDK only — no mocks. Unit tests (record parsing
and error classification) run without a backend. Integration tests in
`tests/integration_test.rs` require `/opt/senzing`, a configured backend
(sqlite suffices), and — for the end-to-end path — a running RabbitMQ. They skip
with a printed reason when prerequisites are absent and fail loudly otherwise.
See the header of `tests/integration_test.rs` for the exact environment to set.

```bash
cargo test --lib                       # unit tests (no backend needed)
cargo test --test integration_test     # integration (needs /opt/senzing + backend)
```
