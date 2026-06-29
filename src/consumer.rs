//! RabbitMQ -> Senzing consumer.
//!
//! # Concurrency model (the lapin <-> std::thread bridge)
//!
//! The Senzing SDK forbids calling the engine from async tasks: engine calls are
//! synchronous/blocking and each OS thread must own its own engine handle
//! (`sz-rust-sdk/CLAUDE.md`). RabbitMQ I/O, on the other hand, is handled by
//! `lapin`, which is async. We therefore split responsibilities cleanly:
//!
//! * A **tokio runtime** owns the lapin `Connection` + `Channel`. It runs the
//!   consume loop, the periodic stats/long-record monitor, and is the *only*
//!   place that issues `basic_ack` / `basic_reject` (lapin requires acks on the
//!   channel's own task).
//! * **N `std::thread` workers**, each with its own `env.get_engine()?`, perform
//!   the blocking `add_record` calls.
//!
//! Two channels bridge the two worlds:
//!
//! * Work: `tokio::sync::mpsc::channel::<WorkItem>(threads)` — bounded to the
//!   worker count. Workers pull with `blocking_recv()`. The bound provides the
//!   same backpressure as Python's `prefetch_count = max_workers`.
//! * Results: `tokio::sync::mpsc::channel::<Outcome>(...)` — workers push with
//!   `blocking_send()`; the async task drains it and performs the ack/reject.
//!
//! Using tokio channels in *both* directions (with the blocking variants on the
//! worker side) avoids wrapping a `std::sync::mpsc::Receiver` in
//! `Arc<Mutex<..>>` and keeps the bridge free of `unsafe`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use futures_lite::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{Connection, ConnectionProperties};
use sz_rust_sdk::prelude::*;
use tokio::sync::{Notify, mpsc};

use crate::config::Config;
use crate::record::{RecordInfo, classify_error, parse_record};

/// Bit 62, historically the Senzing `SZ_WITH_INFO` flag (mirrors the Python
/// `SzEngineFlags.SZ_WITH_INFO` and the sibling redoer's `SZ_WITH_INFO_BITS`).
///
/// IMPORTANT: in this sz-rust-sdk this bit is currently INERT. The SDK's
/// `add_record` UNCONDITIONALLY calls `Sz_addRecordWithInfo_helper` regardless of
/// the flags passed (see `sz-rust-sdk/src/core/engine.rs`), so the engine always
/// returns the WithInfo payload and setting bit 62 changes nothing at the engine
/// level. We keep the constant only to stay consistent with the redoer and the
/// Python flag name; it is NOT what gates the WithInfo data. `--info` gates
/// whether the (always-returned) payload is PRINTED — which is the same
/// observable behavior as Python.
const SZ_WITH_INFO_BITS: u64 = 1 << 62;

/// Builds the `add_record` flags. When `info` is set we pass the bit-62 value to
/// mirror the Python flag, otherwise the default (empty) flags.
///
/// NOTE: this choice is observably a NO-OP at the engine level — the SDK's
/// `add_record` always calls the WithInfo helper and returns the WithInfo payload
/// either way. Whether that payload is shown is gated by `--info` at the print
/// site (see `want_info` in `worker_loop`), matching Python.
fn add_record_flags(info: bool) -> Option<SzFlags> {
    if info {
        Some(SzFlags::from_bits_retain(SZ_WITH_INFO_BITS))
    } else {
        Some(SzFlags::ADD_RECORD_DEFAULT)
    }
}

/// Throughput reporting interval, in processed records (mirrors Python `INTERVAL`).
const STATS_INTERVAL: u64 = 10000;

/// A unit of work handed from the async consumer to a worker thread.
struct WorkItem {
    delivery_tag: u64,
    body: Vec<u8>,
    info: RecordInfo,
}

/// What the async task should do with a delivery once a worker finishes (or the
/// monitor decides it is overdue).
#[derive(Debug)]
enum Action {
    /// Engine accepted the record (optionally carrying the WithInfo response).
    Ack(Option<String>),
    /// Bad data / timeout / SENZ0082 -> dead-letter (reject, no requeue).
    RejectNoRequeue,
    /// A non-recoverable engine error -> trigger graceful shutdown.
    Fatal(String),
}

/// Result message sent from a worker back to the async task.
struct Outcome {
    delivery_tag: u64,
    info: RecordInfo,
    action: Action,
}

/// In-flight bookkeeping for one delivery the async task is tracking.
struct InFlight {
    info: RecordInfo,
    started: Instant,
    /// Set once we have rejected this delivery to the dead-letter queue so we do
    /// not also ack it when the (now ignored) worker result arrives.
    rejected: bool,
}

/// Total shutdown grace window. Bounds BOTH the in-flight result drain and the
/// subsequent bounded worker join so the whole shutdown stays under `docker
/// stop`'s SIGTERM grace (FIX 2).
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

/// Outcome of [`run`], reported to `main` so it can decide whether tearing down
/// the global Senzing environment is safe and what exit code to use.
///
/// `run` returns this for BOTH the clean and the fatal shutdown paths (instead
/// of `bail!`ing on fatal) so the `all_workers_joined` flag always reaches
/// `main` — the use-after-free guard must hold even when shutdown was triggered
/// by a fatal engine error.
pub struct RunOutcome {
    /// `true` only if EVERY engine worker thread actually finished before the
    /// shutdown grace elapsed. When `false`, a worker may still be parked inside
    /// an uninterruptible `add_record` call holding the native engine; calling
    /// `SzEnvironmentCore::destroy_global_instance()` then would free engine
    /// state out from under it -> use-after-free / segfault. `main` must SKIP the
    /// destroy in that case and let process exit reclaim everything (FIX 2).
    pub all_workers_joined: bool,
    /// `Some(message)` if the consumer is shutting down due to a non-recoverable
    /// error; `main` prints it and exits non-zero. `None` for a clean shutdown.
    pub fatal: Option<String>,
}

/// Runs the consumer until SIGINT/SIGTERM or a fatal engine error.
///
/// `env` is the shared Senzing environment singleton; each worker derives its
/// own engine handle from it.
pub async fn run(config: Config, env: Arc<SzEnvironmentCore>) -> Result<RunOutcome> {
    let threads = config.threads;
    tracing::info!("Threads: {}", threads);

    // --- Bridge channels -----------------------------------------------------
    let (work_tx, work_rx) = mpsc::channel::<WorkItem>(threads);
    let (result_tx, mut result_rx) = mpsc::channel::<Outcome>(threads * 2);

    // A single work receiver is shared across workers behind a mutex; only the
    // brief `blocking_recv` lock is held, never across an engine call.
    let work_rx = Arc::new(std::sync::Mutex::new(work_rx));

    // Shutdown signal from the worker side to the async side. A worker that
    // cannot get an engine (fatal misconfiguration) notifies this so the main
    // `select!` begins shutdown instead of the process wedging when the bounded
    // work channel fills (FIX 1).
    let shutdown_notify = Arc::new(Notify::new());

    // --info gates whether the WithInfo payload is PRINTED. The engine returns
    // the payload unconditionally (the SDK always calls the WithInfo helper), so
    // `flags` is effectively inert here; `want_info` is what controls printing,
    // matching Python's observable behavior. Computed once and shared.
    let flags = add_record_flags(config.info);
    let want_info = config.info;

    // --- Spawn engine worker threads ----------------------------------------
    let mut workers = Vec::with_capacity(threads);
    for worker_id in 0..threads {
        let env = env.clone();
        let work_rx = work_rx.clone();
        let result_tx = result_tx.clone();
        let shutdown_notify = shutdown_notify.clone();

        let handle = std::thread::Builder::new()
            .name(format!("sz-worker-{worker_id}"))
            .spawn(move || {
                worker_loop(
                    worker_id,
                    env,
                    work_rx,
                    result_tx,
                    flags,
                    want_info,
                    shutdown_notify,
                )
            })
            .context("failed to spawn worker thread")?;
        workers.push(handle);
    }
    // Drop our extra result sender clone so the channel closes once all workers exit.
    drop(result_tx);

    // --- Connect to RabbitMQ -------------------------------------------------
    tracing::info!("Connecting to RabbitMQ");
    let connection = Connection::connect(&config.url, ConnectionProperties::default())
        .await
        .context("failed to connect to RabbitMQ")?;
    let channel = connection
        .create_channel()
        .await
        .context("failed to create channel")?;

    // Mirror Python's passive queue_declare (assert it exists; do not create).
    channel
        .queue_declare(
            config.queue.as_str().into(),
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await
        .with_context(|| format!("queue '{}' does not exist (passive declare)", config.queue))?;

    // One prefetch per worker thread (== Python basic_qos(prefetch_count=max_workers)).
    channel
        .basic_qos(threads as u16, BasicQosOptions::default())
        .await
        .context("failed to set basic_qos")?;

    let mut consumer = channel
        .basic_consume(
            config.queue.as_str().into(),
            "sz_rabbit_consumer".into(),
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .context("failed to start consuming")?;

    // --- Signal handling -----------------------------------------------------
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .context("failed to install SIGINT handler")?;
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install SIGTERM handler")?;

    // --- Monitor / stats engine (separate OS thread for blocking get_stats) --
    // get_stats() is a blocking engine call, so it must run on an OS thread, not
    // in the async context. We use a dedicated stats engine handle.
    let stats_env = env.clone();
    let (stats_req_tx, stats_req_rx) = std::sync::mpsc::channel::<()>();
    let (stats_resp_tx, mut stats_resp_rx) = mpsc::channel::<String>(1);
    let stats_handle = std::thread::Builder::new()
        .name("sz-stats".to_string())
        .spawn(move || stats_loop(stats_env, stats_req_rx, stats_resp_tx))
        .context("failed to spawn stats thread")?;

    // --- Main event loop -----------------------------------------------------
    let long_record = Duration::from_secs(config.long_record_secs);
    let monitor_interval = Duration::from_secs(config.long_record_secs.max(2) / 2);
    let mut monitor = tokio::time::interval(monitor_interval);
    monitor.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut in_flight: HashMap<u64, InFlight> = HashMap::new();
    let mut processed: u64 = 0;
    let mut last_rate_at = Instant::now();
    let mut shutting_down = false;
    let mut fatal: Option<String> = None;
    // Set once at the moment shutdown begins; shared by the in-flight drain and
    // the bounded worker join so the whole shutdown stays within one grace
    // window (FIX 2).
    let mut shutdown_deadline: Option<Instant> = None;

    loop {
        tokio::select! {
            biased;

            // Signals: begin graceful shutdown.
            _ = sigint.recv(), if !shutting_down => {
                tracing::info!("SIGINT received, shutting down gracefully");
                shutting_down = true;
            }
            _ = sigterm.recv(), if !shutting_down => {
                tracing::info!("SIGTERM received, shutting down gracefully");
                shutting_down = true;
            }

            // A worker hit a fatal startup error (e.g. could not get an engine).
            // The DURABLE fatal signal is the `Fatal` Outcome the worker pushed on
            // the result channel (drained by the `result_rx.recv()` arm below);
            // this `notified()` arm is only a wakeup optimization so the main loop
            // reacts promptly even when the bounded work channel is full and no
            // outcome would otherwise arrive. Because `Notify` stores no permit,
            // this arm must NOT be relied on alone (FIX 3); if it fires first we
            // set a placeholder fatal that the real Fatal Outcome may refine.
            _ = shutdown_notify.notified(), if !shutting_down => {
                tracing::error!("worker signalled fatal shutdown");
                if fatal.is_none() {
                    fatal = Some("worker thread could not initialize engine".to_string());
                }
                shutting_down = true;
            }

            // A worker finished a record.
            maybe_outcome = result_rx.recv() => {
                match maybe_outcome {
                    Some(outcome) => {
                        let before = processed;
                        if let Some(msg) = handle_outcome(&channel, &mut in_flight, outcome, &mut processed).await {
                            fatal = Some(msg);
                            shutting_down = true;
                        }
                        // Throughput line every INTERVAL processed records (FIX 5b,
                        // mirrors Python's "Processed {n} adds, {speed} records/sec").
                        if processed > before && processed.is_multiple_of(STATS_INTERVAL) {
                            let elapsed = last_rate_at.elapsed().as_secs_f64();
                            let speed = if elapsed > 0.0 {
                                (STATS_INTERVAL as f64 / elapsed) as i64
                            } else {
                                -1
                            };
                            println!("Processed {processed} adds, {speed} records per second");
                            last_rate_at = Instant::now();
                        }
                    }
                    None => {
                        // All workers exited.
                        break;
                    }
                }
            }

            // Periodic stats + long-record monitoring.
            _ = monitor.tick() => {
                // Kick the stats thread (non-blocking); print its previous answer if any.
                let _ = stats_req_tx.send(());
                if let Ok(stats) = stats_resp_rx.try_recv() {
                    println!("\n{stats}\n");
                }
                monitor_long_records(&channel, &mut in_flight, long_record, threads).await;
            }

            // Stats answer arrived asynchronously.
            Some(stats) = stats_resp_rx.recv() => {
                println!("\n{stats}\n");
            }

            // Next delivery from RabbitMQ (only when not shutting down and we
            // have capacity; the bounded work channel enforces prefetch).
            delivery = consumer.next(), if !shutting_down => {
                match delivery {
                    Some(Ok(delivery)) => {
                        // A malformed record (unparseable JSON, non-object, or a
                        // missing/non-string DATA_SOURCE / RECORD_ID) is a POISON
                        // message: it can never succeed, so making it fatal would
                        // leave it unacked, the broker would requeue it, and we
                        // would crash-loop on it forever. Instead we DEAD-LETTER it
                        // — basic_reject(requeue=false) to the DLQ — with a loud
                        // warning, then KEEP PROCESSING. This is exactly how the
                        // engine SzBadInputError / SENZ0082 path is handled. The DLQ
                        // is a visible, inspectable destination, so this is NOT a
                        // silent failure (FIX 1).
                        let info = match parse_record(&delivery.data) {
                            Ok(info) => info,
                            Err(e) => {
                                // Truncate the raw body in the log so a giant message
                                // cannot flood the log, but keep enough to inspect.
                                let raw = String::from_utf8_lossy(&delivery.data);
                                let body: String = raw.chars().take(2048).collect();
                                let truncated = if raw.len() > body.len() {
                                    " (truncated)"
                                } else {
                                    ""
                                };
                                tracing::warn!(
                                    "DEAD-LETTERING malformed record: {e} [{body}{truncated}]"
                                );
                                if let Err(re) = channel
                                    .basic_reject(
                                        delivery.delivery_tag,
                                        BasicRejectOptions { requeue: false },
                                    )
                                    .await
                                {
                                    tracing::error!(
                                        "basic_reject failed for malformed record {}: {re:#}",
                                        delivery.delivery_tag
                                    );
                                }
                                // Keep processing the next delivery; do NOT shut down.
                                continue;
                            }
                        };
                        let item = WorkItem {
                            delivery_tag: delivery.delivery_tag,
                            body: delivery.data.clone(),
                            info: info.clone(),
                        };
                        in_flight.insert(delivery.delivery_tag, InFlight {
                            info,
                            started: Instant::now(),
                            rejected: false,
                        });
                        // Send awaits when the pool is saturated (the backpressure we
                        // want), but must be cancellable on shutdown so a worker that
                        // died — leaving the bounded channel full — can never wedge
                        // the loop (FIX 1). Race the send against the shutdown signal.
                        tokio::select! {
                            biased;
                            _ = shutdown_notify.notified() => {
                                if fatal.is_none() {
                                    fatal = Some(
                                        "worker thread could not initialize engine".to_string(),
                                    );
                                }
                                shutting_down = true;
                            }
                            send_res = work_tx.send(item) => {
                                if send_res.is_err() {
                                    fatal = Some("worker pool closed unexpectedly".to_string());
                                    shutting_down = true;
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        fatal = Some(format!("AMQP consume error: {e}"));
                        shutting_down = true;
                    }
                    None => {
                        tracing::info!("consumer stream ended");
                        shutting_down = true;
                    }
                }
            }
        }

        // Once shutting down, stop accepting new work and drain the in-flight set.
        if shutting_down {
            // Single TOTAL grace budget shared by the in-flight drain AND the
            // bounded worker join below (FIX 2): the whole shutdown must finish
            // within `SHUTDOWN_GRACE` regardless of how the time is split.
            let deadline =
                *shutdown_deadline.get_or_insert_with(|| Instant::now() + SHUTDOWN_GRACE);
            // Closing the work sender now lets idle workers observe end-of-stream
            // and exit; in-flight engine calls still complete and report back.
            drop(work_tx);
            // We keep draining results until every in-flight delivery is resolved
            // or the grace window elapses.
            if !in_flight.is_empty() {
                // Bounded drain against the shared deadline (not per-recv): a
                // worker stuck on an uninterruptible engine call must not hang
                // shutdown forever. After the grace, we proceed to reject the
                // still-in-flight deliveries and close the connection regardless.
                while !in_flight.is_empty() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match tokio::time::timeout(remaining, result_rx.recv()).await {
                        Ok(Some(outcome)) => {
                            if let Some(msg) =
                                handle_outcome(&channel, &mut in_flight, outcome, &mut processed)
                                    .await
                                && fatal.is_none()
                            {
                                fatal = Some(msg);
                            }
                        }
                        // Channel closed (all workers exited) or grace elapsed.
                        Ok(None) | Err(_) => break,
                    }
                }
            }
            break;
        }
    }

    // --- Shutdown ------------------------------------------------------------
    // FIX 2: we must NOT call `destroy_global_instance()` (in `main`) while any
    // worker is still inside an uninterruptible `add_record` — that frees the
    // native engine out from under the live call => use-after-free / segfault.
    // But we also must NOT block shutdown UNBOUNDED on a stuck worker (past
    // `docker stop`'s SIGTERM grace, after which we get SIGKILLed). So we do a
    // BOUNDED join within whatever remains of the shared grace window, and report
    // to `main` whether EVERY worker actually finished. `main` only destroys the
    // environment when all workers joined; otherwise it skips the destroy and
    // lets process exit reclaim everything. Tradeoff: a stuck engine call may
    // leak the native environment on exit (the OS reclaims process memory anyway)
    // — a leak-on-exit is acceptable; a use-after-free is not.
    tracing::info!("drain window elapsed; finalizing shutdown");
    drop(stats_req_tx);

    // Bounded join: poll worker completion until they all finish or the shared
    // grace deadline elapses. `JoinHandle::is_finished()` lets us observe
    // completion without an unbounded blocking `join()`.
    let join_deadline = shutdown_deadline.unwrap_or_else(|| Instant::now() + SHUTDOWN_GRACE);
    while Instant::now() < join_deadline && workers.iter().any(|h| !h.is_finished()) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let all_workers_joined = workers.iter().all(|h| h.is_finished());
    if all_workers_joined {
        // Every worker is past its engine call; join to reap them cleanly. These
        // joins return immediately because each handle is already finished.
        for h in workers {
            let _ = h.join();
        }
        tracing::info!("all engine workers finished; safe to destroy environment");
    } else {
        // At least one worker is still inside an uninterruptible engine call.
        // Detach the handles (drop without join) and signal `main` to SKIP the
        // environment destroy so we do not free engine state under a live call.
        tracing::warn!(
            "shutdown grace elapsed with workers still in engine calls; \
             skipping environment destroy to avoid use-after-free (leak-on-exit)"
        );
        drop(workers);
    }
    // The stats thread only ever blocks on a channel recv or a (short) get_stats
    // call; detaching it is safe and it is reaped at process exit.
    drop(stats_handle);

    // Reject (no requeue) any deliveries still tracked. Per Python semantics the
    // broker requeues unacked messages on disconnect; an explicit reject here
    // dead-letters records the long-record monitor already gave up on. Either way
    // nothing is silently acked.
    for (tag, mut f) in in_flight.drain() {
        if !f.rejected {
            f.rejected = true;
            tracing::warn!(
                "REJECTING in-flight on shutdown: {} : {}",
                f.info.data_source,
                f.info.record_id
            );
            let _ = channel
                .basic_reject(tag, BasicRejectOptions { requeue: false })
                .await;
        }
    }

    if let Err(e) = connection.close(0, "shutting down".into()).await {
        tracing::warn!("error closing connection: {e:#}");
    }

    println!("Processed total of {processed} adds");

    // Report the fatal (if any) and the worker-join status to `main` rather than
    // `bail!`ing: `main` needs `all_workers_joined` to gate the environment
    // destroy on every exit path, including the fatal one (FIX 2).
    Ok(RunOutcome {
        all_workers_joined,
        fatal,
    })
}

/// Applies a worker (or monitor) outcome to the AMQP channel.
///
/// Returns `Some(message)` if the outcome was fatal and the caller should begin
/// graceful shutdown.
async fn handle_outcome(
    channel: &lapin::Channel,
    in_flight: &mut HashMap<u64, InFlight>,
    outcome: Outcome,
    processed: &mut u64,
) -> Option<String> {
    let Outcome {
        delivery_tag,
        info,
        action,
    } = outcome;

    // If the monitor already rejected this delivery, ignore the late worker result.
    let already_rejected = in_flight
        .get(&delivery_tag)
        .map(|f| f.rejected)
        .unwrap_or(false);

    match action {
        Action::Ack(maybe_info) => {
            if already_rejected {
                // Was dead-lettered by the long-record monitor; do not ack.
                in_flight.remove(&delivery_tag);
                return None;
            }
            if let Some(resp) = maybe_info {
                println!("{resp}");
            }
            if let Err(e) = channel
                .basic_ack(delivery_tag, BasicAckOptions::default())
                .await
            {
                tracing::error!("basic_ack failed for {delivery_tag}: {e:#}");
            }
            in_flight.remove(&delivery_tag);
            *processed += 1;
            None
        }
        Action::RejectNoRequeue => {
            if !already_rejected {
                println!(
                    "REJECTING due to bad data or timeout: {} : {}",
                    info.data_source, info.record_id
                );
                if let Err(e) = channel
                    .basic_reject(delivery_tag, BasicRejectOptions { requeue: false })
                    .await
                {
                    tracing::error!("basic_reject failed for {delivery_tag}: {e:#}");
                }
            }
            in_flight.remove(&delivery_tag);
            *processed += 1;
            None
        }
        Action::Fatal(msg) => {
            tracing::error!(
                "fatal engine error on {} : {} -> {msg}",
                info.data_source,
                info.record_id
            );
            // Leave the delivery unacked so the broker can redeliver after we exit.
            in_flight.remove(&delivery_tag);
            Some(msg)
        }
    }
}

/// Long-record monitoring (port of the Python stuck-thread logic).
///
/// Records running longer than `2 * LONG_RECORD` are rejected to the dead-letter
/// queue. Records over `LONG_RECORD` are logged as still-processing.
///
/// DEVIATION: Python rejects the delivery but the worker thread keeps running
/// (Senzing engine calls are uninterruptible). We do the same — we dead-letter
/// the broker-side delivery and mark it `rejected` so the eventual (ignored)
/// worker result does not also ack it. We cannot cancel the engine call in
/// either language.
async fn monitor_long_records(
    channel: &lapin::Channel,
    in_flight: &mut HashMap<u64, InFlight>,
    long_record: Duration,
    max_workers: usize,
) {
    let now = Instant::now();
    let mut to_reject: Vec<u64> = Vec::new();
    // Count of records past LONG_RECORD; if it reaches the worker count, every
    // thread is wedged on a long-running record (FIX 5a, mirrors Python).
    let mut num_stuck: usize = 0;

    for (tag, f) in in_flight.iter() {
        let duration = now.duration_since(f.started);
        if !f.rejected && duration > long_record * 2 {
            to_reject.push(*tag);
        }
        if duration > long_record {
            num_stuck += 1;
            tracing::info!(
                "Still processing ({:.3} min, rejected: {}): {} : {}",
                duration.as_secs_f64() / 60.0,
                f.rejected,
                f.info.data_source,
                f.info.record_id
            );
        }
    }

    if num_stuck >= max_workers {
        println!("All {max_workers} threads are stuck on long running records");
    }

    for tag in to_reject {
        if let Some(f) = in_flight.get_mut(&tag) {
            f.rejected = true;
            println!("REJECTING: {} : {}", f.info.data_source, f.info.record_id);
            if let Err(e) = channel
                .basic_reject(tag, BasicRejectOptions { requeue: false })
                .await
            {
                tracing::error!("basic_reject (long record) failed for {tag}: {e:#}");
            }
        }
    }
}

/// Worker thread loop: own engine, pull work, call add_record, report outcome.
fn worker_loop(
    worker_id: usize,
    env: Arc<SzEnvironmentCore>,
    work_rx: Arc<std::sync::Mutex<mpsc::Receiver<WorkItem>>>,
    result_tx: mpsc::Sender<Outcome>,
    flags: Option<SzFlags>,
    want_info: bool,
    shutdown_notify: Arc<Notify>,
) {
    let engine = match env.get_engine() {
        Ok(e) => e,
        Err(e) => {
            // A worker that cannot get an engine is a fatal misconfiguration. If
            // it just returned, the bounded work channel would fill with
            // `thread-count` deliveries and the async loop would block forever on
            // `work_tx.send().await` — no outcome, in_flight never drains, SIGTERM
            // never serviced.
            tracing::error!("worker {worker_id}: failed to get engine: {e}");
            // DURABLE fatal signal (FIX 3): send a Fatal Outcome on the result
            // channel, which the main loop's `result_rx.recv()` arm ALWAYS drains.
            // `Notify::notify_waiters()` stores no permit — if the main `select!`
            // is not parked on `notified()` at that instant the wakeup is lost, so
            // it cannot be the sole carrier of the fatal. The result channel does
            // not lose messages, guaranteeing the fatal reaches `run()` and the
            // process exits non-zero. `delivery_tag = 0` is a sentinel (lapin
            // delivery tags start at 1, so it never collides with a real delivery
            // in `in_flight`). The Notify call below stays purely as a wakeup
            // optimization so the main loop reacts promptly.
            let _ = result_tx.blocking_send(Outcome {
                delivery_tag: 0,
                info: RecordInfo {
                    data_source: String::new(),
                    record_id: String::new(),
                },
                action: Action::Fatal(format!(
                    "worker {worker_id} could not initialize engine: {e}"
                )),
            });
            shutdown_notify.notify_waiters();
            return;
        }
    };

    loop {
        // Acquire the next work item. We hold the lock only for the recv.
        let item = {
            let mut rx = match work_rx.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner(),
            };
            rx.blocking_recv()
        };
        let Some(item) = item else {
            // Channel closed -> graceful exit.
            break;
        };

        let WorkItem {
            delivery_tag,
            body,
            info,
        } = item;

        let body_str = match std::str::from_utf8(&body) {
            Ok(s) => s,
            Err(_) => {
                // Non-UTF-8 body is bad input -> dead-letter.
                tracing::warn!("worker {worker_id}: non-UTF-8 message body");
                let _ = result_tx.blocking_send(Outcome {
                    delivery_tag,
                    info,
                    action: Action::RejectNoRequeue,
                });
                continue;
            }
        };

        let action = match engine.add_record(&info.data_source, &info.record_id, body_str, flags) {
            Ok(resp) => Action::Ack(if want_info { Some(resp) } else { None }),
            Err(e) => match classify_error(&e) {
                crate::record::ErrorClass::BadInputOrTimeout => Action::RejectNoRequeue,
                crate::record::ErrorClass::Fatal => Action::Fatal(e.to_string()),
            },
        };

        if result_tx
            .blocking_send(Outcome {
                delivery_tag,
                info,
                action,
            })
            .is_err()
        {
            // Async side gone; nothing more to do.
            break;
        }
    }
    tracing::debug!("worker {worker_id} finished");
}

/// Dedicated thread that owns one engine and answers stats requests.
fn stats_loop(
    env: Arc<SzEnvironmentCore>,
    req_rx: std::sync::mpsc::Receiver<()>,
    resp_tx: mpsc::Sender<String>,
) {
    let engine = match env.get_engine() {
        Ok(e) => e,
        Err(e) => {
            tracing::error!("stats thread: failed to get engine: {e}");
            return;
        }
    };
    while req_rx.recv().is_ok() {
        match engine.get_stats() {
            Ok(stats) => {
                if resp_tx.blocking_send(stats).is_err() {
                    break;
                }
            }
            Err(e) => tracing::warn!("get_stats failed: {e}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flags_set_bit62_when_info_set() {
        // `add_record_flags(true)` deterministically produces the bit-62 value
        // (mirroring the Python flag name). The bit is INERT at the engine level
        // — the SDK always calls the WithInfo helper — so this only asserts the
        // function's output shape, not any engine behavior (FIX 4).
        assert_eq!(
            add_record_flags(true),
            Some(SzFlags::from_bits_retain(SZ_WITH_INFO_BITS))
        );
        let bits = add_record_flags(true).unwrap().bits();
        assert_ne!(bits & SZ_WITH_INFO_BITS, 0);
    }

    #[test]
    fn flags_default_when_info_unset() {
        assert_eq!(add_record_flags(false), Some(SzFlags::ADD_RECORD_DEFAULT));
        assert_eq!(
            add_record_flags(false).unwrap().bits() & SZ_WITH_INFO_BITS,
            0
        );
    }

    #[test]
    fn with_info_bit_matches_redoer() {
        // Must equal SZ_WITH_INFO = 1 << 62, consistent with sz_simple_redoer_rust.
        assert_eq!(SZ_WITH_INFO_BITS, 1u64 << 62);
    }
}
