//! Entry point for the RabbitMQ -> Senzing consumer.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use sz_rust_sdk::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

use sz_rabbit_consumer::config::{Args, Config};

fn main() -> ExitCode {
    // Logging: SENZING_LOG_LEVEL controls the default level (parity with Python),
    // RUST_LOG (env-filter) can override for finer control.
    let log_level = std::env::var("SENZING_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(map_log_level(&log_level)));
    fmt().with_env_filter(env_filter).with_target(false).init();

    let args = Args::parse();
    let config = match Config::resolve(args) {
        Ok(c) => c,
        Err(msg) => {
            eprintln!("{msg}");
            return ExitCode::from(255); // matches Python exit(-1)
        }
    };

    // Initialize the Senzing environment singleton. Engine calls are blocking and
    // happen on OS threads; per the SDK the environment is created once per
    // process and shared (each thread derives its own engine handle).
    let env: Arc<SzEnvironmentCore> = match SzEnvironmentCore::get_instance(
        "sz_rabbit_consumer",
        &config.engine_config,
        config.debug_trace,
    ) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("Failed to initialize Senzing environment: {e}");
            return ExitCode::from(255);
        }
    };

    // Build a multi-threaded tokio runtime for the AMQP I/O layer only.
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("Failed to build tokio runtime: {e}");
            return ExitCode::from(255);
        }
    };

    let result = runtime.block_on(sz_rabbit_consumer::run(config, env));

    // FIX 2 (use-after-free guard): only tear down the Senzing environment when
    // EVERY engine worker thread actually finished. `run()` does a bounded join
    // within the shutdown grace and reports `all_workers_joined`. If a worker is
    // still parked inside an uninterruptible `add_record`, calling
    // `destroy_global_instance()` would free the native engine out from under the
    // live call -> segfault. In that case we SKIP the destroy and let process
    // exit reclaim everything: a leak-on-exit is acceptable, a use-after-free is
    // not. A startup `Err` from `run()` (e.g. RabbitMQ connect failed) is also
    // treated conservatively as "do not destroy".
    match &result {
        Ok(outcome) => {
            if outcome.all_workers_joined {
                if let Err(e) = SzEnvironmentCore::destroy_global_instance() {
                    tracing::warn!("error destroying Senzing environment: {e}");
                }
            } else {
                tracing::warn!(
                    "skipping Senzing environment destroy: a worker may still be in an \
                     engine call (leak-on-exit to avoid use-after-free)"
                );
            }
            match &outcome.fatal {
                None => ExitCode::SUCCESS,
                Some(msg) => {
                    eprintln!("Shutting down due to error: {msg}");
                    ExitCode::from(255)
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                "skipping Senzing environment destroy after run() error \
                 (leak-on-exit to avoid use-after-free)"
            );
            eprintln!("{e:#}");
            ExitCode::from(255)
        }
    }
}

/// Maps the Python-style SENZING_LOG_LEVEL names onto tracing levels.
fn map_log_level(level: &str) -> &'static str {
    match level.to_lowercase().as_str() {
        "notset" | "debug" => "debug",
        "warning" | "warn" => "warn",
        "error" => "error",
        "fatal" | "critical" => "error",
        _ => "info",
    }
}
