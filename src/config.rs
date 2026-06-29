//! Configuration: CLI arguments (clap derive) with environment-variable fallbacks.
//!
//! Priority: CLI argument > environment variable > default. The environment
//! variable names mirror the Python `sz_rabbit_consumer-v4` exactly so existing
//! deployments need no changes.

use clap::Parser;

/// Default long-record threshold, in seconds (matches the Python `LONG_RECORD`).
pub const DEFAULT_LONG_RECORD_SECS: u64 = 300;

/// RabbitMQ consumer that submits records to the Senzing engine.
#[derive(Parser, Debug, Clone)]
#[command(
    name = "sz_rabbit_consumer",
    version,
    about = "Consume JSON records from RabbitMQ and add them to Senzing",
    long_about = None
)]
pub struct Args {
    /// RabbitMQ server URL (env: SENZING_AMQP_URL).
    #[arg(short = 'u', long = "url", env = "SENZING_AMQP_URL")]
    pub url: Option<String>,

    /// Source queue name (env: SENZING_RABBITMQ_QUEUE).
    #[arg(short = 'q', long = "queue", env = "SENZING_RABBITMQ_QUEUE")]
    pub queue: Option<String>,

    /// Print the WithInfo response for each added record.
    #[arg(short = 'i', long = "info", default_value_t = false)]
    pub info: bool,

    /// Output Senzing engine debug trace information (verbose logging).
    #[arg(short = 't', long = "debugTrace", default_value_t = false)]
    pub debug_trace: bool,
}

/// Fully resolved runtime configuration after applying defaults and validating
/// required values.
#[derive(Debug, Clone)]
pub struct Config {
    pub engine_config: String,
    pub url: String,
    pub queue: String,
    pub info: bool,
    pub debug_trace: bool,
    pub threads: usize,
    pub long_record_secs: u64,
}

impl Config {
    /// Resolves the configuration from parsed [`Args`] plus the environment.
    ///
    /// Returns an error message string for any missing required value so the
    /// caller can print it and exit with a non-zero status (loud failure).
    pub fn resolve(args: Args) -> Result<Self, String> {
        let engine_config = std::env::var("SENZING_ENGINE_CONFIGURATION_JSON")
            .ok()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                concat!(
                    "The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set ",
                    "with a proper JSON configuration.\n",
                    "Please see https://senzing.zendesk.com/hc/en-us/articles/",
                    "360038774134-G2Module-Configuration-and-the-Senzing-API"
                )
                .to_string()
            })?;

        let url = args.url.filter(|s| !s.is_empty()).ok_or_else(|| {
            "No RabbitMQ URL provided (use --url or SENZING_AMQP_URL)".to_string()
        })?;

        let queue = args.queue.filter(|s| !s.is_empty()).ok_or_else(|| {
            "No queue provided (use --queue or SENZING_RABBITMQ_QUEUE)".to_string()
        })?;

        // SENZING_THREADS_PER_PROCESS: 0 or unset => default to the CPU count
        // (the Python lets ThreadPoolExecutor choose, which defaults to CPU-based).
        let threads = match std::env::var("SENZING_THREADS_PER_PROCESS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
        {
            Some(n) if n > 0 => n,
            _ => num_cpus::get(),
        };

        let long_record_secs = std::env::var("LONG_RECORD")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_LONG_RECORD_SECS);

        Ok(Self {
            engine_config,
            url,
            queue,
            info: args.info,
            debug_trace: args.debug_trace,
            threads,
            long_record_secs,
        })
    }
}
