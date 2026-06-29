//! RabbitMQ -> Senzing consumer library.
//!
//! Rust port of the Python `sz_rabbit_consumer-v4`. Consumes JSON records from a
//! RabbitMQ queue and submits them to the Senzing engine via `add_record`.
//!
//! The AMQP I/O layer is async (`lapin` on a tokio runtime); the Senzing engine
//! work runs on a pool of `std::thread` workers, each owning its own engine
//! handle, because the Senzing SDK requires synchronous engine calls on OS
//! threads. See [`consumer`] for the bridge design.

pub mod config;
pub mod consumer;
pub mod record;

pub use config::{Args, Config};
pub use consumer::run;
