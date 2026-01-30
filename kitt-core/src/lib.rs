//! Kitt Core - Kafka throughput testing library
//!
//! This library provides both high-level and low-level APIs for measuring
//! Kafka producer/consumer throughput.

pub mod config;
pub mod events;
pub mod utils;

// Re-export public API
pub use config::{KeyStrategy, MessageSize, ProduceOnlyMode, TestConfig, TestConfigBuilder};
pub use events::{TestEvent, TestPhase, TestResults};
