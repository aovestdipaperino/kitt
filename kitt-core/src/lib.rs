//! Kitt Core - Kafka throughput testing library
//!
//! This library provides both high-level and low-level APIs for measuring
//! Kafka producer/consumer throughput.

pub mod client;
pub mod config;
pub mod consumer;
pub mod events;
pub mod producer;
pub mod runner;
pub mod utils;

// Re-export public API
pub use client::{KafkaClient, TopicMetadata};
pub use config::{KeyStrategy, MessageSize, ProduceOnlyMode, TestConfig, TestConfigBuilder};
pub use consumer::Consumer;
pub use events::{TestEvent, TestPhase, TestResults};
pub use producer::Producer;
pub use runner::{run_test, TestHandle};
