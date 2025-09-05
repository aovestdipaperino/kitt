//! KITT - Kafka Integrated Throughput Testing Library
//!
//! This library provides profiling capabilities and common modules
//! for the KITT Kafka throughput testing tool.

pub mod profiling;

// Re-export commonly used types
pub use profiling::{generate_report, KittOperation};
