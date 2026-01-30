//! Command-line argument types for KITT
//!
//! This module contains CLI argument parsing types including the main Args struct.
//! The MessageSize and KeyStrategy types are re-exported from kitt-core.

use clap::Parser;

// Re-export types from kitt-core for use by main.rs
pub use kitt_core::{KeyStrategy, MessageSize};

/// Command-line arguments for configuring the throughput test
#[derive(Parser)]
#[command(name = "kitt")]
#[command(about = "Kafka throughput measurement tool")]
pub struct Args {
    /// Kafka broker address
    #[arg(short, long, default_value = "localhost:9092")]
    pub broker: String,

    /// Number of partitions assigned to each producer thread
    #[arg(short, long, default_value = "1")]
    pub producer_partitions_per_thread: i32,

    /// Number of partitions assigned to each consumer thread
    #[arg(short, long, default_value = "1")]
    pub consumer_partitions_per_thread: i32,

    /// Number of producer threads
    #[arg(long, default_value = "4")]
    pub producer_threads: i32,

    /// Number of consumer threads
    #[arg(long, default_value = "4")]
    pub consumer_threads: i32,

    /// Use sticky producer-partition assignment with LCM-based partition calculation (legacy mode)
    #[arg(long, default_value = "false")]
    pub sticky: bool,

    /// Number of producer/consumer threads (only used with --sticky mode)
    #[arg(short, long)]
    pub threads: Option<i32>,

    /// Message size in bytes (e.g., "1024") or range (e.g., "100-1000")
    #[arg(short, long, default_value = "1024")]
    pub message_size: String,

    /// Measurement duration in seconds
    #[arg(short, long, default_value = "15")]
    pub duration_secs: u64,

    /// Enable detailed FETCH response diagnostics for troubleshooting
    #[arg(long, default_value = "false")]
    pub debug_fetch: bool,

    /// Enable detailed PRODUCE response diagnostics for troubleshooting
    #[arg(long, default_value = "false")]
    pub debug_produce: bool,

    /// Initial delay in seconds before consumers start fetching (helps test backlog handling)
    #[arg(long, default_value = "0")]
    pub fetch_delay: u64,

    /// Run a profiling demonstration without connecting to Kafka
    #[arg(long, default_value = "false")]
    pub profile_demo: bool,

    /// Enable message validation for produce/consume responses (disabled by default for better performance)
    #[arg(long, default_value = "false")]
    pub message_validation: bool,

    /// Generate and display profiling report at the end (disabled by default)
    #[arg(long, default_value = "false")]
    pub profile_report: bool,

    /// Disable audio playback during animation
    #[arg(long, default_value = "false")]
    pub silent: bool,

    /// Generate random keys for messages. If a value is provided, creates a pool of that size
    /// to pick keys from. If no value is provided, generates unique random keys on the fly.
    #[arg(long, num_args = 0..=1, default_missing_value = "0")]
    pub random_keys: Option<usize>,

    /// Number of messages to include in each record batch (default: 1)
    #[arg(long, default_value = "1")]
    pub messages_per_batch: usize,

    /// Simulated processing time per record in milliseconds (default: 0 = no delay)
    #[arg(long, default_value = "0")]
    pub record_processing_time: u64,

    /// Produce-only mode: skip consumers and backpressure to measure pure producer throughput.
    /// Optionally specify a data target (e.g., "1GB", "500MB") to run until that amount is sent.
    #[arg(long, num_args = 0..=1, default_missing_value = "")]
    pub produce_only: Option<String>,

    /// Quiet mode: suppress all UI output and print machine-readable results
    #[arg(short, long, default_value = "false")]
    pub quiet: bool,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_size_parse_fixed() {
        let size = MessageSize::parse("1024").unwrap();
        match size {
            MessageSize::Fixed(s) => assert_eq!(s, 1024),
            _ => panic!("Expected Fixed variant"),
        }
    }

    #[test]
    fn test_message_size_parse_range() {
        let size = MessageSize::parse("100-1000").unwrap();
        match size {
            MessageSize::Range(min, max) => {
                assert_eq!(min, 100);
                assert_eq!(max, 1000);
            }
            _ => panic!("Expected Range variant"),
        }
    }

    #[test]
    fn test_message_size_parse_invalid_range() {
        // min > max should fail
        let result = MessageSize::parse("1000-100");
        assert!(result.is_err());
    }

    #[test]
    fn test_message_size_parse_invalid_number() {
        let result = MessageSize::parse("abc");
        assert!(result.is_err());
    }

    #[test]
    fn test_message_size_generate_fixed() {
        let size = MessageSize::Fixed(512);
        assert_eq!(size.generate_size(), 512);
    }

    #[test]
    fn test_message_size_generate_range() {
        let size = MessageSize::Range(100, 200);
        for _ in 0..100 {
            let generated = size.generate_size();
            assert!(generated >= 100 && generated <= 200);
        }
    }

    #[test]
    fn test_key_strategy_no_keys() {
        let strategy = KeyStrategy::from_pool_size(None);
        match strategy {
            KeyStrategy::NoKeys => {}
            _ => panic!("Expected NoKeys variant"),
        }
        assert!(strategy.generate_key().is_none());
    }

    #[test]
    fn test_key_strategy_random_on_the_fly() {
        let strategy = KeyStrategy::from_pool_size(Some(0));
        match strategy {
            KeyStrategy::RandomOnTheFly => {}
            _ => panic!("Expected RandomOnTheFly variant"),
        }
        // Should generate unique keys
        let key1 = strategy.generate_key();
        let key2 = strategy.generate_key();
        assert!(key1.is_some());
        assert!(key2.is_some());
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_key_strategy_random_pool() {
        let strategy = KeyStrategy::from_pool_size(Some(10));
        match &strategy {
            KeyStrategy::RandomPool(pool) => {
                assert_eq!(pool.len(), 10);
            }
            _ => panic!("Expected RandomPool variant"),
        }
        // Should generate keys from the pool
        let key = strategy.generate_key();
        assert!(key.is_some());
    }
}
