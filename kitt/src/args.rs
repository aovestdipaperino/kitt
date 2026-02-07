//! Command-line argument types for KITT
//!
//! This module contains CLI argument parsing types including the main Args struct.
//! The MessageSize and KeyStrategy types are re-exported from kitt-core.

use std::time::Duration;

use clap::Parser;

// Re-export types from kitt-core for use by main.rs
pub use kitt_core::{KeyStrategy, MessageSize};

/// Parse a human-readable duration string (e.g., "1h30m", "15s", "2h", "90s")
fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

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

    /// Measurement duration (e.g., "15s", "1m", "1h30m", "2m30s")
    #[arg(short, long, default_value = "15s", value_parser = parse_duration)]
    pub duration: Duration,

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

    /// Number of random keys in the pool (0 = null keys, default: 16)
    #[arg(long, default_value = "16")]
    pub random_keys: usize,

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

    /// Topic name to use (auto-generated if not specified)
    #[arg(long)]
    pub topic: Option<String>,

    /// Use an existing topic instead of creating one (requires --topic)
    #[arg(long, default_value = "false", requires = "topic")]
    pub use_existing_topic: bool,
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

    #[test]
    fn test_default_random_keys() {
        let args = Args::try_parse_from(&["kitt"]).unwrap();
        assert_eq!(args.random_keys, 16);
    }

    #[test]
    fn test_random_keys_zero_means_no_keys() {
        let args = Args::try_parse_from(&["kitt", "--random-keys", "0"]).unwrap();
        assert_eq!(args.random_keys, 0);
    }

    #[test]
    fn test_use_existing_topic_flag() {
        // Test default is false
        let args = Args::try_parse_from(&["kitt"]).unwrap();
        assert!(!args.use_existing_topic);
        assert!(args.topic.is_none());

        // Test flag with topic
        let args = Args::try_parse_from(&["kitt", "--use-existing-topic", "--topic", "my-topic"]).unwrap();
        assert!(args.use_existing_topic);
        assert_eq!(args.topic, Some("my-topic".to_string()));
    }

    #[test]
    fn test_use_existing_topic_requires_topic() {
        // --use-existing-topic without --topic should fail
        let result = Args::try_parse_from(&["kitt", "--use-existing-topic"]);
        assert!(result.is_err());
    }

    #[test]
    fn test_duration_default() {
        let args = Args::try_parse_from(&["kitt"]).unwrap();
        assert_eq!(args.duration, Duration::from_secs(15));
    }

    #[test]
    fn test_duration_seconds() {
        let args = Args::try_parse_from(&["kitt", "--duration", "30s"]).unwrap();
        assert_eq!(args.duration, Duration::from_secs(30));
    }

    #[test]
    fn test_duration_minutes() {
        let args = Args::try_parse_from(&["kitt", "--duration", "2m"]).unwrap();
        assert_eq!(args.duration, Duration::from_secs(120));
    }

    #[test]
    fn test_duration_hours() {
        let args = Args::try_parse_from(&["kitt", "--duration", "1h"]).unwrap();
        assert_eq!(args.duration, Duration::from_secs(3600));
    }

    #[test]
    fn test_duration_composite() {
        let args = Args::try_parse_from(&["kitt", "--duration", "1h30m"]).unwrap();
        assert_eq!(args.duration, Duration::from_secs(5400));

        let args = Args::try_parse_from(&["kitt", "--duration", "2m30s"]).unwrap();
        assert_eq!(args.duration, Duration::from_secs(150));
    }

    #[test]
    fn test_duration_short_flag() {
        let args = Args::try_parse_from(&["kitt", "-d", "45s"]).unwrap();
        assert_eq!(args.duration, Duration::from_secs(45));
    }
}
