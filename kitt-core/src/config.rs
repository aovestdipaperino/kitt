//! Configuration types for throughput tests
//!
//! This module provides the `TestConfig` struct and its builder for
//! configuring Kafka throughput tests.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::sync::Arc;
use std::time::Duration;

/// Message size configuration
#[derive(Debug, Clone)]
pub enum MessageSize {
    /// Fixed message size in bytes
    Fixed(usize),
    /// Variable message size with min and max bounds (inclusive)
    Range(usize, usize),
}

impl MessageSize {
    /// Parses a message size specification from a string
    pub fn parse(s: &str) -> Result<Self> {
        if let Some((min_str, max_str)) = s.split_once('-') {
            let min = min_str.parse::<usize>()?;
            let max = max_str.parse::<usize>()?;
            if min > max {
                return Err(anyhow!("Invalid range: min {} > max {}", min, max));
            }
            Ok(MessageSize::Range(min, max))
        } else {
            let size = s.parse::<usize>()?;
            Ok(MessageSize::Fixed(size))
        }
    }

    /// Generates a message size based on the configuration
    pub fn generate_size(&self) -> usize {
        match self {
            MessageSize::Fixed(size) => *size,
            MessageSize::Range(min, max) => thread_rng().gen_range(*min..=*max),
        }
    }
}

impl Default for MessageSize {
    fn default() -> Self {
        MessageSize::Fixed(1024)
    }
}

/// Strategy for generating message keys
#[derive(Debug, Clone)]
pub enum KeyStrategy {
    /// No keys - messages have null keys
    NoKeys,
    /// Generate unique random keys on the fly
    RandomOnTheFly,
    /// Pick keys randomly from a pre-generated pool
    RandomPool(Arc<Vec<Bytes>>),
}

impl KeyStrategy {
    /// Creates a KeyStrategy from a pool size
    pub fn from_pool_size(pool_size: Option<usize>) -> Self {
        match pool_size {
            None => KeyStrategy::NoKeys,
            Some(0) => KeyStrategy::RandomOnTheFly,
            Some(size) => {
                let keys: Vec<Bytes> = (0..size)
                    .map(|_| Bytes::from(uuid::Uuid::new_v4().to_string()))
                    .collect();
                KeyStrategy::RandomPool(Arc::new(keys))
            }
        }
    }

    /// Generates a key based on the configured strategy
    pub fn generate_key(&self) -> Option<Bytes> {
        match self {
            KeyStrategy::NoKeys => None,
            KeyStrategy::RandomOnTheFly => Some(Bytes::from(uuid::Uuid::new_v4().to_string())),
            KeyStrategy::RandomPool(pool) => {
                let key = pool.choose(&mut thread_rng()).expect("pool should not be empty");
                Some(key.clone())
            }
        }
    }
}

impl Default for KeyStrategy {
    fn default() -> Self {
        KeyStrategy::NoKeys
    }
}

/// Mode for produce-only tests
#[derive(Debug, Clone)]
pub enum ProduceOnlyMode {
    /// Run for the configured duration
    TimeBased,
    /// Run until target bytes are sent
    DataTarget(u64),
}

/// Configuration for a throughput test
#[derive(Debug, Clone)]
pub struct TestConfig {
    /// Kafka broker address (host:port)
    pub broker: String,
    /// Topic name (None = auto-generate)
    pub topic: Option<String>,
    /// Number of partitions for the test topic
    pub partitions: i32,
    /// Replication factor for the test topic
    pub replication_factor: i16,
    /// Message size configuration
    pub message_size: MessageSize,
    /// Key generation strategy
    pub key_strategy: KeyStrategy,
    /// Messages per batch
    pub messages_per_batch: usize,
    /// Number of producer threads
    pub producer_threads: usize,
    /// Number of consumer threads
    pub consumer_threads: usize,
    /// Partitions per producer thread
    pub producer_partitions_per_thread: i32,
    /// Partitions per consumer thread
    pub consumer_partitions_per_thread: i32,
    /// Use sticky partition assignment
    pub sticky: bool,
    /// Test duration
    pub duration: Duration,
    /// Delay before consumers start fetching
    pub fetch_delay: Duration,
    /// Produce-only mode configuration
    pub produce_only: Option<ProduceOnlyMode>,
    /// Maximum backlog before backpressure (None = auto-calculate)
    pub max_backlog: Option<u64>,
    /// Enable message validation
    pub message_validation: bool,
    /// Simulated record processing time (ms)
    pub record_processing_time: u64,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            broker: "localhost:9092".to_string(),
            topic: None,
            partitions: 8,
            replication_factor: 1,
            message_size: MessageSize::default(),
            key_strategy: KeyStrategy::default(),
            messages_per_batch: 1,
            producer_threads: 4,
            consumer_threads: 4,
            producer_partitions_per_thread: 1,
            consumer_partitions_per_thread: 1,
            sticky: false,
            duration: Duration::from_secs(15),
            fetch_delay: Duration::ZERO,
            produce_only: None,
            max_backlog: None,
            message_validation: false,
            record_processing_time: 0,
        }
    }
}

/// Builder for TestConfig
#[derive(Debug, Default)]
pub struct TestConfigBuilder {
    config: TestConfig,
}

impl TestConfigBuilder {
    /// Create a new builder with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the broker address
    pub fn broker(mut self, broker: impl Into<String>) -> Self {
        self.config.broker = broker.into();
        self
    }

    /// Set the topic name (None = auto-generate)
    pub fn topic(mut self, topic: Option<String>) -> Self {
        self.config.topic = topic;
        self
    }

    /// Set the number of partitions
    pub fn partitions(mut self, partitions: i32) -> Self {
        self.config.partitions = partitions;
        self
    }

    /// Set the replication factor
    pub fn replication_factor(mut self, factor: i16) -> Self {
        self.config.replication_factor = factor;
        self
    }

    /// Set the message size
    pub fn message_size(mut self, size: MessageSize) -> Self {
        self.config.message_size = size;
        self
    }

    /// Set the key strategy
    pub fn key_strategy(mut self, strategy: KeyStrategy) -> Self {
        self.config.key_strategy = strategy;
        self
    }

    /// Set messages per batch
    pub fn messages_per_batch(mut self, count: usize) -> Self {
        self.config.messages_per_batch = count;
        self
    }

    /// Set the number of producer threads
    pub fn producer_threads(mut self, threads: usize) -> Self {
        self.config.producer_threads = threads;
        self
    }

    /// Set the number of consumer threads
    pub fn consumer_threads(mut self, threads: usize) -> Self {
        self.config.consumer_threads = threads;
        self
    }

    /// Set partitions per producer thread
    pub fn producer_partitions_per_thread(mut self, partitions: i32) -> Self {
        self.config.producer_partitions_per_thread = partitions;
        self
    }

    /// Set partitions per consumer thread
    pub fn consumer_partitions_per_thread(mut self, partitions: i32) -> Self {
        self.config.consumer_partitions_per_thread = partitions;
        self
    }

    /// Enable sticky partition assignment
    pub fn sticky(mut self, sticky: bool) -> Self {
        self.config.sticky = sticky;
        self
    }

    /// Set the test duration
    pub fn duration(mut self, duration: Duration) -> Self {
        self.config.duration = duration;
        self
    }

    /// Set the fetch delay
    pub fn fetch_delay(mut self, delay: Duration) -> Self {
        self.config.fetch_delay = delay;
        self
    }

    /// Set produce-only mode
    pub fn produce_only(mut self, mode: Option<ProduceOnlyMode>) -> Self {
        self.config.produce_only = mode;
        self
    }

    /// Set maximum backlog
    pub fn max_backlog(mut self, max: Option<u64>) -> Self {
        self.config.max_backlog = max;
        self
    }

    /// Enable message validation
    pub fn message_validation(mut self, enabled: bool) -> Self {
        self.config.message_validation = enabled;
        self
    }

    /// Set record processing time
    pub fn record_processing_time(mut self, time_ms: u64) -> Self {
        self.config.record_processing_time = time_ms;
        self
    }

    /// Build the TestConfig
    pub fn build(self) -> Result<TestConfig> {
        // Validate configuration
        if self.config.producer_threads == 0 {
            return Err(anyhow!("producer_threads must be at least 1"));
        }
        if self.config.consumer_threads == 0 && self.config.produce_only.is_none() {
            return Err(anyhow!("consumer_threads must be at least 1 (unless produce_only)"));
        }
        if self.config.producer_partitions_per_thread <= 0 {
            return Err(anyhow!("producer_partitions_per_thread must be at least 1"));
        }
        if self.config.consumer_partitions_per_thread <= 0 {
            return Err(anyhow!("consumer_partitions_per_thread must be at least 1"));
        }

        Ok(self.config)
    }
}

impl TestConfig {
    /// Create a builder for TestConfig
    pub fn builder() -> TestConfigBuilder {
        TestConfigBuilder::new()
    }
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
    fn test_config_builder() {
        let config = TestConfig::builder()
            .broker("kafka:9092")
            .producer_threads(8)
            .consumer_threads(4)
            .duration(Duration::from_secs(30))
            .build()
            .unwrap();

        assert_eq!(config.broker, "kafka:9092");
        assert_eq!(config.producer_threads, 8);
        assert_eq!(config.consumer_threads, 4);
        assert_eq!(config.duration, Duration::from_secs(30));
    }

    #[test]
    fn test_config_builder_validation() {
        let result = TestConfig::builder()
            .producer_threads(0)
            .build();
        assert!(result.is_err());
    }
}
