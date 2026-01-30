# Kitt Core Extraction Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Extract a reusable `kitt-core` library from the Kitt CLI, providing both high-level runner and low-level building blocks with channel-based event reporting.

**Architecture:** Three-crate workspace (`kitt-core` library, `kitt` CLI, `kitt_throbbler` unchanged). Core exposes `run_test()` returning a `TestHandle` with event receiver. CLI subscribes to events for UI updates.

**Tech Stack:** Rust, tokio (async), kafka-protocol, channel-based events (mpsc)

---

## Task 1: Create kitt-core Crate Structure

**Files:**
- Create: `kitt-core/Cargo.toml`
- Create: `kitt-core/src/lib.rs`
- Modify: `Cargo.toml` (workspace root)

**Step 1: Create kitt-core directory**

```bash
mkdir -p kitt-core/src
```

**Step 2: Create kitt-core/Cargo.toml**

```toml
[package]
name = "kitt-core"
version = "0.1.0"
edition = "2021"
description = "Core library for Kafka throughput testing"
license = "MIT"
authors = ["Enzo Lombardi <enzinol@gmail.com>"]

[lib]
name = "kitt_core"
path = "src/lib.rs"

[dependencies]
kafka-protocol = { version = "0.15.1", features = ["broker", "messages_enums"] }
tokio = { version = "1.41.0", features = ["full"] }
rand = "0.8.5"
anyhow = "1.0.80"
tracing = "0.1.37"
bytes = "1.10.1"
crc32c = "0.6"
serde = { version = "1.0.179", features = ["derive"] }
futures = "0.3"
indexmap = "2.0.0"
uuid = { version = "1.3.4", features = ["v4"] }
```

**Step 3: Create minimal kitt-core/src/lib.rs**

```rust
//! Kitt Core - Kafka throughput testing library
//!
//! This library provides both high-level and low-level APIs for measuring
//! Kafka producer/consumer throughput.

pub mod config;
pub mod events;

// Re-export public API
pub use config::{KeyStrategy, MessageSize, ProduceOnlyMode, TestConfig, TestConfigBuilder};
pub use events::{TestEvent, TestPhase, TestResults};
```

**Step 4: Update workspace Cargo.toml**

Add `"kitt-core"` to the workspace members list.

**Step 5: Run cargo check**

Run: `cargo check -p kitt-core`
Expected: Compilation errors (missing modules) - that's OK, we'll add them next

**Step 6: Commit**

```bash
git add kitt-core/Cargo.toml kitt-core/src/lib.rs Cargo.toml
git commit -m "feat(kitt-core): create crate structure"
```

---

## Task 2: Create Events Module

**Files:**
- Create: `kitt-core/src/events.rs`

**Step 1: Write events.rs**

```rust
//! Event types for test progress reporting
//!
//! This module defines the events emitted during test execution,
//! allowing callers to track progress and display UI feedback.

use std::time::Duration;

/// Events emitted during test execution
#[derive(Debug, Clone)]
pub enum TestEvent {
    /// Test phase has changed
    PhaseChange { phase: TestPhase },

    /// Periodic progress update
    Progress {
        messages_sent: u64,
        messages_received: u64,
        bytes_sent: u64,
        current_rate: f64,
        backlog_percent: u8,
        elapsed: Duration,
    },

    /// Non-fatal warning
    Warning { message: String },

    /// Error during test (may or may not be recoverable)
    Error { message: String, recoverable: bool },

    /// Test completed with final results
    Completed(TestResults),
}

/// Phases of test execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestPhase {
    /// Connecting to Kafka broker
    Connecting,
    /// Creating test topic
    CreatingTopic,
    /// Waiting for topic to be ready
    WaitingForTopic,
    /// Creating producer/consumer connections
    CreatingConnections,
    /// Test is running
    Running,
    /// Cleaning up (deleting topic)
    Cleanup,
}

/// Final results from a completed test
#[derive(Debug, Clone)]
pub struct TestResults {
    /// Total messages sent by all producers
    pub messages_sent: u64,
    /// Total messages received by all consumers
    pub messages_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Actual test duration
    pub duration: Duration,
    /// Overall throughput (messages/second)
    pub throughput: f64,
    /// Minimum observed rate during test
    pub min_rate: f64,
    /// Maximum observed rate during test
    pub max_rate: f64,
    /// Average backlog percentage (0-100)
    pub avg_backlog_percent: u8,
}
```

**Step 2: Run cargo check**

Run: `cargo check -p kitt-core`
Expected: Error about missing `config` module

**Step 3: Commit**

```bash
git add kitt-core/src/events.rs
git commit -m "feat(kitt-core): add events module"
```

---

## Task 3: Create Config Module

**Files:**
- Create: `kitt-core/src/config.rs`

**Step 1: Write config.rs with TestConfig and builder**

```rust
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
```

**Step 2: Run cargo check**

Run: `cargo check -p kitt-core`
Expected: Success

**Step 3: Run tests**

Run: `cargo test -p kitt-core`
Expected: All tests pass

**Step 4: Commit**

```bash
git add kitt-core/src/config.rs
git commit -m "feat(kitt-core): add config module with TestConfig builder"
```

---

## Task 4: Move Utils Module

**Files:**
- Create: `kitt-core/src/utils.rs`
- Modify: `kitt-core/src/lib.rs`

**Step 1: Copy utils.rs to kitt-core**

Copy the contents of `kitt/src/utils.rs` to `kitt-core/src/utils.rs` (unchanged).

**Step 2: Update kitt-core/src/lib.rs**

Add `pub mod utils;` to the module declarations.

**Step 3: Run tests**

Run: `cargo test -p kitt-core`
Expected: All tests pass

**Step 4: Commit**

```bash
git add kitt-core/src/utils.rs kitt-core/src/lib.rs
git commit -m "feat(kitt-core): add utils module"
```

---

## Task 5: Move KafkaClient Module

**Files:**
- Create: `kitt-core/src/client.rs`
- Modify: `kitt-core/src/lib.rs`

**Step 1: Copy kafka_client.rs to kitt-core as client.rs**

Copy the contents of `kitt/src/kafka_client.rs` to `kitt-core/src/client.rs`. Make `KafkaClient` public.

**Step 2: Update kitt-core/src/lib.rs**

```rust
pub mod client;
pub mod config;
pub mod events;
pub mod utils;

// Re-export public API
pub use client::KafkaClient;
pub use config::{KeyStrategy, MessageSize, ProduceOnlyMode, TestConfig, TestConfigBuilder};
pub use events::{TestEvent, TestPhase, TestResults};
```

**Step 3: Run cargo check**

Run: `cargo check -p kitt-core`
Expected: Success

**Step 4: Run tests**

Run: `cargo test -p kitt-core`
Expected: All tests pass

**Step 5: Commit**

```bash
git add kitt-core/src/client.rs kitt-core/src/lib.rs
git commit -m "feat(kitt-core): add KafkaClient module"
```

---

## Task 6: Move Producer Module

**Files:**
- Create: `kitt-core/src/producer.rs`
- Modify: `kitt-core/src/lib.rs`

**Step 1: Copy producer.rs to kitt-core**

Copy `kitt/src/producer.rs` to `kitt-core/src/producer.rs` with these changes:
- Remove `use crate::profiling::KittOperation;`
- Remove `use quantum_pulse::profile;`
- Remove all `profile!()` macro calls (keep the inner code)
- Update imports to use `crate::config::{KeyStrategy, MessageSize}`
- Update imports to use `crate::client::KafkaClient`

**Step 2: Update kitt-core/src/lib.rs**

Add `pub mod producer;` and re-export `Producer`.

**Step 3: Run cargo check**

Run: `cargo check -p kitt-core`
Expected: Success

**Step 4: Commit**

```bash
git add kitt-core/src/producer.rs kitt-core/src/lib.rs
git commit -m "feat(kitt-core): add Producer module (no profiling)"
```

---

## Task 7: Move Consumer Module

**Files:**
- Create: `kitt-core/src/consumer.rs`
- Modify: `kitt-core/src/lib.rs`

**Step 1: Copy consumer.rs to kitt-core**

Copy `kitt/src/consumer.rs` to `kitt-core/src/consumer.rs` with these changes:
- Remove `use crate::profiling::KittOperation;`
- Remove `use quantum_pulse::profile;`
- Remove all `profile!()` macro calls (keep the inner code)
- Update imports to use `crate::client::KafkaClient`
- Update imports to use `crate::utils::verify_record_batch_crc`

**Step 2: Update kitt-core/src/lib.rs**

Add `pub mod consumer;` and re-export `Consumer`.

**Step 3: Run cargo check**

Run: `cargo check -p kitt-core`
Expected: Success

**Step 4: Run tests**

Run: `cargo test -p kitt-core`
Expected: All tests pass

**Step 5: Commit**

```bash
git add kitt-core/src/consumer.rs kitt-core/src/lib.rs
git commit -m "feat(kitt-core): add Consumer module (no profiling)"
```

---

## Task 8: Create Runner Module

**Files:**
- Create: `kitt-core/src/runner.rs`
- Modify: `kitt-core/src/lib.rs`

**Step 1: Create runner.rs with TestHandle and run_test**

```rust
//! Test runner for executing throughput tests
//!
//! This module provides the main `run_test` function and `TestHandle` for
//! running Kafka throughput tests with progress reporting.

use crate::client::KafkaClient;
use crate::config::{ProduceOnlyMode, TestConfig};
use crate::consumer::Consumer;
use crate::events::{TestEvent, TestPhase, TestResults};
use crate::producer::{ProduceConfig, Producer};
use crate::utils::{format_bytes, lcm};

use anyhow::{anyhow, Result};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Instant};
use tracing::{debug, info, warn};

/// Base maximum backlog before applying backpressure
const BASE_MAX_BACKLOG: u64 = 1000;

/// Channel buffer size for events
const EVENT_CHANNEL_SIZE: usize = 100;

/// Handle to a running test
///
/// Provides access to progress events and methods to wait for or abort the test.
pub struct TestHandle {
    /// Receiver for test events
    pub events: mpsc::Receiver<TestEvent>,
    /// Handle to the test task
    handle: JoinHandle<Result<TestResults>>,
}

impl TestHandle {
    /// Wait for the test to complete and return results
    pub async fn wait(self) -> Result<TestResults> {
        self.handle.await?
    }

    /// Abort the test
    pub fn abort(&self) {
        self.handle.abort();
    }
}

/// Run a throughput test with the given configuration
///
/// Returns a `TestHandle` that provides:
/// - `events`: Channel receiver for progress updates
/// - `wait()`: Async method to wait for completion
/// - `abort()`: Method to cancel the test
///
/// # Example
///
/// ```ignore
/// let config = TestConfig::builder()
///     .broker("localhost:9092")
///     .duration(Duration::from_secs(30))
///     .build()?;
///
/// let mut handle = run_test(config).await?;
///
/// while let Some(event) = handle.events.recv().await {
///     match event {
///         TestEvent::Progress { current_rate, .. } => println!("{:.0} msg/s", current_rate),
///         TestEvent::Completed(results) => println!("Done: {:.0} msg/s", results.throughput),
///         _ => {}
///     }
/// }
///
/// let results = handle.wait().await?;
/// ```
pub async fn run_test(config: TestConfig) -> Result<TestHandle> {
    let (tx, rx) = mpsc::channel(EVENT_CHANNEL_SIZE);

    let handle = tokio::spawn(async move {
        run_test_inner(config, tx).await
    });

    Ok(TestHandle { events: rx, handle })
}

/// Internal test runner
async fn run_test_inner(
    config: TestConfig,
    events: mpsc::Sender<TestEvent>,
) -> Result<TestResults> {
    // Phase: Connecting
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::Connecting }).await;

    let admin_client = Arc::new(
        KafkaClient::connect(&config.broker)
            .await
            .map_err(|e| anyhow!("Failed to connect admin client: {}", e))?,
    );
    let api_versions = admin_client.api_versions.clone();

    // Calculate thread configuration
    let (num_producer_threads, num_consumer_threads, total_partitions) =
        calculate_thread_config(&config)?;

    // Generate topic name
    let topic_name = config.topic.clone().unwrap_or_else(generate_topic_name);

    // Phase: Creating topic
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::CreatingTopic }).await;

    admin_client
        .create_topic(&topic_name, total_partitions, config.replication_factor)
        .await
        .map_err(|e| anyhow!("Topic creation failed: {}", e))?;

    // Phase: Waiting for topic
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::WaitingForTopic }).await;
    sleep(Duration::from_secs(3)).await;

    // Phase: Creating connections
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::CreatingConnections }).await;

    let producers = create_producers(
        &config,
        &topic_name,
        total_partitions,
        num_producer_threads,
        &api_versions,
    )
    .await?;

    let consumers = create_consumers(
        &config,
        &topic_name,
        num_consumer_threads,
        &api_versions,
    )
    .await?;

    // Calculate backlog threshold
    let produce_only_target = match &config.produce_only {
        Some(ProduceOnlyMode::DataTarget(bytes)) => Some(*bytes),
        _ => None,
    };
    let max_backlog = calculate_max_backlog(&config, produce_only_target);

    // Phase: Running
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::Running }).await;

    let measurement_duration = config.duration;
    let total_test_duration = measurement_duration + config.fetch_delay;

    // Run the measurement
    let results = run_measurement(
        &config,
        producers,
        consumers,
        total_test_duration,
        measurement_duration,
        max_backlog,
        produce_only_target,
        events.clone(),
    )
    .await;

    // Phase: Cleanup
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::Cleanup }).await;

    if let Err(e) = admin_client.delete_topic(&topic_name).await {
        let _ = events.send(TestEvent::Warning {
            message: format!("Failed to delete topic '{}': {}", topic_name, e),
        }).await;
    }

    // Send completion event
    let _ = events.send(TestEvent::Completed(results.clone())).await;

    Ok(results)
}

/// Calculate producer/consumer threads and partitions based on config
fn calculate_thread_config(config: &TestConfig) -> Result<(usize, usize, i32)> {
    let (num_producer_threads, num_consumer_threads, total_partitions) = if config.sticky {
        let base_threads = config.producer_threads;
        let lcm_partitions_per_thread = lcm(
            config.producer_partitions_per_thread as usize,
            config.consumer_partitions_per_thread as usize,
        );
        let total_partitions = (lcm_partitions_per_thread * base_threads) as i32;
        let num_producer_threads =
            total_partitions as usize / config.producer_partitions_per_thread as usize;
        let num_consumer_threads =
            total_partitions as usize / config.consumer_partitions_per_thread as usize;

        (num_producer_threads, num_consumer_threads, total_partitions)
    } else {
        let num_producer_threads = config.producer_threads;
        let num_consumer_threads = config.consumer_threads;
        let total_partitions =
            (num_consumer_threads * config.consumer_partitions_per_thread as usize) as i32;

        (num_producer_threads, num_consumer_threads, total_partitions)
    };

    if num_producer_threads == 0 {
        return Err(anyhow!("Producer threads must be at least 1"));
    }
    if num_consumer_threads == 0 && config.produce_only.is_none() {
        return Err(anyhow!("Consumer threads must be at least 1"));
    }

    Ok((num_producer_threads, num_consumer_threads, total_partitions))
}

/// Generate random topic name
fn generate_topic_name() -> String {
    let adjectives = [
        "brave", "quick", "silent", "happy", "bright", "calm", "eager", "fierce", "gentle",
        "jolly", "kind", "lively", "mighty", "proud", "silly", "witty", "zany", "bold", "shy",
        "wise",
    ];
    let nouns = [
        "cat", "dog", "fox", "owl", "lion", "wolf", "bear", "mouse", "hawk", "fish", "frog",
        "horse", "duck", "ant", "bee", "bat", "deer", "goat", "rat", "swan",
    ];
    let verbs = [
        "jumps", "runs", "flies", "dives", "sings", "dances", "hops", "swims", "climbs", "rolls",
        "crawls", "slides", "spins", "laughs", "dreams", "thinks", "waits", "looks", "leaps",
        "rests",
    ];

    let mut rng = thread_rng();
    let adj = adjectives.choose(&mut rng).unwrap();
    let noun = nouns.choose(&mut rng).unwrap();
    let verb = verbs.choose(&mut rng).unwrap();
    format!("topic-{}-{}-{}", adj, noun, verb)
}

/// Create producer instances
async fn create_producers(
    config: &TestConfig,
    topic_name: &str,
    total_partitions: i32,
    num_producer_threads: usize,
    api_versions: &HashMap<i16, (i16, i16)>,
) -> Result<Vec<Producer>> {
    let mut producers = Vec::new();

    for i in 0..num_producer_threads {
        let producer_client = Arc::new(
            KafkaClient::connect_with_versions(&config.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect producer client {}: {}", i, e))?,
        );
        producers.push(Producer::new(
            producer_client,
            topic_name.to_string(),
            config.producer_partitions_per_thread,
            total_partitions,
            config.message_size.clone(),
            i,
            config.sticky,
            config.key_strategy.clone(),
            config.messages_per_batch,
        ));
    }

    Ok(producers)
}

/// Create consumer instances
async fn create_consumers(
    config: &TestConfig,
    topic_name: &str,
    num_consumer_threads: usize,
    api_versions: &HashMap<i16, (i16, i16)>,
) -> Result<Vec<Consumer>> {
    if config.produce_only.is_some() {
        return Ok(Vec::new());
    }

    let mut consumers = Vec::new();

    for i in 0..num_consumer_threads {
        let consumer_client = Arc::new(
            KafkaClient::connect_with_versions(&config.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect consumer client {}: {}", i, e))?,
        );
        consumers.push(Consumer::new(
            consumer_client,
            topic_name.to_string(),
            config.consumer_partitions_per_thread,
            i,
            config.fetch_delay.as_secs(),
            config.record_processing_time,
        ));
    }

    Ok(consumers)
}

/// Calculate max backlog threshold
fn calculate_max_backlog(config: &TestConfig, produce_only_target: Option<u64>) -> u64 {
    if config.produce_only.is_some() {
        u64::MAX
    } else if config.record_processing_time > 0 {
        u64::MAX
    } else {
        let fetch_delay_secs = config.fetch_delay.as_secs();
        if fetch_delay_secs > 0 {
            BASE_MAX_BACKLOG * fetch_delay_secs
        } else {
            BASE_MAX_BACKLOG
        }
    }
}

/// Run the measurement loop
async fn run_measurement(
    config: &TestConfig,
    producers: Vec<Producer>,
    consumers: Vec<Consumer>,
    total_test_duration: Duration,
    measurement_duration: Duration,
    max_backlog: u64,
    produce_only_target: Option<u64>,
    events: mpsc::Sender<TestEvent>,
) -> TestResults {
    let messages_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let messages_received = Arc::new(AtomicU64::new(0));
    let backlog_percentage_sum = Arc::new(AtomicU64::new(0));
    let backlog_measurement_count = Arc::new(AtomicU64::new(0));

    let produce_only = config.produce_only.is_some();
    let message_validation = config.message_validation;
    let fetch_delay = config.fetch_delay;

    // Spawn producers
    let mut producer_handles = Vec::new();
    for producer in producers {
        let messages_sent = messages_sent.clone();
        let bytes_sent = bytes_sent.clone();
        let messages_received = messages_received.clone();
        let producer_handle = tokio::spawn(async move {
            producer
                .produce_messages(ProduceConfig {
                    duration: total_test_duration,
                    messages_sent,
                    bytes_sent,
                    messages_received,
                    max_backlog,
                    message_validation,
                    bytes_target: produce_only_target,
                })
                .await
        });
        producer_handles.push(producer_handle);
    }

    // Spawn consumers
    let mut consumer_handles = Vec::new();
    if !produce_only {
        for consumer in consumers {
            let messages_received = messages_received.clone();
            let consumer_handle = tokio::spawn(async move {
                consumer
                    .consume_messages(total_test_duration, messages_received, message_validation)
                    .await
            });
            consumer_handles.push(consumer_handle);
        }
    }

    // Spawn progress reporter
    let progress_handle = {
        let messages_sent = messages_sent.clone();
        let bytes_sent = bytes_sent.clone();
        let messages_received = messages_received.clone();
        let backlog_percentage_sum = backlog_percentage_sum.clone();
        let backlog_measurement_count = backlog_measurement_count.clone();
        let events = events.clone();

        tokio::spawn(async move {
            // Wait for fetch delay before starting measurement
            if !fetch_delay.is_zero() {
                sleep(fetch_delay).await;
            }

            let start_time = Instant::now();
            let end_time = start_time + measurement_duration;
            let mut progress_interval = interval(Duration::from_millis(100));

            let start_count = if produce_only {
                messages_sent.load(Ordering::Relaxed)
            } else {
                messages_received.load(Ordering::Relaxed)
            };
            let mut last_count = start_count;
            let mut last_rate_time = start_time;
            let mut min_rate = f64::MAX;
            let mut max_rate = 0.0f64;
            let mut current_rate = 0.0f64;

            while produce_only_target.is_some() || Instant::now() < end_time {
                if let Some(target) = produce_only_target {
                    if bytes_sent.load(Ordering::Relaxed) >= target {
                        break;
                    }
                }

                progress_interval.tick().await;

                let now = Instant::now();
                let current_count = if produce_only {
                    messages_sent.load(Ordering::Relaxed)
                } else {
                    messages_received.load(Ordering::Relaxed)
                };
                let time_elapsed = now.duration_since(last_rate_time).as_secs_f64();

                if time_elapsed > 0.0 {
                    current_rate = (current_count - last_count) as f64 / time_elapsed;
                    if current_rate > 0.0 {
                        if min_rate == f64::MAX {
                            min_rate = current_rate;
                        } else {
                            min_rate = min_rate.min(current_rate);
                        }
                        max_rate = max_rate.max(current_rate);
                    }
                    last_count = current_count;
                    last_rate_time = now;
                }

                // Calculate backlog percentage
                let current_sent = messages_sent.load(Ordering::Relaxed);
                let current_received = messages_received.load(Ordering::Relaxed);
                let backlog = current_sent.saturating_sub(current_received);
                let backlog_percent = if max_backlog == u64::MAX {
                    0
                } else {
                    (backlog as f64 / max_backlog as f64 * 100.0).min(100.0) as u8
                };

                if !produce_only {
                    backlog_percentage_sum.fetch_add(backlog_percent as u64, Ordering::Relaxed);
                    backlog_measurement_count.fetch_add(1, Ordering::Relaxed);
                }

                let elapsed = start_time.elapsed();
                let _ = events.send(TestEvent::Progress {
                    messages_sent: current_sent,
                    messages_received: current_received,
                    bytes_sent: bytes_sent.load(Ordering::Relaxed),
                    current_rate,
                    backlog_percent,
                    elapsed,
                }).await;
            }

            (min_rate, max_rate, start_time.elapsed())
        })
    };

    // Wait for all tasks
    for handle in producer_handles {
        let _ = handle.await;
    }
    for handle in consumer_handles {
        let _ = handle.await;
    }
    let (min_rate, max_rate, elapsed) = progress_handle.await.unwrap_or((0.0, 0.0, Duration::ZERO));

    // Calculate final results
    let final_sent = messages_sent.load(Ordering::Relaxed);
    let final_received = messages_received.load(Ordering::Relaxed);
    let total_bytes = bytes_sent.load(Ordering::Relaxed);

    let throughput = if produce_only {
        final_sent as f64 / elapsed.as_secs_f64()
    } else {
        final_received as f64 / elapsed.as_secs_f64()
    };

    let backlog_sum = backlog_percentage_sum.load(Ordering::Relaxed);
    let backlog_count = backlog_measurement_count.load(Ordering::Relaxed);
    let avg_backlog = if backlog_count > 0 {
        (backlog_sum / backlog_count) as u8
    } else {
        0
    };

    TestResults {
        messages_sent: final_sent,
        messages_received: final_received,
        bytes_sent: total_bytes,
        duration: elapsed,
        throughput,
        min_rate: if min_rate == f64::MAX { 0.0 } else { min_rate },
        max_rate,
        avg_backlog_percent: avg_backlog,
    }
}
```

**Step 2: Update kitt-core/src/lib.rs**

```rust
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

// Re-export public API (high-level)
pub use config::{KeyStrategy, MessageSize, ProduceOnlyMode, TestConfig, TestConfigBuilder};
pub use events::{TestEvent, TestPhase, TestResults};
pub use runner::{run_test, TestHandle};

// Re-export building blocks (low-level)
pub use client::KafkaClient;
pub use consumer::Consumer;
pub use producer::Producer;
```

**Step 3: Run cargo check**

Run: `cargo check -p kitt-core`
Expected: Success

**Step 4: Commit**

```bash
git add kitt-core/src/runner.rs kitt-core/src/lib.rs
git commit -m "feat(kitt-core): add runner module with run_test and TestHandle"
```

---

## Task 9: Update Kitt CLI to Use Core

**Files:**
- Modify: `kitt/Cargo.toml`
- Modify: `kitt/src/main.rs`
- Modify: `kitt/src/args.rs`

**Step 1: Update kitt/Cargo.toml**

Add `kitt-core` as a dependency:
```toml
kitt-core = { path = "../kitt-core" }
```

**Step 2: Update kitt/src/args.rs**

Remove `MessageSize` and `KeyStrategy` types (now imported from core). Keep only CLI-specific parsing.

**Step 3: Update kitt/src/main.rs**

- Import types from `kitt_core` instead of local modules
- Create a new `ui.rs` module for event handling
- Convert CLI args to `TestConfig` using the builder
- Call `kitt_core::run_test()` and handle events

**Step 4: Run cargo check**

Run: `cargo check -p kitt`
Expected: Success

**Step 5: Run cargo test**

Run: `cargo test`
Expected: All tests pass

**Step 6: Commit**

```bash
git add kitt/Cargo.toml kitt/src/main.rs kitt/src/args.rs
git commit -m "refactor(kitt): use kitt-core library"
```

---

## Task 10: Create UI Module for CLI

**Files:**
- Create: `kitt/src/ui.rs`
- Modify: `kitt/src/main.rs`

**Step 1: Create ui.rs for event handling**

Move throbbler/LED animation code here. Subscribe to events from core and update display.

```rust
//! UI module for Kitt CLI
//!
//! Handles event subscription and visual display using kitt_throbbler.

use kitt_core::{TestEvent, TestPhase, TestResults};
use kitt_throbbler::KnightRiderAnimator;
use tokio::sync::mpsc;

pub const LED_BAR_WIDTH: usize = 25;
pub const LED_MOVEMENT_SPEED: usize = 2;

/// Event handler that displays progress using Knight Rider animation
pub struct UiHandler {
    animator: KnightRiderAnimator,
    quiet: bool,
    silent: bool,
}

impl UiHandler {
    pub fn new(quiet: bool, silent: bool) -> Self {
        let audio_enabled = !quiet && !silent;
        Self {
            animator: KnightRiderAnimator::with_leds(LED_BAR_WIDTH).audio_enabled(audio_enabled),
            quiet,
            silent,
        }
    }

    /// Handle events from the test runner
    pub async fn handle_events(&mut self, mut events: mpsc::Receiver<TestEvent>) -> Option<TestResults> {
        // Implementation handles Progress events with animation
        // Returns final results when Completed event received
        todo!()
    }
}
```

**Step 2: Update main.rs to use UiHandler**

**Step 3: Run cargo build**

Run: `cargo build -p kitt`
Expected: Success

**Step 4: Commit**

```bash
git add kitt/src/ui.rs kitt/src/main.rs
git commit -m "feat(kitt): add UI module for event handling"
```

---

## Task 11: Integration Testing

**Files:**
- No new files

**Step 1: Run full test suite**

Run: `cargo test`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy --all-targets`
Expected: No errors

**Step 3: Build release**

Run: `cargo build --release`
Expected: Success

**Step 4: Manual smoke test (if Kafka available)**

Run: `./target/release/kitt --broker localhost:9092 --duration-secs 5`
Expected: Test completes successfully

**Step 5: Commit any fixes**

```bash
git add -A
git commit -m "fix: integration testing fixes"
```

---

## Task 12: Clean Up Old Code

**Files:**
- Modify: `kitt/src/lib.rs`
- Delete unused modules from `kitt/src/`

**Step 1: Remove duplicate code from kitt**

Now that kitt uses kitt-core, remove:
- `kitt/src/kafka_client.rs` (if not needed)
- `kitt/src/producer.rs` (if not needed)
- `kitt/src/consumer.rs` (if not needed)
- `kitt/src/utils.rs` (if not needed)

Keep only:
- `kitt/src/main.rs`
- `kitt/src/args.rs`
- `kitt/src/ui.rs`
- `kitt/src/profiling.rs`

**Step 2: Update kitt/src/lib.rs**

Update to only export what's needed (likely just profiling).

**Step 3: Run tests**

Run: `cargo test`
Expected: All tests pass

**Step 4: Commit**

```bash
git add -A
git commit -m "refactor(kitt): remove code now in kitt-core"
```

---

## Summary

After completing all tasks:

1. `kitt-core` is a standalone library with:
   - High-level: `run_test()`, `TestConfig`, `TestHandle`, events
   - Low-level: `KafkaClient`, `Producer`, `Consumer`
   - No dependencies on throbbler, quantum-pulse, clap, or mimalloc

2. `kitt` CLI is thin wrapper that:
   - Parses CLI args with clap
   - Builds `TestConfig` from args
   - Calls `kitt_core::run_test()`
   - Handles events for UI (throbbler animation)
   - Optionally adds profiling

3. `kitt_throbbler` remains unchanged
