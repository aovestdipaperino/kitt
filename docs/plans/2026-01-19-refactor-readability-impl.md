# Refactor main.rs for Readability - Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Break up the 2,771-line main.rs into focused modules while simplifying main() to ~100-150 lines.

**Architecture:** Extract Producer, Consumer, ThroughputMeasurer, Args, and utility functions into separate modules. Keep orchestration logic in main.rs but delegate details to modules.

**Tech Stack:** Rust, tokio, clap, kafka-protocol

---

### Task 1: Create utils.rs with helper functions

**Files:**
- Create: `kitt/src/utils.rs`
- Modify: `kitt/src/main.rs` (add mod declaration, update imports)

**Step 1: Create utils.rs with functions extracted from main.rs**

Create `kitt/src/utils.rs`:

```rust
//! Utility functions for KITT

use anyhow::{anyhow, Result};
use tracing::debug;

/// Calculate the Greatest Common Divisor of two numbers
pub fn gcd(a: usize, b: usize) -> usize {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// Calculate the Least Common Multiple of two numbers
pub fn lcm(a: usize, b: usize) -> usize {
    a * b / gcd(a, b)
}

/// Formats a byte count into a human-readable string (KB, MB, GB, etc.)
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

/// Parses a human-readable byte size string (e.g., "1GB", "500MB", "1024KB") into bytes
pub fn parse_bytes(s: &str) -> Result<u64> {
    let s = s.trim().to_uppercase();

    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    // Find where the numeric part ends
    let numeric_end = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());

    let (num_str, unit) = s.split_at(numeric_end);
    let num: f64 = num_str
        .parse()
        .map_err(|_| anyhow!("Invalid number in size: {}", s))?;

    let multiplier = match unit.trim() {
        "" | "B" => 1,
        "K" | "KB" | "KIB" => KB,
        "M" | "MB" | "MIB" => MB,
        "G" | "GB" | "GIB" => GB,
        "T" | "TB" | "TIB" => TB,
        _ => return Err(anyhow!("Unknown unit in size: {}", unit)),
    };

    Ok((num * multiplier as f64) as u64)
}

/// Verify CRC32-C of a Kafka record batch
///
/// Kafka record batch format (v2):
/// - baseOffset: int64 (8 bytes) - offset 0
/// - batchLength: int32 (4 bytes) - offset 8
/// - partitionLeaderEpoch: int32 (4 bytes) - offset 12
/// - magic: int8 (1 byte) - offset 16
/// - crc: int32 (4 bytes) - offset 17
/// - attributes onwards: covered by CRC - offset 21
///
/// Returns Ok(batch_length) if CRC matches, Err with details if not
pub fn verify_record_batch_crc(data: &[u8]) -> Result<usize> {
    // Minimum size for a record batch header
    if data.len() < 21 {
        return Err(anyhow!("Record batch too short: {} bytes", data.len()));
    }

    // Read batch length (offset 8, 4 bytes, big-endian)
    let batch_length = i32::from_be_bytes([data[8], data[9], data[10], data[11]]) as usize;

    // Total batch size = 8 (baseOffset) + 4 (batchLength) + batchLength
    let total_batch_size = 12 + batch_length;
    if data.len() < total_batch_size {
        return Err(anyhow!(
            "Incomplete record batch: expected {} bytes, got {}",
            total_batch_size,
            data.len()
        ));
    }

    // Read magic byte (offset 16)
    let magic = data[16];
    if magic != 2 {
        // Only verify CRC for magic version 2 (modern format)
        debug!("Skipping CRC check for magic version {}", magic);
        return Ok(total_batch_size);
    }

    // Read stored CRC (offset 17, 4 bytes, big-endian)
    let stored_crc = u32::from_be_bytes([data[17], data[18], data[19], data[20]]);

    // Compute CRC32-C over data from attributes (offset 21) to end of batch
    let crc_data = &data[21..total_batch_size];
    let computed_crc = crc32c::crc32c(crc_data);

    if stored_crc != computed_crc {
        return Err(anyhow!(
            "CRC mismatch: stored=0x{:08x}, computed=0x{:08x}",
            stored_crc,
            computed_crc
        ));
    }

    Ok(total_batch_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gcd() {
        assert_eq!(gcd(12, 8), 4);
        assert_eq!(gcd(17, 13), 1);
        assert_eq!(gcd(100, 25), 25);
        assert_eq!(gcd(0, 5), 5);
        assert_eq!(gcd(5, 0), 5);
    }

    #[test]
    fn test_lcm() {
        assert_eq!(lcm(4, 6), 12);
        assert_eq!(lcm(3, 5), 15);
        assert_eq!(lcm(7, 7), 7);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(512), "512 bytes");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.00 GB");
        assert_eq!(format_bytes(3 * 1024 * 1024 * 1024), "3.00 GB");
    }

    #[test]
    fn test_parse_bytes() {
        assert_eq!(parse_bytes("1024").unwrap(), 1024);
        assert_eq!(parse_bytes("1KB").unwrap(), 1024);
        assert_eq!(parse_bytes("1kb").unwrap(), 1024);
        assert_eq!(parse_bytes("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_bytes("3GB").unwrap(), 3 * 1024 * 1024 * 1024);
        assert_eq!(parse_bytes("1.5GB").unwrap(), (1.5 * 1024.0 * 1024.0 * 1024.0) as u64);
        assert!(parse_bytes("invalid").is_err());
        assert!(parse_bytes("1XB").is_err());
    }
}
```

**Step 2: Add mod declaration to main.rs**

Add after line 160 (after `use quantum_pulse::profile;`):

```rust
mod utils;
use utils::{format_bytes, gcd, lcm, parse_bytes, verify_record_batch_crc};
```

**Step 3: Remove duplicated functions from main.rs**

Delete the following from main.rs:
- `fn gcd` (lines 87-93)
- `fn lcm` (lines 95-98)
- `fn format_bytes` (lines 297-312)
- `fn parse_bytes` (lines 314-343)
- `fn verify_record_batch_crc` (lines 100-154)

**Step 4: Run tests**

```bash
cargo test -p kitt
```

Expected: All tests pass

**Step 5: Commit**

```bash
git add kitt/src/utils.rs kitt/src/main.rs
git commit -m "refactor: extract utils.rs module"
```

---

### Task 2: Create args.rs with CLI types

**Files:**
- Create: `kitt/src/args.rs`
- Modify: `kitt/src/main.rs`

**Step 1: Create args.rs**

Create `kitt/src/args.rs`:

```rust
//! Command-line argument parsing for KITT

use anyhow::{anyhow, Result};
use bytes::Bytes;
use clap::Parser;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use std::sync::Arc;

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
}

/// Represents the size configuration for test messages
/// Supports both fixed-size messages and variable-size ranges
#[derive(Debug, Clone)]
pub enum MessageSize {
    /// Fixed message size in bytes
    Fixed(usize),
    /// Variable message size with min and max bounds (inclusive)
    Range(usize, usize),
}

impl MessageSize {
    /// Parses a message size specification from a string
    ///
    /// # Arguments
    /// * `s` - Size specification, either "1024" for fixed size or "100-1000" for range
    ///
    /// # Returns
    /// * `Ok(MessageSize)` - Parsed message size configuration
    /// * `Err(anyhow::Error)` - If the string format is invalid
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
    ///
    /// # Returns
    /// * For `Fixed`: Returns the fixed size
    /// * For `Range`: Returns a random size within the specified range (inclusive)
    pub fn generate_size(&self) -> usize {
        match self {
            MessageSize::Fixed(size) => *size,
            MessageSize::Range(min, max) => thread_rng().gen_range(*min..=*max),
        }
    }
}

/// Strategy for generating message keys
///
/// Kafka message keys determine partition assignment and enable log compaction.
/// This enum allows testing different key generation strategies.
#[derive(Debug, Clone)]
pub enum KeyStrategy {
    /// No keys - messages have null keys (default Kafka behavior)
    NoKeys,
    /// Generate unique random keys on the fly for each message
    RandomOnTheFly,
    /// Pick keys randomly from a pre-generated pool of the specified size
    RandomPool(Arc<Vec<Bytes>>),
}

impl KeyStrategy {
    /// Creates a KeyStrategy from the command-line argument value
    ///
    /// # Arguments
    /// * `pool_size` - None for no keys, Some(0) for on-the-fly generation,
    ///   Some(n) for a pool of n pre-generated keys
    pub fn from_arg(pool_size: Option<usize>) -> Self {
        match pool_size {
            None => KeyStrategy::NoKeys,
            Some(0) => KeyStrategy::RandomOnTheFly,
            Some(size) => {
                // Pre-generate a pool of random keys
                let keys: Vec<Bytes> = (0..size)
                    .map(|_| Bytes::from(uuid::Uuid::new_v4().to_string()))
                    .collect();
                KeyStrategy::RandomPool(Arc::new(keys))
            }
        }
    }

    /// Generates a key based on the configured strategy
    ///
    /// # Returns
    /// * `None` - For NoKeys strategy
    /// * `Some(Bytes)` - A random key for other strategies
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_size_parse_fixed() {
        let size = MessageSize::parse("1024").unwrap();
        assert!(matches!(size, MessageSize::Fixed(1024)));
    }

    #[test]
    fn test_message_size_parse_range() {
        let size = MessageSize::parse("100-1000").unwrap();
        assert!(matches!(size, MessageSize::Range(100, 1000)));
    }

    #[test]
    fn test_message_size_parse_invalid_range() {
        let result = MessageSize::parse("1000-100");
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
        let strategy = KeyStrategy::from_arg(None);
        assert!(matches!(strategy, KeyStrategy::NoKeys));
        assert!(strategy.generate_key().is_none());
    }

    #[test]
    fn test_key_strategy_random_on_the_fly() {
        let strategy = KeyStrategy::from_arg(Some(0));
        assert!(matches!(strategy, KeyStrategy::RandomOnTheFly));
        assert!(strategy.generate_key().is_some());
    }

    #[test]
    fn test_key_strategy_random_pool() {
        let strategy = KeyStrategy::from_arg(Some(10));
        assert!(matches!(strategy, KeyStrategy::RandomPool(_)));
        assert!(strategy.generate_key().is_some());
    }
}
```

**Step 2: Update main.rs imports**

Add mod declaration and update imports at top of main.rs:

```rust
mod args;
mod utils;

use args::{Args, KeyStrategy, MessageSize};
use utils::{format_bytes, gcd, lcm, parse_bytes, verify_record_batch_crc};
```

**Step 3: Remove duplicated types from main.rs**

Delete from main.rs:
- `struct Args` (lines ~166-248)
- `enum MessageSize` and impl (lines ~250-295)
- `enum KeyStrategy` and impl (lines ~345-394)

**Step 4: Run tests**

```bash
cargo test -p kitt
```

Expected: All tests pass

**Step 5: Commit**

```bash
git add kitt/src/args.rs kitt/src/main.rs
git commit -m "refactor: extract args.rs module"
```

---

### Task 3: Create measurer.rs

**Files:**
- Create: `kitt/src/measurer.rs`
- Modify: `kitt/src/main.rs`

**Step 1: Create measurer.rs**

Extract ThroughputMeasurer from main.rs (lines 1810-2003) into `kitt/src/measurer.rs`.

The file should include:
- LED constants (LED_MOVEMENT_SPEED, LED_BAR_WIDTH)
- ThroughputMeasurer struct
- ThroughputMeasurer impl with `with_leds()` and `measure()` methods

Key imports needed:
```rust
use crate::utils::format_bytes;
use kitt_throbbler::KnightRiderAnimator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::time::interval;
use tracing::info;
```

Add `#[cfg(test)]` module with `test_measurement_timing_calculation` test.

**Step 2: Update main.rs**

Add mod declaration:
```rust
mod measurer;
use measurer::{ThroughputMeasurer, LED_BAR_WIDTH};
```

Remove ThroughputMeasurer and constants from main.rs.

**Step 3: Run tests**

```bash
cargo test -p kitt
```

**Step 4: Commit**

```bash
git add kitt/src/measurer.rs kitt/src/main.rs
git commit -m "refactor: extract measurer.rs module"
```

---

### Task 4: Create producer.rs

**Files:**
- Create: `kitt/src/producer.rs`
- Modify: `kitt/src/main.rs`

**Step 1: Create producer.rs**

Extract Producer struct and impl from main.rs (lines 399-1029) into `kitt/src/producer.rs`.

Key imports needed:
```rust
use crate::args::{KeyStrategy, MessageSize};
use crate::kafka_client::KafkaClient;
use crate::profiling::KittOperation;
use crate::utils::verify_record_batch_crc;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use kafka_protocol::messages::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData};
use kafka_protocol::messages::produce_response::ProduceResponse;
use kafka_protocol::messages::{ApiKey, ResponseHeader, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
use quantum_pulse::profile;
use rand::{thread_rng, Rng};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tracing::{debug, error, warn};
```

Include the `test_produce_response_validation` test in `#[cfg(test)]` module.

**Step 2: Update main.rs**

Add mod declaration:
```rust
mod producer;
use producer::Producer;
```

Remove Producer struct and impl from main.rs.

**Step 3: Run tests**

```bash
cargo test -p kitt
```

**Step 4: Commit**

```bash
git add kitt/src/producer.rs kitt/src/main.rs
git commit -m "refactor: extract producer.rs module"
```

---

### Task 5: Create consumer.rs

**Files:**
- Create: `kitt/src/consumer.rs`
- Modify: `kitt/src/main.rs`

**Step 1: Create consumer.rs**

Extract Consumer struct and impl from main.rs (lines 1031-1809) into `kitt/src/consumer.rs`.

Key imports needed:
```rust
use crate::kafka_client::KafkaClient;
use crate::profiling::KittOperation;
use crate::utils::verify_record_batch_crc;
use anyhow::{anyhow, Result};
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::fetch_response::FetchResponse;
use kafka_protocol::messages::{ApiKey, ResponseHeader};
use kafka_protocol::protocol::Decodable;
use kafka_protocol::records::RecordBatchDecoder;
use quantum_pulse::profile;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{debug, error, info, warn};
```

Include the `test_fetch_response_validation` test in `#[cfg(test)]` module.

**Step 2: Update main.rs**

Add mod declaration:
```rust
mod consumer;
use consumer::Consumer;
```

Remove Consumer struct and impl from main.rs.

**Step 3: Run tests**

```bash
cargo test -p kitt
```

**Step 4: Commit**

```bash
git add kitt/src/consumer.rs kitt/src/main.rs
git commit -m "refactor: extract consumer.rs module"
```

---

### Task 6: Simplify main.rs orchestration

**Files:**
- Modify: `kitt/src/main.rs`

**Step 1: Extract helper functions**

Create local helper functions in main.rs to simplify main():

```rust
fn setup_logging(args: &Args) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::filter::EnvFilter::new("info"))
                .add_directive("symphonia_core=warn".parse().unwrap())
                .add_directive("symphonia_bundle_mp3=warn".parse().unwrap()),
        )
        .init();
}

fn calculate_thread_config(args: &Args) -> Result<(usize, usize, i32)> {
    // Returns (num_producer_threads, num_consumer_threads, total_partitions)
    // ... existing logic
}

fn generate_topic_name() -> String {
    // ... existing adjective-noun-verb logic
}

async fn create_producer_clients(
    args: &Args,
    api_versions: &HashMap<i16, i16>,
    num_threads: usize,
) -> Result<Vec<Arc<KafkaClient>>> {
    // ... existing client creation logic
}

// Similar for create_consumer_clients, create_producers, create_consumers
```

**Step 2: Refactor main() to use helpers**

The simplified main() should follow this structure:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_logging(&args);

    if args.profile_demo {
        return run_profile_demo().await;
    }

    run_diagnostics_if_enabled(&args);

    let message_size = MessageSize::parse(&args.message_size)?;
    let key_strategy = KeyStrategy::from_arg(args.random_keys);
    let (num_producer_threads, num_consumer_threads, total_partitions) =
        calculate_thread_config(&args)?;

    print_startup_info(&args, &message_size, num_producer_threads, num_consumer_threads, total_partitions);

    let topic_name = generate_topic_name();
    let admin_client = Arc::new(KafkaClient::connect(&args.broker).await?);
    let api_versions = admin_client.api_versions.clone();

    admin_client.create_topic(&topic_name, total_partitions, 1).await?;
    sleep(Duration::from_secs(3)).await;

    let producers = create_producers(&args, &topic_name, &api_versions, &message_size, &key_strategy, num_producer_threads, total_partitions).await?;
    let consumers = create_consumers(&args, &topic_name, &api_versions, num_consumer_threads, total_partitions).await?;

    let measurer = ThroughputMeasurer::with_leds(LED_BAR_WIDTH, !args.silent);
    run_measurement(&args, producers, consumers, &measurer).await;

    print_final_analysis(&args, &measurer);
    cleanup_topic(&admin_client, &topic_name).await;

    if args.profile_report {
        println!("{}", generate_report());
    }

    Ok(())
}
```

**Step 3: Run full test suite**

```bash
cargo test -p kitt
cargo build --release -p kitt
```

**Step 4: Smoke test**

```bash
# Quick produce-only test (requires Kafka running)
./target/release/kitt --produce-only 50MB -d 5
```

**Step 5: Commit**

```bash
git add kitt/src/main.rs
git commit -m "refactor: simplify main.rs orchestration"
```

---

### Task 7: Final cleanup and verification

**Files:**
- Review all files in `kitt/src/`

**Step 1: Verify module structure**

```bash
wc -l kitt/src/*.rs
```

Expected output (approximate):
- main.rs: ~150-200 lines
- args.rs: ~200 lines
- producer.rs: ~650 lines
- consumer.rs: ~800 lines
- measurer.rs: ~220 lines
- utils.rs: ~130 lines
- kafka_client.rs: ~840 lines (unchanged)
- profiling.rs: ~420 lines (unchanged)

**Step 2: Run full test suite**

```bash
cargo test -p kitt
```

**Step 3: Run clippy**

```bash
cargo clippy -p kitt -- -D warnings
```

**Step 4: Build release**

```bash
cargo build --release -p kitt
```

**Step 5: Final commit**

```bash
git add -A
git commit -m "refactor: complete main.rs readability refactor

- Extract utils.rs (gcd, lcm, format_bytes, parse_bytes, verify_crc)
- Extract args.rs (Args, MessageSize, KeyStrategy)
- Extract measurer.rs (ThroughputMeasurer, LED constants)
- Extract producer.rs (Producer struct and impl)
- Extract consumer.rs (Consumer struct and impl)
- Simplify main() to ~150 lines of readable orchestration
- Add unit tests for all new modules"
```

---

## Validation Checklist

After completing all tasks:

- [ ] `cargo test -p kitt` passes
- [ ] `cargo clippy -p kitt` passes
- [ ] `cargo build --release -p kitt` succeeds
- [ ] `kitt --help` works
- [ ] `kitt --produce-only 50MB -d 5` works (with Kafka)
- [ ] main.rs is ~150-200 lines
- [ ] Each module has focused responsibility
