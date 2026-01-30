# Use Existing Topic Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add `--use-existing-topic` flag to allow kitt to use a pre-existing Kafka topic instead of creating and deleting one.

**Architecture:** Add config field and CLI flag, implement `get_topic_metadata` in KafkaClient, modify runner to conditionally skip create/delete phases.

**Tech Stack:** Rust, kafka-protocol, clap

---

## Task 1: Add `use_existing_topic` field to TestConfig

**Files:**
- Modify: `kitt-core/src/config.rs:109-170` (TestConfig struct and Default impl)
- Modify: `kitt-core/src/config.rs:173-311` (TestConfigBuilder)
- Test: `kitt-core/src/config.rs` (tests module)

**Step 1: Write the failing test**

Add to the `tests` module at the end of `kitt-core/src/config.rs`:

```rust
#[test]
fn test_config_use_existing_topic_requires_topic_name() {
    let result = TestConfig::builder()
        .use_existing_topic(true)
        .build();
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("topic"));
}

#[test]
fn test_config_use_existing_topic_valid() {
    let config = TestConfig::builder()
        .use_existing_topic(true)
        .topic(Some("my-topic".to_string()))
        .build()
        .unwrap();
    assert!(config.use_existing_topic);
    assert_eq!(config.topic, Some("my-topic".to_string()));
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p kitt-core test_config_use_existing_topic`
Expected: FAIL - `use_existing_topic` method doesn't exist

**Step 3: Add the field and builder method**

In `TestConfig` struct (around line 109), add after `topic`:
```rust
    /// Use an existing topic instead of creating one (requires topic to be set)
    pub use_existing_topic: bool,
```

In `Default` impl (around line 148), add after `topic: None,`:
```rust
            use_existing_topic: false,
```

In `TestConfigBuilder`, add method (around line 195, after `topic` method):
```rust
    /// Use an existing topic instead of creating one
    pub fn use_existing_topic(mut self, use_existing: bool) -> Self {
        self.config.use_existing_topic = use_existing;
        self
    }
```

In `build()` method (around line 294), add validation before the final `Ok`:
```rust
        if self.config.use_existing_topic && self.config.topic.is_none() {
            return Err(anyhow!("use_existing_topic requires topic to be set"));
        }
```

**Step 4: Run tests to verify they pass**

Run: `cargo test -p kitt-core test_config_use_existing_topic`
Expected: PASS (2 tests)

**Step 5: Commit**

```bash
git add kitt-core/src/config.rs
git commit -m "feat(kitt-core): add use_existing_topic config field"
```

---

## Task 2: Add TopicMetadata struct and get_topic_metadata to KafkaClient

**Files:**
- Modify: `kitt-core/src/client.rs` (add imports, struct, and method)

**Step 1: Add imports for Metadata API**

At the top of `kitt-core/src/client.rs`, add to the `kafka_protocol::messages` import block:
```rust
        metadata_request::{MetadataRequest, MetadataRequestTopic},
        metadata_response::MetadataResponse,
```

**Step 2: Add TopicMetadata struct**

After the `KafkaClient` struct definition (around line 55), add:
```rust
/// Metadata about an existing Kafka topic
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    /// Number of partitions in the topic
    pub partition_count: i32,
}
```

**Step 3: Add get_topic_metadata method**

Add this method to the `impl KafkaClient` block (after `delete_topic`, around line 555):

```rust
    /// Gets metadata for an existing topic
    ///
    /// Returns topic metadata including partition count. Fails if topic doesn't exist.
    ///
    /// # Arguments
    /// * `topic` - Name of the topic to query
    ///
    /// # Returns
    /// * `Ok(TopicMetadata)` - Topic metadata if topic exists
    /// * `Err(anyhow::Error)` - If topic doesn't exist or query fails
    pub async fn get_topic_metadata(&self, topic: &str) -> Result<TopicMetadata> {
        debug!("Getting metadata for topic '{}'", topic);

        let mut request = MetadataRequest::default();

        let mut topic_request = MetadataRequestTopic::default();
        topic_request.name = Some(TopicName(StrBytes::from_string(topic.to_string())));
        request.topics = Some(vec![topic_request]);

        let version = self.get_supported_version(ApiKey::Metadata, 1);
        let response_bytes = self
            .send_request(ApiKey::Metadata, &request, version)
            .await
            .map_err(|e| anyhow!("Failed to get metadata for topic '{}': {}", topic, e))?;

        let mut cursor = std::io::Cursor::new(response_bytes.as_ref());
        let response_header_version = ApiKey::Metadata.response_header_version(version);
        let _response_header = ResponseHeader::decode(&mut cursor, response_header_version)
            .map_err(|e| anyhow!("Failed to decode metadata response header: {}", e))?;

        let response = MetadataResponse::decode(&mut cursor, version)
            .map_err(|e| anyhow!("Failed to decode metadata response: {}", e))?;

        // Find our topic in the response
        for topic_metadata in response.topics {
            let topic_name = topic_metadata.name.as_ref().map(|n| n.0.as_str()).unwrap_or("");
            if topic_name == topic {
                // Check for errors
                if topic_metadata.error_code != 0 {
                    let error_msg = match topic_metadata.error_code {
                        3 => "Unknown topic or partition".to_string(),
                        _ => format!("Kafka error code: {}", topic_metadata.error_code),
                    };
                    return Err(anyhow!("Topic '{}' not found: {}", topic, error_msg));
                }

                let partition_count = topic_metadata.partitions.len() as i32;
                debug!("Topic '{}' has {} partitions", topic, partition_count);
                return Ok(TopicMetadata { partition_count });
            }
        }

        Err(anyhow!("Topic '{}' not found in metadata response", topic))
    }
```

**Step 4: Run tests to verify compilation**

Run: `cargo test -p kitt-core`
Expected: PASS (compilation succeeds, all existing tests pass)

**Step 5: Commit**

```bash
git add kitt-core/src/client.rs
git commit -m "feat(kitt-core): add get_topic_metadata method to KafkaClient"
```

---

## Task 3: Update runner to handle use_existing_topic

**Files:**
- Modify: `kitt-core/src/runner.rs`
- Modify: `kitt-core/src/lib.rs` (re-export TopicMetadata)

**Step 1: Update lib.rs to export TopicMetadata**

In `kitt-core/src/lib.rs`, update the `pub use client::KafkaClient;` line to:
```rust
pub use client::{KafkaClient, TopicMetadata};
```

**Step 2: Modify run_test_inner in runner.rs**

In `kitt-core/src/runner.rs`, replace the topic creation section (around lines 104-122) with:

```rust
    // Calculate thread configuration
    let (num_producer_threads, num_consumer_threads, total_partitions) =
        calculate_thread_config(&config)?;

    // Get or generate topic name
    let topic_name = config.topic.clone().unwrap_or_else(generate_topic_name);

    // Handle topic: either use existing or create new
    let actual_partitions = if config.use_existing_topic {
        // Phase: Fetching topic metadata (reuse WaitingForTopic phase)
        let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::WaitingForTopic }).await;

        let metadata = admin_client
            .get_topic_metadata(&topic_name)
            .await
            .map_err(|e| anyhow!("Cannot use existing topic '{}': {}", topic_name, e))?;

        metadata.partition_count
    } else {
        // Phase: Creating topic
        let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::CreatingTopic }).await;

        admin_client
            .create_topic(&topic_name, total_partitions, config.replication_factor)
            .await
            .map_err(|e| anyhow!("Topic creation failed: {}", e))?;

        // Phase: Waiting for topic
        let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::WaitingForTopic }).await;
        sleep(Duration::from_secs(3)).await;

        total_partitions
    };
```

**Step 3: Update producer/consumer creation to use actual_partitions**

Replace the calls to `create_producers` and `create_consumers` to use `actual_partitions` instead of `total_partitions`:

```rust
    let producers = create_producers(
        &config,
        &topic_name,
        actual_partitions,
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
```

**Step 4: Update cleanup to skip deletion when use_existing_topic**

In the cleanup section (around line 170-176), change:
```rust
    // Phase: Cleanup
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::Cleanup }).await;

    if !config.use_existing_topic {
        if let Err(e) = admin_client.delete_topic(&topic_name).await {
            let _ = events.send(TestEvent::Warning {
                message: format!("Failed to delete topic '{}': {}", topic_name, e),
            }).await;
        }
    }
```

**Step 5: Run tests**

Run: `cargo test -p kitt-core`
Expected: PASS

**Step 6: Commit**

```bash
git add kitt-core/src/runner.rs kitt-core/src/lib.rs
git commit -m "feat(kitt-core): handle use_existing_topic in runner"
```

---

## Task 4: Add --use-existing-topic CLI flag

**Files:**
- Modify: `kitt/src/args.rs`

**Step 1: Add the CLI flag**

In `kitt/src/args.rs`, add after the `quiet` field (around line 100):

```rust
    /// Topic name to use (auto-generated if not specified)
    #[arg(short, long)]
    pub topic: Option<String>,

    /// Use an existing topic instead of creating one (requires --topic)
    #[arg(long, default_value = "false")]
    pub use_existing_topic: bool,
```

**Step 2: Add test for flag parsing**

Add to the tests module:
```rust
#[test]
fn test_use_existing_topic_flag() {
    use clap::Parser;
    use crate::Args;

    // Test default is false
    let args = Args::try_parse_from(&["kitt"]).unwrap();
    assert!(!args.use_existing_topic);
    assert!(args.topic.is_none());

    // Test flag with topic
    let args = Args::try_parse_from(&["kitt", "--use-existing-topic", "--topic", "my-topic"]).unwrap();
    assert!(args.use_existing_topic);
    assert_eq!(args.topic, Some("my-topic".to_string()));
}
```

**Step 3: Run tests**

Run: `cargo test -p kitt test_use_existing_topic_flag`
Expected: PASS

**Step 4: Commit**

```bash
git add kitt/src/args.rs
git commit -m "feat(kitt): add --use-existing-topic and --topic CLI flags"
```

---

## Task 5: Wire CLI flags to TestConfig in main.rs

**Files:**
- Modify: `kitt/src/main.rs`

**Step 1: Update main.rs to use the flags**

In `kitt/src/main.rs`, after parsing args and before connecting (around line 687), add validation:

```rust
    // Validate use_existing_topic requires topic
    if args.use_existing_topic && args.topic.is_none() {
        return Err(anyhow!("--use-existing-topic requires --topic to be specified"));
    }
```

**Step 2: Update topic name generation**

Replace the `let topic_name = generate_topic_name();` line (around line 687) with:
```rust
    let topic_name = args.topic.clone().unwrap_or_else(generate_topic_name);
```

**Step 3: Update topic creation section**

Replace the topic creation and waiting section (around lines 697-705) with:
```rust
    // Create topic or verify existing topic
    let total_partitions = if args.use_existing_topic {
        info!("Using existing topic: {}", topic_name);
        let metadata = admin_client
            .get_topic_metadata(&topic_name)
            .await
            .map_err(|e| anyhow!("Cannot use existing topic '{}': {}", topic_name, e))?;
        info!("Topic '{}' has {} partitions", topic_name, metadata.partition_count);
        metadata.partition_count
    } else {
        info!("Test topic: {}", topic_name);
        admin_client
            .create_topic(&topic_name, total_partitions, 1)
            .await
            .map_err(|e| anyhow!("Topic creation failed: {}", e))?;
        // 3s delay allows Kafka to propagate metadata to all brokers
        info!("Waiting for topic to be ready...");
        sleep(Duration::from_secs(3)).await;
        total_partitions
    };
```

Note: This requires moving `total_partitions` calculation before topic creation. The variable is already calculated by `calculate_thread_config`.

**Step 4: Update cleanup to skip deletion**

Update the `cleanup_topic` call (around line 792) to pass the flag:
```rust
    cleanup_topic(&admin_client, &topic_name, args.profile_report, args.quiet, args.use_existing_topic).await;
```

Update the `cleanup_topic` function signature and implementation:
```rust
async fn cleanup_topic(admin_client: &KafkaClient, topic_name: &str, profile_report: bool, quiet: bool, use_existing_topic: bool) {
    if !use_existing_topic {
        if let Err(e) = admin_client.delete_topic(topic_name).await {
            if !quiet {
                warn!("Failed to delete topic '{}': {}", topic_name, e);
            }
        }
    }

    if !quiet {
        info!("Kitt measurement completed successfully!");
    }

    if profile_report && !quiet {
        profiling::generate_report();
    }
}
```

**Step 5: Add TopicMetadata import**

Add to the imports at the top of main.rs:
```rust
use kitt_core::TopicMetadata;
```

**Step 6: Run tests and verify build**

Run: `cargo build -p kitt && cargo test -p kitt`
Expected: PASS

**Step 7: Commit**

```bash
git add kitt/src/main.rs
git commit -m "feat(kitt): wire --use-existing-topic to main workflow"
```

---

## Task 6: Final integration test

**Step 1: Run all tests**

Run: `cargo test --workspace`
Expected: All tests pass

**Step 2: Run clippy**

Run: `cargo clippy --workspace`
Expected: No warnings

**Step 3: Verify help text**

Run: `cargo run -p kitt -- --help`
Expected: Shows `--use-existing-topic` and `--topic` flags with descriptions

**Step 4: Commit any fixes if needed**

If any issues found, fix and commit.

**Step 5: Final commit**

```bash
git add -A
git commit -m "test: verify use-existing-topic integration"
```

---

## Summary

| Task | Description | Files |
|------|-------------|-------|
| 1 | Add use_existing_topic to TestConfig | kitt-core/src/config.rs |
| 2 | Add get_topic_metadata to KafkaClient | kitt-core/src/client.rs |
| 3 | Update runner for use_existing_topic | kitt-core/src/runner.rs, lib.rs |
| 4 | Add CLI flags | kitt/src/args.rs |
| 5 | Wire flags in main.rs | kitt/src/main.rs |
| 6 | Integration testing | - |
