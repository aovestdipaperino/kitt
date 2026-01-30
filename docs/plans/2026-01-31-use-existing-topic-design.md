# Use Existing Topic Design

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Allow kitt to use a pre-existing Kafka topic instead of creating and deleting one.

**Architecture:** Add `--use-existing-topic` flag that skips topic creation/deletion and queries the topic metadata to determine partition count.

**Tech Stack:** Rust, kafka-protocol

---

## Configuration Changes

### Core library (`kitt-core/src/config.rs`)

Add to `TestConfig`:
```rust
/// Use an existing topic instead of creating one
pub use_existing_topic: bool,
```

Add to `TestConfigBuilder`:
```rust
pub fn use_existing_topic(mut self, use_existing: bool) -> Self {
    self.config.use_existing_topic = use_existing;
    self
}
```

Validation in `build()`:
- If `use_existing_topic` is `true` and `topic` is `None`, return error

### CLI (`kitt/src/args.rs`)

Add flag:
```rust
/// Use an existing topic instead of creating one (requires --topic)
#[arg(long)]
pub use_existing_topic: bool,
```

Validation: When `--use-existing-topic` is set, `--topic` is required.

Note: `--partitions` and `--replication-factor` are ignored when `--use-existing-topic` is set.

---

## Runner Behavior Changes

### In `kitt-core/src/runner.rs`

When `config.use_existing_topic` is `true`:

1. **Skip CreatingTopic phase** - Go directly from Connecting to fetching metadata

2. **Fetch topic metadata** - Call `admin_client.get_topic_metadata(&topic_name)` to:
   - Verify topic exists (error if not: "Topic 'X' not found")
   - Get actual partition count

3. **Use actual partitions** - Override `total_partitions` with the value from metadata

4. **Skip topic deletion** - In cleanup phase, only delete if `!config.use_existing_topic`

### New KafkaClient method (`kitt-core/src/client.rs`)

```rust
/// Get metadata for an existing topic
/// Returns error if topic doesn't exist
pub async fn get_topic_metadata(&self, topic: &str) -> Result<TopicMetadata>

pub struct TopicMetadata {
    pub partition_count: i32,
}
```

---

## Error Handling

| Condition | Behavior |
|-----------|----------|
| `use_existing_topic=true`, `topic=None` | Config validation error in builder |
| Topic doesn't exist | Error: "Topic 'X' not found. Cannot use --use-existing-topic with non-existent topic." |

---

## CLI Output Changes

When `--use-existing-topic` is set:
- Show "Using existing topic 'X' (Y partitions)" instead of "Creating topic..."
- Skip "Deleting topic..." message in cleanup (topic is preserved)

---

## Files to Modify

1. `kitt-core/src/config.rs` - Add field and builder method, validation
2. `kitt-core/src/client.rs` - Add `get_topic_metadata` method and `TopicMetadata` struct
3. `kitt-core/src/runner.rs` - Conditional create/delete logic, metadata fetch
4. `kitt/src/args.rs` - Add CLI flag
5. `kitt/src/main.rs` - Wire flag to config, update UI messages

---

## Testing

**Unit tests (`kitt-core`):**
- `test_config_use_existing_topic_requires_topic_name` - Builder errors without topic
- `test_config_use_existing_topic_valid` - Builder succeeds with both set

**Manual integration testing:**
1. Create topic: `kafka-topics.sh --create --topic test-topic --partitions 4`
2. Run: `kitt --use-existing-topic --topic test-topic`
3. Verify topic exists after test
4. Verify partition count detected correctly
