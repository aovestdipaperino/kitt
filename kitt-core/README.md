# kitt-core

Core library for Kafka throughput testing. Build custom test harnesses with fine-grained control over producers, consumers, message sizes, partition assignment, and real-time progress reporting.

## Quick start

Add to your `Cargo.toml`:

```toml
[dependencies]
kitt-core = { path = "../kitt-core" }
tokio = { version = "1", features = ["full"] }
anyhow = "1"
```

### Minimal test

```rust
use kitt_core::{run_test, TestConfig, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .duration(Duration::from_secs(10))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        match event {
            TestEvent::Progress { current_rate, messages_sent, messages_received, .. } => {
                println!("{:.0} msg/s (sent: {}, received: {})", current_rate, messages_sent, messages_received);
            }
            TestEvent::Completed(results) => {
                println!("Done: {:.0} msg/s over {:.1}s", results.throughput, results.duration.as_secs_f64());
            }
            _ => {}
        }
    }

    handle.wait().await?;
    Ok(())
}
```

## Examples

### High-throughput producer stress test

Produce-only mode skips consumers entirely, useful for saturating a broker.

```rust
use kitt_core::{run_test, MessageSize, ProduceOnlyMode, TestConfig, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .producer_threads(8)
        .message_size(MessageSize::Fixed(4096))
        .messages_per_batch(10)
        .produce_only(Some(ProduceOnlyMode::TimeBased))
        .duration(Duration::from_secs(30))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        if let TestEvent::Progress { current_rate, bytes_sent, .. } = event {
            let mb_sent = bytes_sent as f64 / (1024.0 * 1024.0);
            println!("{:.0} msg/s | {:.1} MB sent", current_rate, mb_sent);
        }
    }

    let results = handle.wait().await?;
    println!("Peak: {:.0} msg/s | Total: {} messages", results.max_rate, results.messages_sent);
    Ok(())
}
```

### Produce until a data target is reached

Stop after sending a specific amount of data instead of running for a fixed duration.

```rust
use kitt_core::{run_test, ProduceOnlyMode, TestConfig, TestEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let one_gb = 1024 * 1024 * 1024;

    let config = TestConfig::builder()
        .broker("localhost:9092")
        .producer_threads(4)
        .produce_only(Some(ProduceOnlyMode::DataTarget(one_gb)))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        if let TestEvent::Progress { bytes_sent, .. } = event {
            println!("{:.1}% complete", bytes_sent as f64 / one_gb as f64 * 100.0);
        }
    }

    handle.wait().await?;
    Ok(())
}
```

### Variable message sizes

Simulate realistic workloads where messages aren't all the same size.

```rust
use kitt_core::{run_test, MessageSize, TestConfig, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .message_size(MessageSize::Range(64, 8192)) // 64 bytes to 8 KB
        .duration(Duration::from_secs(15))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        if let TestEvent::Completed(results) = event {
            let avg_msg_size = results.bytes_sent as f64 / results.messages_sent as f64;
            println!("Avg message size: {:.0} bytes", avg_msg_size);
            println!("Throughput: {:.0} msg/s", results.throughput);
        }
    }

    handle.wait().await?;
    Ok(())
}
```

### Using an existing topic

Skip topic creation/deletion to test against pre-configured topics.

```rust
use kitt_core::{run_test, TestConfig, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .topic(Some("my-production-topic".to_string()))
        .use_existing_topic(true)
        .duration(Duration::from_secs(20))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        if let TestEvent::Completed(results) = event {
            println!("Throughput: {:.0} msg/s", results.throughput);
        }
    }

    handle.wait().await?;
    Ok(())
}
```

### Consumer fetch delay with backlog testing

Delay consumers to build up a message backlog, then measure how fast consumers drain it.

```rust
use kitt_core::{run_test, TestConfig, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .producer_threads(4)
        .consumer_threads(4)
        .fetch_delay(Duration::from_secs(5)) // producers run 5s before consumers start
        .duration(Duration::from_secs(15))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        match event {
            TestEvent::Progress { backlog_percent, current_rate, .. } => {
                println!("{:.0} msg/s | backlog: {}%", current_rate, backlog_percent);
            }
            TestEvent::Completed(results) => {
                println!("Avg backlog: {}%", results.avg_backlog_percent);
                println!("Throughput: {:.0} msg/s", results.throughput);
            }
            _ => {}
        }
    }

    handle.wait().await?;
    Ok(())
}
```

### Keyed messages with sticky partition assignment

Use sticky partitions so each producer thread writes to dedicated partitions, avoiding cross-thread contention.

```rust
use kitt_core::{run_test, KeyStrategy, TestConfig, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .key_strategy(KeyStrategy::from_pool_size(Some(1000))) // pool of 1000 pre-generated keys
        .sticky(true)
        .producer_partitions_per_thread(2)
        .consumer_partitions_per_thread(2)
        .duration(Duration::from_secs(15))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        if let TestEvent::Completed(results) = event {
            println!("Sent: {} | Received: {}", results.messages_sent, results.messages_received);
            println!("Throughput: {:.0} msg/s", results.throughput);
        }
    }

    handle.wait().await?;
    Ok(())
}
```

### Phase tracking for custom UIs

React to each phase of the test lifecycle to build progress indicators.

```rust
use kitt_core::{run_test, TestConfig, TestEvent, TestPhase};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .duration(Duration::from_secs(10))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        match event {
            TestEvent::PhaseChange { phase } => {
                let label = match phase {
                    TestPhase::Connecting => "Connecting to broker...",
                    TestPhase::CreatingTopic => "Creating topic...",
                    TestPhase::WaitingForTopic => "Waiting for topic...",
                    TestPhase::CreatingConnections => "Opening connections...",
                    TestPhase::Running => "Running test...",
                    TestPhase::Cleanup => "Cleaning up...",
                };
                println!("[PHASE] {}", label);
            }
            TestEvent::Warning { message } => println!("[WARN] {}", message),
            TestEvent::Error { message, recoverable } => {
                println!("[{}] {}", if recoverable { "ERR" } else { "FATAL" }, message);
            }
            TestEvent::Progress { current_rate, .. } => {
                println!("[RATE] {:.0} msg/s", current_rate);
            }
            TestEvent::Completed(results) => {
                println!("[DONE] {:.0} msg/s (min: {:.0}, max: {:.0})",
                    results.throughput, results.min_rate, results.max_rate);
            }
        }
    }

    handle.wait().await?;
    Ok(())
}
```

### Simulated processing delay

Add artificial consumer processing time to simulate real-world consumers that do work per message.

```rust
use kitt_core::{run_test, TestConfig, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .record_processing_time(5) // 5ms per record
        .consumer_threads(8)       // more consumers to compensate
        .duration(Duration::from_secs(15))
        .build()?;

    let mut handle = run_test(config).await?;

    while let Some(event) = handle.events.recv().await {
        if let TestEvent::Completed(results) = event {
            println!("With 5ms processing: {:.0} msg/s", results.throughput);
        }
    }

    handle.wait().await?;
    Ok(())
}
```

### Aborting a test early

Cancel a running test programmatically.

```rust
use kitt_core::{run_test, TestConfig, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .duration(Duration::from_secs(60))
        .build()?;

    let mut handle = run_test(config).await?;

    // Collect a few progress events, then abort
    let mut samples = 0;
    while let Some(event) = handle.events.recv().await {
        if let TestEvent::Progress { current_rate, .. } = event {
            println!("{:.0} msg/s", current_rate);
            samples += 1;
            if samples >= 50 { // ~5 seconds of samples at 100ms intervals
                handle.abort();
                break;
            }
        }
    }

    Ok(())
}
```

## Builder defaults

| Field | Default |
|-------|---------|
| `broker` | `localhost:9092` |
| `topic` | Auto-generated (e.g. `topic-brave-fox-jumps`) |
| `use_existing_topic` | `false` |
| `partitions` | `8` |
| `replication_factor` | `1` |
| `message_size` | `Fixed(1024)` (1 KB) |
| `key_strategy` | `NoKeys` |
| `messages_per_batch` | `1` |
| `producer_threads` | `4` |
| `consumer_threads` | `4` |
| `producer_partitions_per_thread` | `1` |
| `consumer_partitions_per_thread` | `1` |
| `sticky` | `false` |
| `duration` | `15s` |
| `fetch_delay` | `0s` |
| `produce_only` | `None` (produce + consume) |
| `message_validation` | `false` |
| `record_processing_time` | `0` ms |

## Key strategies

| Strategy | Created with | Behavior |
|----------|-------------|----------|
| `NoKeys` | `KeyStrategy::from_pool_size(None)` | Null keys, round-robin partition assignment |
| `RandomOnTheFly` | `KeyStrategy::from_pool_size(Some(0))` | New UUID per message, hash-based partitioning |
| `RandomPool(N)` | `KeyStrategy::from_pool_size(Some(1000))` | Pick from N pre-generated UUIDs, controlled cardinality |

## Event stream

`run_test` returns a `TestHandle` with an `events` channel (`tokio::sync::mpsc::Receiver<TestEvent>`). Events are emitted at ~100ms intervals during the running phase.

| Event | When |
|-------|------|
| `PhaseChange { phase }` | Test transitions between lifecycle phases |
| `Progress { .. }` | Every ~100ms during the running phase |
| `Warning { message }` | Non-fatal issues (e.g. topic cleanup failure) |
| `Error { message, recoverable }` | Errors during test execution |
| `Completed(TestResults)` | Test finished, contains final metrics |
