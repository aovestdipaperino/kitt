# Kitt Core Extraction Design

## Overview

Split the Kitt application into a reusable core library (`kitt-core`) and a CLI that consumes it. The core should be minimal, independent of UI concerns (throbbler, profiling), and provide both high-level and low-level APIs.

## Crate Structure

```
kitt/
├── Cargo.toml              # workspace root
├── kitt-core/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs          # public API exports
│       ├── client.rs       # KafkaClient
│       ├── producer.rs     # Producer
│       ├── consumer.rs     # Consumer
│       ├── config.rs       # TestConfig + builder
│       ├── events.rs       # TestEvent, TestPhase, TestResults
│       ├── runner.rs       # run_test() + TestHandle
│       ├── thread_assignment.rs
│       └── utils.rs        # format_bytes, parse_bytes, lcm
├── kitt/
│   ├── Cargo.toml          # depends on kitt-core, kitt_throbbler
│   └── src/
│       ├── main.rs         # CLI entry point
│       ├── args.rs         # clap argument parsing
│       ├── ui.rs           # event handler (throbbler, logging)
│       └── profiling.rs    # quantum-pulse integration
└── kitt_throbbler/         # unchanged
```

## Dependency Split

| Dependency | `kitt-core` | `kitt` (CLI) |
|------------|-------------|--------------|
| `kafka-protocol` | ✓ | |
| `tokio` | ✓ | ✓ |
| `clap` | | ✓ |
| `rand` | ✓ | |
| `anyhow` | ✓ | ✓ |
| `tracing` | ✓ | |
| `tracing-subscriber` | | ✓ |
| `bytes` | ✓ | |
| `crc32c` | ✓ | |
| `uuid` | ✓ | |
| `chrono` | | ✓ |
| `futures` | ✓ | |
| `indexmap` | ✓ | |
| `kitt_throbbler` | | ✓ |
| `quantum-pulse` | | ✓ |
| `mimalloc` | | ✓ |
| `serde` | ✓ | |

## Public API

### High-Level API

```rust
pub use config::{TestConfig, TestConfigBuilder, MessageSize, KeyStrategy, ProduceOnlyMode};
pub use events::{TestEvent, TestPhase, TestResults};
pub use runner::{run_test, TestHandle};
```

### Low-Level Building Blocks

```rust
pub use client::KafkaClient;
pub use producer::Producer;
pub use consumer::Consumer;
```

## Core Types

### TestConfig

```rust
pub struct TestConfig {
    pub broker: String,
    pub topic: Option<String>,          // None = auto-generate
    pub partitions: i32,
    pub replication_factor: i16,
    pub message_size: MessageSize,
    pub key_strategy: KeyStrategy,
    pub messages_per_batch: usize,
    pub producer_threads: usize,
    pub consumer_threads: usize,
    pub producer_partitions_per_thread: i32,
    pub consumer_partitions_per_thread: i32,
    pub sticky: bool,
    pub duration: Duration,
    pub fetch_delay: Duration,
    pub produce_only: Option<ProduceOnlyMode>,
    pub max_backlog: Option<u64>,
    pub message_validation: bool,
    pub record_processing_time: u64,
}
```

Builder provides defaults matching current CLI defaults.

### TestEvent

```rust
pub enum TestEvent {
    PhaseChange { phase: TestPhase },
    Progress {
        messages_sent: u64,
        messages_received: u64,
        bytes_sent: u64,
        current_rate: f64,
        backlog_percent: u8,
        elapsed: Duration,
    },
    Warning { message: String },
    Error { message: String, recoverable: bool },
    Completed(TestResults),
}

pub enum TestPhase {
    Connecting,
    CreatingTopic,
    WaitingForTopic,
    CreatingConnections,
    Running,
    Cleanup,
}
```

### TestResults

```rust
pub struct TestResults {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub duration: Duration,
    pub throughput: f64,
    pub min_rate: f64,
    pub max_rate: f64,
    pub avg_backlog_percent: u8,
}
```

### TestHandle

```rust
pub struct TestHandle {
    pub events: mpsc::Receiver<TestEvent>,
    handle: JoinHandle<Result<TestResults>>,
}

impl TestHandle {
    pub async fn wait(self) -> Result<TestResults>;
    pub fn abort(&self);
}
```

### Runner Function

```rust
pub async fn run_test(config: TestConfig) -> Result<TestHandle>;
```

## Usage Example

```rust
use kitt_core::{TestConfig, run_test, TestEvent};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = TestConfig::builder()
        .broker("localhost:9092")
        .duration(Duration::from_secs(30))
        .producer_threads(4)
        .consumer_threads(4)
        .build()?;

    let mut handle = run_test(config).await?;

    // Handle events
    while let Some(event) = handle.events.recv().await {
        match event {
            TestEvent::Progress { current_rate, .. } => {
                println!("{:.0} msg/s", current_rate);
            }
            TestEvent::Completed(results) => {
                println!("Final: {:.0} msg/s", results.throughput);
            }
            _ => {}
        }
    }

    let results = handle.wait().await?;
    Ok(())
}
```

## Migration Plan

### Files to Move to `kitt-core`

| Current file | Destination | Changes needed |
|--------------|-------------|----------------|
| `kafka_client.rs` | `client.rs` | Make public |
| `producer.rs` | `producer.rs` | Remove profiling macros |
| `consumer.rs` | `consumer.rs` | Remove profiling macros |
| `utils.rs` | `utils.rs` | As-is |
| `thread_assignment.rs` | `thread_assignment.rs` | As-is |

### Files to Split

| Current | Core portion | CLI portion |
|---------|--------------|-------------|
| `measurer.rs` | Atomic counters → `runner.rs` | LED/throbbler → `ui.rs` |
| `main.rs` | Orchestration → `runner.rs` | CLI glue → `main.rs` |

### Files Staying in CLI

- `args.rs` - Maps clap args to `TestConfig`
- `profiling.rs` - quantum-pulse integration
- New `ui.rs` - Event handler for throbbler/LED display

## Implementation Notes

1. The `measurer.rs` currently mixes atomic counters (core) with LED display (UI). Split them cleanly.

2. Topic name generation stays in core as a useful default, but can be overridden via `TestConfig::topic`.

3. The core uses `tracing` for logging but does not initialize the subscriber - that's the CLI's responsibility.

4. Channel buffer size for events should be reasonable (e.g., 100) to avoid backpressure affecting the test.
