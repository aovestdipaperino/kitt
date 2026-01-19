# Refactor main.rs for Readability

## Overview

Break up the 2,771-line `main.rs` into focused modules while simplifying `main()` to be a readable 100-150 line orchestrator.

## Goals

- Improve code navigation and readability
- Each file has a single, clear purpose
- `main()` shows the full program flow without hiding it behind deep abstractions
- Preserve all existing functionality

## Module Structure

```
kitt/src/
├── main.rs               # CLI entry + orchestration (~100-150 lines)
├── args.rs               # Args struct, MessageSize, KeyStrategy enums
├── producer.rs           # Producer struct + produce_messages()
├── consumer.rs           # Consumer struct + consume_messages()
├── measurer.rs           # ThroughputMeasurer + display logic
├── utils.rs              # format_bytes, parse_bytes, gcd, lcm, verify_record_batch_crc
├── kafka_client.rs       # (unchanged)
├── profiling.rs          # (unchanged)
├── thread_assignment.rs  # (unchanged)
└── lib.rs                # pub mod declarations
```

## Module Details

### main.rs (~100-150 lines)

Readable top-to-bottom orchestration:

```rust
#[tokio::main]
async fn main() -> Result<()> {
    // 1. Setup (~10 lines)
    let args = Args::parse();
    setup_logging(&args);

    // 2. Connect to broker (~15 lines)
    let client = connect_with_retries(&args.broker).await?;
    let api_versions = client.get_api_versions();

    // 3. Create topic (~10 lines)
    let topic_name = create_test_topic(&client, &args).await?;

    // 4. Create producers & consumers (~20 lines)
    let producers = create_producers(&args, &topic_name, &api_versions).await?;
    let consumers = create_consumers(&args, &topic_name, &api_versions).await?;

    // 5. Run measurement (~30 lines)
    let measurer = ThroughputMeasurer::new(LED_BAR_WIDTH, !args.silent);
    let results = run_measurement(&args, producers, consumers, &measurer).await;

    // 6. Report & cleanup (~15 lines)
    print_results(&results, &args);
    delete_topic(&client, &topic_name).await;

    Ok(())
}
```

Helper functions like `connect_with_retries`, `create_test_topic`, `create_producers` stay in main.rs as orchestration glue.

### args.rs

Contains all CLI-related types:

- `Args` struct (clap derive)
- `MessageSize` enum + impl (Fixed, Range)
- `KeyStrategy` enum + impl (NoKeys, RandomKeys)

### producer.rs

- `Producer` struct
- `Producer::new()`
- `Producer::produce_messages()` - main production loop
- `Producer::validate_produce_response()` - private helper
- `Producer::build_record_batch()` - private helper

Dependencies: `kafka_client`, `profiling`, `utils::verify_record_batch_crc`

### consumer.rs

- `Consumer` struct
- `Consumer::new()`
- `Consumer::consume_messages()` - main consumption loop
- `Consumer::validate_fetch_response()` - private helper

Dependencies: `kafka_client`, `profiling`, `utils::verify_record_batch_crc`

### measurer.rs

- `LED_BAR_WIDTH` and `LED_MOVEMENT_SPEED` constants
- `ThroughputMeasurer` struct
- `ThroughputMeasurer::new()`
- `ThroughputMeasurer::measure()` - animation + metrics loop

Dependencies: `utils::format_bytes`, `kitt_throbbler`

### utils.rs

Pure helper functions:

- `gcd(a, b)` - greatest common divisor
- `lcm(a, b)` - least common multiple
- `format_bytes(bytes)` - "274.23 MB"
- `parse_bytes(s)` - "3GB" -> bytes
- `verify_record_batch_crc(data)` - CRC32C validation

## Test Organization

Move tests alongside the code they test:

```rust
// producer.rs
#[cfg(test)]
mod tests {
    #[test]
    fn test_produce_response_validation() { /* existing */ }
}

// consumer.rs
#[cfg(test)]
mod tests {
    #[test]
    fn test_fetch_response_validation() { /* existing */ }
}

// measurer.rs
#[cfg(test)]
mod tests {
    #[test]
    fn test_measurement_timing_calculation() { /* existing */ }
}
```

### Additional Tests

Add to utils.rs:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.00 MB");
        assert_eq!(format_bytes(3 * 1024 * 1024 * 1024), "3.00 GB");
    }

    #[test]
    fn test_parse_bytes() {
        assert_eq!(parse_bytes("1KB").unwrap(), 1024);
        assert_eq!(parse_bytes("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_bytes("3GB").unwrap(), 3 * 1024 * 1024 * 1024);
        assert!(parse_bytes("invalid").is_err());
    }

    #[test]
    fn test_gcd_lcm() {
        assert_eq!(gcd(12, 8), 4);
        assert_eq!(lcm(4, 6), 12);
    }
}
```

Add to args.rs:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn test_message_size_parse() { /* Fixed, Range, invalid */ }

    #[test]
    fn test_key_strategy_parse() { /* NoKeys, RandomKeys */ }
}
```

## Validation Strategy

After each module extraction:
1. `cargo test` - ensure tests pass
2. `cargo build --release` - catch visibility/import issues
3. Smoke test: `kitt --produce-only 100MB -d 5`

## Out of Scope

- `kafka_client.rs` - unchanged
- `profiling.rs` - unchanged
- `thread_assignment.rs` - unchanged
- Performance optimizations
- New features
