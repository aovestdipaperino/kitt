# NASA "Power of 10" Rules Compliance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the kitt codebase into compliance with the NASA "Power of 10" adapted Rust rules documented in `~/Rust-NASA-rules.md`.

**Architecture:** The work is organized into 7 tasks in dependency order. Rule 12 (lints) and Rule 9 (error types) are foundational — they must land first because subsequent work depends on concrete error types and lint enforcement. Rule 8 (no panicking) builds on the new error types. Rule 4 (function decomposition) is the largest task. Rules 2, 5, and 11 are smaller fixes that can be done independently.

**Tech Stack:** Rust (edition 2024), tokio, kafka-protocol, thiserror, anyhow (to be removed from library crates)

---

## File Map

### Files to Create
- `kitt-core/src/error.rs` — Concrete, `#[non_exhaustive]` error enum for kitt-core library

### Files to Modify
- `kitt-core/src/lib.rs` — Add lint attributes, export error module
- `kitt/src/lib.rs` — Add lint attributes
- `kitt/src/main.rs` — Add lint attributes, decompose `main`/`run_measurement`/`print_final_analysis`, replace unwraps
- `kitt_throbbler/src/lib.rs` — Add lint attributes, replace unwraps, bound the audio loop
- `kitt-core/src/client.rs` — Replace `anyhow` with concrete errors, replace `expect` calls, add assertions, add Mutex timeout
- `kitt-core/src/config.rs` — Replace `expect` in `generate_key`
- `kitt-core/src/runner.rs` — Replace `unwrap` on `choose()`, update error types
- `kitt-core/src/utils.rs` — Replace `anyhow` with `KittError`, convert `gcd` from recursive to iterative, convert direct indexing to `.get()`
- `kitt-core/src/consumer.rs` — Replace `anyhow`, decompose `consume_messages`, replace `panic!`
- `kitt-core/src/producer.rs` — Replace `anyhow`, decompose `produce_messages` and `validate_produce_response`
- `kitt-core/src/consts.rs` — Add any new constants for loop guards
- `kitt-core/Cargo.toml` — Add `thiserror` dependency, remove `anyhow`
- `kitt/Cargo.toml` — Add `thiserror` dependency (keep `anyhow` for binary main only)
- `kitt/src/consumer.rs` — Replace `panic!`, decompose `consume_messages`
- `kitt/src/producer.rs` — Decompose `produce_messages`
- `kitt/src/profiling.rs` — Decompose `generate_report`, add assertions
- `kitt/src/measurer.rs` — Decompose `measure`, add assertions
- `kitt/src/ui.rs` — Add assertions

---

## Task 1: Add Crate-Level Lint Attributes (Rule 12)

**Files:**
- Modify: `kitt-core/src/lib.rs:1`
- Modify: `kitt/src/lib.rs:1`
- Modify: `kitt/src/main.rs:1`
- Modify: `kitt_throbbler/src/lib.rs:1`

- [ ] **Step 1: Add lint attributes to `kitt-core/src/lib.rs`**

Add at top of file, before doc comment:
```rust
#![deny(warnings)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::indexing_slicing)]
```

- [ ] **Step 2: Add lint attributes to `kitt/src/lib.rs`**

Add at top of file, before doc comment:
```rust
#![deny(warnings)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::indexing_slicing)]
```

- [ ] **Step 3: Add lint attributes to `kitt/src/main.rs`**

Add at top of file, before doc comment:
```rust
#![deny(warnings)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::indexing_slicing)]
```

- [ ] **Step 4: Add lint attributes to `kitt_throbbler/src/lib.rs`**

Add at top of file, before doc comment:
```rust
#![deny(warnings)]
#![deny(clippy::all)]
#![warn(clippy::pedantic)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::indexing_slicing)]
```

- [ ] **Step 5: Run `cargo clippy` to see all existing violations**

Run: `cargo clippy --workspace 2>&1 | head -100`
Expected: Many errors from existing unwrap/expect/indexing usage and pedantic lints. This establishes the baseline.

Note: Do NOT fix anything yet — this shows us the full scope. Temporarily allow the unwrap/expect/indexing lints with `#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]` in all four crate roots so the build passes while we fix violations in subsequent tasks. Remove these allows at the very end.

- [ ] **Step 6: Commit**

```bash
git add kitt-core/src/lib.rs kitt/src/lib.rs kitt/src/main.rs kitt_throbbler/src/lib.rs
git commit -m "chore: add NASA Rule 12 crate-level lint attributes (with temp allows)"
```

---

## Task 2: Create Concrete Error Types (Rule 9)

**Files:**
- Create: `kitt-core/src/error.rs`
- Modify: `kitt-core/Cargo.toml` (add `thiserror = "2"`)
- Modify: `kitt-core/src/lib.rs` (add `pub mod error;` and re-export)

- [ ] **Step 1: Add `thiserror` to `kitt-core/Cargo.toml`**

Add under `[dependencies]`:
```toml
thiserror = "2"
```

- [ ] **Step 2: Write the failing test for error types**

Create `kitt-core/src/error.rs` with tests:
```rust
//! Concrete error types for kitt-core
//!
//! Replaces `anyhow::Error` in all public APIs per NASA Rule 9.

/// Errors that can occur during Kafka client operations
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum KittError {
    /// TCP connection to broker failed
    #[error("connection failed to {broker}: {source}")]
    Connection {
        broker: String,
        source: std::io::Error,
    },

    /// Kafka protocol encoding/decoding error
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Broker returned an error code for an operation
    #[error("broker error (code {code}): {message}")]
    BrokerError {
        code: i16,
        message: String,
    },

    /// API version not supported by broker
    #[error("unsupported API version: {0}")]
    UnsupportedVersion(String),

    /// Topic operation failed (create, delete, metadata)
    #[error("topic operation failed: {0}")]
    TopicOperation(String),

    /// Request timed out
    #[error("request timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Too many consecutive errors
    #[error("too many consecutive errors ({count}): last error: {last_error}")]
    TooManyErrors {
        count: u32,
        last_error: String,
    },

    /// Response exceeded maximum size
    #[error("response size {size} exceeds maximum {max}")]
    ResponseTooLarge {
        size: usize,
        max: usize,
    },

    /// CRC verification failed
    #[error("CRC verification failed for partition {partition}: {detail}")]
    CrcFailure {
        partition: usize,
        detail: String,
    },

    /// I/O error during network communication
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Produce operation failed
    #[error("produce failed: {0}")]
    ProduceError(String),

    /// Consume operation failed
    #[error("consume failed: {0}")]
    ConsumeError(String),
}

/// Convenience Result alias for kitt-core operations
pub type Result<T> = std::result::Result<T, KittError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_is_non_exhaustive() {
        // This test verifies the error enum compiles with #[non_exhaustive]
        let err = KittError::Protocol("test".to_string());
        assert!(!format!("{err}").is_empty());
    }

    #[test]
    fn error_display_formats_correctly() {
        let err = KittError::Connection {
            broker: "localhost:9092".to_string(),
            source: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
        };
        assert!(format!("{err}").contains("localhost:9092"));
        assert!(format!("{err}").contains("refused"));
    }

    #[test]
    fn error_converts_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broke");
        let kitt_err: KittError = io_err.into();
        assert!(format!("{kitt_err}").contains("pipe broke"));
    }

    #[test]
    fn result_alias_works() {
        let ok: Result<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: Result<i32> = Err(KittError::Protocol("bad".into()));
        assert!(err.is_err());
    }
}
```

- [ ] **Step 3: Run test to verify it compiles and passes**

Run: `cargo test -p kitt-core --lib error`
Expected: PASS — 4 tests pass

- [ ] **Step 4: Export error module from `kitt-core/src/lib.rs`**

Add `pub mod error;` and `pub use error::{KittError, Result as KittResult};` to the lib.

- [ ] **Step 5: Migrate `kitt-core/src/client.rs` from `anyhow` to `KittError`**

Replace `use anyhow::{anyhow, Result};` with `use crate::error::{KittError, Result};`.

Key replacements:
- `anyhow!("msg")` → `KittError::Protocol("msg".to_string())` (or more specific variant)
- `.context("msg")?` → `.map_err(|e| KittError::Protocol(format!("msg: {e}")))?`
- Connection errors → `KittError::Connection { broker, source }`
- Timeout errors → `KittError::Timeout(duration)`
- Topic errors → `KittError::TopicOperation(msg)`

- [ ] **Step 6: Migrate `kitt-core/src/consumer.rs` from `anyhow` to `KittError`**

Replace `use anyhow::Result;` with `use crate::error::Result;`.
The `consume_messages` method returns `Result<()>` — this now uses `KittError`.

- [ ] **Step 7: Migrate `kitt-core/src/producer.rs` from `anyhow` to `KittError`**

Replace `use anyhow::{anyhow, Result};` with `use crate::error::{KittError, Result};`.

- [ ] **Step 8: Migrate `kitt-core/src/utils.rs` from `anyhow` to `KittError`**

Replace `use anyhow::{anyhow, Result};` with `use crate::error::{KittError, Result};`.
`parse_bytes` and `verify_record_batch_crc` are public library functions — they must use concrete types.
Replace `anyhow!("msg")` calls with appropriate `KittError` variants (e.g., `KittError::Protocol(...)` for parse errors).
Also convert direct array indexing in `verify_record_batch_crc` (lines ~94, 107, 115: `data[8]`, `data[16]`, `data[17]`) to `.get()` with error propagation, or add `#[allow(clippy::indexing_slicing)]` with a `// SAFETY:` comment explaining the length is checked on line 89.

- [ ] **Step 9: Migrate `kitt-core/src/runner.rs` from `anyhow` to `KittError`**

Update the runner to use `KittError` for its public API.

- [ ] **Step 10: Update `kitt/src/consumer.rs` and `kitt/src/producer.rs`**

These mirror the core crate. Update imports to use `kitt_core::KittError` or keep `anyhow` at the binary level by converting with `.map_err`. Since these are in the binary crate, `anyhow` is acceptable per Rule 9 ("Do NOT use `Box<dyn Error>` or `anyhow::Error` in **library** code"). But we should still convert for consistency.

- [ ] **Step 11: Run full test suite**

Run: `cargo test --workspace`
Expected: All tests pass with new error types

- [ ] **Step 12: Commit**

```bash
git add kitt-core/src/error.rs kitt-core/src/lib.rs kitt-core/src/client.rs kitt-core/src/consumer.rs kitt-core/src/producer.rs kitt-core/src/runner.rs kitt-core/src/utils.rs kitt-core/Cargo.toml kitt/src/consumer.rs kitt/src/producer.rs
git commit -m "feat: add concrete KittError type, replace anyhow in library code (Rule 9)"
```

---

## Task 3: Eliminate Panicking in Production Paths (Rule 8)

**Files:**
- Modify: `kitt-core/src/runner.rs:283-285` — `.unwrap()` on `choose()`
- Modify: `kitt-core/src/config.rs:85` — `.expect("pool should not be empty")`
- Modify: `kitt-core/src/client.rs:747-844` — `.expect()` on encoding
- Modify: `kitt-core/src/consumer.rs:272` — `panic!()` on CRC failure
- Modify: `kitt/src/consumer.rs:278` — `panic!()` on CRC failure
- Modify: `kitt_throbbler/src/lib.rs:154,305,566` — `.unwrap()` on audio/flush
- Modify: `kitt/src/main.rs:59-60,136-138,845,864` — `.unwrap()` on parse/flush

- [ ] **Step 1: Fix `kitt-core/src/runner.rs:283-285` — `choose().unwrap()`**

Replace:
```rust
let adj = adjectives.choose(&mut rng).unwrap();
let noun = nouns.choose(&mut rng).unwrap();
let verb = verbs.choose(&mut rng).unwrap();
```
With:
```rust
let adj = adjectives.choose(&mut rng).unwrap_or(&"swift");
let noun = nouns.choose(&mut rng).unwrap_or(&"stream");
let verb = verbs.choose(&mut rng).unwrap_or(&"flowing");
```
(These are hardcoded arrays that can never be empty, but we still avoid `.unwrap()` for lint compliance.)

- [ ] **Step 2: Fix `kitt/src/main.rs:136-138` — same pattern**

Same fix as Step 1 for the duplicate `choose().unwrap()` calls in main.rs.

- [ ] **Step 3: Fix `kitt-core/src/config.rs:85` — `expect("pool should not be empty")`**

Replace:
```rust
let key = pool.choose(&mut thread_rng()).expect("pool should not be empty");
```
With:
```rust
let key = match pool.choose(&mut thread_rng()) {
    Some(k) => k,
    None => return None, // Pool somehow empty — treat as no-key
};
```

- [ ] **Step 4: Fix `kitt-core/src/client.rs` — `.expect()` on encoding (test helpers)**

These are in test helper functions (`encode_delete_request`, `encode_create_request`, `build_test_request`). Tests are allowed to use `.expect()` per Rule 8. Add `#[allow(clippy::expect_used)]` on the test module or individual test helper functions.

- [ ] **Step 5: Fix `kitt-core/src/consumer.rs:272` — `panic!()` on CRC failure**

Replace:
```rust
panic!(
    "CRC verification failed for partition {}: {}",
    partition_id, crc_err
);
```
With:
```rust
return Err(KittError::CrcFailure {
    partition: partition_id,
    detail: crc_err.to_string(),
});
```

- [ ] **Step 6: Fix `kitt/src/consumer.rs:278` — same panic pattern**

Same fix as Step 5 for the kitt binary consumer.

- [ ] **Step 7: Fix `kitt_throbbler/src/lib.rs:154` — `Sink::try_new().unwrap()`**

Replace with `.ok()?` or graceful fallback (skip audio if sink creation fails).

- [ ] **Step 8: Fix `kitt_throbbler/src/lib.rs:305,566` — `stdout().flush().unwrap()`**

Replace with:
```rust
let _ = io::stdout().flush();
```
Stdout flush failure is non-critical in an animation library.

- [ ] **Step 9: Fix `kitt/src/main.rs:59-60` — `.parse().unwrap()` in logging setup**

These are in one-time init (`setup_logging`). This is acceptable per Rule 8 ("One-time initialization code where failure is truly unrecoverable"). Add `#[allow(clippy::unwrap_used)]` with a comment: `// Init-time: tracing directive parsing cannot fail for hardcoded strings`.

- [ ] **Step 10: Fix `kitt/src/main.rs:845,864` — `stdout().flush().unwrap()`**

Replace with `let _ = std::io::Write::flush(&mut std::io::stdout());`

- [ ] **Step 11: Run full test suite**

Run: `cargo test --workspace`
Expected: All tests pass, no panicking in production paths

- [ ] **Step 12: Commit**

```bash
git add -u
git commit -m "fix: eliminate panic/unwrap/expect from production paths (Rule 8)"
```

---

## Task 4: Decompose Oversized Functions (Rule 4)

This is the largest task. Each function >60 lines needs decomposition. Prioritized by severity.

**Priority 1: `consume_messages` (~325 lines each in both crates)**

- [ ] **Step 1: Extract fetch request building from `kitt-core/src/consumer.rs`**

Extract lines that build `FetchRequest` into:
```rust
fn build_fetch_request(
    topic: &str,
    start_partition: usize,
    partition_count: usize,
    offsets: &[i64],
    client: &KafkaClient,
) -> (FetchRequest, i16) { ... }
```

- [ ] **Step 2: Extract response processing into `process_fetch_response`**

Extract the `Ok(Ok(response_bytes))` arm into:
```rust
async fn process_fetch_response(
    &self,
    response_bytes: &Bytes,
    version: i16,
    offsets: &mut [i64],
    messages_received: &Arc<AtomicU64>,
    message_validation: bool,
) -> Result<()> { ... }
```

- [ ] **Step 3: Extract partition processing into `process_partition`**

The inner loop over `topic_response.partitions` becomes:
```rust
async fn process_partition(
    &self,
    partition_response: &PartitionData,
    offsets: &mut [i64],
    message_validation: bool,
) -> Result<u64> { ... }
```

- [ ] **Step 4: Extract error handling into `handle_fetch_error`**

The `Ok(Err(...))` and `Err(timeout)` arms become:
```rust
fn handle_fetch_error(
    &self,
    error: &str,
    consecutive_errors: &mut u32,
) -> ControlFlow<(), Duration> { ... }
```
Returns `ControlFlow::Break(())` to stop, or `ControlFlow::Continue(backoff)` with backoff duration.

- [ ] **Step 5: Apply same decomposition to `kitt/src/consumer.rs`**

Mirror the exact same extraction for the binary crate's consumer.

- [ ] **Step 6: Run tests**

Run: `cargo test --workspace`
Expected: All tests pass — behavior unchanged

- [ ] **Step 7: Commit**

```bash
git add -u
git commit -m "refactor: decompose consume_messages into focused sub-functions (Rule 4)"
```

**Priority 2: `produce_messages` (~200 lines each in both crates)**

- [ ] **Step 8: Extract batch building from `kitt-core/src/producer.rs`**

Extract into `fn build_produce_batch(...)`.

- [ ] **Step 9: Extract pending futures cleanup**

The `while i < pending_futures.len()` loop becomes `async fn drain_completed_futures(...)`.

- [ ] **Step 10: Apply same to `kitt/src/producer.rs`**

- [ ] **Step 11: Run tests and commit**

Run: `cargo test --workspace`

```bash
git add -u
git commit -m "refactor: decompose produce_messages into focused sub-functions (Rule 4)"
```

**Priority 3: `validate_produce_response` (~205 lines)**

- [ ] **Step 12: Decompose `validate_produce_response` in `kitt-core/src/producer.rs`**

Split into `decode_produce_response` and `check_partition_errors`.

- [ ] **Step 13: Run tests and commit**

```bash
git add -u
git commit -m "refactor: decompose validate_produce_response (Rule 4)"
```

**Priority 4: `main`, `run_measurement`, `print_final_analysis` in `kitt/src/main.rs`**

- [ ] **Step 14: Extract setup logic from `main`**

Create `fn parse_and_validate_config() -> Result<Config>` and `async fn setup_kafka(config: &Config) -> Result<...>`.

- [ ] **Step 15: Extract `print_final_analysis` sub-functions**

Split into `print_throughput_summary`, `print_rate_analysis`, `print_data_summary`.

- [ ] **Step 16: Decompose `run_measurement`**

Extract producer spawning, consumer spawning, and measurement loop into separate functions.

- [ ] **Step 17: Run tests and commit**

```bash
git add -u
git commit -m "refactor: decompose main.rs oversized functions (Rule 4)"
```

**Priority 5: Remaining functions**

- [ ] **Step 18: Decompose `generate_report` in `kitt/src/profiling.rs`**

Split into `print_operation_table`, `print_category_analysis`, `print_recommendations`.

- [ ] **Step 19: Decompose `run_demo` in `kitt_throbbler/src/lib.rs`**

Extract pattern switching and frame drawing logic.

- [ ] **Step 20: Decompose `measure` in `kitt/src/measurer.rs`**

Extract the measurement loop body into a helper.

- [ ] **Step 21: Decompose remaining oversized functions**

`run_test_inner`, `validate_fetch_response`, `delete_topic` in kitt-core.

- [ ] **Step 22: Run tests and commit**

```bash
git add -u
git commit -m "refactor: decompose remaining oversized functions (Rule 4)"
```

---

## Task 5: Add Production Assertions (Rule 5)

Every function needs ≥2 assertions. Add precondition + postcondition assertions to all non-trivial functions.

**Files:** All `.rs` files with functions

- [ ] **Step 1: Add assertions to `kitt-core/src/client.rs` functions**

Example for `connect`:
```rust
pub async fn connect(broker: &str) -> Result<Self> {
    assert!(!broker.is_empty(), "broker address must not be empty");
    // ... existing code ...
    assert!(!client.api_versions.is_empty(), "must discover at least one API version");
    Ok(client)
}
```

Example for `send_request`:
```rust
pub async fn send_request<T: Encodable + std::fmt::Debug>(
    &self,
    api_key: ApiKey,
    request: &T,
    version: i16,
) -> Result<Bytes> {
    assert!(version >= 0, "protocol version must be non-negative");
    // ... existing code ...
    assert!(!response.is_empty(), "response must not be empty");
    Ok(response)
}
```

- [ ] **Step 2: Add assertions to consumer functions**

Add precondition assertions on `duration > 0`, partition counts, etc.

- [ ] **Step 3: Add assertions to producer functions**

Add precondition assertions on batch sizes, partition counts, message sizes.

- [ ] **Step 4: Add assertions to `kitt_throbbler` functions**

Assert LED count > 0, speed > 0, etc.

- [ ] **Step 5: Add assertions to runner, measurer, UI functions**

- [ ] **Step 6: Run tests**

Run: `cargo test --workspace`
Expected: All pass — assertions should not fire on valid inputs

- [ ] **Step 7: Commit**

```bash
git add -u
git commit -m "feat: add precondition/postcondition assertions to all functions (Rule 5)"
```

---

## Task 6: Fix Recursion, Bounded Loop, and Concurrency Issues (Rules 1, 2, 11)

- [ ] **Step 1: Convert `gcd()` from recursive to iterative in `kitt-core/src/utils.rs`**

The current implementation uses direct recursion, violating Rule 1. Replace:
```rust
pub fn gcd(a: usize, b: usize) -> usize {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}
```
With:
```rust
pub fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}
```

Run: `cargo test -p kitt-core --lib utils` to verify existing tests still pass.

- [ ] **Step 2: Bound the audio loop in `kitt_throbbler/src/lib.rs:159`**

Add a maximum iteration guard:
```rust
const MAX_AUDIO_LOOPS: u64 = 100_000;
let mut audio_guard = 0u64;
loop {
    if stop_flag_clone.load(Ordering::Relaxed) || audio_guard >= MAX_AUDIO_LOOPS {
        break;
    }
    audio_guard += 1;
    // ... existing loop body ...
}
```

- [ ] **Step 3: Add timeout strategy to `Mutex` in `kitt-core/src/client.rs`**

The `stream: Arc<Mutex<TcpStream>>` uses `tokio::sync::Mutex` (not std). Tokio's Mutex doesn't have `try_lock_for`, but we can wrap with `tokio::time::timeout`:

Replace direct `.lock().await` calls with:
```rust
let mut stream = tokio::time::timeout(
    Duration::from_millis(TOPIC_OPERATION_TIMEOUT_MS),
    self.stream.lock(),
).await.map_err(|_| KittError::Timeout(Duration::from_millis(TOPIC_OPERATION_TIMEOUT_MS)))?;
```

Add a comment documenting the lock ordering: "Single lock — no ordering needed. `stream` is the only shared mutable resource."

- [ ] **Step 4: Run tests and commit**

Run: `cargo test --workspace`

```bash
git add kitt-core/src/utils.rs kitt_throbbler/src/lib.rs kitt-core/src/client.rs
git commit -m "fix: convert gcd to iterative, bound audio loop, add mutex timeout (Rules 1, 2, 11)"
```

---

## Task 7: Final Cleanup — Remove Temporary Allows and Verify

- [ ] **Step 1: Remove `#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]` from all crate roots**

Now that all production unwrap/expect/indexing calls are fixed, remove the temporary allows from `kitt-core/src/lib.rs`, `kitt/src/lib.rs`, `kitt/src/main.rs`, `kitt_throbbler/src/lib.rs`.

- [ ] **Step 2: Add targeted `#[allow]` only in test modules**

Add `#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]` to `#[cfg(test)]` modules where test code legitimately uses unwrap/expect/indexing.

- [ ] **Step 3: Run `cargo clippy --workspace`**

Run: `cargo clippy --workspace -- -D warnings 2>&1`
Expected: Zero warnings, zero errors

- [ ] **Step 4: Run full test suite**

Run: `cargo test --workspace`
Expected: All tests pass

- [ ] **Step 5: Run `cargo audit`**

Run: `cargo audit`
Expected: No known vulnerabilities (or document any that are upstream/unavoidable)

- [ ] **Step 6: Remove `anyhow` from `kitt-core/Cargo.toml` if fully migrated**

Verify no remaining `anyhow` imports in kitt-core, then remove from dependencies.

- [ ] **Step 7: Commit**

```bash
git add -u
git commit -m "chore: remove temporary lint allows, verify full NASA rules compliance (Rule 12)"
```

---

## Verification Checklist

After all tasks complete, verify each rule:

- [ ] Rule 1: No recursion — `gcd` converted to iterative (Task 6)
- [ ] Rule 2: All loops bounded — audio loop now guarded
- [ ] Rule 3: N/A — hosted `std` application
- [ ] Rule 4: No function >60 lines — all decomposed
- [ ] Rule 5: ≥2 assertions per function — all annotated
- [ ] Rule 6: Immutability by default — already passing
- [ ] Rule 7: No `unsafe` — already passing
- [ ] Rule 8: No panic/unwrap/expect in production — all replaced
- [ ] Rule 9: Concrete error types, `#[non_exhaustive]` — `KittError` in place
- [ ] Rule 10: Minimal macros — already passing
- [ ] Rule 11: Bounded channels, mutex timeout — fixed
- [ ] Rule 12: Lint attributes active, zero warnings — verified
