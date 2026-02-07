//! Constants used across the kitt-core library
//!
//! Centralizes magic numbers with documentation explaining their purpose
//! and tuning considerations.

// -- Kafka Fetch Request Parameters --

/// Maximum bytes to fetch per partition (1 MB).
/// Prevents one partition from starving others in a multi-partition fetch.
pub const PARTITION_MAX_BYTES: i32 = 1024 * 1024;

/// Maximum total bytes for a fetch response (50 MB).
/// Caps memory usage per fetch to prevent spikes from large responses.
pub const FETCH_MAX_BYTES: i32 = 50 * 1024 * 1024;

/// Maximum time the broker waits for data before responding (1 second).
/// Balances latency (low wait = more requests) vs CPU usage (high wait = fewer requests).
pub const FETCH_MAX_WAIT_MS: i32 = 1000;

/// Timeout for a fetch request to complete (5 seconds).
/// Longer than `FETCH_MAX_WAIT_MS` to account for network RTT.
/// If this triggers, the broker is likely unresponsive.
pub const FETCH_TIMEOUT_MS: u64 = 5000;

// -- Kafka Produce Request Parameters --

/// Timeout for produce acknowledgment from all replicas (30 seconds).
/// Allows for slow replicas without failing prematurely.
pub const PRODUCE_TIMEOUT_MS: i32 = 30000;

/// Pause duration when consumer falls behind producer (10 ms).
/// Short enough to maintain throughput when consumer recovers.
pub const BACKPRESSURE_PAUSE_MS: u64 = 10;

/// Maximum in-flight produce requests per producer thread.
/// Balances throughput (pipelining) vs memory (bounded queues).
pub const MAX_PENDING_REQUESTS: usize = 20;

// -- Topic Operations --

/// Timeout for topic create/delete operations (30 seconds).
pub const TOPIC_OPERATION_TIMEOUT_MS: i32 = 30000;

/// Wait time after topic creation for it to become ready (3 seconds).
pub const TOPIC_READY_WAIT_SECS: u64 = 3;

// -- Connection Retry --

/// Maximum number of connection attempts before giving up.
pub const CONNECTION_MAX_ATTEMPTS: u32 = 7;

/// Base delay for exponential backoff on connection retry (1 second).
/// Actual delay: `base * 2^(attempt-1)` → 1s, 2s, 4s, 8s, 16s, 32s.
pub const CONNECTION_BASE_DELAY_MS: u64 = 1000;

// -- Response Safety --

/// Maximum allowed response size from broker (100 MB).
/// Prevents memory exhaustion from malformed or unexpectedly large responses.
pub const MAX_RESPONSE_SIZE: usize = 100 * 1024 * 1024;

// -- Error Retry / Backoff --

/// Maximum consecutive errors before a producer/consumer thread gives up.
pub const MAX_CONSECUTIVE_ERRORS: u32 = 5;

/// Base delay for exponential backoff on errors (100 ms).
/// Actual delay: `base * 2^(n-1)` capped at `MAX_BACKOFF_MS`.
pub const BASE_BACKOFF_MS: u64 = 100;

/// Maximum backoff delay between retries (3.2 seconds).
pub const MAX_BACKOFF_MS: u64 = 3200;

// -- Test Runner --

/// Base maximum pending messages before applying backpressure.
/// Multiplied by `fetch_delay` to compensate for delayed consumer start.
pub const BASE_MAX_BACKLOG: u64 = 1000;

/// Buffer size for the test event channel.
pub const EVENT_CHANNEL_SIZE: usize = 100;

/// Interval between progress reports during test execution (100 ms).
pub const PROGRESS_INTERVAL_MS: u64 = 100;
