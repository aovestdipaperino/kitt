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
