//! UI module for handling test events and displaying Knight Rider animation
//!
//! This module provides the `UiHandler` struct that receives `TestEvent`s from
//! kitt-core and displays progress using the Knight Rider animation from kitt_throbbler.

use kitt_core::{TestEvent, TestPhase, TestResults};
use kitt_throbbler::{AudioHandle, KnightRiderAnimator};
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::utils::format_bytes;

/// Width of the LED bar in number of positions
///
/// This constant defines how many LED positions are available in the Knight Rider
/// animation display during throughput measurements.
#[allow(dead_code)]
pub const LED_BAR_WIDTH: usize = 25;

/// Constant controlling how many LED positions move per animation frame
///
/// This value affects the visual speed of the Knight Rider animation:
/// - Lower values (1-2) create slower, smoother movement
/// - Higher values (3-5) create faster, more energetic movement
#[allow(dead_code)]
pub const LED_MOVEMENT_SPEED: usize = 2;

/// Handler for displaying test progress with Knight Rider animation
///
/// This struct receives `TestEvent`s from the test runner and displays
/// real-time progress using the Knight Rider LED animation from kitt_throbbler.
#[allow(dead_code)]
pub struct UiHandler {
    /// The Knight Rider animator for visual feedback
    animator: KnightRiderAnimator,
    /// Whether to suppress most output (machine-readable mode)
    quiet: bool,
    /// Whether to disable audio playback
    silent: bool,
    /// Whether running in produce-only mode
    produce_only: bool,
    /// Maximum backlog threshold for percentage calculation
    max_backlog: u64,
    /// Animation position (0 to LED_BAR_WIDTH-1)
    position: usize,
    /// Animation direction (1 = right, -1 = left)
    direction: i32,
    /// Start time for calculating elapsed duration
    start_time: Option<Instant>,
    /// Minimum observed rate during test
    min_rate: f64,
    /// Maximum observed rate during test
    max_rate: f64,
}

#[allow(dead_code)]
impl UiHandler {
    /// Creates a new UiHandler with the specified settings
    ///
    /// # Arguments
    ///
    /// * `quiet` - Whether to suppress UI output (machine-readable mode)
    /// * `silent` - Whether to disable audio playback
    /// * `produce_only` - Whether running in produce-only mode
    /// * `max_backlog` - Maximum backlog threshold for percentage calculation
    pub fn new(quiet: bool, silent: bool, produce_only: bool, max_backlog: u64) -> Self {
        let audio_enabled = !quiet && !silent;
        Self {
            animator: KnightRiderAnimator::with_leds(LED_BAR_WIDTH).audio_enabled(audio_enabled),
            quiet,
            silent,
            produce_only,
            max_backlog,
            position: 0,
            direction: 1,
            start_time: None,
            min_rate: f64::MAX,
            max_rate: 0.0,
        }
    }

    /// Handles events from the test runner and displays progress
    ///
    /// This method receives events through a channel and updates the UI accordingly.
    /// It returns the final test results when the test completes.
    ///
    /// # Arguments
    ///
    /// * `events` - Channel receiver for test events
    ///
    /// # Returns
    ///
    /// The final `TestResults` if the test completed successfully, or `None` if
    /// the event channel was closed before completion.
    pub async fn handle_events(
        &mut self,
        mut events: mpsc::Receiver<TestEvent>,
    ) -> Option<TestResults> {
        // Start audio playback if enabled
        let _audio_handle: Option<AudioHandle> = if !self.quiet && !self.silent {
            self.animator.start_audio()
        } else {
            None
        };

        while let Some(event) = events.recv().await {
            match event {
                TestEvent::PhaseChange { phase } => {
                    self.handle_phase_change(phase);
                }
                TestEvent::Progress {
                    messages_sent,
                    messages_received,
                    bytes_sent,
                    current_rate,
                    backlog_percent,
                    elapsed: _,
                } => {
                    self.handle_progress(
                        messages_sent,
                        messages_received,
                        bytes_sent,
                        current_rate,
                        backlog_percent,
                    );
                }
                TestEvent::Warning { message } => {
                    if !self.quiet {
                        warn!("{}", message);
                    }
                }
                TestEvent::Error {
                    message,
                    recoverable,
                } => {
                    if recoverable {
                        warn!("Recoverable error: {}", message);
                    } else {
                        tracing::error!("Fatal error: {}", message);
                    }
                }
                TestEvent::Completed(results) => {
                    self.handle_completion(&results);
                    return Some(results);
                }
            }
        }

        None
    }

    /// Handles phase change events
    fn handle_phase_change(&mut self, phase: TestPhase) {
        if self.quiet {
            return;
        }

        match phase {
            TestPhase::Connecting => {
                info!("Connecting to Kafka broker...");
            }
            TestPhase::CreatingTopic => {
                info!("Creating test topic...");
            }
            TestPhase::WaitingForTopic => {
                info!("Waiting for topic to be ready...");
            }
            TestPhase::CreatingConnections => {
                info!("Creating producer/consumer connections...");
            }
            TestPhase::Running => {
                info!("Test is running...");
                self.start_time = Some(Instant::now());
                println!(); // Add space for animation
            }
            TestPhase::Cleanup => {
                println!(); // New line after animation
                info!("Cleaning up test resources...");
            }
        }
    }

    /// Handles progress events and updates the animation
    fn handle_progress(
        &mut self,
        _messages_sent: u64,
        _messages_received: u64,
        bytes_sent: u64,
        current_rate: f64,
        backlog_percent: u8,
    ) {
        // Track min/max rates
        if current_rate > 0.0 {
            if self.min_rate == f64::MAX {
                self.min_rate = current_rate;
            } else {
                self.min_rate = self.min_rate.min(current_rate);
            }
            self.max_rate = self.max_rate.max(current_rate);
        }

        if self.quiet {
            return;
        }

        // Update animation position
        self.update_position();

        // Build status string
        let status = if self.produce_only {
            let elapsed_secs = self
                .start_time
                .map(|t| t.elapsed().as_secs_f64())
                .unwrap_or(0.0);
            let avg_mbps = if elapsed_secs > 0.0 {
                bytes_sent as f64 / elapsed_secs / (1024.0 * 1024.0)
            } else {
                0.0
            };
            format!(
                "{:.0} msg/s (min: {:.0}, max: {:.0}, sent: {}, avg: {:.1} MB/s)",
                current_rate,
                if self.min_rate > 1e9 {
                    0.0
                } else {
                    self.min_rate
                },
                self.max_rate,
                format_bytes(bytes_sent),
                avg_mbps
            )
        } else {
            format!(
                "{:.0} msg/s (min: {:.0}, max: {:.0}, backlog: {}%)",
                current_rate,
                if self.min_rate > 1e9 {
                    0.0
                } else {
                    self.min_rate
                },
                self.max_rate,
                backlog_percent
            )
        };

        // Draw the animation frame
        self.animator
            .draw_frame(self.position, self.direction, &status);
    }

    /// Updates the animation position with bounce logic
    fn update_position(&mut self) {
        self.position = if self.direction > 0 {
            if self.position >= (LED_BAR_WIDTH - LED_MOVEMENT_SPEED) {
                self.direction = -1;
                LED_BAR_WIDTH - LED_MOVEMENT_SPEED
            } else {
                self.position + LED_MOVEMENT_SPEED
            }
        } else {
            if self.position < LED_MOVEMENT_SPEED {
                self.direction = 1;
                LED_MOVEMENT_SPEED
            } else {
                self.position - LED_MOVEMENT_SPEED
            }
        };
    }

    /// Handles test completion and prints final results
    fn handle_completion(&self, results: &TestResults) {
        if self.quiet {
            // Machine-readable output: space-separated key=value pairs
            if self.produce_only {
                println!(
                    "messages_sent={} bytes_sent={} throughput_msg_per_sec={:.1} min_rate={:.1} max_rate={:.1}",
                    results.messages_sent,
                    results.bytes_sent,
                    results.throughput,
                    results.min_rate,
                    results.max_rate
                );
            } else {
                println!(
                    "messages_sent={} bytes_sent={} messages_received={} throughput_msg_per_sec={:.1} min_rate={:.1} max_rate={:.1} avg_backlog_pct={}",
                    results.messages_sent,
                    results.bytes_sent,
                    results.messages_received,
                    results.throughput,
                    results.min_rate,
                    results.max_rate,
                    results.avg_backlog_percent
                );
            }
        } else {
            info!("=== FINAL RESULTS ===");
            if self.produce_only {
                info!(
                    "Messages sent: {}, Data sent: {}, Producer throughput: {:.1} messages/second (min: {:.1} msg/s, max: {:.1} msg/s)",
                    results.messages_sent,
                    format_bytes(results.bytes_sent),
                    results.throughput,
                    results.min_rate,
                    results.max_rate
                );
            } else {
                info!(
                    "Messages sent: {}, Data sent: {}, Messages received: {}, Throughput: {:.1} messages/second (min: {:.1} msg/s, max: {:.1} msg/s, avg backlog: {}%)",
                    results.messages_sent,
                    format_bytes(results.bytes_sent),
                    results.messages_received,
                    results.throughput,
                    results.min_rate,
                    results.max_rate,
                    results.avg_backlog_percent
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ui_handler_creation() {
        let handler = UiHandler::new(false, false, false, 1000);
        assert!(!handler.quiet);
        assert!(!handler.silent);
        assert!(!handler.produce_only);
        assert_eq!(handler.max_backlog, 1000);
        assert_eq!(handler.position, 0);
        assert_eq!(handler.direction, 1);
    }

    #[test]
    fn test_ui_handler_quiet_mode() {
        let handler = UiHandler::new(true, false, false, 1000);
        assert!(handler.quiet);
    }

    #[test]
    fn test_ui_handler_produce_only() {
        let handler = UiHandler::new(false, false, true, 1000);
        assert!(handler.produce_only);
    }

    #[test]
    fn test_position_update_bouncing() {
        let mut handler = UiHandler::new(true, true, false, 1000);

        // Start at position 0, moving right
        assert_eq!(handler.position, 0);
        assert_eq!(handler.direction, 1);

        // Move right several times
        for _ in 0..10 {
            handler.update_position();
        }

        // Should have moved right
        assert!(handler.position > 0);

        // Keep moving until we hit the right edge and bounce back
        for _ in 0..20 {
            handler.update_position();
        }

        // Direction should have changed at some point
        // The exact position depends on LED_BAR_WIDTH and LED_MOVEMENT_SPEED
    }
}
