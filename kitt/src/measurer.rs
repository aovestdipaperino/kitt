//! Real-time throughput measurement with Knight Rider animation.
//!
//! This module provides the `ThroughputMeasurer` struct for tracking and displaying
//! message production/consumption rates with visual feedback during tests.

use crate::consts::{LED_BAR_WIDTH, LED_MOVEMENT_SPEED};
use kitt_core::utils::format_bytes;
use kitt_throbbler::KnightRiderAnimator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::time::interval;
use tracing::info;

/// Measures and displays real-time throughput metrics with visual indicators
///
/// This struct tracks message production and consumption rates, displaying them
/// with a Knight Rider-style animated indicator that shows performance levels.
#[derive(Clone)]
pub struct ThroughputMeasurer {
    /// Atomic counter for total messages sent by all producer threads
    pub messages_sent: Arc<AtomicU64>,
    /// Atomic counter for total bytes sent by all producer threads
    pub bytes_sent: Arc<AtomicU64>,
    /// Atomic counter for total messages received by all consumer threads
    pub messages_received: Arc<AtomicU64>,
    /// Sum of all backlog percentage measurements for calculating average
    pub backlog_percentage_sum: Arc<AtomicU64>,
    /// Count of backlog percentage measurements for calculating average
    pub backlog_measurement_count: Arc<AtomicU64>,
    /// Knight Rider animator for visual feedback
    pub animator: KnightRiderAnimator,
}

impl ThroughputMeasurer {
    /// Creates a new ThroughputMeasurer with custom LED count and audio setting
    pub fn with_leds(led_count: usize, audio_enabled: bool) -> Self {
        // Precondition: at least one LED required for meaningful display
        assert!(led_count > 0, "ThroughputMeasurer::with_leds: led_count must be > 0");

        Self {
            messages_sent: Arc::new(AtomicU64::new(0)),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            messages_received: Arc::new(AtomicU64::new(0)),
            backlog_percentage_sum: Arc::new(AtomicU64::new(0)),
            backlog_measurement_count: Arc::new(AtomicU64::new(0)),
            animator: KnightRiderAnimator::with_leds(led_count).audio_enabled(audio_enabled),
        }
    }

    /// Measures throughput over a specified duration without any visual output (quiet mode)
    ///
    /// # Arguments
    /// * `duration` - How long to measure throughput
    /// * `max_backlog` - Maximum backlog threshold for percentage calculation
    /// * `fetch_delay` - Initial delay before starting measurement
    /// * `produce_only` - Whether in produce-only mode (track sent vs received)
    /// * `bytes_target` - Optional data target for produce-only mode
    ///
    /// # Returns
    /// * `(min_rate, max_rate, elapsed)` - Tuple containing minimum/maximum rates and actual elapsed time
    pub async fn measure_quiet(
        &self,
        duration: Duration,
        max_backlog: u64,
        fetch_delay: u64,
        produce_only: bool,
        bytes_target: Option<u64>,
    ) -> (f64, f64, Duration) {
        // Precondition: either a positive duration or a bytes_target must be specified
        assert!(
            bytes_target.is_some() || duration.as_nanos() > 0,
            "measure_quiet: requires either a positive duration or a bytes_target"
        );

        // Wait for fetch delay before starting measurement
        if fetch_delay > 0 {
            tokio::time::sleep(Duration::from_secs(fetch_delay)).await;
        }
        let start_time = Instant::now();
        let end_time = start_time + duration;

        let mut rate_interval = interval(Duration::from_millis(500));

        let start_count = if produce_only {
            self.messages_sent.load(Ordering::Relaxed)
        } else {
            self.messages_received.load(Ordering::Relaxed)
        };
        let mut last_count = start_count;
        let mut last_rate_time = start_time;

        let mut min_rate = f64::MAX;
        let mut max_rate = 0.0f64;

        while bytes_target.is_some() || Instant::now() < end_time {
            if let Some(target) = bytes_target {
                if self.bytes_sent.load(Ordering::Relaxed) >= target {
                    break;
                }
            }

            rate_interval.tick().await;

            let now = Instant::now();
            let current_count = if produce_only {
                self.messages_sent.load(Ordering::Relaxed)
            } else {
                self.messages_received.load(Ordering::Relaxed)
            };
            let time_elapsed = now.duration_since(last_rate_time).as_secs_f64();

            if time_elapsed > 0.0 {
                let current_rate = (current_count - last_count) as f64 / time_elapsed;

                if current_rate > 0.0 {
                    if min_rate == f64::MAX {
                        min_rate = current_rate;
                    } else {
                        min_rate = min_rate.min(current_rate);
                    }
                    max_rate = max_rate.max(current_rate);
                }

                if !produce_only {
                    self.track_backlog_percentage(max_backlog);
                }

                last_count = current_count;
                last_rate_time = now;
            }
        }

        let elapsed = start_time.elapsed();
        (min_rate, max_rate, elapsed)
    }

    /// Measures throughput over a specified duration with real-time visual feedback
    ///
    /// This method runs two concurrent timers:
    /// 1. Animation timer (100ms) - updates the Knight Rider display
    /// 2. Rate calculation timer (500ms) - calculates current throughput
    pub async fn measure(
        &self,
        duration: Duration,
        label: &str,
        max_backlog: u64,
        fetch_delay: u64,
        produce_only: bool,
        bytes_target: Option<u64>,
    ) -> (f64, f64, Duration) {
        // Preconditions: label must not be empty and either duration or bytes_target must be set
        assert!(!label.is_empty(), "measure: label must not be empty");
        assert!(
            bytes_target.is_some() || duration.as_nanos() > 0,
            "measure: requires either a positive duration or a bytes_target"
        );

        // Wait for fetch delay before starting measurement
        if fetch_delay > 0 {
            info!(
                "⏱️  Waiting {}s for consumers to start before beginning measurement",
                fetch_delay
            );
            tokio::time::sleep(Duration::from_secs(fetch_delay)).await;
        }
        let start_time = Instant::now();
        let end_time = start_time + duration;
        let mut animation_interval = interval(Duration::from_millis(100));
        let mut rate_interval = interval(Duration::from_millis(500));

        let start_count = if produce_only {
            self.messages_sent.load(Ordering::Relaxed)
        } else {
            self.messages_received.load(Ordering::Relaxed)
        };
        let mut last_count = start_count;
        let mut last_rate_time = start_time;

        Self::log_measurement_start(label, bytes_target, duration);
        println!(); // Add space for animation

        let _audio_handle = self.animator.start_audio();

        let mut position = 0;
        let mut direction = 1;

        let mut current_rate = 0.0f64;
        let mut min_rate = f64::MAX;
        let mut max_rate = 0.0f64;

        while bytes_target.is_some() || Instant::now() < end_time {
            if let Some(target) = bytes_target {
                if self.bytes_sent.load(Ordering::Relaxed) >= target {
                    break;
                }
            }

            select! {
                _ = animation_interval.tick() => {
                    position = update_animation_position(position, &mut direction);
                    let status = self.build_status_string(
                        produce_only, current_rate, min_rate, max_rate,
                        max_backlog, start_time,
                    );
                    self.animator.draw_frame(position, direction, &status);
                }
                _ = rate_interval.tick() => {
                    let (new_rate, new_min, new_max) = self.calculate_rate(
                        produce_only, &mut last_count, &mut last_rate_time,
                        min_rate, max_rate,
                    );
                    current_rate = new_rate;
                    min_rate = new_min;
                    max_rate = new_max;
                }
            }
        }

        println!(); // New line after animation completes
        let elapsed = start_time.elapsed();
        (min_rate, max_rate, elapsed)
    }

    /// Logs the measurement start message
    fn log_measurement_start(label: &str, bytes_target: Option<u64>, duration: Duration) {
        if let Some(target) = bytes_target {
            info!(
                "🚀 Starting {} phase until {} sent",
                label,
                format_bytes(target)
            );
        } else {
            info!(
                "🚀 Starting {} phase for {} seconds (after delay)",
                label,
                duration.as_secs()
            );
        }
    }

    /// Calculates the current rate and updates min/max tracking
    fn calculate_rate(
        &self,
        produce_only: bool,
        last_count: &mut u64,
        last_rate_time: &mut Instant,
        mut min_rate: f64,
        mut max_rate: f64,
    ) -> (f64, f64, f64) {
        let now = Instant::now();
        let current_count = if produce_only {
            self.messages_sent.load(Ordering::Relaxed)
        } else {
            self.messages_received.load(Ordering::Relaxed)
        };
        let time_elapsed = now.duration_since(*last_rate_time).as_secs_f64();
        let mut current_rate = 0.0;

        if time_elapsed > 0.0 {
            current_rate = (current_count - *last_count) as f64 / time_elapsed;

            if current_rate > 0.0 {
                if min_rate == f64::MAX {
                    min_rate = current_rate;
                } else {
                    min_rate = min_rate.min(current_rate);
                }
                max_rate = max_rate.max(current_rate);
            }

            *last_count = current_count;
            *last_rate_time = now;
        }

        (current_rate, min_rate, max_rate)
    }

    /// Builds the status string for the animation display
    fn build_status_string(
        &self,
        produce_only: bool,
        current_rate: f64,
        min_rate: f64,
        max_rate: f64,
        max_backlog: u64,
        start_time: Instant,
    ) -> String {
        if produce_only {
            let bytes_sent = self.bytes_sent.load(Ordering::Relaxed);
            let elapsed_secs = start_time.elapsed().as_secs_f64();
            let avg_mbps = if elapsed_secs > 0.0 {
                bytes_sent as f64 / elapsed_secs / (1024.0 * 1024.0)
            } else {
                0.0
            };
            format!(
                "{:.0} msg/s (min: {:.0}, max: {:.0}, sent: {}, avg: {:.1} MB/s)",
                current_rate,
                if min_rate > 1e9 { 0.0 } else { min_rate },
                max_rate,
                format_bytes(bytes_sent),
                avg_mbps
            )
        } else {
            let current_sent = self.messages_sent.load(Ordering::Relaxed);
            let current_received = self.messages_received.load(Ordering::Relaxed);
            let backlog = current_sent.saturating_sub(current_received);
            let backlog_percentage = (backlog as f64 / max_backlog as f64 * 100.0).min(100.0);

            // Track backlog percentage for average calculation
            let backlog_percentage_int = backlog_percentage as u64;
            self.backlog_percentage_sum.fetch_add(backlog_percentage_int, Ordering::Relaxed);
            self.backlog_measurement_count.fetch_add(1, Ordering::Relaxed);

            format!(
                "{:.0} msg/s (min: {:.0}, max: {:.0}, backlog: {:.1}%)",
                current_rate,
                if min_rate > 1e9 { 0.0 } else { min_rate },
                max_rate,
                backlog_percentage
            )
        }
    }

    /// Tracks backlog percentage for average calculation
    fn track_backlog_percentage(&self, max_backlog: u64) {
        let current_sent = self.messages_sent.load(Ordering::Relaxed);
        let current_received = self.messages_received.load(Ordering::Relaxed);
        let backlog = current_sent.saturating_sub(current_received);
        let backlog_percentage =
            (backlog as f64 / max_backlog as f64 * 100.0).min(100.0) as u64;
        self.backlog_percentage_sum
            .fetch_add(backlog_percentage, Ordering::Relaxed);
        self.backlog_measurement_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Updates the animation position, bouncing between edges
fn update_animation_position(position: usize, direction: &mut i32) -> usize {
    if *direction > 0 {
        if position >= (LED_BAR_WIDTH - LED_MOVEMENT_SPEED) {
            *direction = -1;
            LED_BAR_WIDTH - LED_MOVEMENT_SPEED
        } else {
            position + LED_MOVEMENT_SPEED
        }
    } else {
        if position < LED_MOVEMENT_SPEED {
            *direction = 1;
            LED_MOVEMENT_SPEED
        } else {
            position - LED_MOVEMENT_SPEED
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    #[test]
    fn test_measurement_timing_calculation() {
        use std::time::Duration;

        // Test case 1: No delay - measurement duration equals total duration
        let measurement_duration = Duration::from_secs(15);
        let fetch_delay = 0u64;
        let total_test_duration = measurement_duration + Duration::from_secs(fetch_delay);

        assert_eq!(total_test_duration.as_secs(), 15);
        assert_eq!(measurement_duration.as_secs(), 15);

        // Test case 2: With delay - total duration includes delay + measurement
        let measurement_duration = Duration::from_secs(15);
        let fetch_delay = 3u64;
        let total_test_duration = measurement_duration + Duration::from_secs(fetch_delay);

        assert_eq!(total_test_duration.as_secs(), 18);
        assert_eq!(measurement_duration.as_secs(), 15);

        // Test case 3: Extended delay
        let measurement_duration = Duration::from_secs(20);
        let fetch_delay = 10u64;
        let total_test_duration = measurement_duration + Duration::from_secs(fetch_delay);

        assert_eq!(total_test_duration.as_secs(), 30);
        assert_eq!(measurement_duration.as_secs(), 20);
    }
}
