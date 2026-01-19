//! Real-time throughput measurement with Knight Rider animation.
//!
//! This module provides the `ThroughputMeasurer` struct for tracking and displaying
//! message production/consumption rates with visual feedback during tests.

use crate::utils::format_bytes;
use kitt_throbbler::KnightRiderAnimator;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::select;
use tokio::time::interval;
use tracing::info;

/// Constant controlling how many LED positions move per animation frame
///
/// This value affects the visual speed of the Knight Rider animation:
/// - Lower values (1-2) create slower, smoother movement
/// - Higher values (3-5) create faster, more energetic movement
///
/// # Values
/// - `1`: Slowest movement, individual LED changes
/// - `2`: Standard movement speed (current default)
/// - `3`: Faster movement, good for quick tests
/// - `4-5`: Rapid movement, may be harder to follow
///
/// # Usage
/// This value affects the bounce calculations at display edges and should
/// be kept reasonable relative to the total LED count (50 positions).
pub const LED_MOVEMENT_SPEED: usize = 2;

/// Width of the LED bar in number of positions
///
/// This constant defines how many LED positions are available in the Knight Rider
/// animation display during throughput measurements.
///
/// # Values
/// - `30`: Compact display for narrow terminals
/// - `50`: Standard width (current default)
/// - `80`: Wide display for detailed visualization
/// - `100+`: Very wide display (ensure terminal width is sufficient)
///
/// # Technical Details
/// The LED animation bounces between position 0 and (LED_BAR_WIDTH - 1).
/// Movement speed and bounce calculations are based on this width.
///
/// # Usage
/// This value should be adjusted based on terminal width and visual preference.
/// Ensure LED_MOVEMENT_SPEED is reasonable relative to this width.
pub const LED_BAR_WIDTH: usize = 25;

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
        Self {
            messages_sent: Arc::new(AtomicU64::new(0)),
            bytes_sent: Arc::new(AtomicU64::new(0)),
            messages_received: Arc::new(AtomicU64::new(0)),
            backlog_percentage_sum: Arc::new(AtomicU64::new(0)),
            backlog_measurement_count: Arc::new(AtomicU64::new(0)),
            animator: KnightRiderAnimator::with_leds(led_count).audio_enabled(audio_enabled),
        }
    }

    /// Measures throughput over a specified duration with real-time visual feedback
    ///
    /// This method runs two concurrent timers:
    /// 1. Animation timer (100ms) - updates the Knight Rider display
    /// 2. Rate calculation timer (500ms) - calculates current throughput
    ///
    /// # Arguments
    /// * `duration` - How long to measure throughput
    /// * `label` - Label for display purposes
    /// * `max_backlog` - Maximum backlog threshold for percentage calculation
    /// * `fetch_delay` - Initial delay before starting measurement
    ///
    /// # Returns
    /// * `(min_rate, max_rate, elapsed)` - Tuple containing minimum/maximum rates and actual elapsed time
    pub async fn measure(
        &self,
        duration: Duration,
        label: &str,
        max_backlog: u64,
        fetch_delay: u64,
        produce_only: bool,
        bytes_target: Option<u64>,
    ) -> (f64, f64, Duration) {
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
        // 100ms animation interval provides smooth visual feedback at ~10fps
        let mut animation_interval = interval(Duration::from_millis(100));
        // 500ms rate interval balances responsiveness vs noise in measurements
        let mut rate_interval = interval(Duration::from_millis(500));

        // Baseline counters for rate calculation
        // In produce-only mode, track sent messages; otherwise track received
        let start_count = if produce_only {
            self.messages_sent.load(Ordering::Relaxed)
        } else {
            self.messages_received.load(Ordering::Relaxed)
        };
        let mut last_count = start_count;
        let mut last_rate_time = start_time;

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
        println!(); // Add space for animation

        // Start audio playback for the measurement duration (audio is already configured in constructor)
        let _audio_handle = self.animator.start_audio();

        // Animation state: position bounces between 0 and LED_BAR_WIDTH
        let mut position = 0;
        let mut direction = 1; // 1 = moving right, -1 = moving left

        // Rate tracking: min starts at MAX so first valid measurement becomes minimum
        let mut current_rate = 0.0f64;
        let mut min_rate = f64::MAX;
        let mut max_rate = 0.0f64;

        // Main loop uses tokio::select! to run animation and rate calculation concurrently.
        // This avoids blocking either task while maintaining precise timing for both.
        while bytes_target.is_some() || Instant::now() < end_time {
            // Check if bytes target reached (if specified)
            if let Some(target) = bytes_target {
                if self.bytes_sent.load(Ordering::Relaxed) >= target {
                    break;
                }
            }

            select! {
                _ = animation_interval.tick() => {
                    // Bounce logic: reverse direction when hitting edges.
                    // We check LED_MOVEMENT_SPEED from edge to prevent overshooting.
                    position = if direction > 0 {
                        if position >= (LED_BAR_WIDTH - LED_MOVEMENT_SPEED) {
                            direction = -1;
                            LED_BAR_WIDTH - LED_MOVEMENT_SPEED
                        } else {
                            position + LED_MOVEMENT_SPEED
                        }
                    } else {
                        if position < LED_MOVEMENT_SPEED {
                            direction = 1;
                            LED_MOVEMENT_SPEED
                        } else {
                            position - LED_MOVEMENT_SPEED
                        }
                    };

                    // Build status string for display
                    let status = if produce_only {
                        // In produce-only mode, show data sent instead of backlog
                        let bytes_sent = self.bytes_sent.load(Ordering::Relaxed);
                        format!(
                            "{:.0} msg/s (min: {:.0}, max: {:.0}, sent: {})",
                            current_rate,
                            if min_rate > 1e9 { 0.0 } else { min_rate },
                            max_rate,
                            format_bytes(bytes_sent)
                        )
                    } else {
                        // Normal mode: show backlog percentage
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
                    };

                    // Update the visual display with current metrics
                    self.animator.draw_frame(position, direction, &status);
                }
                _ = rate_interval.tick() => {
                    // Calculate instantaneous rate as delta_messages / delta_time.
                    // Using deltas instead of cumulative rate gives more responsive feedback.
                    let now = Instant::now();
                    let current_count = if produce_only {
                        self.messages_sent.load(Ordering::Relaxed)
                    } else {
                        self.messages_received.load(Ordering::Relaxed)
                    };
                    let time_elapsed = now.duration_since(last_rate_time).as_secs_f64();

                    if time_elapsed > 0.0 {
                        current_rate = (current_count - last_count) as f64 / time_elapsed;

                        // Skip zero rates (e.g., startup) to avoid skewing min
                        if current_rate > 0.0 {
                            if min_rate == f64::MAX {
                                min_rate = current_rate;
                            } else {
                                min_rate = min_rate.min(current_rate);
                            }
                            max_rate = max_rate.max(current_rate);
                        }

                        // Slide the measurement window forward
                        last_count = current_count;
                        last_rate_time = now;
                    }
                }
            }
        }

        println!(); // New line after animation completes
        let elapsed = start_time.elapsed();
        (min_rate, max_rate, elapsed)
    }
}

#[cfg(test)]
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
