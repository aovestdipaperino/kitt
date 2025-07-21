//! KITT Throbbler - A Knight Rider style LED animation for terminal output
//!
//! This crate provides a simple, customizable Knight Rider style animation
//! that can be used to display progress, activity, or throughput metrics
//! in terminal applications.

use std::io::{self, Write};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Animator for creating Knight Rider style LED animations in the terminal
///
/// The KnightRiderAnimator creates a classic "scanning" effect with a bright LED
/// that moves back and forth across the terminal with a trailing fade effect.
#[derive(Clone)]
pub struct KnightRiderAnimator {
    /// Number of LEDs in the animation bar
    led_count: usize,
    /// Whether to show throughput rate information with the animation
    show_metrics: bool,
}

/// Animation pattern options for varying the data visualization
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnimationPattern {
    /// Sine wave: smooth oscillation between min and max
    Sine,
    /// Sawtooth wave: linear ramp up, instant drop
    Sawtooth,
    /// Square wave: alternates between high and low values
    Square,
    /// Pulse wave: occasional spikes
    Pulse,
}

impl KnightRiderAnimator {
    /// Creates a new KnightRiderAnimator with default settings
    ///
    /// # Examples
    ///
    /// ```
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// let animator = KnightRiderAnimator::new();
    /// ```
    pub fn new() -> Self {
        Self {
            led_count: 50,
            show_metrics: true,
        }
    }

    /// Creates a new KnightRiderAnimator with custom LED count
    ///
    /// # Arguments
    ///
    /// * `led_count` - Number of LEDs to display in the animation bar
    ///
    /// # Examples
    ///
    /// ```
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// let animator = KnightRiderAnimator::with_leds(30);
    /// ```
    pub fn with_leds(led_count: usize) -> Self {
        Self {
            led_count,
            show_metrics: true,
        }
    }

    /// Set whether to show metrics alongside the animation
    ///
    /// # Arguments
    ///
    /// * `show` - Whether to display rate metrics with the animation
    ///
    /// # Examples
    ///
    /// ```
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// let animator = KnightRiderAnimator::new().show_metrics(false);
    /// ```
    pub fn show_metrics(mut self, show: bool) -> Self {
        self.show_metrics = show;
        self
    }

    /// Draws a single frame of the Knight Rider animation
    ///
    /// # Arguments
    ///
    /// * `position` - Current animation position (0 to led_count-1)
    /// * `direction` - Current animation direction (positive = right, negative = left)
    /// * `rate` - Current rate value to display (e.g., messages/second)
    /// * `min_rate` - Minimum observed rate (for display)
    /// * `max_rate` - Maximum observed rate (for display)
    ///
    /// # Examples
    ///
    /// ```
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// let animator = KnightRiderAnimator::new();
    /// animator.draw_frame(5, 1, 100.0, 50.0, 150.0);
    /// ```
    pub fn draw_frame(
        &self,
        position: usize,
        direction: i32,
        rate: f64,
        min_rate: f64,
        max_rate: f64,
    ) {
        /// ANSI color codes for different LED intensities
        const BRIGHT_RED: &str = "\x1b[38;5;196m"; // Core bright red
        const RED: &str = "\x1b[38;5;160m"; // Standard red
        const DIM_RED_1: &str = "\x1b[38;5;124m"; // Dim red (level 1)
        const DIM_RED_2: &str = "\x1b[38;5;88m"; // Dimmer red (level 2)
        const DIM_RED_3: &str = "\x1b[38;5;52m"; // Dimmest red (level 3)
        const RESET: &str = "\x1b[0m";

        // Initialize display with empty spaces
        let mut display = vec![" ".to_string(); self.led_count];

        // Create the moving LED pattern with performance-based color intensity
        // Main LED (brightest) - shows current position
        if position < self.led_count {
            display[position] = format!("{}█{}", BRIGHT_RED, RESET);
        }

        // Create a more gradual fade effect with multiple intensity levels
        // Trailing LEDs (comet tail effect) - direction based on animation direction
        for i in 1..12 {
            // When moving right, trail extends to the left
            // When moving left, trail extends to the right
            let trail_pos = if direction > 0 {
                // Moving right - trail is behind (to the left)
                if position >= i {
                    position - i
                } else {
                    0
                }
            } else {
                // Moving left - trail is behind (to the right)
                if position + i < self.led_count {
                    position + i
                } else {
                    self.led_count - 1
                }
            };

            // Only proceed if trail position is valid
            if trail_pos < self.led_count {
                let color = match i {
                    1 => BRIGHT_RED,
                    2 => BRIGHT_RED,
                    3 => RED,
                    4 => RED,
                    5 => DIM_RED_1,
                    6 => DIM_RED_1,
                    7 => DIM_RED_2,
                    8 => DIM_RED_2,
                    9 => DIM_RED_3,
                    10 => DIM_RED_3,
                    _ => "\x1b[38;5;52;2m", // Extra dim red with reduced intensity
                };
                display[trail_pos] = format!("{}█{}", color, RESET);
            }
        }

        // Fade the brightest point slightly if rate is lower (dimmer when slower)
        if position < self.led_count && rate < max_rate * 0.7 {
            let dim_factor = rate / max_rate;
            let main_color = if dim_factor < 0.3 {
                DIM_RED_1
            } else if dim_factor < 0.5 {
                RED
            } else {
                BRIGHT_RED
            };
            display[position] = format!("{}█{}", main_color, RESET);
        }

        // Combine LED elements into a single display string
        let pattern: String = display.join("");

        // Print animated throughput display with carriage return for in-place updates
        if self.show_metrics {
            print!(
                "\r[{}] {:.0} msg/s (min: {:.0}, max: {:.0})      ",
                pattern,
                rate,
                // Handle uninitialized min_rate (f64::MAX) by showing 0
                if min_rate > 1e9 { 0.0 } else { min_rate },
                max_rate
            );
        } else {
            print!("\r[{}]", pattern);
        }

        // Force immediate output to terminal (bypass buffering)
        io::stdout().flush().unwrap();
    }

    /// Run a demo animation for a specified duration
    ///
    /// # Arguments
    ///
    /// * `duration_secs` - How long the animation should run, in seconds
    /// * `base_speed_ms` - Base animation speed in milliseconds (lower is faster)
    /// * `max_rate_value` - Maximum simulated throughput rate for the animation
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let animator = KnightRiderAnimator::new();
    ///     animator.run_demo(10, 20, 10000.0).await;
    /// }
    /// ```
    pub async fn run_demo(&self, duration_secs: u64, base_speed_ms: u64, max_rate_value: f64) {
        println!("Running Knight Rider animation demo...");
        println!(
            "Duration: {}s, Base speed: {}ms, Max rate: {}",
            duration_secs, base_speed_ms, max_rate_value
        );
        println!("Press Ctrl+C to exit");

        let duration = Duration::from_secs(duration_secs);
        let start = Instant::now();
        let mut position = 0;
        let mut direction = 1;

        // Simulated rate variables
        let mut rate: f64;
        let mut min_rate: f64 = f64::MAX;
        let mut max_rate: f64 = 0.0;
        let half_max_rate = max_rate_value / 2.0;

        // Animation pattern options
        let patterns = [
            AnimationPattern::Sine,
            AnimationPattern::Sawtooth,
            AnimationPattern::Square,
            AnimationPattern::Pulse,
        ];
        let mut current_pattern = 0;
        let pattern_duration = Duration::from_secs(5);
        let mut pattern_start = start;

        while start.elapsed() < duration {
            // Switch patterns every few seconds
            if pattern_start.elapsed() >= pattern_duration {
                current_pattern = (current_pattern + 1) % patterns.len();
                pattern_start = Instant::now();
                println!(
                    "\nSwitching to {:?} wave pattern",
                    patterns[current_pattern]
                );
            }

            // Create different wave patterns for the simulated throughput rate
            let elapsed = start.elapsed().as_secs_f64();
            let pattern_progress =
                pattern_start.elapsed().as_secs_f64() / pattern_duration.as_secs_f64();

            rate = match patterns[current_pattern] {
                // Sine wave: smooth oscillation
                AnimationPattern::Sine => half_max_rate + half_max_rate * (elapsed * 0.2).sin(),

                // Sawtooth wave: linear ramp up, instant drop
                AnimationPattern::Sawtooth => {
                    let saw_val = pattern_progress % 1.0;
                    saw_val * max_rate_value
                }

                // Square wave: alternating between min and max
                AnimationPattern::Square => {
                    if (elapsed * 0.2).sin() >= 0.0 {
                        max_rate_value
                    } else {
                        max_rate_value * 0.1
                    }
                }

                // Pulse wave: occasional spikes
                AnimationPattern::Pulse => {
                    let pulse_val = (elapsed * 2.0).sin();
                    if pulse_val > 0.9 {
                        max_rate_value
                    } else {
                        max_rate_value * 0.2
                    }
                }
            };

            // Update min/max
            min_rate = min_rate.min(rate);
            max_rate = max_rate.max(rate);

            // Update animation position
            self.draw_frame(position, direction, rate, min_rate, max_rate);

            // Move position and handle direction changes
            position = (position as i32 + direction) as usize;
            if position >= self.led_count - 1 {
                direction = -1;
            } else if position == 0 {
                direction = 1;
            }

            // Adjust speed based on simulated rate
            let speed_factor = 1.0 - (rate / max_rate_value).min(1.0).max(0.0);
            let delay_ms = base_speed_ms + (speed_factor * 80.0) as u64;
            sleep(Duration::from_millis(delay_ms)).await;
        }

        println!("\nAnimation demo completed!");
    }
}

impl Default for KnightRiderAnimator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_animator() {
        let animator = KnightRiderAnimator::new();
        assert_eq!(animator.led_count, 50);
        assert_eq!(animator.show_metrics, true);

        let custom_animator = KnightRiderAnimator::with_leds(30);
        assert_eq!(custom_animator.led_count, 30);

        let no_metrics = KnightRiderAnimator::new().show_metrics(false);
        assert_eq!(no_metrics.show_metrics, false);
    }
}
