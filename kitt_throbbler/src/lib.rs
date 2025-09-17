//! KITT Throbbler - A Knight Rider style LED animation for terminal output
//!
//! This crate provides a simple, customizable Knight Rider style animation
//! that can be used to display progress, activity, or throughput metrics
//! in terminal applications.

use std::io::{self, Cursor, Write};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Duration between audio playback loops in seconds
///
/// This constant defines how often the KITT sound effect will play during animations.
/// When audio is enabled, the embedded `sfx.mp3` file will play repeatedly at this interval.
///
/// # Default Value
/// The default value of 3.0 seconds provides an authentic KITT experience without being
/// too intrusive or overwhelming.
///
/// # Usage
/// This constant is used internally by the audio playback system and can be referenced
/// by users who need to know the audio timing for synchronization purposes.
pub const AUDIO_LOOP_DURATION_SECS: f32 = 3.0;

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
    /// Whether audio hardware is enabled for sound effects
    audio_enabled: bool,
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
            audio_enabled: false,
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
            audio_enabled: false,
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

    /// Set whether audio hardware is enabled for sound effects
    ///
    /// # Arguments
    ///
    /// * `enabled` - Whether to enable audio playback during animation
    ///
    /// # Examples
    ///
    /// ```
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// let animator = KnightRiderAnimator::new().audio_enabled(true);
    /// ```
    pub fn audio_enabled(mut self, enabled: bool) -> Self {
        self.audio_enabled = enabled;
        self
    }

    /// Starts the audio playback loop in a separate thread
    fn start_audio_loop(&self) -> Option<(thread::JoinHandle<()>, Arc<AtomicBool>)> {
        if !self.audio_enabled {
            return None;
        }

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop_flag_clone = stop_flag.clone();

        let handle = thread::spawn(move || {
            // Try to initialize audio system
            let (_stream, stream_handle) = match rodio::OutputStream::try_default() {
                Ok(output) => output,
                Err(_) => {
                    eprintln!("Warning: Could not initialize audio output");
                    return;
                }
            };

            let sink = rodio::Sink::try_new(&stream_handle).unwrap();

            // Embed the MP3 file data at compile time
            const SFX_DATA: &[u8] = include_bytes!("assets/sfx.mp3");

            loop {
                if stop_flag_clone.load(Ordering::Relaxed) {
                    break;
                }

                // Create a cursor from the embedded audio data
                let cursor = Cursor::new(SFX_DATA);
                if let Ok(source) = rodio::Decoder::new(cursor) {
                    sink.append(source);
                }

                // Wait for the configured duration before next play, checking stop flag periodically
                let wait_iterations = (AUDIO_LOOP_DURATION_SECS * 10.0) as u32;
                for _ in 0..wait_iterations {
                    if stop_flag_clone.load(Ordering::Relaxed) {
                        break;
                    }
                    thread::sleep(Duration::from_millis(100));
                }
            }

            // Wait for any remaining audio to finish
            sink.sleep_until_end();
        });

        Some((handle, stop_flag))
    }

    /// Starts audio playback independently (for use with draw_frame)
    ///
    /// Returns a handle to control the audio playback. Audio will play every [`AUDIO_LOOP_DURATION_SECS`] seconds
    /// until the returned handle is dropped or stop() is called on it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// let animator = KnightRiderAnimator::new().audio_enabled(true);
    /// let audio_handle = animator.start_audio();
    ///
    /// // Do animation with draw_frame calls...
    /// // Audio plays automatically every [`AUDIO_LOOP_DURATION_SECS`] seconds
    ///
    /// // Stop audio when done
    /// if let Some(handle) = audio_handle {
    ///     handle.stop();
    /// }
    /// ```
    pub fn start_audio(&self) -> Option<AudioHandle> {
        if let Some((handle, stop_flag)) = self.start_audio_loop() {
            Some(AudioHandle {
                handle: Some(handle),
                stop_flag,
            })
        } else {
            None
        }
    }

    /// Draws a single frame of the Knight Rider animation
    ///
    /// # Arguments
    ///
    /// * `position` - Current animation position (0 to led_count-1)
    /// * `direction` - Current animation direction (positive = right, negative = left)
    /// * `status` - Current status to display (e.g., messages/second)
    ///
    /// # Examples
    ///
    /// ```
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// let animator = KnightRiderAnimator::new();
    /// animator.draw_frame(5, 1, "100 msg/s (min: 50, max: 150, backlog: 1.0%)");
    /// ```
    pub fn draw_frame(&self, position: usize, direction: i32, status: &str) {
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

        // Combine LED elements into a single display string
        let pattern: String = display.join("");

        // Print animated throughput display with carriage return for in-place updates
        if self.show_metrics {
            print!("\r[{}] {}      ", pattern, status);
        } else {
            print!("\r[{}]", pattern);
        }

        // Force immediate output to terminal (bypass buffering)
        io::stdout().flush().unwrap();
    }

    /// Starts an animation loop that continues until stopped
    ///
    /// Returns a handle that can be used to control and stop the animation.
    /// The animation runs in the background and updates the display continuously.
    ///
    /// # Arguments
    ///
    /// * `base_speed_ms` - Base animation speed in milliseconds (lower is faster)
    /// * `status_fn` - Function that provides status text for each frame
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kitt_throbbler::{KnightRiderAnimator, AnimationHandle};
    /// use std::time::Instant;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let animator = KnightRiderAnimator::new().audio_enabled(true);
    ///
    ///     let start_time = Instant::now();
    ///     let handle = animator.start_animation(50, Box::new(move |_| {
    ///         format!("Running for {:.1}s", start_time.elapsed().as_secs_f64())
    ///     })).await;
    ///
    ///     // Do other work...
    ///     tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    ///
    ///     handle.stop().await;
    /// }
    /// ```
    pub async fn start_animation(
        &self,
        base_speed_ms: u64,
        status_fn: Box<dyn Fn(usize) -> String + Send + Sync>,
    ) -> AnimationHandle {
        let stop_flag = Arc::new(AtomicBool::new(false));
        let audio_control = self.start_audio_loop();

        let animator = self.clone();
        let stop_flag_clone = stop_flag.clone();

        let handle = tokio::spawn(async move {
            let mut position = 0;
            let mut direction = 1;

            while !stop_flag_clone.load(Ordering::Relaxed) {
                let status = status_fn(position);
                animator.draw_frame(position, direction, &status);

                // Move position and handle direction changes
                position = (position as i32 + direction) as usize;
                if position >= animator.led_count - 1 {
                    direction = -1;
                } else if position == 0 {
                    direction = 1;
                }

                sleep(Duration::from_millis(base_speed_ms)).await;
            }
        });

        AnimationHandle {
            task_handle: Some(handle),
            stop_flag,
            audio_control,
        }
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
            "Duration: {}s, Base speed: {}ms, Max rate: {}, Audio: {}",
            duration_secs,
            base_speed_ms,
            max_rate_value,
            if self.audio_enabled {
                "enabled"
            } else {
                "disabled"
            }
        );
        println!("Press Ctrl+C to exit");

        // Start audio loop if enabled
        let audio_control = self.start_audio_loop();

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
            let status = format!(
                "{:.0} msg/s (min: {:.0}, max: {:.0}, backlog: 0.0%)",
                rate,
                if min_rate > 1e9 { 0.0 } else { min_rate },
                max_rate
            );
            self.draw_frame(position, direction, &status);

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

        // Stop audio playback
        if let Some((handle, stop_flag)) = audio_control {
            stop_flag.store(true, Ordering::Relaxed);
            let _ = handle.join();
        }

        println!("\nAnimation demo completed!");
    }
}

/// Handle for controlling a running animation
///
/// This handle allows you to stop an animation that was started with
/// `start_animation()`. When dropped, it will automatically stop the animation.
pub struct AnimationHandle {
    task_handle: Option<tokio::task::JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
    audio_control: Option<(thread::JoinHandle<()>, Arc<AtomicBool>)>,
}

impl AnimationHandle {
    /// Stops the animation and audio playback
    ///
    /// This method will gracefully stop the animation loop and any associated
    /// audio playback, then wait for all threads to complete.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use kitt_throbbler::KnightRiderAnimator;
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let animator = KnightRiderAnimator::new();
    ///     let handle = animator.start_animation(50, Box::new(|_| "Running...".to_string())).await;
    ///
    ///     // Later...
    ///     handle.stop().await;
    /// }
    /// ```
    pub async fn stop(mut self) {
        // Signal the animation to stop
        self.stop_flag.store(true, Ordering::Relaxed);

        // Wait for the animation task to complete
        if let Some(handle) = self.task_handle.take() {
            let _ = handle.await;
        }

        // Stop audio playback
        if let Some((audio_handle, audio_stop_flag)) = self.audio_control.take() {
            audio_stop_flag.store(true, Ordering::Relaxed);
            let _ = audio_handle.join();
        }

        // Clear the animation line
        print!("\r");
        io::stdout().flush().unwrap();
    }
}

impl Drop for AnimationHandle {
    fn drop(&mut self) {
        // Signal stop if not already stopped
        self.stop_flag.store(true, Ordering::Relaxed);

        // Stop audio if present
        if let Some((_, audio_stop_flag)) = &self.audio_control {
            audio_stop_flag.store(true, Ordering::Relaxed);
        }
    }
}

/// Handle for controlling audio playback independently
pub struct AudioHandle {
    handle: Option<thread::JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
}

impl AudioHandle {
    /// Stops the audio playback
    pub fn stop(mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for AudioHandle {
    fn drop(&mut self) {
        self.stop_flag.store(true, Ordering::Relaxed);
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
        assert_eq!(animator.audio_enabled, false);

        let custom_animator = KnightRiderAnimator::with_leds(30);
        assert_eq!(custom_animator.led_count, 30);

        let no_metrics = KnightRiderAnimator::new().show_metrics(false);
        assert_eq!(no_metrics.show_metrics, false);

        let audio_enabled = KnightRiderAnimator::new().audio_enabled(true);
        assert_eq!(audio_enabled.audio_enabled, true);
    }
}
