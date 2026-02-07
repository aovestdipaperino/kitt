//! Constants used by the kitt binary
//!
//! Kafka protocol and retry constants are imported from kitt-core.
//! UI animation constants are defined here.

/// Re-export all kitt-core constants for use in kitt's producer/consumer modules.
pub use kitt_core::consts::*;

// -- UI Animation --

/// Number of LED positions in the Knight Rider animation display.
/// Adjust based on terminal width and visual preference.
pub const LED_BAR_WIDTH: usize = 25;

/// Number of LED positions that move per animation frame.
/// Lower values (1-2) create slower, smoother movement;
/// higher values (3-5) create faster, more energetic movement.
pub const LED_MOVEMENT_SPEED: usize = 2;
