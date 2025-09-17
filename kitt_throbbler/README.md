# KITT Throbbler

A Knight Rider-style LED animation library for terminal applications with optional audio support.

This crate provides a customizable Knight Rider style animation that can be used to display progress, activity, or throughput metrics in terminal applications. Features include customizable LED counts, metrics display, and authentic KITT sound effects.

## Features

- **Classic Knight Rider Animation**: Scanning LED effect with trailing fade
- **Audio Support**: Optional KITT sound effects that play every 3 seconds
- **Customizable Display**: Adjustable LED count and metrics
- **Async/Await Support**: Built on Tokio for non-blocking operation
- **Thread-Safe**: Audio runs in separate thread from animation
- **Flexible Control**: Start/stop animations programmatically

## Quick Start

Add this to your `Cargo.toml`:

```toml
[dependencies]
kitt_throbbler = "0.1.3"
tokio = { version = "1", features = ["time", "macros", "rt-multi-thread"] }
```

## Basic Usage

### Simple Demo Animation

```rust
use kitt_throbbler::KnightRiderAnimator;

#[tokio::main]
async fn main() {
    let animator = KnightRiderAnimator::new();
    
    // Run demo for 10 seconds at medium speed with max rate of 5000
    animator.run_demo(10, 20, 5000.0).await;
}
```

### Animation with Audio

```rust
use kitt_throbbler::KnightRiderAnimator;

#[tokio::main]
async fn main() {
    let animator = KnightRiderAnimator::with_leds(40)  // 40 LEDs wide
        .audio_enabled(true)   // Enable KITT sound effects
        .show_metrics(true);   // Show throughput metrics
    
    animator.run_demo(15, 25, 8000.0).await;
}
```

### Programmatic Control

```rust
use kitt_throbbler::KnightRiderAnimator;
use std::time::Instant;

#[tokio::main]
async fn main() {
    let animator = KnightRiderAnimator::new().audio_enabled(true);
    
    let start_time = Instant::now();
    let handle = animator.start_animation(
        30, // animation speed in ms
        Box::new(move |_| {
            format!("Processing... {:.1}s", start_time.elapsed().as_secs_f64())
        })
    ).await;
    
    // Do your work here...
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    
    // Stop the animation
    handle.stop().await;
}
```

## Configuration Options

### KnightRiderAnimator Builder Methods

- `new()` - Creates animator with default settings (50 LEDs, metrics enabled, no audio)
- `with_leds(count)` - Creates animator with custom LED count (use this as starting point for chaining)
- `show_metrics(bool)` - Whether to display throughput metrics
- `audio_enabled(bool)` - Whether to play KITT sound effects (requires audio hardware)

**Note**: Use `with_leds()` instead of `new()` when you need custom LED count, as it serves as the constructor for method chaining.

### Animation Control

- `run_demo(duration, speed, max_rate)` - Run a complete demo with simulated data patterns
- `start_animation(speed, status_fn)` - Start animation with custom status function
- `draw_frame(position, direction, status)` - Draw a single animation frame

## Audio Features

When `audio_enabled(true)` is set:

- The embedded `sfx.mp3` audio data plays in a loop every `AUDIO_LOOP_DURATION_SECS` seconds (currently 3.0 seconds)
- Audio runs in a separate thread and won't block animation
- Automatically handles audio hardware detection
- Gracefully degrades if audio hardware is unavailable
- Audio stops when animation ends
- Audio data is statically compiled into the binary (no external files needed)

### Audio Requirements

- Audio hardware must be available on the system
- The `rodio` crate handles audio playback
- Works on Windows, macOS, and Linux
- No external audio files required - embedded at compile time

## Animation Patterns

The demo mode cycles through different data visualization patterns:

- **Sine Wave**: Smooth oscillation between min and max values
- **Sawtooth Wave**: Linear ramp up with instant drop
- **Square Wave**: Alternating between high and low values
- **Pulse Wave**: Occasional spikes in activity

## Examples

Run the included examples to see the library in action:

```bash
# Basic demo without audio
cargo run --example simple_demo

# Audio functionality test
cargo run --example audio_test

# Practical usage patterns
cargo run --example practical_demo

# Audio hardware check
cargo run --example audio_check
```

## Display Format

The animation displays as:

```
[████████████                                      ] 1,234 msg/s (min: 100, max: 2,000, backlog: 0.0%)
```

- LED bar shows scanning animation with fade trail
- Current throughput rate
- Min/max rates observed
- Simulated backlog percentage

## Thread Safety

- Animation runs on the main async runtime
- Audio playback uses a dedicated system thread
- All control structures are thread-safe
- Clean shutdown of all threads when animation stops

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions welcome! Please ensure:

- Code follows Rust formatting standards
- Tests pass with `cargo test`
- Examples compile and run correctly
- Audio features gracefully handle missing hardware
- Audio assets are properly embedded using `include_bytes!` macro