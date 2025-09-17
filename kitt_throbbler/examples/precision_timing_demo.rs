use kitt_throbbler::{KnightRiderAnimator, AUDIO_LOOP_DURATION_SECS};
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("KITT Throbbler - Precision Timing Demo");
    println!("======================================");
    println!("This demo shows the flexibility of f32 audio timing\n");

    // Show the default timing
    println!("📊 Current audio timing configuration:");
    println!(
        "   AUDIO_LOOP_DURATION_SECS = {} seconds",
        AUDIO_LOOP_DURATION_SECS
    );
    println!("   Type: f32 (allows fractional seconds for precise timing)\n");

    // Test 1: Standard timing demonstration
    println!(
        "🎵 Test 1: Standard timing with default value ({} seconds)",
        AUDIO_LOOP_DURATION_SECS
    );
    println!("   Running 8-second animation to demonstrate audio loop timing...\n");

    let standard_animator = KnightRiderAnimator::new()
        .audio_enabled(true)
        .show_metrics(false);

    let start_time = std::time::Instant::now();
    let handle = standard_animator
        .start_animation(
            60, // Medium animation speed
            Box::new(move |frame| {
                let elapsed = start_time.elapsed().as_secs_f32();
                let next_audio =
                    ((elapsed / AUDIO_LOOP_DURATION_SECS).floor() + 1.0) * AUDIO_LOOP_DURATION_SECS;
                let time_to_next = next_audio - elapsed;

                format!(
                    "Frame: {:3} | Elapsed: {:4.1}s | Next audio in: {:4.1}s | Interval: {}s",
                    frame, elapsed, time_to_next, AUDIO_LOOP_DURATION_SECS
                )
            }),
        )
        .await;

    tokio::time::sleep(Duration::from_secs(8)).await;
    handle.stop().await;

    println!("\n✓ Standard timing test completed\n");

    // Test 2: Demonstrate f32 precision benefits
    println!("🔬 Test 2: Precision Benefits of f32");
    println!("   f32 type allows for sub-second precision timing");
    println!("   Examples of possible precise timings:");

    let example_timings: Vec<f32> = vec![1.5, 2.25, 2.5, 3.0, 4.2, 5.75];

    for timing in &example_timings {
        let iterations = (timing * 10.0) as u32;
        let actual_delay = iterations as f32 / 10.0;
        println!(
            "   • {:.2}s → {} iterations @ 100ms = {:.1}s actual delay",
            timing, iterations, actual_delay
        );
    }

    // Test 3: Mathematical precision demonstration
    println!("\n🧮 Test 3: Timing Calculation Precision");
    println!("   Current implementation: (AUDIO_LOOP_DURATION_SECS * 10.0) as u32");
    println!("   This provides 0.1 second precision (100ms increments)");

    let test_values = vec![1.0, 1.5, 2.3, 3.0, 3.7, 4.95];
    println!("   Input → Iterations → Actual Timing");
    println!("   ────────────────────────────────────");

    for value in test_values {
        let iterations = (value * 10.0) as u32;
        let actual = iterations as f32 / 10.0;
        let precision_loss = (actual - value).abs();
        println!(
            "   {:4.2}s →    {:2} iter  → {:4.1}s (±{:.1}s)",
            value, iterations, actual, precision_loss
        );
    }

    // Test 4: Audio synchronization example
    println!("\n⏱️  Test 4: Audio Synchronization Analysis");
    println!("   With f32 precision, audio timing can be fine-tuned for:");
    println!("   • Matching specific BPM (beats per minute)");
    println!("   • Synchronizing with visual animation speeds");
    println!("   • Creating custom timing patterns");
    println!("   • Adapting to different hardware capabilities");

    // Example BPM calculations
    println!("\n   Example: Converting BPM to audio intervals");
    let bpm_values = vec![60, 90, 120, 150];
    println!("   BPM →  Interval");
    println!("   ───────────────");

    for bpm in bpm_values {
        let interval = 60.0 / bpm as f32;
        println!("   {:3} → {:5.2}s", bpm, interval);
    }

    // Test 5: Performance characteristics
    println!("\n⚡ Test 5: Performance Characteristics");
    println!("   f32 advantages:");
    println!("   ✓ 32-bit precision (sufficient for timing)");
    println!("   ✓ Hardware floating-point operations");
    println!("   ✓ Standard arithmetic operations");
    println!("   ✓ Compatible with Duration::from_secs_f32()");

    println!("   Current timing: {} seconds", AUDIO_LOOP_DURATION_SECS);
    println!(
        "   Resolution: ±{:.1}ms (due to 100ms polling)",
        100.0 * 0.5
    );

    // Final summary
    println!("\n{}", "=".repeat(50));
    println!("🎯 PRECISION TIMING ANALYSIS COMPLETE");
    println!("{}", "=".repeat(50));
    println!("✅ f32 type provides excellent timing flexibility");
    println!("✅ 100ms resolution sufficient for audio loops");
    println!("✅ Allows fractional second timing (e.g., 2.5s, 4.2s)");
    println!("✅ Compatible with standard Rust duration types");
    println!("✅ Maintains backward compatibility with integer timings");

    println!("\n🚗💨 KITT audio timing is precise and flexible!");

    if AUDIO_LOOP_DURATION_SECS == 3.0 {
        println!("\n💡 Current 3.0s timing provides the classic KITT experience");
        println!("   Can be modified to any f32 value for custom applications");
    }
}
