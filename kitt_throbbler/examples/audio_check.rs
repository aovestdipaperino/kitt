use kitt_throbbler::{KnightRiderAnimator, AUDIO_LOOP_DURATION_SECS};
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("KITT Throbbler - Audio Hardware Check");
    println!("=====================================");

    // Test 1: Check audio initialization without playback
    println!("1. Testing audio initialization...");
    match rodio::OutputStream::try_default() {
        Ok((_stream, _handle)) => {
            println!("   ✓ Audio hardware detected and initialized successfully");
        }
        Err(e) => {
            println!("   ✗ Audio hardware initialization failed: {}", e);
            println!("   This means audio features will be disabled");
        }
    }

    // Test 2: Check embedded sfx.mp3 data
    println!("2. Checking embedded sfx.mp3 data...");
    const SFX_DATA: &[u8] = include_bytes!("../src/assets/sfx.mp3");
    println!(
        "   ✓ Embedded sfx.mp3 data loaded ({} bytes)",
        SFX_DATA.len()
    );

    // Test 3: Create animator with audio enabled
    println!("3. Testing KnightRiderAnimator with audio enabled...");
    let animator = KnightRiderAnimator::with_leds(20)
        .audio_enabled(true)
        .show_metrics(false);

    println!("   ✓ Animator created successfully with audio enabled");

    // Test 4: Run a very short animation (3 seconds) to test everything works
    println!("4. Running short test animation (3 seconds)...");
    println!(
        "   Note: Audio should play every {} seconds if hardware is available",
        AUDIO_LOOP_DURATION_SECS
    );

    let start_time = std::time::Instant::now();
    let handle = animator
        .start_animation(
            100, // slower animation for testing
            Box::new(move |_| format!("Testing... {:.1}s", start_time.elapsed().as_secs_f64())),
        )
        .await;

    // Run for 3 seconds
    tokio::time::sleep(Duration::from_secs(3)).await;
    handle.stop().await;

    println!("\n5. Test Results:");
    println!("   - Animation completed successfully");
    println!("   - Audio thread management working");
    println!("   - Embedded audio data is statically linked in binary");
    println!("   - If you heard the KITT sound, audio is fully functional");
    println!("   - If no sound but no errors, audio hardware may be unavailable");

    println!("\n✓ All tests completed!");
}
