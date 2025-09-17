use kitt_throbbler::{KnightRiderAnimator, AUDIO_LOOP_DURATION_SECS};

#[tokio::main]
async fn main() {
    println!("KITT Throbbler - Audio Test");
    println!("============================");

    // Test with audio enabled
    println!("Testing with audio enabled for 10 seconds...");
    println!(
        "Audio will play every {} seconds during the animation",
        AUDIO_LOOP_DURATION_SECS
    );
    let animator = KnightRiderAnimator::new()
        .audio_enabled(true)
        .show_metrics(true);

    // Run a short demo with audio
    animator.run_demo(10, 50, 2000.0).await;

    println!("\nAudio test completed!");
    println!("If you heard the KITT sound effect repeating every {} seconds, the feature is working correctly.", AUDIO_LOOP_DURATION_SECS);
}
