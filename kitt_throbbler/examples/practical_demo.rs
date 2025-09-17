use kitt_throbbler::KnightRiderAnimator;
use std::time::Instant;

#[tokio::main]
async fn main() {
    println!("KITT Throbbler - Practical Demo");
    println!("===============================");
    println!("This demo shows how to start/stop animations with audio control");
    println!();

    // Example 1: Simple animation with audio
    println!("1. Starting animation with audio for 5 seconds...");
    let animator_with_audio = KnightRiderAnimator::new()
        .audio_enabled(true)
        .show_metrics(true);

    let start_time = Instant::now();
    let handle = animator_with_audio
        .start_animation(
            30,
            Box::new(move |_| {
                format!(
                    "Processing data... ({:.1}s elapsed)",
                    start_time.elapsed().as_secs_f64()
                )
            }),
        )
        .await;

    // Let it run for 5 seconds
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    handle.stop().await;
    println!("Animation stopped.\n");

    // Example 2: Animation without audio
    println!("2. Starting silent animation for 3 seconds...");
    let silent_animator = KnightRiderAnimator::with_leds(25)
        .audio_enabled(false)
        .show_metrics(false);

    let handle2 = silent_animator
        .start_animation(20, Box::new(|_| "Silent mode".to_string()))
        .await;

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    handle2.stop().await;
    println!("Silent animation stopped.\n");

    // Example 3: Simulated work with progress animation
    println!("3. Simulating work with progress animation and audio...");
    let work_animator = KnightRiderAnimator::with_leds(40)
        .audio_enabled(true)
        .show_metrics(true);

    let work_start = Instant::now();
    let total_work_items = 100;

    let handle3 = work_animator
        .start_animation(
            25,
            Box::new(move |_| {
                let elapsed = work_start.elapsed().as_secs_f64();
                let items_processed = (elapsed * 10.0).min(total_work_items as f64) as usize;
                let rate = items_processed as f64 / elapsed.max(0.1);

                format!(
                    "Processing {}/{} items ({:.1} items/sec)",
                    items_processed, total_work_items, rate
                )
            }),
        )
        .await;

    // Simulate work for 10 seconds
    tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    handle3.stop().await;
    println!("Work simulation completed.\n");

    // Example 4: Multiple sequential animations
    println!("4. Running multiple sequential animations...");

    let multi_animator = KnightRiderAnimator::with_leds(60).audio_enabled(true);

    for i in 1..=3 {
        println!("  Starting phase {} animation...", i);
        let phase_start = Instant::now();

        let handle = multi_animator
            .start_animation(
                15,
                Box::new(move |_| {
                    format!("Phase {} - {:.1}s", i, phase_start.elapsed().as_secs_f64())
                }),
            )
            .await;

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        handle.stop().await;
        println!("  Phase {} complete", i);
    }

    println!("\nAll demos completed!");
    println!("The audio feature plays the sfx.mp3 file every 3 seconds when enabled.");
    println!("Each animation runs in its own thread and can be controlled independently.");
}
