use kitt_throbbler::KnightRiderAnimator;

#[tokio::main]
async fn main() {
    println!("KITT Throbbler - Simple Demo");
    println!("============================");

    // Create a default animator
    let animator = KnightRiderAnimator::new();

    // Run the demo for 15 seconds with medium speed and 5000 max rate
    animator.run_demo(15, 15, 5000.0).await;

    // You can also create a custom animator with audio enabled
    println!("\nCustom animation (smaller, no metrics, with audio):");
    let custom = KnightRiderAnimator::with_leds(30)
        .audio_enabled(true)
        .show_metrics(false);

    // Run a shorter demo with faster speed and audio
    custom.run_demo(10, 8, 1000.0).await;

    // Demo with just audio enabled on default settings
    println!("\nDefault animation with audio enabled:");
    let audio_demo = KnightRiderAnimator::new().audio_enabled(true);
    audio_demo.run_demo(8, 20, 3000.0).await;

    println!("Demo completed!");
}
