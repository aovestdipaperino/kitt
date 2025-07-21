use kitt_throbbler::KnightRiderAnimator;

#[tokio::main]
async fn main() {
    println!("KITT Throbbler - Simple Demo");
    println!("============================");

    // Create a default animator
    let animator = KnightRiderAnimator::new();

    // Run the demo for 15 seconds with medium speed and 5000 max rate
    animator.run_demo(15, 15, 5000.0).await;

    // You can also create a custom animator
    println!("\nCustom animation (smaller, no metrics):");
    let custom = KnightRiderAnimator::with_leds(30).show_metrics(false);

    // Run a shorter demo with faster speed
    custom.run_demo(5, 8, 1000.0).await;

    println!("Demo completed!");
}
