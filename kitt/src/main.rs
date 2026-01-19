//! Kafka Integrated Throughput Testing (KITT) - A high-performance Kafka throughput measurement tool
//!
//! This tool measures Kafka producer and consumer throughput by creating a temporary topic,
//! producing messages at high rates, and measuring the end-to-end latency and throughput.

use anyhow::{anyhow, Result};
use clap::Parser;

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::{
    sync::{
        atomic::Ordering,
        Arc,
    },
    time::Duration,
};
use tokio::time::sleep;
use tracing::{info, warn};

/// Base maximum number of pending messages allowed before applying backpressure
/// This prevents memory exhaustion during high-throughput testing
/// Will be multiplied by fetch_delay to compensate for delayed consumer start
const BASE_MAX_BACKLOG: u64 = 1000;

mod args;
mod consumer;
mod kafka_client;
mod measurer;
mod producer;
pub mod profiling;
mod utils;
use args::{Args, KeyStrategy, MessageSize};
use consumer::Consumer;
use kafka_client::KafkaClient;
use measurer::{ThroughputMeasurer, LED_BAR_WIDTH};
use producer::Producer;
use profiling::KittOperation;
use quantum_pulse::profile;
use utils::{format_bytes, lcm, parse_bytes};

/// Main entry point for the Kafka Integrated Throughput Testing (KITT) tool
///
/// This function orchestrates the complete throughput test workflow:
/// 1. Parse command-line arguments and validate configuration
/// 2. Establish connection to Kafka broker
/// 3. Create temporary test topic with specified parameters
/// 4. Launch producer and consumer threads for parallel processing
/// 5. Measure and display real-time throughput metrics
/// 6. Clean up resources and report final results

#[tokio::main]
async fn main() -> Result<()> {
    // Profiling is now handled automatically by quantum-pulse

    // Initialize structured logging for better observability
    // Filter out noisy audio library logs and default to info level
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::filter::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::filter::EnvFilter::new("info"))
                .add_directive("symphonia_core=warn".parse().unwrap())
                .add_directive("symphonia_bundle_mp3=warn".parse().unwrap()),
        )
        .init();

    // Parse and validate command-line arguments
    let args = Args::parse();

    // If profile demo requested, run it and exit
    if args.profile_demo {
        return run_profile_demo().await;
    }

    // Run diagnostics tests if requested
    if args.debug_fetch {
        consumer::test_fetch_response_validation();
        println!("🔧 FETCH Response Diagnostics Enabled - Enhanced logging will show detailed error analysis");
        println!("📋 Check the following when consumer isn't receiving messages:");
        println!("   1. Error codes in partition responses (authentication, authorization, topic existence)");
        println!("   2. Offset positioning (out of range, beyond high watermark)");
        println!("   3. Records field presence and content");
        println!("   4. Broker protocol version compatibility");
        println!("   5. Network connectivity and broker availability");
        println!();
    }

    if args.debug_produce {
        producer::test_produce_response_validation();
        println!("🔧 PRODUCE Response Diagnostics Enabled - Enhanced logging will validate message delivery");
        println!(
            "ℹ️  NOTE: Offset=0 in PRODUCE requests is CORRECT - brokers assign actual offsets"
        );
        println!("📋 Check the following when producer isn't sending messages:");
        println!("   1. Error codes in partition responses (authentication, authorization, topic existence)");
        println!("   2. Partition leadership (broker must be leader for partition)");
        println!("   3. Message size limits (check broker max.message.bytes setting)");
        println!(
            "   4. Offset assignment tracking (successful responses show broker-assigned offsets)"
        );
        println!("   5. Network connectivity and broker availability");
        println!("   6. Topic existence and partition count on target broker");
        println!();
    }

    let message_size = profile!(KittOperation::MessageSizeGeneration, {
        MessageSize::parse(&args.message_size)?
    });

    // Create key strategy from command-line argument
    let key_strategy = KeyStrategy::from_arg(args.random_keys);
    if let Some(pool_size) = args.random_keys {
        if pool_size == 0 {
            info!("Using random keys: generating unique keys on the fly");
        } else {
            info!("Using random keys: pool of {} pre-generated keys", pool_size);
        }
    }

    // Calculate partitions and threads based on mode
    let (num_producer_threads, num_consumer_threads, total_partitions) = if args.sticky {
        // Legacy sticky mode: use LCM-based calculation
        let base_threads = args.threads.unwrap_or(4) as usize;

        let lcm_partitions_per_thread = lcm(
            args.producer_partitions_per_thread as usize,
            args.consumer_partitions_per_thread as usize,
        );
        let total_partitions = (lcm_partitions_per_thread * base_threads) as i32;

        let num_producer_threads =
            total_partitions as usize / args.producer_partitions_per_thread as usize;
        let num_consumer_threads =
            total_partitions as usize / args.consumer_partitions_per_thread as usize;

        (num_producer_threads, num_consumer_threads, total_partitions)
    } else {
        // New random mode: consumer-driven partition calculation
        let num_producer_threads = args.producer_threads as usize;
        let num_consumer_threads = args.consumer_threads as usize;
        let total_partitions =
            (num_consumer_threads * args.consumer_partitions_per_thread as usize) as i32;

        (num_producer_threads, num_consumer_threads, total_partitions)
    };

    println!("{}", include_str!("logo.txt"));

    if num_producer_threads == 0 {
        return Err(anyhow!("Producer threads must be at least 1"));
    }

    if num_consumer_threads == 0 {
        return Err(anyhow!("Consumer threads must be at least 1"));
    }

    if args.producer_partitions_per_thread <= 0 {
        return Err(anyhow!("Producer partitions per thread must be at least 1"));
    }

    if args.consumer_partitions_per_thread <= 0 {
        return Err(anyhow!("Consumer partitions per thread must be at least 1"));
    }

    info!("Starting Kitt - Kafka Implementation Throughput Tool");
    if args.sticky {
        info!(
            "Mode: STICKY (LCM-based) - Broker: {}, Producer partitions per thread: {}, Consumer partitions per thread: {}, Total partitions: {}, Producer threads: {}, Consumer threads: {}, message size: {:?}",
            args.broker, args.producer_partitions_per_thread, args.consumer_partitions_per_thread, total_partitions, num_producer_threads, num_consumer_threads, message_size
        );
    } else {
        info!(
            "Mode: RANDOM - Broker: {}, Producer partitions per thread: {}, Consumer partitions per thread: {}, Total partitions: {}, Producer threads: {}, Consumer threads: {}, message size: {:?}",
            args.broker, args.producer_partitions_per_thread, args.consumer_partitions_per_thread, total_partitions, num_producer_threads, num_consumer_threads, message_size
        );
    }
    info!("Running for: {}s", args.duration_secs);

    // Log validation setting
    if args.message_validation {
        info!("🔍 Message validation: ENABLED - Response validation will be performed");
    } else {
        info!("⚡ Message validation: DISABLED - Optimized for maximum performance");
    }

    // Generate topic name using random adjective-noun-verb triplet
    let adjectives = [
        "brave", "quick", "silent", "happy", "bright", "calm", "eager", "fierce", "gentle",
        "jolly", "kind", "lively", "mighty", "proud", "silly", "witty", "zany", "bold", "shy",
        "wise",
    ];
    let nouns = [
        "cat", "dog", "fox", "owl", "lion", "wolf", "bear", "mouse", "hawk", "fish", "frog",
        "horse", "duck", "ant", "bee", "bat", "deer", "goat", "rat", "swan",
    ];
    let verbs = [
        "jumps", "runs", "flies", "dives", "sings", "dances", "hops", "swims", "climbs", "rolls",
        "crawls", "slides", "spins", "laughs", "dreams", "thinks", "waits", "looks", "leaps",
        "rests",
    ];

    let mut rng = thread_rng();
    let adj = adjectives.choose(&mut rng).unwrap();
    let noun = nouns.choose(&mut rng).unwrap();
    let verb = verbs.choose(&mut rng).unwrap();
    let topic_name = format!("topic-{}-{}-{}", adj, noun, verb);
    info!("Test topic: {}", topic_name);

    // Connect to Kafka for admin operations and discover API versions
    let admin_client = Arc::new(
        KafkaClient::connect(&args.broker)
            .await
            .map_err(|e| anyhow!("Failed to connect admin client: {}", e))?,
    );

    // Get discovered API versions to reuse for other connections
    let api_versions = admin_client.api_versions.clone();

    // Create topic
    admin_client
        .create_topic(&topic_name, total_partitions, 1)
        .await
        .map_err(|e| anyhow!("Topic creation failed: {}", e))?;

    // Wait for topic to be ready
    info!("Waiting for topic to be ready...");
    sleep(Duration::from_secs(3)).await;

    // Initialize components
    let measurer = ThroughputMeasurer::with_leds(LED_BAR_WIDTH, !args.silent);

    // Reset message counters
    measurer.messages_sent.store(0, Ordering::Relaxed);
    measurer.bytes_sent.store(0, Ordering::Relaxed);
    measurer.messages_received.store(0, Ordering::Relaxed);

    // Create multiple producer and consumer clients
    let mut producer_clients = Vec::new();
    let mut consumer_clients = Vec::new();

    if args.produce_only.is_some() {
        info!(
            "Creating {} producer connections (produce-only mode)...",
            num_producer_threads
        );
    } else {
        info!(
            "Creating {} producer and {} consumer connections...",
            num_producer_threads, num_consumer_threads
        );
    }

    // Create producer clients
    for i in 0..num_producer_threads {
        let producer_client = Arc::new(
            KafkaClient::connect_with_versions(&args.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect producer client {}: {}", i, e))?,
        );
        producer_clients.push(producer_client);
    }

    // Create consumer clients (skip in produce-only mode)
    if args.produce_only.is_none() {
        for i in 0..num_consumer_threads {
            let consumer_client = Arc::new(
                KafkaClient::connect_with_versions(&args.broker, api_versions.clone())
                    .await
                    .map_err(|e| anyhow!("Failed to connect consumer client {}: {}", i, e))?,
            );
            consumer_clients.push(consumer_client);
        }
    }

    // Create producers and consumers for each thread
    let mut producers = Vec::new();
    let mut consumers = Vec::new();

    for i in 0..num_producer_threads {
        producers.push(Producer::new(
            producer_clients[i].clone(),
            topic_name.clone(),
            args.producer_partitions_per_thread,
            total_partitions,
            message_size.clone(),
            i,
            args.sticky,
            key_strategy.clone(),
            args.messages_per_batch,
        ));
    }

    if args.produce_only.is_none() {
        for i in 0..num_consumer_threads {
            consumers.push(Consumer::new(
                consumer_clients[i].clone(),
                topic_name.clone(),
                args.consumer_partitions_per_thread,
                i,
                args.fetch_delay,
                args.record_processing_time,
            ));
        }
    }
    info!("All client connections established successfully");

    // Parse produce-only data target if specified
    let produce_only_target: Option<u64> = if let Some(ref target_str) = args.produce_only {
        if target_str.is_empty() {
            None // --produce-only without value: time-based
        } else {
            Some(parse_bytes(target_str)?) // --produce-only 1GB: data-target based
        }
    } else {
        None
    };

    // Calculate adjusted max backlog to compensate for fetch delay
    // When produce_only or record_processing_time is set, disable backpressure to measure max producer throughput
    let max_backlog = if args.produce_only.is_some() {
        if let Some(target) = produce_only_target {
            info!(
                "🚀 Produce-only mode: will send {} then stop",
                format_bytes(target)
            );
        } else {
            info!("🚀 Produce-only mode: consumers disabled, measuring pure producer throughput");
        }
        info!("📊 Backpressure disabled");
        u64::MAX
    } else if args.record_processing_time > 0 {
        info!(
            "🔧 Slow consumer simulation enabled: {}ms per record",
            args.record_processing_time
        );
        info!("📊 Backpressure disabled to measure maximum producer throughput");
        u64::MAX
    } else if args.fetch_delay > 0 {
        let adjusted_backlog = BASE_MAX_BACKLOG * args.fetch_delay;
        info!(
            "🔧 Fetch delay compensation enabled: {}s delay",
            args.fetch_delay
        );
        info!(
            "📊 Max backlog threshold adjusted: {} → {} ({}x multiplier)",
            BASE_MAX_BACKLOG, adjusted_backlog, args.fetch_delay
        );
        info!("💡 This compensates for delayed consumer start by allowing higher backlog buildup");
        adjusted_backlog
    } else {
        BASE_MAX_BACKLOG
    };

    // Calculate durations accounting for fetch delay
    let measurement_duration = Duration::from_secs(args.duration_secs);
    let total_test_duration = measurement_duration + Duration::from_secs(args.fetch_delay);

    if args.fetch_delay > 0 {
        info!(
            "🕐 Total test duration: {}s ({}s delay + {}s measurement)",
            total_test_duration.as_secs(),
            args.fetch_delay,
            measurement_duration.as_secs()
        );
        info!("📋 Test timing:");
        info!("   1. Producers start immediately and build backlog");
        info!("   2. Consumers wait {}s before starting", args.fetch_delay);
        info!("   3. Measurement begins after {}s delay", args.fetch_delay);
        info!(
            "   4. Balanced throughput measured for {}s",
            measurement_duration.as_secs()
        );
    } else {
        info!(
            "📋 Standard test timing: immediate start, {}s measurement",
            measurement_duration.as_secs()
        );
    }

    // Reset counters before measurement
    measurer.messages_sent.store(0, Ordering::Relaxed);
    measurer.bytes_sent.store(0, Ordering::Relaxed);
    measurer.messages_received.store(0, Ordering::Relaxed);

    // Start multiple producer and consumer threads
    let mut producer_handles = Vec::new();
    let mut consumer_handles = Vec::new();

    for i in 0..num_producer_threads {
        let producer = producers[i].clone();
        let messages_sent = measurer.messages_sent.clone();
        let bytes_sent = measurer.bytes_sent.clone();
        let messages_received = measurer.messages_received.clone();
        let message_validation = args.message_validation;
        let bytes_target = produce_only_target;
        let producer_handle = tokio::spawn(async move {
            producer
                .produce_messages(
                    total_test_duration,
                    messages_sent,
                    bytes_sent,
                    messages_received,
                    max_backlog,
                    message_validation,
                    bytes_target,
                )
                .await
        });
        producer_handles.push(producer_handle);
    }

    if args.produce_only.is_none() {
        for i in 0..num_consumer_threads {
            let consumer = consumers[i].clone();
            let messages_received = measurer.messages_received.clone();
            let message_validation = args.message_validation;
            let consumer_handle = tokio::spawn(async move {
                consumer
                    .consume_messages(total_test_duration, messages_received, message_validation)
                    .await
            });
            consumer_handles.push(consumer_handle);
        }
    }

    let measurement_handle = tokio::spawn({
        let measurer = measurer.clone();
        let produce_only = args.produce_only.is_some();
        let bytes_target = produce_only_target;
        async move {
            let (min_rate, max_rate, elapsed) = measurer
                .measure(
                    measurement_duration,
                    "MEASUREMENT",
                    max_backlog,
                    args.fetch_delay,
                    produce_only,
                    bytes_target,
                )
                .await;

            let final_sent = measurer.messages_sent.load(Ordering::Relaxed);
            let final_received = measurer.messages_received.load(Ordering::Relaxed);
            let total_data_sent = measurer.bytes_sent.load(Ordering::Relaxed);

            info!("=== FINAL RESULTS ===");
            if produce_only {
                let throughput = final_sent as f64 / elapsed.as_secs_f64();
                info!(
                    "Messages sent: {}, Data sent: {}, Producer throughput: {:.1} messages/second (min: {:.1} msg/s, max: {:.1} msg/s)",
                    final_sent, format_bytes(total_data_sent), throughput, min_rate, max_rate
                );
            } else {
                let throughput = final_received as f64 / elapsed.as_secs_f64();
                let backlog_sum = measurer.backlog_percentage_sum.load(Ordering::Relaxed);
                let backlog_count = measurer.backlog_measurement_count.load(Ordering::Relaxed);
                let avg_backlog = if backlog_count > 0 {
                    backlog_sum / backlog_count
                } else {
                    0
                };
                info!("Messages sent: {}, Data sent: {}, Messages received: {}, Throughput: {:.1} messages/second (min: {:.1} msg/s, max: {:.1} msg/s, avg backlog: {}%)",
                    final_sent, format_bytes(total_data_sent), final_received, throughput, min_rate, max_rate, avg_backlog
                );
            }
        }
    });

    // Wait for all tasks to complete
    for handle in producer_handles {
        let _ = handle.await;
    }
    for handle in consumer_handles {
        let _ = handle.await;
    }
    let _ = measurement_handle.await;

    // Analyze final producer and consumer performance and provide troubleshooting guidance
    let final_sent = measurer.messages_sent.load(Ordering::Relaxed);
    let final_received = measurer.messages_received.load(Ordering::Relaxed);

    let show_troubleshooting = args.debug_fetch
        || args.debug_produce
        || final_sent == 0
        || (args.produce_only.is_none() && final_received == 0);

    if show_troubleshooting {
        println!("\n🔍 PERFORMANCE ANALYSIS");
        println!("================================");

        if final_sent == 0 {
            println!("❌ CRITICAL: No messages were sent by producers!");
            println!("\n🔧 PRODUCER TROUBLESHOOTING CHECKLIST:");
            println!("1. ✅ Broker Connectivity:");
            println!("   - Verify broker address: {}", args.broker);
            println!("   - Check network connectivity to broker");
            println!("   - Ensure broker is running and accepting connections");

            println!("\n2. ✅ Topic & Partition Configuration:");
            println!(
                "   - Topic '{}' was created with {} partitions",
                topic_name, total_partitions
            );
            println!("   - Verify topic exists on the target broker");
            println!("   - Check partition leadership assignments");

            println!("\n3. ✅ Authentication & Authorization:");
            println!("   - Verify client has WRITE permissions on topic");
            println!("   - Check ACLs and security settings");
            println!("   - Ensure authentication credentials are correct");

            println!("\n4. ✅ Re-run with enhanced diagnostics:");
            println!("   cargo run -- --broker {} --debug-produce", args.broker);
        } else if final_received == 0 {
            println!("❌ CRITICAL: No messages were received by consumers!");
            println!("\n🔧 CONSUMER TROUBLESHOOTING CHECKLIST:");
            println!("1. ✅ Broker Connectivity:");
            println!("   - Verify broker address: {}", args.broker);
            println!("   - Check network connectivity to broker");
            println!("   - Ensure broker is running and accepting connections");

            println!("\n2. ✅ Topic & Partition Configuration:");
            println!(
                "   - Topic '{}' was created with {} partitions",
                topic_name, total_partitions
            );
            println!("   - Verify topic exists on the target broker");
            println!("   - Check partition leadership and replica assignments");

            println!("\n3. ✅ Authentication & Authorization:");
            println!("   - Verify client has READ permissions on topic");
            println!("   - Check ACLs and security settings");
            println!("   - Ensure authentication credentials are correct");

            println!("\n4. ✅ Protocol Compatibility:");
            println!("   - Different brokers may use different Kafka versions");
            println!("   - Check broker logs for protocol errors");
            println!("   - Verify FETCH request version compatibility");

            println!("\n5. ✅ Re-run with enhanced diagnostics:");
            println!("   cargo run -- --broker {} --debug-fetch", args.broker);
            println!("   cargo run -- --broker {} --debug-produce", args.broker);
        } else if final_received < final_sent {
            let loss_rate = ((final_sent - final_received) as f64 / final_sent as f64) * 100.0;
            println!(
                "⚠️  Message loss detected: {:.1}% ({} sent, {} received)",
                loss_rate, final_sent, final_received
            );

            if loss_rate > 5.0 {
                println!("\n🔧 HIGH LOSS RATE TROUBLESHOOTING:");
                println!("   - Consumer may be falling behind producer");
                println!("   - Broker may be dropping messages under load");
                println!("   - Network issues causing incomplete fetches");
                println!("   - Consider increasing fetch timeout or reducing producer rate");
            }
        } else {
            println!("✅ Consumer performance looks healthy");
            println!(
                "   Messages sent: {}, received: {}",
                final_sent, final_received
            );
        }

        println!("\n📊 Consumer Configuration Used:");
        println!("   - Consumer threads: {}", num_consumer_threads);
        println!(
            "   - Partitions per thread: {}",
            args.consumer_partitions_per_thread
        );
        println!("   - Fetch timeout: 5000ms");
        println!("   - Max fetch size: 50MB");
        println!("   - Isolation level: READ_UNCOMMITTED");
    }

    // Cleanup: delete topic
    if let Err(e) = admin_client.delete_topic(&topic_name).await {
        warn!("Failed to delete topic '{}': {}", topic_name, e);
    }

    info!("Kitt measurement completed successfully!");

    // Generate and display the profiling report if requested
    if args.profile_report {
        profiling::generate_report();
    }

    Ok(())
}

/// Run a profiling demonstration without connecting to Kafka
async fn run_profile_demo() -> Result<()> {
    use rand::random;
    use std::time::Duration;
    use tokio::time::sleep;

    println!("🚀 KITT Profiling Demonstration");
    println!("================================\n");
    println!("Simulating various operations to show profiling capabilities...");
    println!("Note: Message validation is disabled by default for better performance.");
    println!("Use --message-validation to enable response validation.\n");

    // Simulate message production and consumption operations

    // Simulate producer operations
    println!("📤 Simulating Producer Operations...");
    for batch in 0..5 {
        for _msg in 0..100 {
            profile!(KittOperation::MessageProduce, {
                sleep(Duration::from_micros(20)).await;
            });
            profile!(KittOperation::MessageSizeGeneration, {
                sleep(Duration::from_micros(5)).await;
            });
            // Only simulate validation if it would be enabled
            // In real usage, this is controlled by --message-validation flag
            if random::<bool>() && random::<bool>() {
                profile!(KittOperation::ResponseValidation, {
                    sleep(Duration::from_micros(10)).await;
                });
            }
        }

        // Simulate occasional backlog waiting
        if batch == 2 || batch == 4 {
            profile!(KittOperation::BacklogWaiting, {
                sleep(Duration::from_millis(5)).await;
            });
        }

        if batch < 4 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    println!(" ✓");

    // Simulate consumer operations
    println!("📥 Simulating Consumer Operations...");
    for fetch in 0..8 {
        profile!(KittOperation::MessageConsume, {
            sleep(Duration::from_millis(30)).await;
        });
        // Only simulate validation if it would be enabled
        if random::<bool>() && random::<bool>() {
            profile!(KittOperation::ResponseValidation, {
                sleep(Duration::from_millis(5)).await;
            });
        }
        if fetch < 7 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    println!(" ✓");

    // Simulate additional message size generation for variety
    println!("⚙️  Simulating Additional Processing...");
    for _i in 0..50 {
        profile!(KittOperation::MessageSizeGeneration, {
            sleep(Duration::from_micros(20)).await;
        });
    }

    println!("✓ Simulation complete!\n");

    // Always generate the profiling report in demo mode
    profiling::generate_report();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{lcm, BASE_MAX_BACKLOG};

    #[test]
    fn test_fetch_delay_backlog_compensation() {
        // Test case 1: No delay should use base backlog
        let fetch_delay = 0u64;
        let max_backlog = if fetch_delay > 0 {
            BASE_MAX_BACKLOG * fetch_delay
        } else {
            BASE_MAX_BACKLOG
        };
        assert_eq!(max_backlog, 1000);

        // Test case 2: 3 second delay should multiply backlog by 3
        let fetch_delay = 3u64;
        let max_backlog = if fetch_delay > 0 {
            BASE_MAX_BACKLOG * fetch_delay
        } else {
            BASE_MAX_BACKLOG
        };
        assert_eq!(max_backlog, 3000);

        // Test case 3: 10 second delay should multiply backlog by 10
        let fetch_delay = 10u64;
        let max_backlog = if fetch_delay > 0 {
            BASE_MAX_BACKLOG * fetch_delay
        } else {
            BASE_MAX_BACKLOG
        };
        assert_eq!(max_backlog, 10000);
    }

    #[test]
    fn test_fetch_delay_default_value() {
        use crate::Args;
        use clap::Parser;

        // Test that default fetch_delay is 0
        let args = Args::try_parse_from(&["kitt"]).unwrap();
        assert_eq!(args.fetch_delay, 0);

        // Test custom value works
        let args = Args::try_parse_from(&["kitt", "--fetch-delay", "5"]).unwrap();
        assert_eq!(args.fetch_delay, 5);
    }

    #[test]
    fn test_partition_and_thread_calculation() {
        // Test case 1: Random mode (new default)
        let producer_threads = 4;
        let consumer_threads = 2;
        let consumer_partitions_per_thread = 3;

        let total_partitions = consumer_threads * consumer_partitions_per_thread;

        assert_eq!(total_partitions, 6); // 2 * 3 = 6
        assert_eq!(producer_threads, 4); // Configured directly
        assert_eq!(consumer_threads, 2); // Configured directly

        // Test case 2: Sticky mode (LCM-based legacy)
        let producer_partitions_per_thread = 2;
        let consumer_partitions_per_thread = 3;
        let base_threads = 4;

        let lcm_partitions_per_thread = lcm(
            producer_partitions_per_thread,
            consumer_partitions_per_thread,
        );
        let total_partitions = lcm_partitions_per_thread * base_threads;
        let num_producer_threads = total_partitions / producer_partitions_per_thread;
        let num_consumer_threads = total_partitions / consumer_partitions_per_thread;

        assert_eq!(total_partitions, 24); // lcm(2,3) * 4 = 6 * 4 = 24
        assert_eq!(num_producer_threads, 12); // 24/2 = 12
        assert_eq!(num_consumer_threads, 8); // 24/3 = 8

        // Test case 3: Complex LCM case
        let producer_partitions_per_thread = 4;
        let consumer_partitions_per_thread = 6;
        let base_threads = 2;

        let lcm_partitions_per_thread = lcm(
            producer_partitions_per_thread,
            consumer_partitions_per_thread,
        );
        let total_partitions = lcm_partitions_per_thread * base_threads;
        let num_producer_threads = total_partitions / producer_partitions_per_thread;
        let num_consumer_threads = total_partitions / consumer_partitions_per_thread;

        assert_eq!(total_partitions, 24); // lcm(4,6) * 2 = 12 * 2 = 24
        assert_eq!(num_producer_threads, 6); // 24/4 = 6
        assert_eq!(num_consumer_threads, 4); // 24/6 = 4
    }
}
