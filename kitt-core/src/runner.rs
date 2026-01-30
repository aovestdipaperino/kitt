//! Test runner for executing throughput tests
//!
//! This module provides the main `run_test` function and `TestHandle` for
//! running Kafka throughput tests with progress reporting.

use crate::client::KafkaClient;
use crate::config::{ProduceOnlyMode, TestConfig};
use crate::consumer::Consumer;
use crate::events::{TestEvent, TestPhase, TestResults};
use crate::producer::{ProduceConfig, Producer};
use crate::utils::lcm;

use anyhow::{anyhow, Result};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Instant};

/// Base maximum backlog before applying backpressure
const BASE_MAX_BACKLOG: u64 = 1000;

/// Channel buffer size for events
const EVENT_CHANNEL_SIZE: usize = 100;

/// Handle to a running test
///
/// Provides access to progress events and methods to wait for or abort the test.
pub struct TestHandle {
    /// Receiver for test events
    pub events: mpsc::Receiver<TestEvent>,
    /// Handle to the test task
    handle: JoinHandle<Result<TestResults>>,
}

impl TestHandle {
    /// Wait for the test to complete and return results
    pub async fn wait(self) -> Result<TestResults> {
        self.handle.await?
    }

    /// Abort the test
    pub fn abort(&self) {
        self.handle.abort();
    }
}

/// Run a throughput test with the given configuration
///
/// Returns a `TestHandle` that provides:
/// - `events`: Channel receiver for progress updates
/// - `wait()`: Async method to wait for completion
/// - `abort()`: Method to cancel the test
///
/// # Example
///
/// ```ignore
/// let config = TestConfig::builder()
///     .broker("localhost:9092")
///     .duration(Duration::from_secs(30))
///     .build()?;
///
/// let mut handle = run_test(config).await?;
///
/// while let Some(event) = handle.events.recv().await {
///     match event {
///         TestEvent::Progress { current_rate, .. } => println!("{:.0} msg/s", current_rate),
///         TestEvent::Completed(results) => println!("Done: {:.0} msg/s", results.throughput),
///         _ => {}
///     }
/// }
///
/// let results = handle.wait().await?;
/// ```
pub async fn run_test(config: TestConfig) -> Result<TestHandle> {
    let (tx, rx) = mpsc::channel(EVENT_CHANNEL_SIZE);

    let handle = tokio::spawn(async move {
        run_test_inner(config, tx).await
    });

    Ok(TestHandle { events: rx, handle })
}

/// Internal test runner
async fn run_test_inner(
    config: TestConfig,
    events: mpsc::Sender<TestEvent>,
) -> Result<TestResults> {
    // Phase: Connecting
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::Connecting }).await;

    let admin_client = Arc::new(
        KafkaClient::connect(&config.broker)
            .await
            .map_err(|e| anyhow!("Failed to connect admin client: {}", e))?,
    );
    let api_versions = admin_client.api_versions.clone();

    // Calculate thread configuration
    let (num_producer_threads, num_consumer_threads, total_partitions) =
        calculate_thread_config(&config)?;

    // Get or generate topic name
    let topic_name = config.topic.clone().unwrap_or_else(generate_topic_name);

    // Handle topic: either use existing or create new
    let actual_partitions = if config.use_existing_topic {
        // Phase: Fetching topic metadata (reuse WaitingForTopic phase)
        let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::WaitingForTopic }).await;

        let metadata = admin_client
            .get_topic_metadata(&topic_name)
            .await
            .map_err(|e| anyhow!("Cannot use existing topic '{}': {}", topic_name, e))?;

        metadata.partition_count
    } else {
        // Phase: Creating topic
        let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::CreatingTopic }).await;

        admin_client
            .create_topic(&topic_name, total_partitions, config.replication_factor)
            .await
            .map_err(|e| anyhow!("Topic creation failed: {}", e))?;

        // Phase: Waiting for topic
        let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::WaitingForTopic }).await;
        sleep(Duration::from_secs(3)).await;

        total_partitions
    };

    // Recalculate thread counts based on actual partitions when using existing topic
    let (num_producer_threads, num_consumer_threads) = if config.use_existing_topic {
        if config.sticky {
            // In sticky mode, recalculate based on actual partitions
            let num_producer_threads =
                actual_partitions as usize / config.producer_partitions_per_thread as usize;
            let num_consumer_threads =
                actual_partitions as usize / config.consumer_partitions_per_thread as usize;
            (num_producer_threads, num_consumer_threads)
        } else {
            // In random mode, use config values directly
            (num_producer_threads, num_consumer_threads)
        }
    } else {
        (num_producer_threads, num_consumer_threads)
    };

    // Validate recalculated thread counts when using existing topic
    if config.use_existing_topic && config.sticky {
        if num_producer_threads == 0 {
            return Err(anyhow!(
                "Existing topic has {} partitions, not enough for {} partitions per producer thread",
                actual_partitions, config.producer_partitions_per_thread
            ));
        }
        if num_consumer_threads == 0 && config.produce_only.is_none() {
            return Err(anyhow!(
                "Existing topic has {} partitions, not enough for {} partitions per consumer thread",
                actual_partitions, config.consumer_partitions_per_thread
            ));
        }
    }

    // Phase: Creating connections
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::CreatingConnections }).await;

    let producers = create_producers(
        &config,
        &topic_name,
        actual_partitions,
        num_producer_threads,
        &api_versions,
    )
    .await?;

    let consumers = create_consumers(
        &config,
        &topic_name,
        num_consumer_threads,
        &api_versions,
    )
    .await?;

    // Calculate backlog threshold
    let produce_only_target = match &config.produce_only {
        Some(ProduceOnlyMode::DataTarget(bytes)) => Some(*bytes),
        _ => None,
    };
    let max_backlog = calculate_max_backlog(&config, produce_only_target);

    // Phase: Running
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::Running }).await;

    let measurement_duration = config.duration;
    let total_test_duration = measurement_duration + config.fetch_delay;

    // Run the measurement
    let results = run_measurement(
        &config,
        producers,
        consumers,
        total_test_duration,
        measurement_duration,
        max_backlog,
        produce_only_target,
        events.clone(),
    )
    .await;

    // Phase: Cleanup
    let _ = events.send(TestEvent::PhaseChange { phase: TestPhase::Cleanup }).await;

    if !config.use_existing_topic {
        if let Err(e) = admin_client.delete_topic(&topic_name).await {
            let _ = events.send(TestEvent::Warning {
                message: format!("Failed to delete topic '{}': {}", topic_name, e),
            }).await;
        }
    }

    // Send completion event
    let _ = events.send(TestEvent::Completed(results.clone())).await;

    Ok(results)
}

/// Calculate producer/consumer threads and partitions based on config
fn calculate_thread_config(config: &TestConfig) -> Result<(usize, usize, i32)> {
    let (num_producer_threads, num_consumer_threads, total_partitions) = if config.sticky {
        let base_threads = config.producer_threads;
        let lcm_partitions_per_thread = lcm(
            config.producer_partitions_per_thread as usize,
            config.consumer_partitions_per_thread as usize,
        );
        let total_partitions = (lcm_partitions_per_thread * base_threads) as i32;
        let num_producer_threads =
            total_partitions as usize / config.producer_partitions_per_thread as usize;
        let num_consumer_threads =
            total_partitions as usize / config.consumer_partitions_per_thread as usize;

        (num_producer_threads, num_consumer_threads, total_partitions)
    } else {
        let num_producer_threads = config.producer_threads;
        let num_consumer_threads = config.consumer_threads;
        let total_partitions =
            (num_consumer_threads * config.consumer_partitions_per_thread as usize) as i32;

        (num_producer_threads, num_consumer_threads, total_partitions)
    };

    if num_producer_threads == 0 {
        return Err(anyhow!("Producer threads must be at least 1"));
    }
    if num_consumer_threads == 0 && config.produce_only.is_none() {
        return Err(anyhow!("Consumer threads must be at least 1"));
    }

    Ok((num_producer_threads, num_consumer_threads, total_partitions))
}

/// Generate random topic name
fn generate_topic_name() -> String {
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
    format!("topic-{}-{}-{}", adj, noun, verb)
}

/// Create producer instances
async fn create_producers(
    config: &TestConfig,
    topic_name: &str,
    total_partitions: i32,
    num_producer_threads: usize,
    api_versions: &HashMap<i16, (i16, i16)>,
) -> Result<Vec<Producer>> {
    let mut producers = Vec::new();

    for i in 0..num_producer_threads {
        let producer_client = Arc::new(
            KafkaClient::connect_with_versions(&config.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect producer client {}: {}", i, e))?,
        );
        producers.push(Producer::new(
            producer_client,
            topic_name.to_string(),
            config.producer_partitions_per_thread,
            total_partitions,
            config.message_size.clone(),
            i,
            config.sticky,
            config.key_strategy.clone(),
            config.messages_per_batch,
        ));
    }

    Ok(producers)
}

/// Create consumer instances
async fn create_consumers(
    config: &TestConfig,
    topic_name: &str,
    num_consumer_threads: usize,
    api_versions: &HashMap<i16, (i16, i16)>,
) -> Result<Vec<Consumer>> {
    if config.produce_only.is_some() {
        return Ok(Vec::new());
    }

    let mut consumers = Vec::new();

    for i in 0..num_consumer_threads {
        let consumer_client = Arc::new(
            KafkaClient::connect_with_versions(&config.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect consumer client {}: {}", i, e))?,
        );
        consumers.push(Consumer::new(
            consumer_client,
            topic_name.to_string(),
            config.consumer_partitions_per_thread,
            i,
            config.fetch_delay.as_secs(),
            config.record_processing_time,
        ));
    }

    Ok(consumers)
}

/// Calculate max backlog threshold
fn calculate_max_backlog(config: &TestConfig, _produce_only_target: Option<u64>) -> u64 {
    if config.produce_only.is_some() {
        u64::MAX
    } else if config.record_processing_time > 0 {
        u64::MAX
    } else {
        let fetch_delay_secs = config.fetch_delay.as_secs();
        if fetch_delay_secs > 0 {
            BASE_MAX_BACKLOG * fetch_delay_secs
        } else {
            BASE_MAX_BACKLOG
        }
    }
}

/// Run the measurement loop
async fn run_measurement(
    config: &TestConfig,
    producers: Vec<Producer>,
    consumers: Vec<Consumer>,
    total_test_duration: Duration,
    measurement_duration: Duration,
    max_backlog: u64,
    produce_only_target: Option<u64>,
    events: mpsc::Sender<TestEvent>,
) -> TestResults {
    let messages_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));
    let messages_received = Arc::new(AtomicU64::new(0));
    let backlog_percentage_sum = Arc::new(AtomicU64::new(0));
    let backlog_measurement_count = Arc::new(AtomicU64::new(0));

    let produce_only = config.produce_only.is_some();
    let message_validation = config.message_validation;
    let fetch_delay = config.fetch_delay;

    // Spawn producers
    let mut producer_handles = Vec::new();
    for producer in producers {
        let messages_sent = messages_sent.clone();
        let bytes_sent = bytes_sent.clone();
        let messages_received = messages_received.clone();
        let producer_handle = tokio::spawn(async move {
            producer
                .produce_messages(ProduceConfig {
                    duration: total_test_duration,
                    messages_sent,
                    bytes_sent,
                    messages_received,
                    max_backlog,
                    message_validation,
                    bytes_target: produce_only_target,
                })
                .await
        });
        producer_handles.push(producer_handle);
    }

    // Spawn consumers
    let mut consumer_handles = Vec::new();
    if !produce_only {
        for consumer in consumers {
            let messages_received = messages_received.clone();
            let consumer_handle = tokio::spawn(async move {
                consumer
                    .consume_messages(total_test_duration, messages_received, message_validation)
                    .await
            });
            consumer_handles.push(consumer_handle);
        }
    }

    // Spawn progress reporter
    let progress_handle = {
        let messages_sent = messages_sent.clone();
        let bytes_sent = bytes_sent.clone();
        let messages_received = messages_received.clone();
        let backlog_percentage_sum = backlog_percentage_sum.clone();
        let backlog_measurement_count = backlog_measurement_count.clone();
        let events = events.clone();

        tokio::spawn(async move {
            // Wait for fetch delay before starting measurement
            if !fetch_delay.is_zero() {
                sleep(fetch_delay).await;
            }

            let start_time = Instant::now();
            let end_time = start_time + measurement_duration;
            let mut progress_interval = interval(Duration::from_millis(100));

            let start_count = if produce_only {
                messages_sent.load(Ordering::Relaxed)
            } else {
                messages_received.load(Ordering::Relaxed)
            };
            let mut last_count = start_count;
            let mut last_rate_time = start_time;
            let mut min_rate = f64::MAX;
            let mut max_rate = 0.0f64;
            let mut current_rate = 0.0f64;

            while produce_only_target.is_some() || Instant::now() < end_time {
                if let Some(target) = produce_only_target {
                    if bytes_sent.load(Ordering::Relaxed) >= target {
                        break;
                    }
                }

                progress_interval.tick().await;

                let now = Instant::now();
                let current_count = if produce_only {
                    messages_sent.load(Ordering::Relaxed)
                } else {
                    messages_received.load(Ordering::Relaxed)
                };
                let time_elapsed = now.duration_since(last_rate_time).as_secs_f64();

                if time_elapsed > 0.0 {
                    current_rate = (current_count - last_count) as f64 / time_elapsed;
                    if current_rate > 0.0 {
                        if min_rate == f64::MAX {
                            min_rate = current_rate;
                        } else {
                            min_rate = min_rate.min(current_rate);
                        }
                        max_rate = max_rate.max(current_rate);
                    }
                    last_count = current_count;
                    last_rate_time = now;
                }

                // Calculate backlog percentage
                let current_sent = messages_sent.load(Ordering::Relaxed);
                let current_received = messages_received.load(Ordering::Relaxed);
                let backlog = current_sent.saturating_sub(current_received);
                let backlog_percent = if max_backlog == u64::MAX {
                    0
                } else {
                    (backlog as f64 / max_backlog as f64 * 100.0).min(100.0) as u8
                };

                if !produce_only {
                    backlog_percentage_sum.fetch_add(backlog_percent as u64, Ordering::Relaxed);
                    backlog_measurement_count.fetch_add(1, Ordering::Relaxed);
                }

                let elapsed = start_time.elapsed();
                let _ = events.send(TestEvent::Progress {
                    messages_sent: current_sent,
                    messages_received: current_received,
                    bytes_sent: bytes_sent.load(Ordering::Relaxed),
                    current_rate,
                    backlog_percent,
                    elapsed,
                }).await;
            }

            (min_rate, max_rate, start_time.elapsed())
        })
    };

    // Wait for all tasks
    for handle in producer_handles {
        let _ = handle.await;
    }
    for handle in consumer_handles {
        let _ = handle.await;
    }
    let (min_rate, max_rate, elapsed) = progress_handle.await.unwrap_or((0.0, 0.0, Duration::ZERO));

    // Calculate final results
    let final_sent = messages_sent.load(Ordering::Relaxed);
    let final_received = messages_received.load(Ordering::Relaxed);
    let total_bytes = bytes_sent.load(Ordering::Relaxed);

    let throughput = if produce_only {
        final_sent as f64 / elapsed.as_secs_f64()
    } else {
        final_received as f64 / elapsed.as_secs_f64()
    };

    let backlog_sum = backlog_percentage_sum.load(Ordering::Relaxed);
    let backlog_count = backlog_measurement_count.load(Ordering::Relaxed);
    let avg_backlog = if backlog_count > 0 {
        (backlog_sum / backlog_count) as u8
    } else {
        0
    };

    TestResults {
        messages_sent: final_sent,
        messages_received: final_received,
        bytes_sent: total_bytes,
        duration: elapsed,
        throughput,
        min_rate: if min_rate == f64::MAX { 0.0 } else { min_rate },
        max_rate,
        avg_backlog_percent: avg_backlog,
    }
}
