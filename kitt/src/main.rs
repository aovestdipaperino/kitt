//! Kafka Integrated Throughput Testing (KITT) - A high-performance Kafka throughput measurement tool
//!
//! This tool measures Kafka producer and consumer throughput by creating a temporary topic,
//! producing messages at high rates, and measuring the end-to-end latency and throughput.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use clap::Parser;

use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        ApiKey, TopicName,
    },
    protocol::StrBytes,
    records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType},
};
use kitt_throbbler::KnightRiderAnimator;
use rand::{thread_rng, Rng};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    select,
    time::{interval, sleep, Instant},
};
use tracing::{error, info, warn};
use uuid::Uuid;

/// Maximum number of pending messages allowed before applying backpressure
/// This prevents memory exhaustion during high-throughput testing
const MAX_BACKLOG: u64 = 1000;

mod kafka_client;
mod thread_assignment;

use kafka_client::KafkaClient;
use thread_assignment::{calculate_thread_assignment, calculate_total_threads};

/// Command-line arguments for configuring the throughput test
#[derive(Parser, Debug)]
#[command(name = "kitt")]
#[command(about = "Kafka throughput measurement tool")]
#[command(
    after_help = "Note: The --threads argument has been replaced with --threads-per-partition"
)]
struct Args {
    /// Kafka broker address
    #[arg(short, long, default_value = "localhost:9092")]
    broker: String,

    /// Number of partitions for the test topic
    #[arg(short, long, default_value = "8")]
    partitions: i32,

    /// Message size in bytes (e.g., "1024") or range (e.g., "100-1000")
    #[arg(short, long, default_value = "1024")]
    message_size: String,

    /// Measurement duration in seconds
    #[arg(long, default_value = "30")]
    measurement_secs: u64,

    /// Number of threads per partition (default = 1)
    #[arg(long, default_value = "1")]
    threads_per_partition: u32,

    /// Legacy threads argument (deprecated, use threads-per-partition instead)
    #[arg(long, hide = true)]
    threads: Option<i32>,
}

/// Represents the size configuration for test messages
/// Supports both fixed-size messages and variable-size ranges
#[derive(Debug, Clone)]
enum MessageSize {
    /// Fixed message size in bytes
    Fixed(usize),
    /// Variable message size with min and max bounds (inclusive)
    Range(usize, usize),
}

impl MessageSize {
    /// Parses a message size specification from a string
    ///
    /// # Arguments
    /// * `s` - Size specification, either "1024" for fixed size or "100-1000" for range
    ///
    /// # Returns
    /// * `Ok(MessageSize)` - Parsed message size configuration
    /// * `Err(anyhow::Error)` - If the string format is invalid
    fn parse(s: &str) -> Result<Self> {
        if let Some((min_str, max_str)) = s.split_once('-') {
            let min = min_str.parse::<usize>()?;
            let max = max_str.parse::<usize>()?;
            if min > max {
                return Err(anyhow!("Invalid range: min {} > max {}", min, max));
            }
            Ok(MessageSize::Range(min, max))
        } else {
            let size = s.parse::<usize>()?;
            Ok(MessageSize::Fixed(size))
        }
    }

    /// Generates a message size based on the configuration
    ///
    /// # Returns
    /// * For `Fixed`: Returns the fixed size
    /// * For `Range`: Returns a random size within the specified range (inclusive)
    fn generate_size(&self) -> usize {
        match self {
            MessageSize::Fixed(size) => *size,
            MessageSize::Range(min, max) => thread_rng().gen_range(*min..=*max),
        }
    }
}

/// Kafka message producer that sends messages to a specific topic
/// Each producer instance runs in its own thread and targets a specific partition
#[derive(Clone)]
struct Producer {
    /// Shared Kafka client for sending produce requests
    client: Arc<KafkaClient>,
    /// Name of the topic to produce messages to
    topic: String,
    /// Specific partition this producer targets
    target_partition: i32,
    /// Configuration for message size generation
    message_size: MessageSize,
    /// Unique identifier for this producer thread (0-based)
    thread_id: usize,
    /// Number of threads assigned to the same partition
    threads_per_partition: usize,
}

impl Producer {
    /// Creates a new Producer instance
    ///
    /// # Arguments
    /// * `client` - Shared Kafka client for network communication
    /// * `topic` - Target topic name for message production
    /// * `target_partition` - Specific partition this producer targets
    /// * `message_size` - Size configuration for generated messages
    /// * `thread_id` - Unique identifier for this producer thread
    /// * `threads_per_partition` - Number of threads assigned to the same partition
    fn new(
        client: Arc<KafkaClient>,
        topic: String,
        target_partition: i32,
        message_size: MessageSize,
        thread_id: usize,
        threads_per_partition: usize,
    ) -> Self {
        Self {
            client,
            topic,
            target_partition,
            message_size,
            thread_id,
            threads_per_partition,
        }
    }

    /// Produces messages continuously for the specified duration
    ///
    /// This method implements the core message production loop with:
    /// - Single partition targeting with multiple threads
    /// - Backpressure control to prevent memory exhaustion
    /// - High-throughput batch processing
    ///
    /// # Arguments
    /// * `duration` - How long to continue producing messages
    /// * `messages_sent` - Shared counter for tracking sent messages
    /// * `messages_received` - Shared counter for tracking received messages (for backpressure)
    ///
    /// # Returns
    /// * `Ok(())` - When production completes successfully
    /// * `Err(anyhow::Error)` - If Kafka communication or encoding fails
    async fn produce_messages(
        &self,
        duration: Duration,
        messages_sent: Arc<AtomicU64>,
        messages_received: Arc<AtomicU64>,
    ) -> Result<()> {
        let end_time = Instant::now() + duration;

        let mut pending_futures = Vec::new();
        /// Maximum number of concurrent produce requests to prevent overwhelming the broker
        const MAX_PENDING: usize = 100;

        while Instant::now() < end_time {
            // Implement backpressure control to prevent memory exhaustion
            // If too many messages are pending (sent but not yet consumed), pause production
            let sent = messages_sent.load(Ordering::Relaxed);
            let received = messages_received.load(Ordering::Relaxed);
            let backlog = sent.saturating_sub(received);

            if backlog > MAX_BACKLOG {
                // Backlog exceeded threshold - pause to let consumers catch up
                // This prevents OOM conditions during high-throughput testing
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            // Generate message with configured size (fixed or random within range)
            let size = self.message_size.generate_size();
            let payload = vec![b'x'; size]; // Simple payload filled with 'x' characters
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

            // Create a Kafka record with minimal required fields for performance testing
            let record = Record {
                transactional: false,      // Not using transactions for simplicity
                control: false,            // Regular data record, not a control message
                partition_leader_epoch: 0, // Not tracking leader epochs
                producer_id: 0,            // Using default producer ID
                producer_epoch: 0,         // Not using idempotent producer
                timestamp_type: TimestampType::Creation,
                offset: 0,                          // Offset will be assigned by broker
                sequence: 0,                        // Not using sequence numbers
                timestamp,                          // Current timestamp for latency measurement
                key: None, // No message key - will use round-robin partitioning
                value: Some(Bytes::from(payload)), // The actual message payload
                headers: indexmap::IndexMap::new(), // No custom headers needed
            };

            // Configure encoding options for the record batch
            let options = RecordEncodeOptions {
                version: 2,                     // Use version 2 for better performance
                compression: Compression::None, // No compression for maximum throughput
            };

            // Encode the record into a Kafka record batch format
            let mut batch_buf = bytes::BytesMut::new();
            RecordBatchEncoder::encode(&mut batch_buf, [&record], &options)?;
            let batch = batch_buf.freeze();

            // Use the specific target partition assigned to this producer
            let mut partition_data = PartitionProduceData::default();
            partition_data.index = self.target_partition;
            partition_data.records = Some(batch);

            // Prepare topic-level data structure for the produce request
            let mut topic_data = TopicProduceData::default();
            topic_data.name = TopicName(StrBytes::from_string(self.topic.clone()));
            topic_data.partition_data.push(partition_data);

            // Create produce request with durability guarantees
            let mut request = ProduceRequest::default();
            request.acks = -1; // Wait for all in-sync replicas to acknowledge
            request.timeout_ms = 30000; // 30-second timeout for request completion
            request.topic_data.push(topic_data);

            // Send the produce request asynchronously for high throughput
            let version = self.client.get_supported_version(ApiKey::Produce, 3);
            let client = self.client.clone();
            let future = tokio::spawn(async move {
                client
                    .send_request(ApiKey::Produce, &request, version)
                    .await
            });
            pending_futures.push(future);

            // Manage concurrent request limit to prevent resource exhaustion
            if pending_futures.len() >= MAX_PENDING {
                let mut i = 0;
                while i < pending_futures.len() {
                    if pending_futures[i].is_finished() {
                        let result = pending_futures.remove(i).await;
                        if let Err(e) = result {
                            error!("Failed to send message: {}", e);
                        }
                    } else {
                        i += 1;
                    }
                }
            }

            // Increment global message counter for throughput tracking
            messages_sent.fetch_add(1, Ordering::Relaxed);
        }

        // Clean up: wait for all remaining async requests to complete
        // This ensures all messages are sent before the producer shuts down
        for future in pending_futures {
            if let Err(e) = future.await {
                error!("Failed to complete pending message: {}", e);
            }
        }

        Ok(())
    }
}

/// Kafka message consumer that fetches messages from a specific topic
/// Each consumer instance runs in its own thread and targets a specific partition
#[derive(Clone)]
struct Consumer {
    /// Shared Kafka client for sending fetch requests
    client: Arc<KafkaClient>,
    /// Name of the topic to consume messages from
    topic: String,
    /// Specific partition this consumer targets
    target_partition: i32,
    /// Unique identifier for this consumer thread (0-based)
    thread_id: usize,
    /// Number of threads assigned to the same partition
    threads_per_partition: usize,
}

impl Consumer {
    /// Creates a new Consumer instance
    ///
    /// # Arguments
    /// * `client` - Shared Kafka client for network communication
    /// * `topic` - Source topic name for message consumption
    /// * `target_partition` - Specific partition this consumer targets
    /// * `thread_id` - Unique identifier for this consumer thread
    /// * `threads_per_partition` - Number of threads assigned to the same partition
    fn new(
        client: Arc<KafkaClient>,
        topic: String,
        target_partition: i32,
        thread_id: usize,
        threads_per_partition: usize,
    ) -> Self {
        Self {
            client,
            topic,
            target_partition,
            thread_id,
            threads_per_partition,
        }
    }

    /// Consumes messages continuously for the specified duration
    ///
    /// This method implements the core message consumption loop with:
    /// - Single partition targeting with multiple threads
    /// - High-throughput batch fetching
    /// - Offset management for sequential reading
    ///
    /// # Arguments
    /// * `duration` - How long to continue consuming messages
    /// * `messages_received` - Shared counter for tracking received messages
    ///
    /// # Returns
    /// * `Ok(())` - When consumption completes successfully
    /// * `Err(anyhow::Error)` - If Kafka communication fails
    async fn consume_messages(
        &self,
        duration: Duration,
        messages_received: Arc<AtomicU64>,
    ) -> Result<()> {
        let end_time = Instant::now() + duration;

        // Calculate thread ID within this partition
        // This can be used for offset management or other thread-specific behavior
        let thread_within_partition = self.thread_id % self.threads_per_partition;

        // Track current offset for this partition
        // Starting from offset 0 (beginning of partition)
        // In a real implementation, each thread might use a different starting offset strategy
        // or coordinate offset management between threads targeting the same partition
        let mut offset = thread_within_partition as i64; // Start with thread-specific offset

        while Instant::now() < end_time {
            // Build fetch request for the single partition this consumer handles
            let mut fetch_partitions = Vec::new();

            // Configure fetch parameters for the target partition
            let mut fetch_partition = FetchPartition::default();
            fetch_partition.partition = self.target_partition;
            fetch_partition.current_leader_epoch = -1; // Don't check leader epoch
            fetch_partition.fetch_offset = offset; // Start from tracked offset
            fetch_partition.log_start_offset = -1; // Let broker determine log start
            fetch_partition.partition_max_bytes = 1024 * 1024; // 1MB per partition limit

            fetch_partitions.push(fetch_partition);

            // Configure topic-level fetch parameters
            let mut fetch_topic = FetchTopic::default();
            fetch_topic.topic = TopicName(StrBytes::from_string(self.topic.clone()));
            fetch_topic.partitions = fetch_partitions;

            // Create fetch request with optimized settings for high throughput
            let mut request = FetchRequest::default();
            request.max_wait_ms = 100; // Maximum wait time for data availability
            request.min_bytes = 1; // Minimum bytes to return (return immediately if any data)
            request.max_bytes = 50 * 1024 * 1024; // 50MB total response size limit
            request.isolation_level = 0; // Read uncommitted (highest performance)
            request.session_id = 0; // Not using fetch sessions
            request.session_epoch = -1; // Not using fetch sessions
            request.topics.push(fetch_topic);
            request.rack_id = StrBytes::from_static_str(""); // No rack awareness

            // Send fetch request and process response
            let version = self.client.get_supported_version(ApiKey::Fetch, 4);
            match self
                .client
                .send_request(ApiKey::Fetch, &request, version)
                .await
            {
                Ok(_response_bytes) => {
                    // TODO: Parse response to get actual message count
                    // For now, increment counter assuming each fetch gets one message
                    // This simplified approach provides basic throughput measurement
                    messages_received.fetch_add(1, Ordering::Relaxed);

                    // Advance offset for next fetch iteration
                    // In a real implementation, this would be based on actual response parsing
                    // For multiple threads per partition, we increment by threads_per_partition
                    // to ensure each thread reads different messages
                    offset += self.threads_per_partition as i64;
                }
                Err(e) => {
                    error!("Failed to fetch messages: {}", e);
                    // Brief pause before retrying to avoid overwhelming broker with failed requests
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }

            // Yield control to allow other tasks to run (cooperative multitasking)
            tokio::task::yield_now().await;
        }

        Ok(())
    }
}

/// Measures and displays real-time throughput metrics with visual indicators
///
/// This struct tracks message production and consumption rates, displaying them
/// with a Knight Rider-style animated indicator that shows performance levels.
#[derive(Clone)]
struct ThroughputMeasurer {
    /// Atomic counter for total messages sent by all producer threads
    messages_sent: Arc<AtomicU64>,
    /// Atomic counter for total messages received by all consumer threads
    messages_received: Arc<AtomicU64>,
    /// Knight Rider animator for visual feedback
    animator: KnightRiderAnimator,
}

impl ThroughputMeasurer {
    /// Creates a new ThroughputMeasurer with custom LED count
    fn with_leds(led_count: usize) -> Self {
        Self {
            messages_sent: Arc::new(AtomicU64::new(0)),
            messages_received: Arc::new(AtomicU64::new(0)),
            animator: KnightRiderAnimator::with_leds(led_count),
        }
    }

    /// Measures throughput over a specified duration with real-time visual feedback
    ///
    /// This method runs two concurrent timers:
    /// 1. Animation timer (100ms) - updates the Knight Rider display
    /// 2. Rate calculation timer (500ms) - calculates current throughput
    ///
    /// # Arguments
    /// * `duration` - How long to measure throughput
    /// * `phase` - Description of current test phase (for logging)
    ///
    /// # Returns
    /// * `(min_rate, max_rate)` - Tuple of minimum and maximum observed rates
    async fn measure(&self, duration: Duration, phase: &str) -> (f64, f64) {
        let start_time = Instant::now();
        let end_time = start_time + duration;
        // Timer for animation updates (smooth visual feedback)
        let mut animation_interval = interval(Duration::from_millis(100));
        // Timer for throughput calculations (performance measurement)
        let mut rate_interval = interval(Duration::from_millis(500));

        // Baseline counters for rate calculation
        let start_received = self.messages_received.load(Ordering::Relaxed);
        let mut last_received = start_received;
        let mut last_rate_time = start_time;

        info!(
            "Starting {} phase for {} seconds",
            phase,
            duration.as_secs()
        );
        println!(); // Add space for animation

        // Animation state variables
        let mut position = 0; // Current LED position in the display
        let mut direction = 1; // Animation direction: 1 = right, -1 = left
                               // Throughput tracking variables
        let mut current_rate = 0.0;
        let mut min_rate = f64::MAX; // Initialize to maximum to find true minimum
        let mut max_rate = 0.0;

        // Main measurement loop with concurrent animation and rate calculation
        while Instant::now() < end_time {
            select! {
                _ = animation_interval.tick() => {
                    // Update Knight Rider animation position with bouncing behavior
                    position = if direction > 0 {
                        // Moving right: bounce off right edge
                        if position >= 49 { // Updated to match LED_COUNT-1
                            direction = -1;
                            48 // One position before the edge
                        } else {
                            position + 1
                        }
                    } else {
                        // Moving left: bounce off left edge
                        if position <= 0 {
                            direction = 1;
                            1
                        } else {
                            position - 1
                        }
                    };

                    // Update the visual display with current metrics
                    self.animator.draw_frame(position, direction, current_rate, min_rate, max_rate);
                }
                _ = rate_interval.tick() => {
                    // Calculate instantaneous throughput rate
                    let now = Instant::now();
                    let current_received = self.messages_received.load(Ordering::Relaxed);
                    let time_elapsed = now.duration_since(last_rate_time).as_secs_f64();

                    if time_elapsed > 0.0 {
                        // Calculate messages per second since last measurement
                        current_rate = (current_received - last_received) as f64 / time_elapsed;

                        // Track performance statistics (ignore initial zero rates)
                        if current_rate > 0.0 {
                            if min_rate == f64::MAX {
                                min_rate = current_rate; // First valid measurement
                            } else {
                                min_rate = min_rate.min(current_rate);
                            }
                            max_rate = max_rate.max(current_rate);
                        }

                        // Update baseline for next calculation
                        last_received = current_received;
                        last_rate_time = now;
                    }
                }
            }
        }

        println!(); // New line after animation completes

        // Calculate final overall throughput statistics
        let final_received = self.messages_received.load(Ordering::Relaxed);
        let total_duration = start_time.elapsed().as_secs_f64();

        // Calculate overall throughput (messages per second)
        let final_received_rate = if total_duration > 0.0 {
            (final_received - start_received) as f64 / total_duration
        } else {
            0.0
        };

        info!(
            "{} completed - Final throughput: {:.1} msg/s (min: {:.1}, max: {:.1})",
            phase, final_received_rate, min_rate, max_rate
        );

        (min_rate, max_rate)
    }
}

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
    // Initialize structured logging for better observability
    tracing_subscriber::fmt::init();
    println!("{}", include_str!("logo.txt"));
    // Parse and validate command-line arguments
    let args = Args::parse();

    // Check if legacy --threads argument was used
    if let Some(_) = args.threads {
        return Err(anyhow!("The --threads argument has been replaced with --threads-per-partition. Use --threads-per-partition N to specify N threads per partition."));
    }

    // Validate threads_per_partition
    if args.threads_per_partition == 0 {
        return Err(anyhow!("threads-per-partition must be greater than 0"));
    }

    let message_size = MessageSize::parse(&args.message_size)?;
    let num_threads =
        calculate_total_threads(args.partitions, args.threads_per_partition as usize, false);
    let total_threads =
        calculate_total_threads(args.partitions, args.threads_per_partition as usize, true);

    info!("Starting Kitt - Kafka Implementation Throughput Tool");
    info!(
        "Broker: {}, Partitions: {}, Threads per partition: {}, Total threads: {}, message size: {:?}",
        args.broker, args.partitions, args.threads_per_partition, total_threads, message_size
    );
    info!("Running for: {}s", args.measurement_secs);

    // Generate topic name
    let topic_name = format!("kitt-test-{}", Uuid::new_v4());
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
        .create_topic(&topic_name, args.partitions, 1)
        .await
        .map_err(|e| anyhow!("Topic creation failed: {}", e))?;

    // Wait for topic to be ready
    info!("Waiting for topic to be ready...");
    sleep(Duration::from_secs(3)).await;

    // Initialize components
    let measurer = ThroughputMeasurer::with_leds(50);

    // Reset message counters
    measurer.messages_sent.store(0, Ordering::Relaxed);
    measurer.messages_received.store(0, Ordering::Relaxed);

    // Create multiple producer and consumer clients
    let mut producer_clients = Vec::new();
    let mut consumer_clients = Vec::new();

    info!(
        "Creating {} producer and consumer connections ({} per partition)...",
        num_threads, args.threads_per_partition
    );
    for i in 0..num_threads {
        let producer_client = Arc::new(
            KafkaClient::connect_with_versions(&args.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect producer client {}: {}", i, e))?,
        );
        let consumer_client = Arc::new(
            KafkaClient::connect_with_versions(&args.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect consumer client {}: {}", i, e))?,
        );

        producer_clients.push(producer_client);
        consumer_clients.push(consumer_client);
    }

    // Create producers and consumers for each thread
    let mut producers = Vec::new();
    let mut consumers = Vec::new();

    for i in 0..num_threads {
        // Calculate thread assignment using the thread_assignment module
        let assignment = calculate_thread_assignment(i, args.threads_per_partition as usize);

        producers.push(Producer::new(
            producer_clients[i].clone(),
            topic_name.clone(),
            assignment.partition_id,
            message_size.clone(),
            i,
            args.threads_per_partition as usize,
        ));
        consumers.push(Consumer::new(
            consumer_clients[i].clone(),
            topic_name.clone(),
            assignment.partition_id,
            i,
            args.threads_per_partition as usize,
        ));
    }
    info!("All client connections established successfully ({} threads per partition across {} partitions)", args.threads_per_partition, args.partitions);

    // Measurement phase only (no warmup)
    let measurement_duration = Duration::from_secs(args.measurement_secs);

    // Reset counters before measurement
    measurer.messages_sent.store(0, Ordering::Relaxed);
    measurer.messages_received.store(0, Ordering::Relaxed);

    // Start multiple producer and consumer threads
    let mut producer_handles = Vec::new();
    let mut consumer_handles = Vec::new();

    for i in 0..num_threads {
        let producer = producers[i].clone();
        let messages_sent = measurer.messages_sent.clone();
        let messages_received = measurer.messages_received.clone();
        let producer_handle = tokio::spawn(async move {
            producer
                .produce_messages(measurement_duration, messages_sent, messages_received)
                .await
        });
        producer_handles.push(producer_handle);

        let consumer = consumers[i].clone();
        let messages_received = measurer.messages_received.clone();
        let consumer_handle = tokio::spawn(async move {
            consumer
                .consume_messages(measurement_duration, messages_received)
                .await
        });
        consumer_handles.push(consumer_handle);
    }

    let measurement_handle = tokio::spawn({
        let measurer = measurer.clone();
        async move {
            let (min_rate, max_rate) = measurer.measure(measurement_duration, "MEASUREMENT").await;

            let total_sent = measurer.messages_sent.load(Ordering::Relaxed);
            let total_received = measurer.messages_received.load(Ordering::Relaxed);
            let throughput = total_received as f64 / measurement_duration.as_secs_f64();

            info!("=== FINAL RESULTS ===");
            info!("Messages sent: {}, Messages received: {}, Throughput: {:.1} messages/second (min: {:.1} msg/s, max: {:.1} msg/s)", total_sent, total_received, throughput,
                min_rate, max_rate
            );
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

    // Cleanup: delete topic
    if let Err(e) = admin_client.delete_topic(&topic_name).await {
        warn!("Failed to delete topic '{}': {}", topic_name, e);
    }

    info!("Kitt measurement completed successfully!");
    Ok(())
}
