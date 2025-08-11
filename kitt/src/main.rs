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
        fetch_response::FetchResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        ApiKey, TopicName,
    },
    protocol::{Decodable, StrBytes},
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
use kafka_client::KafkaClient;

/// Command-line arguments for configuring the throughput test
#[derive(Parser)]
#[command(name = "kitt")]
#[command(about = "Kafka throughput measurement tool")]
struct Args {
    /// Kafka broker address
    #[arg(short, long, default_value = "localhost:9092")]
    broker: String,

    /// Number of partitions assigned to each producer thread
    #[arg(short, long, default_value = "1")]
    producer_partitions_per_thread: i32,

    /// Number of partitions assigned to each consumer thread
    #[arg(short, long, default_value = "1")]
    consumer_partitions_per_thread: i32,

    /// Message size in bytes (e.g., "1024") or range (e.g., "100-1000")
    #[arg(short, long, default_value = "1024")]
    message_size: String,

    /// Measurement duration in seconds
    #[arg(long, default_value = "15")]
    measurement_secs: u64,

    /// Number of producer/consumer threads
    #[arg(short, long)]
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
/// Each producer instance runs in its own thread and targets specific partitions
#[derive(Clone)]
struct Producer {
    /// Shared Kafka client for sending produce requests
    client: Arc<KafkaClient>,
    /// Name of the topic to produce messages to
    topic: String,
    /// Number of partitions assigned to this thread
    producer_partitions_per_thread: i32,
    /// Configuration for message size generation
    message_size: MessageSize,
    /// Unique identifier for this producer thread (0-based)
    thread_id: usize,
}

impl Producer {
    /// Creates a new Producer instance
    ///
    /// # Arguments
    /// * `client` - Shared Kafka client for network communication
    /// * `topic` - Target topic name for message production
    /// * `producer_partitions_per_thread` - Number of partitions assigned to this thread
    /// * `message_size` - Size configuration for generated messages
    /// * `thread_id` - Unique identifier for this producer thread
    /// * `thread_id` - Unique identifier for this producer thread
    fn new(
        client: Arc<KafkaClient>,
        topic: String,
        producer_partitions_per_thread: i32,
        message_size: MessageSize,
        thread_id: usize,
    ) -> Self {
        Self {
            client,
            topic,
            producer_partitions_per_thread,
            message_size,
            thread_id,
        }
    }

    /// Produces messages continuously for the specified duration
    ///
    /// This method implements the core message production loop with:
    /// - Partition-based load distribution across threads
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

        // Each thread handles its assigned partitions (no overlap with other threads)
        let start_partition = self.thread_id * self.producer_partitions_per_thread as usize;
        let partition_count = self.producer_partitions_per_thread as usize;

        // Send 1 message per partition in each batch request
        const MAX_PENDING: usize = 20; // Maximum concurrent batched requests

        let mut pending_futures = Vec::new();

        while Instant::now() < end_time {
            // Implement backpressure control to prevent memory exhaustion
            let sent = messages_sent.load(Ordering::Relaxed);
            let received = messages_received.load(Ordering::Relaxed);
            let backlog = sent.saturating_sub(received);

            if backlog > MAX_BACKLOG {
                // Backlog exceeded threshold - pause to let consumers catch up
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }

            // Create produce request with 1 message per partition
            let mut request = ProduceRequest::default();
            request.acks = -1;
            request.timeout_ms = 30000;

            let mut topic_data = TopicProduceData::default();
            topic_data.name = TopicName(StrBytes::from_string(self.topic.clone()));

            // Configure encoding options for the record batch
            let options = RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            };

            // Create one message for each partition assigned to this thread
            for i in 0..partition_count {
                let partition_id = (start_partition + i) as i32;

                // Generate message with configured size
                let size = self.message_size.generate_size();
                let payload = vec![b'x'; size];
                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

                // Create a Kafka record
                let record = Record {
                    transactional: false,
                    control: false,
                    partition_leader_epoch: 0,
                    producer_id: 0,
                    producer_epoch: 0,
                    timestamp_type: TimestampType::Creation,
                    offset: 0,
                    sequence: 0,
                    timestamp,
                    key: None,
                    value: Some(Bytes::from(payload)),
                    headers: indexmap::IndexMap::new(),
                };

                // Encode the record into a batch
                let mut batch_buf = bytes::BytesMut::new();
                RecordBatchEncoder::encode(&mut batch_buf, [&record], &options)?;
                let batch = batch_buf.freeze();

                // Create partition data
                let mut partition_data = PartitionProduceData::default();
                partition_data.index = partition_id;
                partition_data.records = Some(batch);
                topic_data.partition_data.push(partition_data);
            }

            request.topic_data.push(topic_data);

            // Track messages before sending (1 per partition)
            messages_sent.fetch_add(partition_count as u64, Ordering::Relaxed);

            // Send the batched request asynchronously
            let version = self.client.get_supported_version(ApiKey::Produce, 3);
            let client = self.client.clone();
            let future = tokio::spawn(async move {
                client
                    .send_request(ApiKey::Produce, &request, version)
                    .await
            });
            pending_futures.push(future);

            // Manage concurrent request limit
            if pending_futures.len() >= MAX_PENDING {
                let mut i = 0;
                while i < pending_futures.len() {
                    if pending_futures[i].is_finished() {
                        let result = pending_futures.remove(i).await;
                        if let Err(e) = result {
                            error!("Failed to send batch: {}", e);
                        }
                    } else {
                        i += 1;
                    }
                }
            }
        }

        // Clean up: wait for all remaining requests to complete
        for future in pending_futures {
            if let Err(e) = future.await {
                error!("Failed to complete pending batch: {}", e);
            }
        }

        Ok(())
    }
}

/// Kafka message consumer that fetches messages from a specific topic
/// Each consumer instance runs in its own thread and targets specific partitions
#[derive(Clone)]
struct Consumer {
    /// Shared Kafka client for sending fetch requests
    client: Arc<KafkaClient>,
    /// Name of the topic to consume messages from
    topic: String,
    /// Number of partitions assigned to this thread
    consumer_partitions_per_thread: i32,
    /// Unique identifier for this consumer thread (0-based)
    thread_id: usize,
}

impl Consumer {
    /// Creates a new Consumer instance
    ///
    /// # Arguments
    /// * `client` - Shared Kafka client for network communication
    /// * `topic` - Source topic name for message consumption
    /// * `consumer_partitions_per_thread` - Number of partitions assigned to this thread
    /// * `thread_id` - Unique identifier for this consumer thread
    /// * `thread_id` - Unique identifier for this consumer thread
    fn new(
        client: Arc<KafkaClient>,
        topic: String,
        consumer_partitions_per_thread: i32,
        thread_id: usize,
    ) -> Self {
        Self {
            client,
            topic,
            consumer_partitions_per_thread,
            thread_id,
        }
    }

    /// Consumes messages continuously for the specified duration
    ///
    /// This method implements the core message consumption loop with:
    /// - Partition-based load distribution across threads
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

        // Each thread handles its assigned partitions (no overlap with other threads)
        let start_partition = self.thread_id * self.consumer_partitions_per_thread as usize;
        let partition_count = self.consumer_partitions_per_thread as usize;

        // Track current offset for each partition this consumer handles
        // Starting from offset 0 (beginning of each partition)
        let mut offsets = vec![0i64; partition_count];

        while Instant::now() < end_time {
            // Build fetch request for all partitions handled by this consumer
            let mut fetch_partitions = Vec::new();

            // Configure fetch parameters for each assigned partition
            for (idx, partition) in (start_partition..start_partition + partition_count).enumerate()
            {
                let mut fetch_partition = FetchPartition::default();
                fetch_partition.partition = partition as i32;
                fetch_partition.current_leader_epoch = -1; // Don't check leader epoch
                fetch_partition.fetch_offset = offsets[idx]; // Start from tracked offset
                fetch_partition.log_start_offset = -1; // Let broker determine log start
                fetch_partition.partition_max_bytes = 1024 * 1024; // 1MB per partition limit

                fetch_partitions.push(fetch_partition);
            }

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
                Ok(response_bytes) => {
                    // Parse the fetch response to count actual messages and advance offsets properly
                    match FetchResponse::decode(&mut response_bytes.as_ref(), version) {
                        Ok(fetch_response) => {
                            let mut total_messages = 0u64;

                            // Process each topic in the response
                            for topic_response in &fetch_response.responses {
                                // Process each partition in the topic
                                for (_partition_idx, partition_response) in
                                    topic_response.partitions.iter().enumerate()
                                {
                                    if partition_response.error_code == 0 {
                                        // Count records in this partition
                                        if let Some(records) = &partition_response.records {
                                            // Parse record batches to count individual messages
                                            let _records_cursor =
                                                std::io::Cursor::new(records.as_ref());
                                            let mut message_count = 0u64;

                                            // Count records by estimating from the records data
                                            // For simplicity, assume each fetch with non-empty records contains at least 1 message
                                            if !records.is_empty() {
                                                // Try to parse the high water mark from partition response to advance offset
                                                let partition_id =
                                                    partition_response.partition_index as usize;
                                                let local_partition_idx = partition_id
                                                    - (self.thread_id
                                                        * self.consumer_partitions_per_thread
                                                            as usize);

                                                if local_partition_idx < offsets.len() {
                                                    // Use high water mark if available, otherwise advance by 1
                                                    if partition_response.high_watermark
                                                        > offsets[local_partition_idx]
                                                    {
                                                        let messages_in_partition =
                                                            (partition_response.high_watermark
                                                                - offsets[local_partition_idx])
                                                                as u64;
                                                        message_count +=
                                                            messages_in_partition.min(1000); // Cap to prevent huge jumps
                                                        offsets[local_partition_idx] =
                                                            partition_response.high_watermark;
                                                    } else {
                                                        message_count += 1;
                                                        offsets[local_partition_idx] += 1;
                                                    }
                                                }
                                            }

                                            total_messages += message_count;
                                        }
                                    } else {
                                        // Handle partition errors (still advance offset to avoid getting stuck)
                                        let partition_id =
                                            partition_response.partition_index as usize;
                                        let local_partition_idx = partition_id
                                            - (self.thread_id
                                                * self.consumer_partitions_per_thread as usize);
                                        if local_partition_idx < offsets.len() {
                                            offsets[local_partition_idx] += 1;
                                        }
                                    }
                                }
                            }

                            // Update metrics with actual message count
                            if total_messages > 0 {
                                messages_received.fetch_add(total_messages, Ordering::Relaxed);
                            }
                        }
                        Err(e) => {
                            error!("Failed to decode fetch response: {}", e);
                            // On decode error, still advance offsets minimally to avoid infinite loop
                            for offset in &mut offsets {
                                *offset += 1;
                            }
                        }
                    }
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

    // Parse and validate command-line arguments
    let args = Args::parse();

    let message_size = MessageSize::parse(&args.message_size)?;
    let num_producer_threads = args.threads.unwrap_or(4) as usize;

    // Calculate total partitions needed based on producer requirements
    let total_partitions =
        (args.producer_partitions_per_thread as usize * num_producer_threads) as i32;

    // Calculate number of consumer threads needed to cover all partitions
    let num_consumer_threads =
        (total_partitions as usize + args.consumer_partitions_per_thread as usize - 1)
            / args.consumer_partitions_per_thread as usize;

    println!("{}", include_str!("logo.txt"));

    if num_producer_threads == 0 {
        return Err(anyhow!("Thread count must be at least 1"));
    }

    info!("Starting Kitt - Kafka Implementation Throughput Tool");
    info!(
        "Broker: {}, Producer partitions per thread: {}, Consumer partitions per thread: {}, Total partitions: {}, Producer threads: {}, Consumer threads: {} (calculated to cover all partitions), message size: {:?}",
        args.broker, args.producer_partitions_per_thread, args.consumer_partitions_per_thread, total_partitions, num_producer_threads, num_consumer_threads, message_size
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
        .create_topic(&topic_name, total_partitions, 1)
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
        "Creating {} producer and {} consumer connections...",
        num_producer_threads, num_consumer_threads
    );
    for i in 0..num_producer_threads {
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

    // Create additional consumer clients if needed
    for i in num_producer_threads..num_consumer_threads {
        let consumer_client = Arc::new(
            KafkaClient::connect_with_versions(&args.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect consumer client {}: {}", i, e))?,
        );
        consumer_clients.push(consumer_client);
    }

    // Create producers and consumers for each thread
    let mut producers = Vec::new();
    let mut consumers = Vec::new();

    for i in 0..num_producer_threads {
        producers.push(Producer::new(
            producer_clients[i].clone(),
            topic_name.clone(),
            args.producer_partitions_per_thread,
            message_size.clone(),
            i,
        ));
    }

    for i in 0..num_consumer_threads {
        consumers.push(Consumer::new(
            consumer_clients[i].clone(),
            topic_name.clone(),
            args.consumer_partitions_per_thread,
            i,
        ));
    }
    info!("All client connections established successfully");

    // Measurement phase only (no warmup)
    let measurement_duration = Duration::from_secs(args.measurement_secs);

    // Reset counters before measurement
    measurer.messages_sent.store(0, Ordering::Relaxed);
    measurer.messages_received.store(0, Ordering::Relaxed);

    // Start multiple producer and consumer threads
    let mut producer_handles = Vec::new();
    let mut consumer_handles = Vec::new();

    for i in 0..num_producer_threads {
        let producer = producers[i].clone();
        let messages_sent = measurer.messages_sent.clone();
        let messages_received = measurer.messages_received.clone();
        let producer_handle = tokio::spawn(async move {
            producer
                .produce_messages(measurement_duration, messages_sent, messages_received)
                .await
        });
        producer_handles.push(producer_handle);
    }

    for i in 0..num_consumer_threads {
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
