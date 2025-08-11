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
        ApiKey, ResponseHeader, TopicName,
    },
    protocol::{Decodable, StrBytes},
    records::{
        Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
        TimestampType,
    },
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
use tracing::{debug, error, info, warn};
use uuid::Uuid;

/// Maximum number of pending messages allowed before applying backpressure
/// This prevents memory exhaustion during high-throughput testing
const MAX_BACKLOG: u64 = 1000;

/// Calculate the Greatest Common Divisor of two numbers
fn gcd(a: usize, b: usize) -> usize {
    if b == 0 {
        a
    } else {
        gcd(b, a % b)
    }
}

/// Calculate the Least Common Multiple of two numbers
fn lcm(a: usize, b: usize) -> usize {
    a * b / gcd(a, b)
}

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
    #[arg(short, long, default_value = "15")]
    duration_secs: u64,

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
                // Yield to allow consumers to continue processing
                tokio::task::yield_now().await;
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

        // Add periodic logging to track consumer activity
        let mut last_log_time = Instant::now();
        let mut fetch_count = 0u64;

        while Instant::now() < end_time {
            // Log at start of each loop iteration to track consumer activity
            let time_remaining = end_time
                .saturating_duration_since(Instant::now())
                .as_secs_f64();
            debug!(
                "Consumer thread {} starting fetch loop iteration, time remaining: {:.1}s",
                self.thread_id, time_remaining
            );

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

                debug!(
                    "Thread {}: requesting partition {} at offset {}",
                    self.thread_id, partition, offsets[idx]
                );
                fetch_partitions.push(fetch_partition);
            }

            debug!(
                "Thread {}: sending fetch request for {} partitions ({}..{})",
                self.thread_id,
                partition_count,
                start_partition,
                start_partition + partition_count - 1
            );

            // Configure topic-level fetch parameters
            let partition_count_for_log = fetch_partitions.len();
            let mut fetch_topic = FetchTopic::default();
            fetch_topic.topic = TopicName(StrBytes::from_string(self.topic.clone()));
            fetch_topic.partitions = fetch_partitions;

            // Create fetch request with optimized settings for high throughput
            let mut request = FetchRequest::default();
            request.max_wait_ms = 1000; // Maximum wait time for data availability
            request.min_bytes = 1; // Minimum bytes to return (return immediately if any data)
            request.max_bytes = 50 * 1024 * 1024; // 50MB total response size limit
            request.isolation_level = 0; // Read uncommitted (highest performance)
            request.session_id = 0; // Not using fetch sessions
            request.session_epoch = -1; // Not using fetch sessions
            request.topics.push(fetch_topic);
            request.rack_id = StrBytes::from_static_str(""); // No rack awareness

            // Send fetch request and process response
            debug!(
                "Consumer thread {} sending fetch request for {} partitions",
                self.thread_id, partition_count_for_log
            );
            let version = self.client.get_supported_version(ApiKey::Fetch, 4);

            // Add timeout to prevent hanging indefinitely
            let fetch_timeout = Duration::from_millis(5000); // 5 second timeout
            match tokio::time::timeout(
                fetch_timeout,
                self.client.send_request(ApiKey::Fetch, &request, version),
            )
            .await
            {
                Ok(Ok(response_bytes)) => {
                    // First decode the response header, then the fetch response payload
                    let mut response_cursor = std::io::Cursor::new(response_bytes.as_ref());

                    // Decode response header first
                    let header_version = ApiKey::Fetch.response_header_version(version);
                    match ResponseHeader::decode(&mut response_cursor, header_version) {
                        Ok(_response_header) => {
                            // Now decode the fetch response payload
                            match FetchResponse::decode(&mut response_cursor, version) {
                                Ok(fetch_response) => {
                                    let mut total_messages = 0u64;
                                    debug!(
                                        "Fetch response has {} topics",
                                        fetch_response.responses.len()
                                    );

                                    // Process each topic in the response
                                    for topic_response in &fetch_response.responses {
                                        debug!(
                                            "Topic has {} partitions",
                                            topic_response.partitions.len()
                                        );

                                        // Process each partition in the topic
                                        for partition_response in &topic_response.partitions {
                                            let partition_id =
                                                partition_response.partition_index as usize;
                                            let local_partition_idx = partition_id
                                                - (self.thread_id
                                                    * self.consumer_partitions_per_thread as usize);

                                            let current_offset =
                                                if local_partition_idx < offsets.len() {
                                                    offsets[local_partition_idx]
                                                } else {
                                                    -1
                                                };

                                            debug!(
                                                "Partition {}: error_code={}, has_records={}, records_len={}, high_watermark={}, current_offset={}",
                                                partition_id,
                                                partition_response.error_code,
                                                partition_response.records.is_some(),
                                                partition_response.records.as_ref().map(|r| r.len()).unwrap_or(0),
                                                partition_response.high_watermark,
                                                current_offset
                                            );

                                            if partition_response.error_code == 0
                                                && local_partition_idx < offsets.len()
                                            {
                                                // Check if we got any records
                                                if let Some(records) = &partition_response.records {
                                                    if !records.is_empty() {
                                                        // Parse record batches to count actual records
                                                        let mut records_cursor =
                                                            std::io::Cursor::new(records.as_ref());
                                                        let mut partition_message_count = 0u64;

                                                        // Decode all record batches in this partition
                                                        while records_cursor.position()
                                                            < records.len() as u64
                                                        {
                                                            match RecordBatchDecoder::decode(
                                                                &mut records_cursor,
                                                            ) {
                                                                Ok(record_set) => {
                                                                    partition_message_count +=
                                                                        record_set.records.len()
                                                                            as u64;
                                                                    debug!(
                                                                        "Decoded record batch with {} records",
                                                                        record_set.records.len()
                                                                    );
                                                                }
                                                                Err(e) => {
                                                                    debug!(
                                                                        "Failed to decode record batch: {}",
                                                                        e
                                                                    );
                                                                    break;
                                                                }
                                                            }
                                                        }

                                                        if partition_message_count > 0 {
                                                            total_messages +=
                                                                partition_message_count;
                                                            // Advance offset by the actual number of messages
                                                            offsets[local_partition_idx] +=
                                                                partition_message_count as i64;
                                                        } else {
                                                            // No messages in record batches - advance by 1 to make progress
                                                            //offsets[local_partition_idx] += 1;
                                                        }

                                                        debug!(
                                                            "Found {} records in partition {}, new offset: {}",
                                                            partition_message_count,
                                                            partition_id,
                                                            offsets[local_partition_idx]
                                                        );
                                                    } else {
                                                        debug!(
                                                            "Empty records in partition {}, log_start_offset={}, high_watermark={}",
                                                            partition_id,
                                                            partition_response.log_start_offset,
                                                            partition_response.high_watermark
                                                        );
                                                        // Empty records - always advance offset to make progress
                                                        // If we're behind log_start_offset, jump to it; otherwise increment
                                                        if offsets[local_partition_idx]
                                                            < partition_response.log_start_offset
                                                        {
                                                            offsets[local_partition_idx] =
                                                                partition_response.log_start_offset;
                                                        } else if offsets[local_partition_idx]
                                                            < partition_response.high_watermark
                                                        {
                                                            // If there should be messages but we got empty response, advance to high watermark
                                                            offsets[local_partition_idx] =
                                                                partition_response.high_watermark;
                                                        } else {
                                                            // At or past high watermark, just advance by 1 to keep polling for new messages
                                                            //offsets[local_partition_idx] += 1;
                                                        }
                                                    }
                                                } else {
                                                    debug!(
                                                        "No records field in partition {}",
                                                        partition_id
                                                    );
                                                    // No records field - always advance to make progress
                                                    if offsets[local_partition_idx]
                                                        < partition_response.log_start_offset
                                                    {
                                                        offsets[local_partition_idx] =
                                                            partition_response.log_start_offset;
                                                    } else if offsets[local_partition_idx]
                                                        < partition_response.high_watermark
                                                    {
                                                        offsets[local_partition_idx] =
                                                            partition_response.high_watermark;
                                                    } else {
                                                        offsets[local_partition_idx] += 1;
                                                    }
                                                }
                                            } else if local_partition_idx < offsets.len() {
                                                if partition_response.error_code != 0 {
                                                    debug!(
                                                        "Partition {} has error code: {}",
                                                        partition_id, partition_response.error_code
                                                    );
                                                }
                                                // Error case or invalid partition - still advance to avoid getting stuck
                                                offsets[local_partition_idx] += 1;
                                            }
                                        }
                                    }

                                    debug!("Total messages found: {}", total_messages);
                                    // Update metrics with actual message count
                                    if total_messages > 0 {
                                        messages_received
                                            .fetch_add(total_messages, Ordering::Relaxed);
                                    }
                                }
                                Err(e) => {
                                    error!("Failed to decode fetch response payload: {}", e);
                                    // On decode error, still advance offsets minimally to avoid infinite loop
                                    for offset in &mut offsets {
                                        *offset += 1;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to decode response header: {}", e);
                            // On header decode error, still advance offsets minimally to avoid infinite loop
                            for offset in &mut offsets {
                                *offset += 1;
                            }
                        }
                    }
                }
                Ok(Err(request_err)) => {
                    error!(
                        "Consumer thread {} failed to fetch messages: {}",
                        self.thread_id, request_err
                    );
                    // Brief pause before retrying to avoid overwhelming broker with failed requests
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    // Yield to allow other tasks to continue processing
                    tokio::task::yield_now().await;
                }
                Err(_timeout_elapsed) => {
                    error!(
                        "Consumer thread {} fetch request timed out after 5s",
                        self.thread_id
                    );
                    // Brief pause before retrying to avoid overwhelming broker with failed requests
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    // Yield to allow other tasks to continue processing
                    tokio::task::yield_now().await;
                }
            }

            // Track fetch attempts and log periodically to detect stalling
            fetch_count += 1;
            if Instant::now().duration_since(last_log_time) > Duration::from_secs(5) {
                let current_messages = messages_received.load(Ordering::Relaxed);
                debug!(
                    "Consumer thread {}: {} fetch attempts, {} messages received, current offsets: {:?}",
                    self.thread_id, fetch_count, current_messages, offsets
                );
                last_log_time = Instant::now();
                fetch_count = 0;
            }

            // Yield control to allow other tasks to run (cooperative multitasking)
            tokio::task::yield_now().await;

            debug!(
                "Consumer thread {} completed loop iteration",
                self.thread_id
            );
        }

        debug!(
            "Consumer thread {} exiting after duration completed",
            self.thread_id
        );

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
        let mut current_rate = 0.0f64;
        let mut min_rate = f64::MAX; // Initialize to maximum to find true minimum
        let mut max_rate = 0.0f64;

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

                    // Calculate current backlog
                    let current_sent = self.messages_sent.load(Ordering::Relaxed);
                    let current_received = self.messages_received.load(Ordering::Relaxed);
                    let backlog = current_sent.saturating_sub(current_received);

                    // Build status string for display
                    let backlog_percentage = (backlog as f64 / MAX_BACKLOG as f64 * 100.0).min(100.0);
                    let status = format!(
                        "{:.0} msg/s (min: {:.0}, max: {:.0}, backlog: {:.1}%)",
                        current_rate,
                        if min_rate > 1e9 { 0.0 } else { min_rate },
                        max_rate,
                        backlog_percentage
                    );

                    // Update the visual display with current metrics
                    self.animator.draw_frame(position, direction, &status);
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
    let base_threads = args.threads.unwrap_or(4) as usize;

    // Calculate total partitions based on the LCM of partitions per thread to ensure integer thread counts
    let lcm_partitions_per_thread = lcm(
        args.producer_partitions_per_thread as usize,
        args.consumer_partitions_per_thread as usize,
    );
    let total_partitions = (lcm_partitions_per_thread * base_threads) as i32;

    // Calculate number of threads needed for each role (now guaranteed to be integers)
    let num_producer_threads =
        total_partitions as usize / args.producer_partitions_per_thread as usize;
    let num_consumer_threads =
        total_partitions as usize / args.consumer_partitions_per_thread as usize;

    println!("{}", include_str!("logo.txt"));

    if num_producer_threads == 0 {
        return Err(anyhow!("Thread count must be at least 1"));
    }

    info!("Starting Kitt - Kafka Implementation Throughput Tool");
    info!(
        "Broker: {}, Producer partitions per thread: {}, Consumer partitions per thread: {}, LCM partitions per thread: {}, Base threads: {}, Total partitions: {}, Producer threads: {}, Consumer threads: {} (calculated to cover all partitions), message size: {:?}",
        args.broker, args.producer_partitions_per_thread, args.consumer_partitions_per_thread, lcm_partitions_per_thread, base_threads, total_partitions, num_producer_threads, num_consumer_threads, message_size
    );
    info!("Running for: {}s", args.duration_secs);

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

    // Create producer clients
    for i in 0..num_producer_threads {
        let producer_client = Arc::new(
            KafkaClient::connect_with_versions(&args.broker, api_versions.clone())
                .await
                .map_err(|e| anyhow!("Failed to connect producer client {}: {}", i, e))?,
        );
        producer_clients.push(producer_client);
    }

    // Create consumer clients
    for i in 0..num_consumer_threads {
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
    let measurement_duration = Duration::from_secs(args.duration_secs);

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

#[cfg(test)]
mod tests {
    use super::lcm;

    #[test]
    fn test_partition_and_thread_calculation() {
        // Test case 1: Equal partitions per thread
        let producer_partitions_per_thread = 2;
        let consumer_partitions_per_thread = 2;
        let base_threads = 4;

        let lcm_partitions_per_thread = lcm(
            producer_partitions_per_thread,
            consumer_partitions_per_thread,
        );
        let total_partitions = lcm_partitions_per_thread * base_threads;
        let num_producer_threads = total_partitions / producer_partitions_per_thread;
        let num_consumer_threads = total_partitions / consumer_partitions_per_thread;

        assert_eq!(total_partitions, 8); // lcm(2,2) * 4 = 2 * 4 = 8
        assert_eq!(num_producer_threads, 4); // 8/2 = 4
        assert_eq!(num_consumer_threads, 4); // 8/2 = 4

        // Test case 2: Producer has more partitions per thread
        let producer_partitions_per_thread = 3;
        let consumer_partitions_per_thread = 2;
        let base_threads = 4;

        let lcm_partitions_per_thread = lcm(
            producer_partitions_per_thread,
            consumer_partitions_per_thread,
        );
        let total_partitions = lcm_partitions_per_thread * base_threads;
        let num_producer_threads = total_partitions / producer_partitions_per_thread;
        let num_consumer_threads = total_partitions / consumer_partitions_per_thread;

        assert_eq!(total_partitions, 24); // lcm(3,2) * 4 = 6 * 4 = 24
        assert_eq!(num_producer_threads, 8); // 24/3 = 8
        assert_eq!(num_consumer_threads, 12); // 24/2 = 12

        // Test case 3: Consumer has more partitions per thread
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

        // Verify all partitions are covered exactly (no ceiling needed)
        assert_eq!(num_producer_threads * producer_partitions_per_thread, 24);
        assert_eq!(num_consumer_threads * consumer_partitions_per_thread, 24);

        // Test case 4: More complex LCM case
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
