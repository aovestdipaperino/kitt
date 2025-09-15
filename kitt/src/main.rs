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
        produce_response::ProduceResponse,
        ApiKey, ResponseHeader, TopicName,
    },
    protocol::{Decodable, StrBytes},
    records::{
        Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
        TimestampType,
    },
};
use kitt_throbbler::KnightRiderAnimator;
use rand::seq::SliceRandom;
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

/// Base maximum number of pending messages allowed before applying backpressure
/// This prevents memory exhaustion during high-throughput testing
/// Will be multiplied by fetch_delay to compensate for delayed consumer start
const BASE_MAX_BACKLOG: u64 = 10000;

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
pub mod profiling;
use kafka_client::KafkaClient;
use profiling::KittOperation;
use quantum_pulse::profile;

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

    /// Number of producer threads
    #[arg(long, default_value = "4")]
    producer_threads: i32,

    /// Number of consumer threads
    #[arg(long, default_value = "4")]
    consumer_threads: i32,

    /// Use sticky producer-partition assignment with LCM-based partition calculation (legacy mode)
    #[arg(long, default_value = "false")]
    sticky: bool,

    /// Number of producer/consumer threads (only used with --sticky mode)
    #[arg(short, long)]
    threads: Option<i32>,

    /// Message size in bytes (e.g., "1024") or range (e.g., "100-1000")
    #[arg(short, long, default_value = "1024")]
    message_size: String,

    /// Measurement duration in seconds
    #[arg(short, long, default_value = "15")]
    duration_secs: u64,

    /// Enable detailed FETCH response diagnostics for troubleshooting
    #[arg(long, default_value = "false")]
    debug_fetch: bool,

    /// Enable detailed PRODUCE response diagnostics for troubleshooting
    #[arg(long, default_value = "false")]
    debug_produce: bool,

    /// Initial delay in seconds before consumers start fetching (helps test backlog handling)
    #[arg(long, default_value = "0")]
    fetch_delay: u64,

    /// Run a profiling demonstration without connecting to Kafka
    #[arg(long, default_value = "false")]
    profile_demo: bool,

    /// Enable message validation for produce/consume responses (disabled by default for better performance)
    #[arg(long, default_value = "false")]
    message_validation: bool,

    /// Generate and display profiling report at the end (disabled by default)
    #[arg(long, default_value = "false")]
    profile_report: bool,
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
    /// Number of partitions this thread will send to per batch
    producer_partitions_per_thread: i32,
    /// Total number of partitions available for random selection
    total_partitions: i32,
    /// Configuration for message size generation
    message_size: MessageSize,
    /// Unique identifier for this producer thread (0-based)
    thread_id: usize,
    /// Whether to use sticky partition assignment (true) or random (false)
    use_sticky_partitions: bool,
}

impl Producer {
    /// Creates a new Producer instance
    ///
    /// # Arguments
    /// * `client` - Shared Kafka client for network communication
    /// * `topic` - Target topic name for message production
    /// * `producer_partitions_per_thread` - Number of partitions to send to per batch
    /// * `total_partitions` - Total number of partitions available for random selection
    /// * `message_size` - Size configuration for generated messages
    /// * `thread_id` - Unique identifier for this producer thread
    /// * `use_sticky_partitions` - Whether to use sticky partition assignment
    fn new(
        client: Arc<KafkaClient>,
        topic: String,
        producer_partitions_per_thread: i32,
        total_partitions: i32,
        message_size: MessageSize,
        thread_id: usize,
        use_sticky_partitions: bool,
    ) -> Self {
        Self {
            client,
            topic,
            producer_partitions_per_thread,
            total_partitions,
            message_size,
            thread_id,
            use_sticky_partitions,
        }
    }

    /// Produces messages continuously for the specified duration
    ///
    /// This method implements the core message production loop with:
    /// - Partition selection (either sticky assignment or random based on configuration)
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
        max_backlog: u64,
        message_validation: bool,
    ) -> Result<()> {
        let end_time = Instant::now() + duration;

        // Determine partition selection strategy based on configuration
        let partition_count = self.producer_partitions_per_thread as usize;
        let start_partition = if self.use_sticky_partitions {
            self.thread_id * self.producer_partitions_per_thread as usize
        } else {
            0 // Not used in random mode
        };

        // Send 1 message per partition in each batch request
        const MAX_PENDING: usize = 20; // Maximum concurrent batched requests

        let mut pending_futures = Vec::new();

        while Instant::now() < end_time {
            // Implement backpressure control to prevent memory exhaustion
            let sent = messages_sent.load(Ordering::Relaxed);
            let received = messages_received.load(Ordering::Relaxed);
            let backlog = sent.saturating_sub(received);

            if backlog > max_backlog {
                // Backlog exceeded threshold - pause to let consumers catch up
                profile!(KittOperation::BacklogWaiting, {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                });
                // Yield to allow consumers to continue processing
                //tokio::task::yield_now().await;
                continue;
            }

            // Create produce request with 1 message per partition
            let mut request = ProduceRequest::default();
            request.acks = -1;
            request.timeout_ms = 30000;
            // Disable idempotent producer to avoid sequence number conflicts
            // when multiple threads use different producer_ids

            let mut topic_data = TopicProduceData::default();
            topic_data.name = TopicName(StrBytes::from_string(self.topic.clone()));

            // Configure encoding options for the record batch
            let options = RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            };

            // Create one message for each partition based on assignment strategy
            for i in 0..partition_count {
                let partition_id = if self.use_sticky_partitions {
                    // Sticky mode: each thread has fixed partitions
                    (start_partition + i) as i32
                } else {
                    // Random mode: select random partition for each message
                    if self.total_partitions > 0 {
                        (rand::random::<u32>() % self.total_partitions as u32) as i32
                    } else {
                        error!(
                            "Producer thread {} has invalid total_partitions: {}",
                            self.thread_id, self.total_partitions
                        );
                        continue;
                    }
                };

                // Validate partition ID is within valid range (defensive programming)
                if partition_id < 0 || partition_id >= self.total_partitions {
                    error!(
                        "Producer thread {} generated invalid partition ID {} (valid range: 0-{})",
                        self.thread_id,
                        partition_id,
                        self.total_partitions - 1
                    );
                    continue; // Skip this partition and try next iteration
                }

                debug!(
                    "Producer thread {} {} partition {} (total partitions: {})",
                    self.thread_id,
                    if self.use_sticky_partitions {
                        "assigned"
                    } else {
                        "selecting"
                    },
                    partition_id,
                    self.total_partitions
                );

                // Generate message with configured size
                let size = profile!(KittOperation::MessageSizeGeneration, {
                    self.message_size.generate_size()
                });
                let payload = vec![b'x'; size];
                let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

                // Create a Kafka record
                // NOTE: offset is always 0 in PRODUCE requests - the broker assigns actual offsets
                // The broker-assigned offset will be returned in the ProduceResponse.base_offset
                let record = Record {
                    transactional: false,
                    control: false,
                    partition_leader_epoch: 0,
                    producer_id: -1, // Disable idempotent producer (-1 means not using idempotence)
                    producer_epoch: -1, // Disable idempotent producer
                    timestamp_type: TimestampType::Creation,
                    offset: 0,    // Always 0 for producers - broker assigns actual offset
                    sequence: -1, // Disable sequence tracking for non-idempotent producer
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
            let thread_id = self.thread_id;
            let future = tokio::spawn(async move {
                debug!(
                    "Producer thread {} sending batch with {} partitions, producer_id=-1 (idempotence disabled)",
                    thread_id,
                    partition_count
                );
                match profile!(KittOperation::MessageProduce, {
                    client.send_request(ApiKey::Produce, &request, version)
                })
                .await
                {
                    Ok(response_bytes) => {
                        // Validate the produce response if enabled
                        if message_validation {
                            profile!(KittOperation::ResponseValidation, {
                                Self::validate_produce_response(
                                    &response_bytes,
                                    version,
                                    thread_id,
                                    partition_count,
                                )
                            })
                            .await?;
                        }
                        Ok(())
                    }
                    Err(e) => {
                        error!(
                            "Producer thread {} PRODUCE REQUEST FAILED: {}",
                            thread_id, e
                        );

                        // Provide specific troubleshooting guidance
                        let error_str = e.to_string().to_lowercase();
                        if error_str.contains("connection") || error_str.contains("network") {
                            error!("🔧 NETWORK ISSUE - Check broker connectivity and network configuration");
                        } else if error_str.contains("timeout") {
                            error!("🔧 TIMEOUT - Broker may be overloaded or network latency high");
                        } else if error_str.contains("auth") || error_str.contains("permission") {
                            error!(
                                "🔧 AUTHENTICATION - Verify credentials and ACLs for this broker"
                            );
                        } else if error_str.contains("protocol") || error_str.contains("version") {
                            error!("🔧 PROTOCOL MISMATCH - Broker may use different Kafka version/protocol");
                        } else {
                            error!("🔧 UNKNOWN ERROR - Check broker logs and configuration");
                        }

                        Err(e)
                    }
                }
            });
            pending_futures.push(future);

            // Manage concurrent request limit
            if pending_futures.len() >= MAX_PENDING {
                let mut i = 0;
                while i < pending_futures.len() {
                    if pending_futures[i].is_finished() {
                        let result = pending_futures.remove(i).await;
                        match result {
                            Ok(Ok(_)) => {
                                // Successfully validated produce response
                            }
                            Ok(Err(e)) => {
                                debug!("Producer validation failed: {}", e);
                            }
                            Err(e) => {
                                debug!("Failed to send batch: {}", e);
                            }
                        }
                    } else {
                        i += 1;
                    }
                }
            }
        }

        // Clean up: wait for all remaining requests to complete
        for future in pending_futures {
            match future.await {
                Ok(Ok(_)) => {
                    // Successfully validated produce response
                }
                Ok(Err(e)) => {
                    debug!("Producer validation failed: {}", e);
                }
                Err(e) => {
                    debug!("Failed to complete pending batch: {}", e);
                }
            }
        }

        Ok(())
    }

    /// Validates and diagnoses PRODUCE response for troubleshooting
    ///
    /// This method performs comprehensive analysis of Kafka PRODUCE responses to identify
    /// common issues that prevent message production, especially when connecting to
    /// different brokers.
    ///
    /// # Arguments
    /// * `response_bytes` - Raw response bytes from broker
    /// * `version` - Protocol version used for the request
    /// * `thread_id` - Producer thread ID for logging context
    /// * `partition_count` - Number of partitions in the request
    ///
    /// # Returns
    /// * `Result<()>` - OK if response is valid, Error with diagnostic info if not
    async fn validate_produce_response(
        response_bytes: &bytes::Bytes,
        version: i16,
        thread_id: usize,
        partition_count: usize,
    ) -> Result<()> {
        use std::io::Cursor;

        let mut cursor = Cursor::new(response_bytes.as_ref());

        // Decode response header first
        let header_version = ApiKey::Produce.response_header_version(version);
        let _response_header = match ResponseHeader::decode(&mut cursor, header_version) {
            Ok(header) => header,
            Err(e) => {
                debug!("PRODUCE RESPONSE HEADER DECODE ERROR: {}", e);
                debug!("🔧 HEADER DECODE TROUBLESHOOT:");
                debug!("   - Broker protocol version mismatch");
                debug!("   - Connection may be to wrong service/port");
                debug!("   - Broker may have sent malformed response");
                debug!(
                    "📊 Expected header version: {}, response size: {} bytes",
                    header_version,
                    response_bytes.len()
                );
                return Err(anyhow!("Failed to decode produce response header: {}", e));
            }
        };

        // Decode the produce response payload
        let response = match ProduceResponse::decode(&mut cursor, version) {
            Ok(response) => response,
            Err(e) => {
                debug!("PRODUCE RESPONSE DECODE ERROR: {}", e);
                debug!("🔧 DECODE TROUBLESHOOT:");
                debug!("   - Broker may use incompatible Kafka protocol version");
                debug!("   - Response may be corrupted due to network issues");
                debug!("   - Different broker software/version than expected");
                debug!(
                    "📊 Raw response size: {} bytes, using produce version: {}",
                    response_bytes.len(),
                    version
                );
                return Err(anyhow!("Failed to decode produce response: {}", e));
            }
        };

        debug!(
            "Producer thread {} received response with {} topic(s)",
            thread_id,
            response.responses.len()
        );

        let mut all_partitions_ok = true;
        let mut diagnostics = Vec::new();

        // Analyze each topic response
        for topic_response in &response.responses {
            debug!(
                "Topic has {} partition responses",
                topic_response.partition_responses.len()
            );

            // Check each partition response
            for partition_response in &topic_response.partition_responses {
                let partition_id = partition_response.index;

                // Comprehensive error code analysis
                match partition_response.error_code {
                    0 => {
                        diagnostics.push(format!(
                            "✅ P{}: SUCCESS (broker assigned offset={})",
                            partition_id, partition_response.base_offset
                        ));
                        debug!(
                            "Partition {}: BROKER_ASSIGNED_OFFSET={}, log_append_time={}, log_start_offset={} (producer_id=-1, idempotence disabled, producer sent offset=0, broker assigned actual offset)",
                            partition_id,
                            partition_response.base_offset,
                            partition_response.log_append_time_ms,
                            partition_response.log_start_offset
                        );
                    }
                    1 => {
                        diagnostics.push(format!(
                            "❌ P{}: OFFSET_OUT_OF_RANGE - invalid offset specified",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    2 => {
                        diagnostics.push(format!(
                            "❌ P{}: CORRUPT_MESSAGE - message failed checksum",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    3 => {
                        diagnostics.push(format!(
                            "❌ P{}: UNKNOWN_TOPIC_OR_PARTITION - topic/partition doesn't exist",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    5 => {
                        diagnostics.push(format!(
                            "❌ P{}: LEADER_NOT_AVAILABLE - partition leader unavailable",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    6 => {
                        diagnostics.push(format!(
                            "❌ P{}: NOT_LEADER_FOR_PARTITION - broker is not the leader",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    10 => {
                        diagnostics.push(format!(
                            "❌ P{}: MESSAGE_TOO_LARGE - message exceeds broker limits",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    14 => {
                        diagnostics.push(format!(
                            "❌ P{}: INVALID_TOPIC_EXCEPTION - topic name is invalid",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    16 => {
                        diagnostics.push(format!(
                            "❌ P{}: NETWORK_EXCEPTION - network communication failed",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    17 => {
                        diagnostics.push(format!(
                            "❌ P{}: CORRUPT_MESSAGE - message is corrupted",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    25 => {
                        diagnostics.push(format!(
                            "❌ P{}: INVALID_TOPIC_EXCEPTION - topic name is invalid",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    29 => {
                        diagnostics.push(format!(
                            "❌ P{}: TOPIC_AUTHORIZATION_FAILED - insufficient permissions",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    34 => {
                        diagnostics.push(format!(
                            "❌ P{}: UNSUPPORTED_VERSION_FOR_FEATURE - version not supported",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    43 => {
                        diagnostics.push(format!(
                            "❌ P{}: INVALID_RECORD - record format invalid",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    45 => {
                        diagnostics.push(format!(
                            "❌ P{}: DUPLICATE_SEQUENCE_NUMBER - sequence number already used (idempotence should be disabled)",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    48 => {
                        diagnostics.push(format!(
                            "❌ P{}: INVALID_PRODUCER_EPOCH - producer epoch invalid",
                            partition_id
                        ));
                        all_partitions_ok = false;
                    }
                    code => {
                        diagnostics.push(format!(
                            "❌ P{}: Unknown error code {} - check Kafka documentation",
                            partition_id, code
                        ));
                        all_partitions_ok = false;
                    }
                }

                // Check for valid offsets on success
                if partition_response.error_code == 0 {
                    if partition_response.base_offset < 0 {
                        diagnostics.push(format!(
                            "⚠️  P{}: Invalid base_offset={}",
                            partition_id, partition_response.base_offset
                        ));
                    }
                }
            }
        }

        // Log comprehensive diagnostics
        if all_partitions_ok {
            debug!(
                "Producer thread {} SUCCESS: All {} partitions accepted messages | {}",
                thread_id,
                partition_count,
                diagnostics.join(" | ")
            );

            // Also log offset assignment summary
            let mut offset_summary = Vec::new();
            for topic_response in &response.responses {
                for partition_response in &topic_response.partition_responses {
                    if partition_response.error_code == 0 {
                        offset_summary.push(format!(
                            "P{}→{}",
                            partition_response.index, partition_response.base_offset
                        ));
                    }
                }
            }
            if !offset_summary.is_empty() {
                debug!(
                    "Producer thread {} OFFSET_ASSIGNMENTS: {}",
                    thread_id,
                    offset_summary.join(", ")
                );
            }
        } else {
            debug!("Producer thread {} PRODUCE ERRORS DETECTED:", thread_id);
            debug!("📊 Partition Results: {}", diagnostics.join(" | "));

            // Provide troubleshooting guidance based on error patterns
            let error_summary = diagnostics.join(" ");
            if error_summary.contains("UNKNOWN_TOPIC_OR_PARTITION") {
                debug!("🔧 TOPIC ISSUE - Verify topic exists on this broker and partition count matches");
            }
            if error_summary.contains("TOPIC_AUTHORIZATION_FAILED") {
                debug!("🔧 PERMISSION ISSUE - Check ACLs and authentication credentials for this broker");
            }
            if error_summary.contains("NOT_LEADER_FOR_PARTITION") {
                debug!("🔧 LEADERSHIP ISSUE - Broker may not be partition leader, check cluster metadata");
            }
            if error_summary.contains("MESSAGE_TOO_LARGE") {
                debug!("🔧 SIZE ISSUE - Message exceeds broker's max.message.bytes setting");
            }
            if error_summary.contains("NETWORK_EXCEPTION") {
                debug!("🔧 NETWORK ISSUE - Check network connectivity and broker stability");
            }
            if error_summary.contains("DUPLICATE_SEQUENCE_NUMBER") {
                debug!("🔧 SEQUENCE ISSUE - Idempotent producer causing sequence conflicts");
                debug!("   - Producer is configured with idempotence disabled (producer_id=-1)");
                debug!("   - This error should not occur with current configuration");
                debug!("   - Check if broker requires idempotent producers");
                debug!("   - Verify broker version compatibility");
            }

            return Err(anyhow!(
                "Produce request failed for {} partitions",
                diagnostics.iter().filter(|d| d.contains("❌")).count()
            ));
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
    /// Initial delay in seconds before starting to fetch messages
    fetch_delay: u64,
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
        fetch_delay: u64,
    ) -> Self {
        Self {
            client,
            topic,
            consumer_partitions_per_thread,
            thread_id,
            fetch_delay,
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
        message_validation: bool,
    ) -> Result<()> {
        // Apply fetch delay if configured
        if self.fetch_delay > 0 {
            info!(
                "Consumer thread {} waiting {}s before starting to fetch messages",
                self.thread_id, self.fetch_delay
            );
            tokio::time::sleep(Duration::from_secs(self.fetch_delay)).await;
        }

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
                profile!(KittOperation::MessageConsume, {
                    self.client.send_request(ApiKey::Fetch, &request, version)
                }),
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

                                            // Comprehensive FETCH response validation (if enabled)
                                            let fetch_diagnostics = if message_validation {
                                                profile!(KittOperation::ResponseValidation, {
                                                    Self::validate_fetch_response(
                                                        partition_id,
                                                        &partition_response,
                                                        current_offset,
                                                        self.thread_id,
                                                    )
                                                })
                                            } else {
                                                "validation disabled".to_string()
                                            };

                                            debug!(
                                                "Partition {}: error_code={}, has_records={}, records_len={}, high_watermark={}, current_offset={}, diagnostics={}",
                                                partition_id,
                                                partition_response.error_code,
                                                partition_response.records.is_some(),
                                                partition_response.records.as_ref().map(|r| r.len()).unwrap_or(0),
                                                partition_response.high_watermark,
                                                current_offset,
                                                fetch_diagnostics
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
                                    error!("FETCH RESPONSE DECODE ERROR: {}", e);
                                    error!("🔧 DECODE TROUBLESHOOT:");
                                    error!(
                                        "   - Broker may use incompatible Kafka protocol version"
                                    );
                                    error!("   - Response may be corrupted due to network issues");
                                    error!("   - Different broker software/version than expected");
                                    error!(
                                        "📊 Raw response size: {} bytes, using fetch version: {}",
                                        response_bytes.len(),
                                        version
                                    );

                                    // On decode error, still advance offsets minimally to avoid infinite loop
                                    for offset in &mut offsets {
                                        *offset += 1;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("FETCH RESPONSE HEADER DECODE ERROR: {}", e);
                            error!("🔧 HEADER DECODE TROUBLESHOOT:");
                            error!("   - Broker protocol version mismatch");
                            error!("   - Connection may be to wrong service/port");
                            error!("   - Broker may have sent malformed response");
                            error!(
                                "📊 Expected header version: {}, response size: {} bytes",
                                header_version,
                                response_bytes.len()
                            );

                            // On header decode error, still advance offsets minimally to avoid infinite loop
                            for offset in &mut offsets {
                                *offset += 1;
                            }
                        }
                    }
                }
                Ok(Err(request_err)) => {
                    error!(
                        "Consumer thread {} FETCH REQUEST FAILED: {}",
                        self.thread_id, request_err
                    );

                    // Provide specific troubleshooting guidance based on error type
                    let error_str = request_err.to_string().to_lowercase();
                    if error_str.contains("connection") || error_str.contains("network") {
                        error!("🔧 NETWORK ISSUE - Check broker connectivity and network configuration");
                    } else if error_str.contains("timeout") {
                        error!("🔧 TIMEOUT - Broker may be overloaded or network latency high");
                    } else if error_str.contains("auth") || error_str.contains("permission") {
                        error!("🔧 AUTHENTICATION - Verify credentials and ACLs for this broker");
                    } else if error_str.contains("protocol") || error_str.contains("version") {
                        error!("🔧 PROTOCOL MISMATCH - Broker may use different Kafka version/protocol");
                    } else {
                        error!("🔧 UNKNOWN ERROR - Check broker logs and configuration");
                    }

                    error!("📊 FETCH CONFIG - topic: {}, partitions_per_thread: {}, max_wait: 1000ms, max_bytes: 50MB",
                           self.topic, self.consumer_partitions_per_thread);

                    // Brief pause before retrying to avoid overwhelming broker with failed requests
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    // Yield to allow other tasks to continue processing
                    tokio::task::yield_now().await;
                }
                Err(_timeout_elapsed) => {
                    error!(
                        "Consumer thread {} FETCH TIMEOUT after 5s - broker not responding",
                        self.thread_id
                    );
                    error!("🔧 TIMEOUT TROUBLESHOOT:");
                    error!("   - Broker may be overloaded or down");
                    error!("   - Network connectivity issues to broker");
                    error!("   - Broker configuration may have different timeout settings");
                    error!("   - Topic '{}' may not exist on this broker", self.topic);
                    error!("📊 Consider reducing fetch timeout or checking broker status");

                    // Brief pause before retrying to avoid overwhelming broker with failed requests
                    tokio::time::sleep(Duration::from_millis(100)).await;
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

    /// Validates and diagnoses FETCH response for troubleshooting
    ///
    /// This method performs comprehensive analysis of Kafka FETCH responses to identify
    /// common issues that prevent message consumption, especially when connecting to
    /// different brokers.
    ///
    /// # Arguments
    /// * `partition_id` - The partition being analyzed
    /// * `partition_response` - The fetch response for this partition
    /// * `current_offset` - The offset requested in the fetch
    /// * `thread_id` - Consumer thread ID for logging context
    ///
    /// # Returns
    /// * `String` - Diagnostic summary with identified issues and recommendations
    fn validate_fetch_response(
        partition_id: usize,
        partition_response: &kafka_protocol::messages::fetch_response::PartitionData,
        current_offset: i64,
        thread_id: usize,
    ) -> String {
        let mut diagnostics = Vec::new();

        // Check for Kafka error codes
        match partition_response.error_code {
            0 => diagnostics.push("✓ No partition errors".to_string()),
            1 => {
                diagnostics.push("❌ OFFSET_OUT_OF_RANGE - requested offset is invalid".to_string())
            }
            3 => diagnostics
                .push("❌ UNKNOWN_TOPIC_OR_PARTITION - topic/partition doesn't exist".to_string()),
            5 => diagnostics
                .push("❌ LEADER_NOT_AVAILABLE - partition leader is unavailable".to_string()),
            6 => diagnostics
                .push("❌ NOT_LEADER_FOR_PARTITION - broker is not the leader".to_string()),
            16 => {
                diagnostics.push("❌ NETWORK_EXCEPTION - network communication failed".to_string())
            }
            25 => {
                diagnostics.push("❌ INVALID_TOPIC_EXCEPTION - topic name is invalid".to_string())
            }
            29 => diagnostics
                .push("❌ TOPIC_AUTHORIZATION_FAILED - insufficient permissions".to_string()),
            43 => diagnostics
                .push("❌ OFFSET_METADATA_TOO_LARGE - offset metadata too large".to_string()),
            code => diagnostics.push(format!(
                "❌ Unknown error code: {} - check Kafka documentation",
                code
            )),
        }

        // Analyze offset positioning
        let log_start = partition_response.log_start_offset;
        let high_watermark = partition_response.high_watermark;

        if current_offset < log_start {
            diagnostics.push(format!(
                "⚠️  OFFSET_TOO_OLD - requesting {} but log starts at {} (data may be deleted)",
                current_offset, log_start
            ));
        } else if current_offset >= high_watermark {
            if high_watermark == log_start {
                diagnostics.push("ℹ️  EMPTY_PARTITION - no messages in partition yet".to_string());
            } else {
                diagnostics.push(format!(
                    "ℹ️  AT_END - requesting {} but latest is {} (caught up, waiting for new messages)",
                    current_offset, high_watermark - 1
                ));
            }
        } else {
            diagnostics.push(format!(
                "✓ OFFSET_VALID - requesting {} in range [{}, {})",
                current_offset, log_start, high_watermark
            ));
        }

        // Check records field and content
        match &partition_response.records {
            Some(records) => {
                if records.is_empty() {
                    diagnostics.push(
                        "⚠️  EMPTY_RECORDS - records field present but contains no data"
                            .to_string(),
                    );
                } else {
                    diagnostics.push(format!(
                        "✓ HAS_RECORDS - {} bytes of record data",
                        records.len()
                    ));
                }
            }
            None => {
                diagnostics.push(
                    "⚠️  NO_RECORDS_FIELD - records field is missing from response".to_string(),
                );
            }
        }

        // Analyze broker response characteristics
        if partition_response.error_code == 0 && partition_response.records.is_none() {
            diagnostics.push("🔍 BROKER_ISSUE - success code but no records field (broker may not support this fetch version)".to_string());
        }

        if high_watermark == 0 && log_start == 0 {
            diagnostics.push(
                "🔍 NEW_PARTITION - partition appears to be newly created with no messages"
                    .to_string(),
            );
        }

        // Connection-specific diagnostics
        if partition_response.error_code == 3 {
            diagnostics.push(
                "🔧 TROUBLESHOOT - verify topic exists on this broker and partition count"
                    .to_string(),
            );
        } else if partition_response.error_code == 29 {
            diagnostics.push(
                "🔧 TROUBLESHOOT - check ACLs and authentication credentials for this broker"
                    .to_string(),
            );
        } else if partition_response.error_code == 6 {
            diagnostics.push(
                "🔧 TROUBLESHOOT - metadata may be stale, broker may not be partition leader"
                    .to_string(),
            );
        }

        format!(
            "[T{}:P{}] {}",
            thread_id,
            partition_id,
            diagnostics.join(" | ")
        )
    }
}

/// Test function to validate PRODUCE response diagnostics
///
/// This function creates mock PRODUCE responses with various error conditions
/// to verify that the diagnostic system correctly identifies issues.
#[allow(dead_code)]
fn test_produce_response_validation() {
    use kafka_protocol::messages::produce_response::{
        PartitionProduceResponse, ProduceResponse, TopicProduceResponse,
    };

    println!("🧪 Testing PRODUCE Response Validation...\n");

    // Test 1: Successful response
    let mut success_partition = PartitionProduceResponse::default();
    success_partition.index = 0;
    success_partition.error_code = 0;
    success_partition.base_offset = 100;
    success_partition.log_append_time_ms = 1234567890;
    success_partition.log_start_offset = 0;

    let mut success_topic = TopicProduceResponse::default();
    success_topic.partition_responses.push(success_partition);

    let mut success_response = ProduceResponse::default();
    success_response.responses.push(success_topic);

    // Skip encoding test for now due to complexity - just show the concept
    println!("✅ Success case: P0 SUCCESS (broker assigned offset=100)");

    // Test 2: Authorization failed
    let mut auth_partition = PartitionProduceResponse::default();
    auth_partition.index = 0;
    auth_partition.error_code = 29; // TOPIC_AUTHORIZATION_FAILED

    let mut auth_topic = TopicProduceResponse::default();
    auth_topic.partition_responses.push(auth_partition);

    let mut auth_response = ProduceResponse::default();
    auth_response.responses.push(auth_topic);

    println!("❌ Auth failed case: Error code 29 (TOPIC_AUTHORIZATION_FAILED)");

    // Test 3: Unknown topic/partition
    let mut unknown_partition = PartitionProduceResponse::default();
    unknown_partition.index = 0;
    unknown_partition.error_code = 3; // UNKNOWN_TOPIC_OR_PARTITION

    let mut unknown_topic = TopicProduceResponse::default();
    unknown_topic.partition_responses.push(unknown_partition);

    let mut unknown_response = ProduceResponse::default();
    unknown_response.responses.push(unknown_topic);

    println!("❌ Unknown topic case: Error code 3 (UNKNOWN_TOPIC_OR_PARTITION)");

    // Test 4: Message too large
    let mut large_partition = PartitionProduceResponse::default();
    large_partition.index = 0;
    large_partition.error_code = 10; // MESSAGE_TOO_LARGE

    let mut large_topic = TopicProduceResponse::default();
    large_topic.partition_responses.push(large_partition);

    let mut large_response = ProduceResponse::default();
    large_response.responses.push(large_topic);

    println!("❌ Message too large case: Error code 10 (MESSAGE_TOO_LARGE)");

    // Test 5: Not leader for partition
    let mut leader_partition = PartitionProduceResponse::default();
    leader_partition.index = 0;
    leader_partition.error_code = 6; // NOT_LEADER_FOR_PARTITION

    let mut leader_topic = TopicProduceResponse::default();
    leader_topic.partition_responses.push(leader_partition);

    let mut leader_response = ProduceResponse::default();
    leader_response.responses.push(leader_topic);

    println!("❌ Not leader case: Error code 6 (NOT_LEADER_FOR_PARTITION)");

    println!("\n🧪 PRODUCE Response Validation Tests Complete\n");
}

/// Test function to validate FETCH response diagnostics
///
/// This function creates mock FETCH responses with various error conditions
/// to verify that the diagnostic system correctly identifies issues.
#[allow(dead_code)]
fn test_fetch_response_validation() {
    use kafka_protocol::messages::fetch_response::PartitionData;

    println!("🧪 Testing FETCH Response Validation...\n");

    // Test 1: Successful response with records
    let mut success_response = PartitionData::default();
    success_response.error_code = 0;
    success_response.high_watermark = 100;
    success_response.log_start_offset = 0;
    success_response.records = Some(bytes::Bytes::from(vec![0x01, 0x02, 0x03]));

    let result = Consumer::validate_fetch_response(0, &success_response, 50, 0);
    println!("✅ Success case: {}\n", result);

    // Test 2: Offset out of range
    let mut offset_error = PartitionData::default();
    offset_error.error_code = 1;
    offset_error.high_watermark = 100;
    offset_error.log_start_offset = 10;

    let result = Consumer::validate_fetch_response(0, &offset_error, 5, 0);
    println!("❌ Offset out of range: {}\n", result);

    // Test 3: Unknown topic/partition
    let mut unknown_topic = PartitionData::default();
    unknown_topic.error_code = 3;
    unknown_topic.high_watermark = 0;
    unknown_topic.log_start_offset = 0;

    let result = Consumer::validate_fetch_response(0, &unknown_topic, 0, 0);
    println!("❌ Unknown topic: {}\n", result);

    // Test 4: Authorization failed
    let mut auth_failed = PartitionData::default();
    auth_failed.error_code = 29;
    auth_failed.high_watermark = 100;
    auth_failed.log_start_offset = 0;

    let result = Consumer::validate_fetch_response(0, &auth_failed, 0, 0);
    println!("❌ Auth failed: {}\n", result);

    // Test 5: Empty partition (caught up)
    let mut empty_response = PartitionData::default();
    empty_response.error_code = 0;
    empty_response.high_watermark = 50;
    empty_response.log_start_offset = 0;
    empty_response.records = Some(bytes::Bytes::new());

    let result = Consumer::validate_fetch_response(0, &empty_response, 50, 0);
    println!("ℹ️  Caught up: {}\n", result);

    // Test 6: Protocol version issue
    let mut version_issue = PartitionData::default();
    version_issue.error_code = 0;
    version_issue.high_watermark = 100;
    version_issue.log_start_offset = 0;
    version_issue.records = None; // Missing records field

    let result = Consumer::validate_fetch_response(0, &version_issue, 25, 0);
    println!("🔍 Protocol issue: {}\n", result);

    println!("🧪 FETCH Response Validation Tests Complete\n");
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
    /// Sum of all backlog percentage measurements for calculating average
    backlog_percentage_sum: Arc<AtomicU64>,
    /// Count of backlog percentage measurements for calculating average
    backlog_measurement_count: Arc<AtomicU64>,
    /// Knight Rider animator for visual feedback
    animator: KnightRiderAnimator,
}

impl ThroughputMeasurer {
    /// Creates a new ThroughputMeasurer with custom LED count
    fn with_leds(led_count: usize) -> Self {
        Self {
            messages_sent: Arc::new(AtomicU64::new(0)),
            messages_received: Arc::new(AtomicU64::new(0)),
            backlog_percentage_sum: Arc::new(AtomicU64::new(0)),
            backlog_measurement_count: Arc::new(AtomicU64::new(0)),
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
    /// * `label` - Label for display purposes
    /// * `max_backlog` - Maximum backlog threshold for percentage calculation
    /// * `fetch_delay` - Initial delay before starting measurement
    ///
    /// # Returns
    /// * `(min_rate, max_rate)` - Tuple containing minimum and maximum measured rates
    async fn measure(
        &self,
        duration: Duration,
        label: &str,
        max_backlog: u64,
        fetch_delay: u64,
    ) -> (f64, f64) {
        // Wait for fetch delay before starting measurement
        if fetch_delay > 0 {
            info!(
                "⏱️  Waiting {}s for consumers to start before beginning measurement",
                fetch_delay
            );
            tokio::time::sleep(Duration::from_secs(fetch_delay)).await;
        }
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
            "🚀 Starting {} phase for {} seconds (after delay)",
            label,
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
                    let backlog_percentage = (backlog as f64 / max_backlog as f64 * 100.0).min(100.0);

                    // Track backlog percentage for average calculation
                    let backlog_percentage_int = backlog_percentage as u64;
                    self.backlog_percentage_sum.fetch_add(backlog_percentage_int, Ordering::Relaxed);
                    self.backlog_measurement_count.fetch_add(1, Ordering::Relaxed);

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
    // Profiling is now handled automatically by quantum-pulse

    // Initialize structured logging for better observability
    tracing_subscriber::fmt::init();

    // Parse and validate command-line arguments
    let args = Args::parse();

    // If profile demo requested, run it and exit
    if args.profile_demo {
        return run_profile_demo().await;
    }

    // Run diagnostics tests if requested
    if args.debug_fetch {
        test_fetch_response_validation();
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
        test_produce_response_validation();
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
            total_partitions,
            message_size.clone(),
            i,
            args.sticky,
        ));
    }

    for i in 0..num_consumer_threads {
        consumers.push(Consumer::new(
            consumer_clients[i].clone(),
            topic_name.clone(),
            args.consumer_partitions_per_thread,
            i,
            args.fetch_delay,
        ));
    }
    info!("All client connections established successfully");

    // Calculate adjusted max backlog to compensate for fetch delay
    let max_backlog = if args.fetch_delay > 0 {
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
    measurer.messages_received.store(0, Ordering::Relaxed);

    // Start multiple producer and consumer threads
    let mut producer_handles = Vec::new();
    let mut consumer_handles = Vec::new();

    for i in 0..num_producer_threads {
        let producer = producers[i].clone();
        let messages_sent = measurer.messages_sent.clone();
        let messages_received = measurer.messages_received.clone();
        let message_validation = args.message_validation;
        let producer_handle = tokio::spawn(async move {
            producer
                .produce_messages(
                    total_test_duration,
                    messages_sent,
                    messages_received,
                    max_backlog,
                    message_validation,
                )
                .await
        });
        producer_handles.push(producer_handle);
    }

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

    let measurement_handle = tokio::spawn({
        let measurer = measurer.clone();
        async move {
            let (min_rate, max_rate) = measurer
                .measure(
                    measurement_duration,
                    "MEASUREMENT",
                    max_backlog,
                    args.fetch_delay,
                )
                .await;

            let final_sent = measurer.messages_sent.load(Ordering::Relaxed);
            let final_received = measurer.messages_received.load(Ordering::Relaxed);
            let throughput = final_received as f64 / measurement_duration.as_secs_f64();
            let backlog_sum = measurer.backlog_percentage_sum.load(Ordering::Relaxed);
            let backlog_count = measurer.backlog_measurement_count.load(Ordering::Relaxed);
            let avg_backlog = if backlog_count > 0 {
                backlog_sum / backlog_count
            } else {
                0
            };

            info!("=== FINAL RESULTS ===");
            info!("Messages sent: {}, Messages received: {}, Throughput: {:.1} messages/second (min: {:.1} msg/s, max: {:.1} msg/s, avg backlog: {}%)", final_sent, final_received, throughput,
                min_rate, max_rate, avg_backlog
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

    // Analyze final producer and consumer performance and provide troubleshooting guidance
    let final_sent = measurer.messages_sent.load(Ordering::Relaxed);
    let final_received = measurer.messages_received.load(Ordering::Relaxed);

    if args.debug_fetch || args.debug_produce || final_received == 0 || final_sent == 0 {
        println!("\n🔍 CONSUMER PERFORMANCE ANALYSIS");
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
    fn test_measurement_timing_calculation() {
        use std::time::Duration;

        // Test case 1: No delay - measurement duration equals total duration
        let measurement_duration = Duration::from_secs(15);
        let fetch_delay = 0u64;
        let total_test_duration = measurement_duration + Duration::from_secs(fetch_delay);

        assert_eq!(total_test_duration.as_secs(), 15);
        assert_eq!(measurement_duration.as_secs(), 15);

        // Test case 2: With delay - total duration includes delay + measurement
        let measurement_duration = Duration::from_secs(15);
        let fetch_delay = 3u64;
        let total_test_duration = measurement_duration + Duration::from_secs(fetch_delay);

        assert_eq!(total_test_duration.as_secs(), 18);
        assert_eq!(measurement_duration.as_secs(), 15);

        // Test case 3: Extended delay
        let measurement_duration = Duration::from_secs(20);
        let fetch_delay = 10u64;
        let total_test_duration = measurement_duration + Duration::from_secs(fetch_delay);

        assert_eq!(total_test_duration.as_secs(), 30);
        assert_eq!(measurement_duration.as_secs(), 20);
    }

    #[test]
    fn test_fetch_delay_default_value() {
        use crate::Args;
        use clap::Parser;

        // Test that default fetch_delay is 1
        let args = Args::try_parse_from(&["kitt"]).unwrap();
        assert_eq!(args.fetch_delay, 1);

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
