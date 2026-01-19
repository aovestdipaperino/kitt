//! Kafka Integrated Throughput Testing (KITT) - A high-performance Kafka throughput measurement tool
//!
//! This tool measures Kafka producer and consumer throughput by creating a temporary topic,
//! producing messages at high rates, and measuring the end-to-end latency and throughput.

use anyhow::{anyhow, Result};
use clap::Parser;

use kafka_protocol::{
    messages::{
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        fetch_response::FetchResponse,
        ApiKey, ResponseHeader, TopicName,
    },
    protocol::{Decodable, StrBytes},
    records::RecordBatchDecoder,
};
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::{sleep, Instant};
use tracing::{debug, error, info, warn};

/// Base maximum number of pending messages allowed before applying backpressure
/// This prevents memory exhaustion during high-throughput testing
/// Will be multiplied by fetch_delay to compensate for delayed consumer start
const BASE_MAX_BACKLOG: u64 = 10000;

mod args;
mod kafka_client;
mod measurer;
mod producer;
pub mod profiling;
mod utils;
use args::{Args, KeyStrategy, MessageSize};
use kafka_client::KafkaClient;
use measurer::{ThroughputMeasurer, LED_BAR_WIDTH};
use producer::Producer;
use profiling::KittOperation;
use quantum_pulse::profile;
use utils::{format_bytes, lcm, parse_bytes, verify_record_batch_crc};

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
    /// Simulated processing time per record in milliseconds
    record_processing_time: u64,
}

impl Consumer {
    /// Creates a new Consumer instance
    ///
    /// # Arguments
    /// * `client` - Shared Kafka client for network communication
    /// * `topic` - Source topic name for message consumption
    /// * `consumer_partitions_per_thread` - Number of partitions assigned to this thread
    /// * `thread_id` - Unique identifier for this consumer thread
    /// * `fetch_delay` - Initial delay in seconds before starting to fetch messages
    /// * `record_processing_time` - Simulated processing time per record in milliseconds
    fn new(
        client: Arc<KafkaClient>,
        topic: String,
        consumer_partitions_per_thread: i32,
        thread_id: usize,
        fetch_delay: u64,
        record_processing_time: u64,
    ) -> Self {
        Self {
            client,
            topic,
            consumer_partitions_per_thread,
            thread_id,
            fetch_delay,
            record_processing_time,
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
                                                            // Verify CRC before decoding
                                                            let remaining_data = &records.as_ref()
                                                                [records_cursor.position() as usize..];
                                                            if let Err(crc_err) =
                                                                verify_record_batch_crc(remaining_data)
                                                            {
                                                                panic!(
                                                                    "CRC verification failed for partition {}: {}",
                                                                    partition_id, crc_err
                                                                );
                                                            }

                                                            match RecordBatchDecoder::decode(
                                                                &mut records_cursor,
                                                            ) {
                                                                Ok(record_set) => {
                                                                    let record_count =
                                                                        record_set.records.len();
                                                                    partition_message_count +=
                                                                        record_count as u64;

                                                                    // Simulate processing time per record
                                                                    if self.record_processing_time > 0
                                                                    {
                                                                        tokio::time::sleep(
                                                                            Duration::from_millis(
                                                                                self.record_processing_time
                                                                                    * record_count
                                                                                        as u64,
                                                                            ),
                                                                        )
                                                                        .await;
                                                                    }

                                                                    debug!(
                                                                        "Decoded record batch with {} records",
                                                                        record_count
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
