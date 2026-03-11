//! Kafka message consumer module for KITT
//!
//! This module provides the Consumer struct for fetching messages from Kafka topics.
//! Each consumer instance runs in its own thread and targets specific partitions.

use crate::consts::{
    BASE_BACKOFF_MS, FETCH_MAX_BYTES, FETCH_MAX_WAIT_MS, FETCH_TIMEOUT_MS,
    MAX_BACKOFF_MS, MAX_CONSECUTIVE_ERRORS, PARTITION_MAX_BYTES,
};
use crate::profiling::KittOperation;
use kitt_core::utils::verify_record_batch_crc;
use kitt_core::KafkaClient;
use anyhow::Result;
use kafka_protocol::messages::fetch_request::{FetchPartition, FetchRequest, FetchTopic};
use kafka_protocol::messages::fetch_response::FetchResponse;
use kafka_protocol::messages::{ApiKey, ResponseHeader, TopicName};
use kafka_protocol::protocol::{Decodable, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use quantum_pulse::profile;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

/// Kafka message consumer that fetches messages from a specific topic
/// Each consumer instance runs in its own thread and targets specific partitions
#[derive(Clone)]
pub struct Consumer {
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
    pub fn new(
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
    pub async fn consume_messages(
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

        // Partition assignment: thread N handles partitions [N*count, (N+1)*count).
        // This ensures no overlap - each partition has exactly one consumer.
        let start_partition = self.thread_id * self.consumer_partitions_per_thread as usize;
        let partition_count = self.consumer_partitions_per_thread as usize;

        // Per-partition offset tracking. Starting at 0 reads from the beginning.
        // Invariant: offsets[i] always points to the next message to fetch.
        let mut offsets = vec![0i64; partition_count];

        // Add periodic logging to track consumer activity
        let mut last_log_time = Instant::now();
        let mut fetch_count = 0u64;

        // Retry logic: give up after MAX_CONSECUTIVE_ERRORS with exponential backoff
        let mut consecutive_errors: u32 = 0;

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
                fetch_partition.current_leader_epoch = -1; // -1 skips epoch check (simpler but less safe)
                fetch_partition.fetch_offset = offsets[idx];
                fetch_partition.log_start_offset = -1;  // Broker will tell us where log starts
                fetch_partition.partition_max_bytes = PARTITION_MAX_BYTES;

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

            // Fetch parameters tuned for throughput testing:
            let mut request = FetchRequest::default();
            request.max_wait_ms = FETCH_MAX_WAIT_MS;
            request.min_bytes = 1;       // Return immediately when any data available
            request.max_bytes = FETCH_MAX_BYTES;
            request.isolation_level = 0; // 0=READ_UNCOMMITTED for maximum speed (no transaction overhead)
            request.session_id = 0;      // Fetch sessions disabled for simplicity
            request.session_epoch = -1;
            request.topics.push(fetch_topic);
            request.rack_id = StrBytes::from_static_str("");

            // Send fetch request and process response
            debug!(
                "Consumer thread {} sending fetch request for {} partitions",
                self.thread_id, partition_count_for_log
            );
            let version = self.client.get_supported_version(ApiKey::Fetch, 4);

            let fetch_timeout = Duration::from_millis(FETCH_TIMEOUT_MS);
            match tokio::time::timeout(
                fetch_timeout,
                profile!(KittOperation::MessageConsume, {
                    self.client.send_request(ApiKey::Fetch, &request, version)
                }),
            )
            .await
            {
                Ok(Ok(response_bytes)) => {
                    // Successful response received, reset consecutive error counter
                    consecutive_errors = 0;

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
                                                    kitt_core::Consumer::validate_fetch_response(
                                                        partition_id,
                                                        partition_response,
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
                                                            // Offset advances by message count since Kafka offsets are per-message.
                                                            // This assumes batch.base_offset + record_index = message offset.
                                                            offsets[local_partition_idx] +=
                                                                partition_message_count as i64;
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
                                                        // Empty response handling: jump to valid offset to avoid getting stuck.
                                                        // log_start_offset is the earliest available; high_watermark is latest committed.
                                                        if offsets[local_partition_idx]
                                                            < partition_response.log_start_offset
                                                        {
                                                            // Our offset was compacted away; jump to oldest available
                                                            offsets[local_partition_idx] =
                                                                partition_response.log_start_offset;
                                                        } else if offsets[local_partition_idx]
                                                            < partition_response.high_watermark
                                                        {
                                                            // Gap in data; skip to latest to resume progress
                                                            offsets[local_partition_idx] =
                                                                partition_response.high_watermark;
                                                        }
                                                        // At high_watermark means we're caught up; wait for new messages
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
                                    error!("DECODE TROUBLESHOOT:");
                                    error!(
                                        "   - Broker may use incompatible Kafka protocol version"
                                    );
                                    error!("   - Response may be corrupted due to network issues");
                                    error!("   - Different broker software/version than expected");
                                    error!(
                                        "Raw response size: {} bytes, using fetch version: {}",
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
                            error!("HEADER DECODE TROUBLESHOOT:");
                            error!("   - Broker protocol version mismatch");
                            error!("   - Connection may be to wrong service/port");
                            error!("   - Broker may have sent malformed response");
                            error!(
                                "Expected header version: {}, response size: {} bytes",
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
                    consecutive_errors += 1;
                    error!(
                        "Consumer thread {} FETCH REQUEST FAILED: {}",
                        self.thread_id, request_err
                    );

                    // Provide specific troubleshooting guidance based on error type
                    let error_str = request_err.to_string().to_lowercase();
                    if error_str.contains("connection") || error_str.contains("network") {
                        error!("NETWORK ISSUE - Check broker connectivity and network configuration");
                    } else if error_str.contains("timeout") {
                        error!("TIMEOUT - Broker may be overloaded or network latency high");
                    } else if error_str.contains("auth") || error_str.contains("permission") {
                        error!("AUTHENTICATION - Verify credentials and ACLs for this broker");
                    } else if error_str.contains("protocol") || error_str.contains("version") {
                        error!("PROTOCOL MISMATCH - Broker may use different Kafka version/protocol");
                    } else {
                        error!("UNKNOWN ERROR - Check broker logs and configuration");
                    }

                    error!("FETCH CONFIG - topic: {}, partitions_per_thread: {}, max_wait: 1000ms, max_bytes: 50MB",
                           self.topic, self.consumer_partitions_per_thread);

                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!(
                            "Consumer thread {}: giving up after {} consecutive errors",
                            self.thread_id, MAX_CONSECUTIVE_ERRORS
                        );
                        break;
                    }

                    let backoff_ms = BASE_BACKOFF_MS * 2u64.pow(consecutive_errors - 1);
                    let backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
                    warn!(
                        "Consumer thread {}: retry {}/{} after fetch error, backing off {}ms",
                        self.thread_id, consecutive_errors, MAX_CONSECUTIVE_ERRORS, backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    // Yield to allow other tasks to continue processing
                    tokio::task::yield_now().await;
                }
                Err(_timeout_elapsed) => {
                    consecutive_errors += 1;
                    error!(
                        "Consumer thread {} FETCH TIMEOUT after {}ms - broker not responding",
                        self.thread_id, FETCH_TIMEOUT_MS
                    );
                    error!("TIMEOUT TROUBLESHOOT:");
                    error!("   - Broker may be overloaded or down");
                    error!("   - Network connectivity issues to broker");
                    error!("   - Broker configuration may have different timeout settings");
                    error!("   - Topic '{}' may not exist on this broker", self.topic);
                    error!("Consider reducing fetch timeout or checking broker status");

                    if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                        error!(
                            "Consumer thread {}: giving up after {} consecutive errors",
                            self.thread_id, MAX_CONSECUTIVE_ERRORS
                        );
                        break;
                    }

                    let backoff_ms = BASE_BACKOFF_MS * 2u64.pow(consecutive_errors - 1);
                    let backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
                    warn!(
                        "Consumer thread {}: retry {}/{} after fetch timeout, backing off {}ms",
                        self.thread_id, consecutive_errors, MAX_CONSECUTIVE_ERRORS, backoff_ms
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
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

            // Yield allows other tokio tasks (producers, measurer) to make progress.
            // Essential for cooperative scheduling in single-threaded runtime mode.
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

/// Delegates to kitt_core's fetch response validation test
#[allow(dead_code)]
pub fn test_fetch_response_validation() {
    kitt_core::consumer::test_fetch_response_validation();
}

#[cfg(test)]
mod tests {
    use kafka_protocol::messages::fetch_response::PartitionData;

    #[test]
    fn test_fetch_response_validation() {
        // Test via kitt_core's Consumer which owns the validation logic
        let mut success_response = PartitionData::default();
        success_response.error_code = 0;
        success_response.high_watermark = 100;
        success_response.log_start_offset = 0;
        success_response.records = Some(bytes::Bytes::from(vec![0x01, 0x02, 0x03]));

        let result = kitt_core::Consumer::validate_fetch_response(0, &success_response, 50, 0);
        assert!(result.contains("No partition errors"));
        assert!(result.contains("OFFSET_VALID"));
        assert!(result.contains("HAS_RECORDS"));
    }
}
