//! Kafka message producer for high-throughput message production
//!
//! This module contains the `Producer` struct and its implementation for
//! producing messages to Kafka topics with configurable batching, partitioning,
//! and backpressure control.

use crate::consts::{
    BACKPRESSURE_PAUSE_MS, BASE_BACKOFF_MS, MAX_BACKOFF_MS, MAX_CONSECUTIVE_ERRORS,
    MAX_PENDING_REQUESTS, PRODUCE_TIMEOUT_MS,
};
use crate::profiling::KittOperation;
use kitt_core::{KafkaClient, KeyStrategy, MessageSize};
use anyhow::Result;
use bytes::Bytes;
use kafka_protocol::messages::produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData};
use kafka_protocol::messages::{ApiKey, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType};
use quantum_pulse::profile;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use tracing::{debug, error, warn};

/// Configuration for the `produce_messages` method
///
/// This struct groups the parameters needed for message production,
/// reducing the number of arguments passed to the function.
pub struct ProduceConfig {
    /// How long to continue producing messages
    pub duration: Duration,
    /// Shared counter for tracking sent messages
    pub messages_sent: Arc<AtomicU64>,
    /// Shared counter for tracking bytes sent
    pub bytes_sent: Arc<AtomicU64>,
    /// Shared counter for tracking received messages (for backpressure)
    pub messages_received: Arc<AtomicU64>,
    /// Maximum number of pending messages before applying backpressure
    pub max_backlog: u64,
    /// Whether to validate produce responses
    pub message_validation: bool,
    /// Optional target bytes to produce (overrides duration if set)
    pub bytes_target: Option<u64>,
}

/// Kafka message producer that sends messages to a specific topic
/// Each producer instance runs in its own thread and targets specific partitions
#[derive(Clone)]
pub struct Producer {
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
    /// Strategy for generating message keys
    key_strategy: KeyStrategy,
    /// Number of messages to include in each record batch
    messages_per_batch: usize,
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
    /// * `key_strategy` - Strategy for generating message keys
    /// * `messages_per_batch` - Number of messages to include in each record batch
    #[expect(clippy::too_many_arguments, reason = "Producer configuration requires multiple parameters")]
    pub fn new(
        client: Arc<KafkaClient>,
        topic: String,
        producer_partitions_per_thread: i32,
        total_partitions: i32,
        message_size: MessageSize,
        thread_id: usize,
        use_sticky_partitions: bool,
        key_strategy: KeyStrategy,
        messages_per_batch: usize,
    ) -> Self {
        Self {
            client,
            topic,
            producer_partitions_per_thread,
            total_partitions,
            message_size,
            thread_id,
            use_sticky_partitions,
            key_strategy,
            messages_per_batch,
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
    /// * `config` - Configuration for message production including duration, counters, and options
    ///
    /// # Returns
    /// * `Ok(())` - When production completes successfully
    /// * `Err(anyhow::Error)` - If Kafka communication or encoding fails
    pub async fn produce_messages(&self, config: ProduceConfig) -> Result<()> {
        let ProduceConfig {
            duration,
            messages_sent,
            bytes_sent,
            messages_received,
            max_backlog,
            message_validation,
            bytes_target,
        } = config;
        let end_time = Instant::now() + duration;

        // Determine partition selection strategy based on configuration
        let partition_count = self.producer_partitions_per_thread as usize;
        let start_partition = if self.use_sticky_partitions {
            self.thread_id * self.producer_partitions_per_thread as usize
        } else {
            0 // Not used in random mode
        };

        let mut pending_futures = Vec::new();

        // Retry logic: give up after MAX_CONSECUTIVE_ERRORS with exponential backoff
        let mut consecutive_errors: u32 = 0;

        // When bytes_target is specified, ignore time limit and run until target is reached
        while bytes_target.is_some() || Instant::now() < end_time {
            // Check if we've reached the bytes target (if specified)
            if let Some(target) = bytes_target {
                if bytes_sent.load(Ordering::Relaxed) >= target {
                    break;
                }
            }

            // Implement backpressure control to prevent memory exhaustion
            let sent = messages_sent.load(Ordering::Relaxed);
            let received = messages_received.load(Ordering::Relaxed);
            let backlog = sent.saturating_sub(received);

            if backlog > max_backlog {
                // Backpressure: pause producer when consumer falls behind to prevent unbounded
                // memory growth. 10ms is short enough to maintain throughput when consumer recovers.
                profile!(KittOperation::BacklogWaiting, {
                    tokio::time::sleep(Duration::from_millis(BACKPRESSURE_PAUSE_MS)).await;
                });
                continue;
            }

            // Create produce request with 1 message per partition
            let mut request = ProduceRequest::default();
            // acks=-1 (all): wait for all replicas. Strongest durability guarantee.
            request.acks = -1;
            request.timeout_ms = PRODUCE_TIMEOUT_MS;

            let mut topic_data = TopicProduceData::default();
            topic_data.name = TopicName(StrBytes::from_string(self.topic.clone()));

            // Configure encoding options for the record batch
            let options = RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            };

            // Track total bytes for this request
            let mut total_bytes_in_request: u64 = 0;

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

                // Create multiple records for this partition's batch
                let mut records = Vec::with_capacity(self.messages_per_batch);
                for _ in 0..self.messages_per_batch {
                    // Generate message with configured size
                    let size = profile!(KittOperation::MessageSizeGeneration, {
                        self.message_size.generate_size()
                    });
                    total_bytes_in_request += size as u64;
                    let payload = vec![b'x'; size];
                    let timestamp =
                        SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;

                    // Generate message key based on configured strategy (different key for each message)
                    let key = self.key_strategy.generate_key();

                    // Idempotence is disabled (producer_id=-1) because multiple threads would need
                    // coordinated sequence numbers, which adds complexity without benefit for throughput testing.
                    records.push(Record {
                        transactional: false,
                        control: false,
                        partition_leader_epoch: 0,
                        producer_id: -1,  // -1 disables idempotence
                        producer_epoch: -1,
                        timestamp_type: TimestampType::Creation,
                        offset: 0,    // Broker assigns actual offset; 0 is placeholder
                        sequence: -1, // Required when idempotence disabled
                        timestamp,
                        key,
                        value: Some(Bytes::from(payload)),
                        headers: indexmap::IndexMap::new(),
                    });
                }

                // Encode all records into a single batch
                let mut batch_buf = bytes::BytesMut::new();
                RecordBatchEncoder::encode(
                    &mut batch_buf,
                    records.iter().collect::<Vec<_>>(),
                    &options,
                )?;
                let batch = batch_buf.freeze();

                // Create partition data
                let mut partition_data = PartitionProduceData::default();
                partition_data.index = partition_id;
                partition_data.records = Some(batch);
                topic_data.partition_data.push(partition_data);
            }

            request.topic_data.push(topic_data);

            // Send the batched request asynchronously.
            // Messages are counted AFTER the server acknowledges, not before,
            // to prevent inflated backlog from eagerly-counted but not-yet-sent messages.
            let version = self.client.get_supported_version(ApiKey::Produce, 3);
            let client = self.client.clone();
            let thread_id = self.thread_id;
            let msg_count = partition_count as u64 * self.messages_per_batch as u64;
            let byte_count = total_bytes_in_request;
            let msgs_sent = messages_sent.clone();
            let bts_sent = bytes_sent.clone();
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
                        // Count messages only after server acknowledges
                        msgs_sent.fetch_add(msg_count, Ordering::Relaxed);
                        bts_sent.fetch_add(byte_count, Ordering::Relaxed);
                        // Validate the produce response if enabled
                        if message_validation {
                            profile!(KittOperation::ResponseValidation, {
                                kitt_core::Producer::validate_produce_response(
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
                            error!("NETWORK ISSUE - Check broker connectivity and network configuration");
                        } else if error_str.contains("timeout") {
                            error!("TIMEOUT - Broker may be overloaded or network latency high");
                        } else if error_str.contains("auth") || error_str.contains("permission") {
                            error!(
                                "AUTHENTICATION - Verify credentials and ACLs for this broker"
                            );
                        } else if error_str.contains("protocol") || error_str.contains("version") {
                            error!("PROTOCOL MISMATCH - Broker may use different Kafka version/protocol");
                        } else {
                            error!("UNKNOWN ERROR - Check broker logs and configuration");
                        }

                        Err(e)
                    }
                }
            });
            pending_futures.push(future);

            // Drain completed futures; block when at capacity to bound in-flight requests.
            if pending_futures.len() >= MAX_PENDING_REQUESTS {
                // First pass: reap any already-completed futures
                let mut i = 0;
                while i < pending_futures.len() {
                    if pending_futures[i].is_finished() {
                        let result = pending_futures.remove(i).await;
                        match result {
                            Ok(Ok(_)) => {
                                consecutive_errors = 0;
                            }
                            Ok(Err(e)) => {
                                consecutive_errors += 1;
                                debug!("Producer validation failed: {}", e);
                            }
                            Err(e) => {
                                consecutive_errors += 1;
                                debug!("Failed to send batch: {}", e);
                            }
                        }
                    } else {
                        i += 1;
                    }
                }
                // If still at capacity, await the oldest request to prevent unbounded growth
                if pending_futures.len() >= MAX_PENDING_REQUESTS {
                    let result = pending_futures.remove(0).await;
                    match result {
                        Ok(Ok(_)) => {
                            consecutive_errors = 0;
                        }
                        Ok(Err(e)) => {
                            consecutive_errors += 1;
                            debug!("Producer validation failed: {}", e);
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            debug!("Failed to send batch: {}", e);
                        }
                    }
                }
            }

            // Check consecutive errors and apply exponential backoff or give up
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                error!(
                    "Producer thread {}: giving up after {} consecutive errors",
                    self.thread_id, MAX_CONSECUTIVE_ERRORS
                );
                break;
            } else if consecutive_errors > 0 {
                let backoff_ms = BASE_BACKOFF_MS * 2u64.pow(consecutive_errors - 1);
                let backoff_ms = backoff_ms.min(MAX_BACKOFF_MS);
                warn!(
                    "Producer thread {}: retry {}/{} after produce error, backing off {}ms",
                    self.thread_id, consecutive_errors, MAX_CONSECUTIVE_ERRORS, backoff_ms
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }

        // Clean up: wait for all remaining requests to complete
        for future in pending_futures {
            match future.await {
                Ok(Ok(_)) => {}
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

}

/// Delegates to kitt_core's produce response validation test
#[allow(dead_code)]
pub fn test_produce_response_validation() {
    kitt_core::producer::test_produce_response_validation();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_response_validation_runs() {
        test_produce_response_validation();
    }
}
