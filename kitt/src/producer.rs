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

        let partition_count = self.producer_partitions_per_thread as usize;
        let start_partition = if self.use_sticky_partitions {
            self.thread_id * self.producer_partitions_per_thread as usize
        } else {
            0
        };

        let mut pending_futures = Vec::new();
        let mut consecutive_errors: u32 = 0;

        while bytes_target.is_some() || Instant::now() < end_time {
            if let Some(target) = bytes_target {
                if bytes_sent.load(Ordering::Relaxed) >= target {
                    break;
                }
            }

            // Implement backpressure control
            let sent = messages_sent.load(Ordering::Relaxed);
            let received = messages_received.load(Ordering::Relaxed);
            let backlog = sent.saturating_sub(received);

            if backlog > max_backlog {
                profile!(KittOperation::BacklogWaiting, {
                    tokio::time::sleep(Duration::from_millis(BACKPRESSURE_PAUSE_MS)).await;
                });
                continue;
            }

            // Build and send produce request
            let (request, total_bytes_in_request) = self.build_produce_request(
                partition_count,
                start_partition,
            )?;

            let future = self.spawn_produce_task(
                request,
                partition_count,
                total_bytes_in_request,
                &messages_sent,
                &bytes_sent,
                message_validation,
            );
            pending_futures.push(future);

            // Drain completed futures
            drain_pending_futures(
                &mut pending_futures,
                &mut consecutive_errors,
            )
            .await;

            // Check consecutive errors and apply backoff
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
        drain_remaining_futures(pending_futures).await;

        Ok(())
    }

    /// Builds a produce request with batched records for all assigned partitions
    fn build_produce_request(
        &self,
        partition_count: usize,
        start_partition: usize,
    ) -> Result<(ProduceRequest, u64)> {
        let mut request = ProduceRequest::default();
        request.acks = -1;
        request.timeout_ms = PRODUCE_TIMEOUT_MS;

        let mut topic_data = TopicProduceData::default();
        topic_data.name = TopicName(StrBytes::from_string(self.topic.clone()));

        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };

        let mut total_bytes_in_request: u64 = 0;

        for i in 0..partition_count {
            let partition_id = self.select_partition(i, start_partition);

            if partition_id < 0 || partition_id >= self.total_partitions {
                error!(
                    "Producer thread {} generated invalid partition ID {} (valid range: 0-{})",
                    self.thread_id, partition_id, self.total_partitions - 1
                );
                continue;
            }

            debug!(
                "Producer thread {} {} partition {} (total partitions: {})",
                self.thread_id,
                if self.use_sticky_partitions { "assigned" } else { "selecting" },
                partition_id,
                self.total_partitions
            );

            let (partition_data, bytes) = self.build_partition_batch(partition_id, &options)?;
            total_bytes_in_request += bytes;
            topic_data.partition_data.push(partition_data);
        }

        request.topic_data.push(topic_data);
        Ok((request, total_bytes_in_request))
    }

    /// Selects a partition ID based on the configured strategy
    fn select_partition(&self, index: usize, start_partition: usize) -> i32 {
        if self.use_sticky_partitions {
            (start_partition + index) as i32
        } else {
            if self.total_partitions > 0 {
                (rand::random::<u32>() % self.total_partitions as u32) as i32
            } else {
                error!(
                    "Producer thread {} has invalid total_partitions: {}",
                    self.thread_id, self.total_partitions
                );
                -1 // Will be caught by validation
            }
        }
    }

    /// Builds a batch of records for a single partition
    fn build_partition_batch(
        &self,
        partition_id: i32,
        options: &RecordEncodeOptions,
    ) -> Result<(PartitionProduceData, u64)> {
        let mut records = Vec::with_capacity(self.messages_per_batch);
        let mut total_bytes = 0u64;

        for _ in 0..self.messages_per_batch {
            let size = profile!(KittOperation::MessageSizeGeneration, {
                self.message_size.generate_size()
            });
            total_bytes += size as u64;
            let payload = vec![b'x'; size];
            let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
            let key = self.key_strategy.generate_key();

            records.push(Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: -1,
                producer_epoch: -1,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: -1,
                timestamp,
                key,
                value: Some(Bytes::from(payload)),
                headers: indexmap::IndexMap::new(),
            });
        }

        let mut batch_buf = bytes::BytesMut::new();
        RecordBatchEncoder::encode(
            &mut batch_buf,
            records.iter().collect::<Vec<_>>(),
            options,
        )?;
        let batch = batch_buf.freeze();

        let mut partition_data = PartitionProduceData::default();
        partition_data.index = partition_id;
        partition_data.records = Some(batch);

        Ok((partition_data, total_bytes))
    }

    /// Spawns a tokio task to send a produce request
    fn spawn_produce_task(
        &self,
        request: ProduceRequest,
        partition_count: usize,
        total_bytes_in_request: u64,
        messages_sent: &Arc<AtomicU64>,
        bytes_sent: &Arc<AtomicU64>,
        message_validation: bool,
    ) -> tokio::task::JoinHandle<Result<()>> {
        let version = self.client.get_supported_version(ApiKey::Produce, 3);
        let client = self.client.clone();
        let thread_id = self.thread_id;
        let msg_count = partition_count as u64 * self.messages_per_batch as u64;
        let byte_count = total_bytes_in_request;
        let msgs_sent = messages_sent.clone();
        let bts_sent = bytes_sent.clone();

        tokio::spawn(async move {
            debug!(
                "Producer thread {} sending batch with {} partitions, producer_id=-1 (idempotence disabled)",
                thread_id, partition_count
            );
            let send_result = profile!(KittOperation::MessageProduce, {
                client.send_request(ApiKey::Produce, &request, version)
            })
            .await;
            match send_result {
                Ok(response_bytes) => {
                    msgs_sent.fetch_add(msg_count, Ordering::Relaxed);
                    bts_sent.fetch_add(byte_count, Ordering::Relaxed);
                    if message_validation {
                        profile!(KittOperation::ResponseValidation, {
                            kitt_core::Producer::validate_produce_response(
                                &response_bytes,
                                version,
                                thread_id,
                                partition_count,
                            )
                        })
                        .await
                        .map_err(|e| anyhow::anyhow!("{}", e))?;
                    }
                    Ok(())
                }
                Err(e) => {
                    log_produce_request_error(thread_id, &e);
                    Err(anyhow::anyhow!("{}", e))
                }
            }
        })
    }
}

/// Logs a produce request error with troubleshooting guidance
fn log_produce_request_error(thread_id: usize, err: &impl std::fmt::Display) {
    error!(
        "Producer thread {} PRODUCE REQUEST FAILED: {}",
        thread_id, err
    );

    let error_str = err.to_string().to_lowercase();
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
}

/// Drains completed futures from the pending list, blocking if at capacity
#[allow(clippy::indexing_slicing)] // Safety: i < pending_futures.len() is checked by while loop condition
async fn drain_pending_futures(
    pending_futures: &mut Vec<tokio::task::JoinHandle<Result<()>>>,
    consecutive_errors: &mut u32,
) {
    if pending_futures.len() < MAX_PENDING_REQUESTS {
        return;
    }

    // First pass: reap any already-completed futures
    let mut i = 0;
    while i < pending_futures.len() {
        if pending_futures[i].is_finished() {
            let result = pending_futures.remove(i).await;
            handle_future_result(result, consecutive_errors);
        } else {
            i += 1;
        }
    }

    // If still at capacity, await the oldest request
    if pending_futures.len() >= MAX_PENDING_REQUESTS {
        let result = pending_futures.remove(0).await;
        handle_future_result(result, consecutive_errors);
    }
}

/// Handles the result of a completed produce future
fn handle_future_result(
    result: std::result::Result<Result<()>, tokio::task::JoinError>,
    consecutive_errors: &mut u32,
) {
    match result {
        Ok(Ok(_)) => {
            *consecutive_errors = 0;
        }
        Ok(Err(e)) => {
            *consecutive_errors += 1;
            debug!("Producer validation failed: {}", e);
        }
        Err(e) => {
            *consecutive_errors += 1;
            debug!("Failed to send batch: {}", e);
        }
    }
}

/// Waits for all remaining pending futures to complete
async fn drain_remaining_futures(pending_futures: Vec<tokio::task::JoinHandle<Result<()>>>) {
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
}

/// Delegates to kitt_core's produce response validation test
#[allow(dead_code)]
pub fn test_produce_response_validation() {
    kitt_core::producer::test_produce_response_validation();
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_response_validation_runs() {
        test_produce_response_validation();
    }
}
