//! Kafka Client Implementation
//!
//! This module provides a low-level Kafka client that communicates directly with Kafka brokers
//! using the Kafka protocol. It handles connection management, API version negotiation,
//! and provides methods for topic management and message operations.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use kafka_protocol::{
    messages::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        delete_topics_request::DeleteTopicsRequest,
        metadata_request::MetadataRequest,
        metadata_response::MetadataResponse,
        ApiKey, RequestHeader, ResponseHeader, TopicName,
    },
    protocol::{Decodable, Encodable, StrBytes},
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};
use tracing::{debug, info, warn};

/// Low-level Kafka client for direct broker communication
///
/// This client maintains a persistent TCP connection to a Kafka broker and handles:
/// - Protocol message encoding/decoding
/// - API version negotiation and compatibility
/// - Request correlation and response matching
/// - Topic management operations
pub struct KafkaClient {
    /// Thread-safe TCP stream for broker communication
    /// Uses Arc<Mutex<>> to allow sharing across async tasks while ensuring exclusive access
    stream: Arc<Mutex<TcpStream>>,

    /// Monotonically increasing correlation ID for request/response matching
    /// Each request gets a unique ID to correlate with its corresponding response
    correlation_id: AtomicU64,

    /// Supported API versions discovered from the broker
    /// Maps API key (i16) to (min_version, max_version) tuple
    /// Used to select compatible protocol versions for requests
    pub api_versions: HashMap<i16, (i16, i16)>, // (min_version, max_version)
}

impl KafkaClient {
    /// Establishes a connection to a Kafka broker and discovers supported API versions
    ///
    /// This method performs a complete client initialization:
    /// 1. Establishes TCP connection to the broker
    /// 2. Sends ApiVersions request to discover broker capabilities
    /// 3. Stores supported API versions for future request compatibility
    ///
    /// # Arguments
    /// * `broker` - Broker address in "host:port" format (e.g., "localhost:9092")
    ///
    /// # Returns
    /// * `Ok(KafkaClient)` - Fully initialized client ready for operations
    /// * `Err(anyhow::Error)` - If connection or version discovery fails
    ///
    /// # Examples
    /// ```
    /// let client = KafkaClient::connect("localhost:9092").await?;
    /// ```
    pub async fn connect(broker: &str) -> Result<Self> {
        info!("Connecting to Kafka broker at {}", broker);

        // Establish TCP connection with detailed error context
        let stream = TcpStream::connect(broker)
            .await
            .map_err(|e| anyhow!("Failed to connect to Kafka broker at {}: {}", broker, e))?;

        info!("Successfully connected to Kafka broker");

        // Initialize client with default state
        let mut client = KafkaClient {
            stream: Arc::new(Mutex::new(stream)),
            correlation_id: AtomicU64::new(1), // Start correlation IDs at 1
            api_versions: HashMap::new(),
        };

        // Discover broker's supported API versions for compatibility
        client.discover_api_versions().await?;

        Ok(client)
    }

    /// Establishes a connection using pre-discovered API versions
    ///
    /// This is an optimization for cases where API versions are already known,
    /// allowing to skip the version discovery handshake. Useful for connection pooling
    /// or when reconnecting to the same broker type.
    ///
    /// # Arguments
    /// * `broker` - Broker address in "host:port" format
    /// * `api_versions` - Pre-discovered API version mappings
    ///
    /// # Returns
    /// * `Ok(KafkaClient)` - Client ready for operations (skips version discovery)
    /// * `Err(anyhow::Error)` - If connection fails
    pub async fn connect_with_versions(
        broker: &str,
        api_versions: HashMap<i16, (i16, i16)>,
    ) -> Result<Self> {
        debug!(
            "Connecting to Kafka broker at {} (using pre-discovered API versions)",
            broker
        );

        // Establish TCP connection (same as connect() but without version discovery)
        let stream = TcpStream::connect(broker)
            .await
            .map_err(|e| anyhow!("Failed to connect to Kafka broker at {}: {}", broker, e))?;

        debug!("Successfully connected to Kafka broker");
        Ok(KafkaClient {
            stream: Arc::new(Mutex::new(stream)),
            correlation_id: AtomicU64::new(1),
            api_versions, // Use provided versions instead of discovering
        })
    }

    /// Sends a Kafka protocol request and returns the raw response bytes
    ///
    /// This is the core communication method that handles the complete request/response cycle:
    /// 1. Generates unique correlation ID for request tracking
    /// 2. Encodes request header and body using Kafka protocol format
    /// 3. Sends framed message over TCP (4-byte length prefix + payload)
    /// 4. Reads and validates response from broker
    ///
    /// # Type Parameters
    /// * `T` - Request type that implements Kafka protocol encoding
    ///
    /// # Arguments
    /// * `api_key` - Kafka API identifier (e.g., Produce, Fetch, CreateTopics)
    /// * `request` - The request object to send
    /// * `version` - Protocol version to use for encoding
    ///
    /// # Returns
    /// * `Ok(Bytes)` - Raw response bytes from broker
    /// * `Err(anyhow::Error)` - If encoding, network I/O, or protocol validation fails
    pub async fn send_request<T: Encodable + std::fmt::Debug>(
        &self,
        api_key: ApiKey,
        request: &T,
        version: i16,
    ) -> Result<Bytes> {
        // Generate unique correlation ID for this request
        // Used to match response with request in async environments
        let correlation_id = self.correlation_id.fetch_add(1, Ordering::SeqCst) as i32;

        debug!(
            "Sending {:?} request (correlation_id: {}, version: {})",
            api_key, correlation_id, version
        );

        // Build Kafka request header with standard fields
        let mut header = RequestHeader::default();
        header.request_api_key = api_key as i16;
        header.request_api_version = version;
        header.correlation_id = correlation_id;
        header.client_id = Some(StrBytes::from_static_str("kitt")); // Client identifier

        // Determine header version based on API and version
        // Different API versions may use different header formats
        let header_version = api_key.request_header_version(version);

        // Encode header and request body into a single buffer
        let mut buf = Vec::new();
        header
            .encode(&mut buf, header_version)
            .map_err(|e| anyhow!("Failed to encode request header: {}", e))?;
        request
            .encode(&mut buf, version)
            .map_err(|e| anyhow!("Failed to encode request body: {}", e))?;

        // Kafka protocol uses 4-byte big-endian length prefix
        let message_size = buf.len() as i32;
        let mut message = Vec::with_capacity(4 + buf.len());
        message.extend_from_slice(&message_size.to_be_bytes()); // Length prefix
        message.extend_from_slice(&buf); // Actual message content

        debug!("Sending message of {} bytes", message.len());

        // Send request over TCP connection (thread-safe access)
        let mut stream = self.stream.lock().await;
        stream
            .write_all(&message)
            .await
            .map_err(|e| anyhow!("Failed to write request to stream: {}", e))?;

        // Read response using Kafka protocol framing
        debug!("Reading response size");
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await
            .map_err(|e| anyhow!("Failed to read response size: {} (this could indicate the broker closed the connection)", e))?;

        let response_size = i32::from_be_bytes(size_buf) as usize;
        debug!("Reading response body of {} bytes", response_size);

        // Sanity check to prevent memory exhaustion from malformed responses
        if response_size > 100 * 1024 * 1024 {
            // 100MB limit for safety
            return Err(anyhow!("Response size too large: {} bytes", response_size));
        }

        // Read the complete response payload
        let mut response_buf = vec![0u8; response_size];
        stream
            .read_exact(&mut response_buf)
            .await
            .map_err(|e| anyhow!("Failed to read response body: {}", e))?;

        debug!("Successfully received response");
        Ok(Bytes::from(response_buf))
    }

    /// Discovers and caches the broker's supported API versions
    ///
    /// This method sends an ApiVersions request to learn which APIs and versions
    /// the broker supports. This information is used to select compatible protocol
    /// versions for subsequent requests, ensuring maximum compatibility across
    /// different Kafka broker versions.
    ///
    /// # Returns
    /// * `Ok(())` - API versions successfully discovered and cached
    /// * `Err(anyhow::Error)` - If version discovery request fails
    async fn discover_api_versions(&mut self) -> Result<()> {
        debug!("Discovering supported API versions...");

        // Send ApiVersions request using version 0 (universally supported)
        let request = ApiVersionsRequest::default();
        let response_bytes = self.send_request(ApiKey::ApiVersions, &request, 0).await?;

        // Parse the response using protocol decoders
        let mut cursor = std::io::Cursor::new(response_bytes.as_ref());

        // Decode response header (contains correlation ID, error codes, etc.)
        let _response_header = ResponseHeader::decode(&mut cursor, 0)
            .map_err(|e| anyhow!("Failed to decode response header: {}", e))?;

        // Decode the ApiVersions response payload
        let response = ApiVersionsResponse::decode(&mut cursor, 0)
            .map_err(|e| anyhow!("Failed to decode ApiVersions response: {}", e))?;

        // Cache supported version ranges for each API
        // This allows us to select compatible versions for future requests
        for api_version in response.api_keys {
            self.api_versions.insert(
                api_version.api_key,
                (api_version.min_version, api_version.max_version),
            );

            // Log discovered versions for debugging (if API key is recognized)
            if let Ok(api_key) = ApiKey::try_from(api_version.api_key) {
                debug!(
                    "API {:?}: versions {}-{}",
                    api_key, api_version.min_version, api_version.max_version
                );
            }
        }

        debug!("Discovered {} supported APIs", self.api_versions.len());
        Ok(())
    }

    /// Selects a compatible protocol version for the given API
    ///
    /// This method implements version negotiation logic:
    /// 1. If preferred version is within broker's supported range, use it
    /// 2. If preferred version is too high, use broker's maximum supported version
    /// 3. If API wasn't discovered, fall back to preferred version (best effort)
    ///
    /// # Arguments
    /// * `api_key` - The Kafka API to get version for
    /// * `preferred_version` - The client's preferred protocol version
    ///
    /// # Returns
    /// * Compatible protocol version to use for requests
    ///
    /// # Examples
    /// ```
    /// let version = client.get_supported_version(ApiKey::Produce, 7);
    /// // Returns 7 if broker supports it, otherwise broker's max supported version
    /// ```
    pub fn get_supported_version(&self, api_key: ApiKey, preferred_version: i16) -> i16 {
        if let Some((min_version, max_version)) = self.api_versions.get(&(api_key as i16)) {
            // Check if preferred version falls within broker's supported range
            if preferred_version >= *min_version && preferred_version <= *max_version {
                preferred_version // Use preferred version - it's supported
            } else {
                debug!(
                    "Preferred version {} for {:?} not supported (range: {}-{}), using {}",
                    preferred_version, api_key, min_version, max_version, max_version
                );
                *max_version // Use highest version broker supports
            }
        } else {
            // API not found in discovery - broker might not support it or discovery failed
            // Fall back to preferred version and hope for the best
            warn!(
                "API {:?} not found in version discovery, using version {}",
                api_key, preferred_version
            );
            preferred_version
        }
    }

    /// Creates a new Kafka topic with specified configuration
    ///
    /// This method sends a CreateTopics request to the broker to create a new topic.
    /// The topic will be created with the specified number of partitions and
    /// replication factor. If the topic already exists, the broker will return
    /// an appropriate error.
    ///
    /// # Arguments
    /// * `topic` - Name of the topic to create
    /// * `partitions` - Number of partitions for the topic (affects parallelism)
    /// * `replication_factor` - Number of replicas for each partition (affects durability)
    ///
    /// # Returns
    /// * `Ok(())` - Topic created successfully
    /// * `Err(anyhow::Error)` - If topic creation fails (network, permissions, already exists, etc.)
    ///
    /// # Examples
    /// ```
    /// client.create_topic("test-topic", 4, 1).await?;
    /// ```
    pub async fn create_topic(
        &self,
        topic: &str,
        partitions: i32,
        replication_factor: i16,
    ) -> Result<()> {
        debug!("Creating topic '{}' with {} partitions", topic, partitions);

        // Build CreateTopics request with topic configuration
        let mut request = CreateTopicsRequest::default();

        let mut creatable_topic = CreatableTopic::default();
        creatable_topic.name = TopicName(StrBytes::from_string(topic.to_string()));
        creatable_topic.num_partitions = partitions;
        creatable_topic.replication_factor = replication_factor;
        // Additional topic configs (retention, cleanup policy, etc.) could be added here

        request.topics.push(creatable_topic);
        request.timeout_ms = 30000; // 30-second timeout for topic creation

        // Use compatible protocol version for this broker
        let version = self.get_supported_version(ApiKey::CreateTopics, 1);
        let _response_bytes = self
            .send_request(ApiKey::CreateTopics, &request, version)
            .await
            .map_err(|e| anyhow!("Failed to create topic '{}': {}", topic, e))?;

        // TODO: Parse response to check for errors (topic already exists, insufficient replicas, etc.)
        info!(
            "Successfully created topic '{}' with {} partitions",
            topic, partitions
        );
        Ok(())
    }

    /// Deletes an existing Kafka topic
    ///
    /// This method sends a DeleteTopics request to remove a topic from the cluster.
    /// Once deleted, all data in the topic is permanently lost. The operation
    /// requires appropriate permissions and the topic must exist.
    ///
    /// # Arguments
    /// * `topic` - Name of the topic to delete
    ///
    /// # Returns
    /// * `Ok(())` - Topic deleted successfully
    /// * `Err(anyhow::Error)` - If deletion fails (network, permissions, topic not found, etc.)
    ///
    /// # Examples
    /// ```
    /// client.delete_topic("test-topic").await?;
    /// ```
    ///
    /// # Warning
    /// This operation is irreversible and will permanently delete all data in the topic.
    pub async fn delete_topic(&self, topic: &str) -> Result<()> {
        debug!("Deleting topic '{}'", topic);

        // Build DeleteTopics request
        let mut request = DeleteTopicsRequest::default();
        request
            .topic_names
            .push(TopicName(StrBytes::from_string(topic.to_string())));
        request.timeout_ms = 30000; // 30-second timeout for topic deletion

        // Use compatible protocol version for this broker
        let version = self.get_supported_version(ApiKey::DeleteTopics, 1);
        let _response_bytes = self
            .send_request(ApiKey::DeleteTopics, &request, version)
            .await
            .map_err(|e| anyhow!("Failed to delete topic '{}': {}", topic, e))?;

        // TODO: Parse response to check for errors (topic not found, not authorized, etc.)
        info!("Successfully deleted topic '{}'", topic);
        Ok(())
    }

    /// Fetches cluster metadata, including broker and topic information
    ///
    /// This method sends a Metadata request to the broker to discover:
    /// - Cluster topology (brokers, their addresses)
    /// - Topic details (partitions, leaders, replicas)
    ///
    /// # Arguments
    /// * `topic` - Optional topic name to fetch metadata for. If None, fetches all topics.
    ///
    /// # Returns
    /// * `Ok(MetadataResponse)` - Parsed metadata response from the broker
    /// * `Err(anyhow::Error)` - If the metadata request fails
    pub async fn fetch_metadata(&self, topic: Option<&str>) -> Result<MetadataResponse> {
        debug!("Fetching metadata for topic: {:?}", topic);

        let mut request = MetadataRequest::default();
        if let Some(topic_name) = topic {
            let mut topic_request =
                kafka_protocol::messages::metadata_request::MetadataRequestTopic::default();
            topic_request.name = Some(TopicName(StrBytes::from_string(topic_name.to_string())));
            request.topics = Some(vec![topic_request]);
        }

        let version = self.get_supported_version(ApiKey::Metadata, 1);
        let response_bytes = self
            .send_request(ApiKey::Metadata, &request, version)
            .await?;

        let mut cursor = std::io::Cursor::new(response_bytes.as_ref());
        let _response_header = ResponseHeader::decode(&mut cursor, 0)?;
        let response = MetadataResponse::decode(&mut cursor, version)?;

        Ok(response)
    }
}
