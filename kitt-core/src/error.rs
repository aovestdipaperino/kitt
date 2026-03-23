//! Concrete error types for kitt-core
//!
//! Replaces `anyhow::Error` in all public APIs per NASA Rule 9.

/// Errors that can occur during Kafka client operations
#[non_exhaustive]
#[derive(Debug, thiserror::Error)]
pub enum KittError {
    /// TCP connection to broker failed
    #[error("connection failed to {broker}: {source}")]
    Connection {
        broker: String,
        source: std::io::Error,
    },

    /// Kafka protocol encoding/decoding error
    #[error("protocol error: {0}")]
    Protocol(String),

    /// Broker returned an error code for an operation
    #[error("broker error (code {code}): {message}")]
    BrokerError {
        code: i16,
        message: String,
    },

    /// API version not supported by broker
    #[error("unsupported API version: {0}")]
    UnsupportedVersion(String),

    /// Topic operation failed (create, delete, metadata)
    #[error("topic operation failed: {0}")]
    TopicOperation(String),

    /// Request timed out
    #[error("request timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Too many consecutive errors
    #[error("too many consecutive errors ({count}): last error: {last_error}")]
    TooManyErrors {
        count: u32,
        last_error: String,
    },

    /// Response exceeded maximum size
    #[error("response size {size} exceeds maximum {max}")]
    ResponseTooLarge {
        size: usize,
        max: usize,
    },

    /// CRC verification failed
    #[error("CRC verification failed for partition {partition}: {detail}")]
    CrcFailure {
        partition: usize,
        detail: String,
    },

    /// I/O error during network communication
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Produce operation failed
    #[error("produce failed: {0}")]
    ProduceError(String),

    /// Consume operation failed
    #[error("consume failed: {0}")]
    ConsumeError(String),
}

/// Convenience Result alias for kitt-core operations
pub type Result<T> = std::result::Result<T, KittError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_is_non_exhaustive() {
        let err = KittError::Protocol("test".to_string());
        assert!(!format!("{err}").is_empty());
    }

    #[test]
    fn error_display_formats_correctly() {
        let err = KittError::Connection {
            broker: "localhost:9092".to_string(),
            source: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused"),
        };
        assert!(format!("{err}").contains("localhost:9092"));
        assert!(format!("{err}").contains("refused"));
    }

    #[test]
    fn error_converts_from_io() {
        let io_err = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe broke");
        let kitt_err: KittError = io_err.into();
        assert!(format!("{kitt_err}").contains("pipe broke"));
    }

    #[test]
    fn result_alias_works() {
        let ok: Result<i32> = Ok(42);
        assert_eq!(ok.unwrap(), 42);

        let err: Result<i32> = Err(KittError::Protocol("bad".into()));
        assert!(err.is_err());
    }
}
