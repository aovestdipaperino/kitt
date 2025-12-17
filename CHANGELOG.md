# Changelog

All notable changes to this project will be documented in this file.

## [0.2.0] - 2025-12-17

### Added

- CRC32-C verification for Kafka record batches in consumer
- `--messages-per-batch` option to batch multiple messages per record batch in producer
- `crc32c` dependency for CRC verification

### Changed

- Producer now supports sending multiple messages in a single record batch for improved throughput

## [0.1.0] - Initial Release

### Added

- Kafka throughput measurement tool
- Producer and consumer benchmarking
- Configurable message sizes and batch options
- Multi-threaded producer/consumer support
- LED animation and audio controls
- Sticky partition assignment
- Random key generation strategies
