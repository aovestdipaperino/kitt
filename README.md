# Kitt - Kafka Implementation Throughput Tool

A high-performance Kafka throughput measurement tool that uses the `kafka-protocol` crate to directly connect to Kafka brokers and measure sustainable message throughput.

## Overview

Kitt creates a temporary Kafka topic and measures the maximum sustainable throughput in messages per second when producer and consumer are balanced (no backlog accumulation). It's designed to provide accurate throughput measurements by:

- Creating a random-named test topic with configurable partitions
- Supporting variable message sizes (fixed or range)
- Running a warmup period to stabilize performance
- Measuring sustained throughput over a specified duration
- Automatically cleaning up test topics

## Features

- **Direct Protocol Communication**: Uses `kafka-protocol` for efficient, low-level Kafka communication
- **Balanced Load Testing**: Ensures no backlog accumulation for realistic throughput measurements
- **Flexible Message Sizing**: Supports both fixed sizes and random ranges
- **Configurable Partitioning**: Test with configurable partitions per thread for parallel processing
- **Real-time Monitoring**: Shows throughput metrics every 5 seconds during testing
- **Automatic Cleanup**: Removes test topics after completion

## Installation

Build the tool from the project root:

```bash
cd kitt
cargo build --release
```

## Usage

### Basic Usage

```bash
# Test with default settings (localhost:9092, 2 partitions per thread, 1024 byte messages, 4 threads)
./target/release/kitt

# Test with custom broker
./target/release/kitt --broker kafka-broker:9092

# Test with multiple partitions per thread and threads
./target/release/kitt --partitions-per-thread 4 --threads 8

# Test with different message size
./target/release/kitt --message-size 4096
```

### Advanced Usage

```bash
# Test with variable message sizes (100-1000 bytes)
./target/release/kitt --message-size 100-1000

# Test with custom partitions per thread and thread count
./target/release/kitt --partitions-per-thread 3 --threads 4

# Full configuration example
./target/release/kitt \
  --broker localhost:9092 \
  --partitions-per-thread 4 \
  --threads 8 \
  --message-size 512-2048 \
  --measurement-secs 120
```

## Command Line Options

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--broker` | `-b` | `localhost:9092` | Kafka broker address |
| `--partitions-per-thread` | `-p` | `2` | Number of partitions assigned to each thread |
| `--message-size` | `-m` | `1024` | Message size in bytes or range (e.g., "100-1000") |
| `--threads` | `-t` | `4` | Number of producer/consumer threads |
| `--measurement-secs` | | `15` | Measurement duration in seconds |

## Message Size Formats

- **Fixed size**: `1024` - All messages will be exactly 1024 bytes
- **Range**: `100-1000` - Messages will be randomly sized between 100 and 1000 bytes

## Output Example
### Example Kitt Output

```
2025-07-20T10:00:59.893986Z  INFO kitt: Starting Kitt - Kafka Implementation Throughput Tool
2025-07-20T10:00:59.894008Z  INFO kitt: Broker: localhost:9092, Partitions per thread: 2, Total partitions: 8, Threads: 4, message size: Fixed(1024)
2025-07-20T10:00:59.894017Z  INFO kitt: Running for: 15s
2025-07-20T10:00:59.894025Z  INFO kitt: Test topic: kitt-test-c33ae558-715d-4ce7-98f7-c9ff7bb4d3fa
2025-07-20T10:01:00.109673Z  INFO kitt: Waiting for topic to be ready...
2025-07-20T10:01:03.121464Z  INFO kitt: Starting MEASUREMENT phase for 60 seconds

[░▓█▓░               ] 2847 msg/s (min: 2650, max: 2950)
[    ░▓█▓░           ] 3021 msg/s (min: 2650, max: 3021)
[        ░▓█▓░       ] 2956 msg/s (min: 2650, max: 3021)
[            ░▓█▓░   ] 3105 msg/s (min: 2650, max: 3105)
[                ░▓█▓] 3198 msg/s (min: 2650, max: 3198)
[            ░▓█▓░   ] 3156 msg/s (min: 2650, max: 3198)

Note: The LED pattern (░▓█▓░) appears in bright, saturated red colors, just like KITT from Knight Rider!

2025-07-20T10:03:47.716541Z  INFO kitt: MEASUREMENT completed - Final throughput: 3299.8 msg/s (min: 1767.1, max: 3742.8)
2025-07-20T10:03:47.716571Z  INFO kitt: === FINAL RESULTS ===
2025-07-20T10:03:47.716580Z  INFO kitt: Messages sent: 198966, Messages received: 197988, Throughput: 3299.8 messages/second (min: 1767.1 msg/s, max: 3742.8 msg/s)
2025-07-20T10:03:48.923291Z  INFO kitt::kafka_client: Successfully deleted topic 'kitt-test-d0d252ff-8f9e-4d63-b5c3-55b514d7024c'
2025-07-20T10:03:48.923345Z  INFO kitt: Kitt measurement completed successfully!
```

## How It Works

1. **Initialization**: Connects to the specified Kafka broker and creates a temporary test topic
2. **Multi-threaded Operations**: Starts multiple producer and consumer threads in parallel
   - Each thread is assigned exactly N partitions (no overlap between threads)
   - Total partitions = partitions_per_thread × number_of_threads
   - Each thread produces and consumes from its dedicated set of partitions
3. **Flow Control**: Producers monitor backlog (sent - received messages)
   - When backlog > 1000 messages: producers pause to let consumers catch up
   - When backlog < 1000 messages: producers resume at full speed
4. **Knight Rider Animation**: During testing, shows:
   - Animated bright red LED pattern moving left-to-right and back (just like KITT!)
   - Real-time throughput rate (msg/s)
   - Running minimum and maximum throughput achieved
5. **Natural Balance**: System automatically finds sustainable throughput where producers and consumers are balanced
6. **Results**: Reports final balanced throughput and backlog
7. **Cleanup**: Automatically deletes the test topic

## Requirements

- Rust 1.70+ (for building)
- Access to a running Kafka broker
- Sufficient permissions to create and delete topics on the broker

## Performance Considerations

- The tool aims to measure sustainable throughput, not peak burst capacity
- Backlog monitoring ensures consumers can keep up with producers across all threads
- Each thread gets dedicated partitions (no overlap) for true parallel processing
- **Backlog-based flow control**: Automatically maintains optimal throughput without manual tuning
- **Visual feedback**: Knight Rider animation provides engaging real-time monitoring
- Message size affects throughput - smaller messages typically achieve higher message rates
- Network latency and broker configuration significantly impact results

## Troubleshooting

### Connection Issues
- Verify the broker address is correct and accessible
- Check firewall settings and network connectivity
- Ensure the broker is running and accepting connections

### Permission Issues
- Verify your client has permissions to create and delete topics
- Check Kafka ACLs if security is enabled

### Low Throughput
- Try increasing the number of partitions per thread or number of threads
- Experiment with different message sizes
- Check broker configuration and resources
- Monitor network and disk I/O on the broker
