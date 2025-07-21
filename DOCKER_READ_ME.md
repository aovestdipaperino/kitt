kitt/DOCKER_READ_ME.md
# Docker Instructions for Kitt

This guide explains how to build and run the Kitt Kafka throughput measurement tool using Docker.

---

## 1. Build the Docker Image

From the project root (where the `Dockerfile` is located):

```bash
docker build -t kitt:latest -f kitt/Dockerfile .
```

This command will:
- Use the official Rust Alpine image to build the binary
- Install necessary build dependencies
- Copy the built binary into a minimal Alpine runtime container

---

## 2. Run Kitt in Docker

You need to provide the Kafka broker address accessible from the Docker container. By default, Kitt connects to `localhost:9092`, but this may need to be changed depending on your environment.

### Example: Run with Default Settings

```bash
docker run --rm kitt:latest
```

### Example: Specify a Kafka Broker

```bash
docker run --rm kitt:latest --broker <KAFKA_BROKER_HOST>:9092
```

Replace `<KAFKA_BROKER_HOST>` with the hostname or IP address of your Kafka broker **as seen from inside the container**.

---

## 3. Advanced Usage

You can pass any command-line arguments supported by Kitt:

```bash
docker run --rm kitt:latest --broker kafka:9092 --partitions 8 --threads 8 --message-size 4096
```

See the main `README.md` for all available options.

---

## 4. Networking Tips

- If your Kafka broker is running on your host machine, you may need to use `host.docker.internal` (on Mac/Windows) or your host's IP address (on Linux) as the broker address.
- If running Kafka in Docker Compose, use the service name as the broker host.

---

## 5. Example with Docker Compose

If you have a Kafka broker running as a service named `kafka` in Docker Compose, you can run:

```bash
docker run --rm --network <your_docker_network> kitt:latest --broker kafka:9092
```

Replace `<your_docker_network>` with the name of your Docker network (e.g., `default`).

---

## 6. Clean Up

The container is run with `--rm` so it will be automatically removed after execution.

---

## 7. Troubleshooting

- Ensure the broker address is reachable from inside the container.
- Check your Kafka broker's network/firewall settings.
- For more help, see the main `README.md` or use `--help`:

```bash
docker run --rm kitt:latest --help
```

---

## 8. Dockerfile Details

The provided `Dockerfile`:
- Uses a multi-stage build to keep the final image small
- Builds with Rust 1.70 in an Alpine container
- Runs in a minimal Alpine container with only required runtime dependencies
- The build command must be run from the project root directory, not from inside the kitt directory

If you need to customize the Dockerfile, see the provided `Dockerfile` in this directory.

---

Happy benchmarking!