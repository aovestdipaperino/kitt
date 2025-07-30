# syntax=docker/dockerfile:1

# ---- Build Stage ----
FROM rust:1.88-alpine as builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache build-base pkgconfig openssl-dev

# Copy manifests and source
COPY ./Cargo.toml ./Cargo.lock ./
COPY ./kitt ./kitt
COPY ./kitt_throbbler ./kitt_throbbler

# Build the release binary
RUN cargo build --release

# ---- Runtime Stage ----
FROM alpine:3.18

# Install runtime dependencies
RUN apk add --no-cache libssl3 libgcc

WORKDIR /app

# Copy the compiled binary from the builder
COPY --from=builder /app/target/release/kitt /usr/local/bin/kitt

# Set default command
ENTRYPOINT ["kitt"]

# Example usage:
# docker build -t kitt:latest .
# docker run --rm kitt:latest --help
