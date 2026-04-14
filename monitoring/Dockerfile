# Multi-stage build for Rust application
FROM rust:1.88-slim as builder

WORKDIR /build

# Install dependencies for building
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the ttn-ingest project
COPY ttn-ingest/Cargo.toml ./Cargo.toml
COPY ttn-ingest/src ./src

# Build the application in release mode
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 ingestor

# State directory for the TTN cursor file (see TTN_CURSOR_FILE).
RUN mkdir -p /var/lib/ttn-ingest && chown ingestor:ingestor /var/lib/ttn-ingest
VOLUME ["/var/lib/ttn-ingest"]

# Copy the built binary from builder stage
COPY --from=builder /build/target/release/ttn-ingest /usr/local/bin/ttn-ingest

# Switch to non-root user
USER ingestor

# Run the binary
CMD ["ttn-ingest"]
