# Build replited
FROM rust:1.81-slim-bookworm AS builder
WORKDIR /usr/src/replited
COPY . .
# Install protoc for tonic-build
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
# Build replited with release profile
RUN cargo build --release --bin replited

# Extract PocketBase from the official/custom image
FROM ghcr.io/muchobien/pocketbase:0.35.0 AS pocketbase

# Final Stage: Combine them
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy binaries
COPY --from=pocketbase /usr/local/bin/pocketbase /usr/local/bin/pocketbase
COPY --from=builder /usr/src/replited/target/release/replited /usr/local/bin/replited

# Create data directory
RUN mkdir -p /pb_data

ENTRYPOINT ["/usr/local/bin/replited"]
CMD ["replica-sidecar", "--help"]
