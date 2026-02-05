# Build replited
FROM rust:1.81-slim-bookworm AS builder
WORKDIR /usr/src/replited
COPY . .
# Install protoc for tonic-build
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
# Build replited with release profile
RUN cargo build --release --bin replited

# Extract TrailBase from the official image
FROM trailbase/trailbase:latest AS trailbase

# Final Stage: Combine them
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy binaries
COPY --from=trailbase /app/trail /app/trail
COPY --from=builder /usr/src/replited/target/release/replited /usr/local/bin/replited

# Create data directory
RUN mkdir -p /app/traildepot

WORKDIR /app

ENTRYPOINT ["/usr/local/bin/replited"]
CMD ["replica-sidecar", "--help"]
