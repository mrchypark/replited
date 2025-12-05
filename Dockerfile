FROM debian:bookworm-slim

ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY docker-context/replited-${TARGETARCH} /usr/local/bin/replited

RUN chmod +x /usr/local/bin/replited

ENTRYPOINT ["/usr/local/bin/replited"]
