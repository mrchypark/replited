#!/bin/bash
# Run all benchmarks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Build release binary
echo "Building release binary..."
cargo build --release --manifest-path ../../Cargo.toml

# Create output directory
mkdir -p results

echo ""
echo "=== Running Throughput Benchmark ==="
python3 throughput_bench.py -n 500 -o results/throughput_results.json

echo ""
echo "=== Running Latency Benchmark ==="
python3 latency_bench.py -n 50 -o results/latency_results.json

echo ""
echo "All benchmarks complete! Results saved in results/"
