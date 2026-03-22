#!/bin/bash
# Run all benchmarks

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

cleanup_replited() {
  pkill -f 'target/release/replited' >/dev/null 2>&1 || true
  sleep 1
}

# Build release binary
echo "Building release binary..."
cargo build --release --manifest-path ../../Cargo.toml

# Create output directory
mkdir -p results

echo "JSON summaries are written to tests/benchmarks/results/."
echo "Transient DB/log artifacts are recreated under tests/output/benchmarks/ or local benchmark cwd directories."

echo ""
echo "=== Running Throughput Benchmark ==="
python3 throughput_bench.py -n 300 -p 1024,10240 -o results/throughput_results.json
cleanup_replited

echo ""
echo "=== Running Latency Benchmark ==="
python3 latency_bench.py -n 50 -o results/latency_results.json
cleanup_replited

echo ""
echo "=== Running Archival Benchmark ==="
python3 archival_bench.py -n 2000 -p 4096 -o results/archival_results.json
cleanup_replited

echo ""
echo "All benchmarks complete! Results saved in results/"
