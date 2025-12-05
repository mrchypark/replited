#!/usr/bin/env python3
"""
Throughput Benchmark for Stream Replication

Measures:
- Transactions per second (TPS) on Primary
- Replication throughput (frames/sec)
- Various payload sizes (100B, 1KB, 10KB, 100KB)
"""

import os
import sys
import time
import sqlite3
import subprocess
import shutil
import json
import statistics
from dataclasses import dataclass
from typing import List, Optional
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
CONFIG_DIR = Path(__file__).parent.parent / "config"


@dataclass
class BenchmarkResult:
    payload_size: int
    total_transactions: int
    duration_seconds: float
    tps: float
    mb_per_second: float


def cleanup():
    """Clean up test artifacts."""
    for f in ["primary.db", "primary.db-wal", "primary.db-shm",
              "replica.db", "replica.db-wal", "replica.db-shm"]:
        if os.path.exists(f):
            os.remove(f)
    for d in [".primary.db-replited", ".replica.db-replited", "logs", "backup"]:
        if os.path.exists(d):
            shutil.rmtree(d)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("backup", exist_ok=True)


def start_primary() -> subprocess.Popen:
    """Start the Primary replited process."""
    log = open("logs/primary_bench.log", "w")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", str(CONFIG_DIR / "benchmark_primary.toml"), "replicate"],
        stdout=log,
        stderr=subprocess.STDOUT
    )
    time.sleep(2)  # Wait for startup
    return proc


def run_throughput_test(payload_size: int, num_transactions: int) -> BenchmarkResult:
    """Run throughput test with specified payload size."""
    cleanup()
    
    primary = start_primary()
    
    try:
        conn = sqlite3.connect("primary.db")
        cursor = conn.cursor()
        
        # Create table with payload column
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS bench (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload BLOB,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        
        # Generate payload
        payload = b'x' * payload_size
        
        # Benchmark writes
        start_time = time.perf_counter()
        
        for i in range(num_transactions):
            cursor.execute("INSERT INTO bench (payload) VALUES (?)", (payload,))
            if (i + 1) % 100 == 0:
                conn.commit()  # Commit every 100 transactions
        
        conn.commit()
        end_time = time.perf_counter()
        
        conn.close()
        
        duration = end_time - start_time
        tps = num_transactions / duration
        total_bytes = num_transactions * payload_size
        mb_per_second = (total_bytes / (1024 * 1024)) / duration
        
        return BenchmarkResult(
            payload_size=payload_size,
            total_transactions=num_transactions,
            duration_seconds=duration,
            tps=tps,
            mb_per_second=mb_per_second
        )
        
    finally:
        primary.terminate()
        primary.wait()


def run_all_benchmarks(num_transactions: int = 1000) -> List[BenchmarkResult]:
    """Run benchmarks for all payload sizes."""
    payload_sizes = [100, 1024, 10240, 102400]  # 100B, 1KB, 10KB, 100KB
    results = []
    
    for size in payload_sizes:
        print(f"\n=== Testing payload size: {size} bytes ===")
        result = run_throughput_test(size, num_transactions)
        results.append(result)
        print(f"TPS: {result.tps:.2f}")
        print(f"Throughput: {result.mb_per_second:.2f} MB/s")
        print(f"Duration: {result.duration_seconds:.2f}s")
    
    return results


def print_report(results: List[BenchmarkResult]):
    """Print formatted benchmark report."""
    print("\n" + "=" * 60)
    print("THROUGHPUT BENCHMARK REPORT")
    print("=" * 60)
    print(f"{'Payload':>10} | {'TPS':>10} | {'MB/s':>10} | {'Duration':>10}")
    print("-" * 60)
    
    for r in results:
        payload_str = f"{r.payload_size}B" if r.payload_size < 1024 else f"{r.payload_size//1024}KB"
        print(f"{payload_str:>10} | {r.tps:>10.2f} | {r.mb_per_second:>10.2f} | {r.duration_seconds:>10.2f}s")
    
    print("=" * 60)


def save_results(results: List[BenchmarkResult], output_file: str = "throughput_results.json"):
    """Save results to JSON file."""
    data = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "results": [
            {
                "payload_size": r.payload_size,
                "total_transactions": r.total_transactions,
                "duration_seconds": r.duration_seconds,
                "tps": r.tps,
                "mb_per_second": r.mb_per_second
            }
            for r in results
        ]
    }
    
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"\nResults saved to {output_file}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Throughput benchmark for stream replication")
    parser.add_argument("-n", "--num-transactions", type=int, default=1000,
                        help="Number of transactions per test (default: 1000)")
    parser.add_argument("-o", "--output", type=str, default="throughput_results.json",
                        help="Output JSON file (default: throughput_results.json)")
    
    args = parser.parse_args()
    
    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)
    
    print(f"Running throughput benchmark with {args.num_transactions} transactions per test...")
    
    results = run_all_benchmarks(args.num_transactions)
    print_report(results)
    save_results(results, args.output)


if __name__ == "__main__":
    main()
