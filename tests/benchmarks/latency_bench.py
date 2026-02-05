#!/usr/bin/env python3
"""
Latency Benchmark for Stream Replication

Measures:
- Replication latency (time from Primary write to Replica visibility)
- P50, P90, P99 percentiles
- Latency histogram
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
from typing import List, Tuple
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
CONFIG_DIR = Path(__file__).parent.parent / "config"


@dataclass
class LatencyResult:
    sample_count: int
    min_ms: float
    max_ms: float
    mean_ms: float
    median_ms: float
    p90_ms: float
    p95_ms: float
    p99_ms: float
    std_dev_ms: float


def cleanup():
    """Clean up test artifacts."""
    for f in ["primary.db", "primary.db-wal", "primary.db-shm",
              "replica.db", "replica.db-wal", "replica.db-shm"]:
        if os.path.exists(f):
            os.remove(f)
    for d in [".primary.db-replited", ".replica.db-replited", "logs", "backup", "replica_cwd"]:
        if os.path.exists(d):
            shutil.rmtree(d)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("backup", exist_ok=True)


def start_primary() -> subprocess.Popen:
    """Start the Primary replited process."""
    log = open("logs/primary_latency.log", "w")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", str(CONFIG_DIR / "benchmark_primary.toml"), "replicate"],
        stdout=log,
        stderr=subprocess.STDOUT
    )
    time.sleep(3)
    return proc


def prepare_replica() -> Tuple[subprocess.Popen, Path]:
    """Prepare and start replica."""
    replica_cwd = Path("replica_cwd")
    replica_cwd.mkdir(exist_ok=True)
    
    # Create replica config
    config = """
[log]
level = "Info"
dir = "logs"

[[database]]
db = "replica.db"
min_checkpoint_page_number = 100
max_checkpoint_page_number = 1000
truncate_page_number = 50000
checkpoint_interval_secs = 30
apply_checkpoint_frame_interval = 5
apply_checkpoint_interval_ms = 50
wal_retention_count = 5

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:50051"
remote_db_name = "primary.db"
"""
    (replica_cwd / "replica.toml").write_text(config)
    (replica_cwd / "logs").mkdir(exist_ok=True)
    
    log = open(replica_cwd / "replica.log", "w")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", "replica.toml", "replica-sidecar", "--force-restore"],
        cwd=str(replica_cwd),
        stdout=log,
        stderr=subprocess.STDOUT
    )
    return proc, replica_cwd


def measure_replication_latency(num_samples: int = 100, warmup: int = 10) -> LatencyResult:
    """Measure replication latency by writing to Primary and polling Replica."""
    cleanup()
    
    primary = start_primary()
    
    try:
        # Initialize Primary database
        conn = sqlite3.connect("primary.db", timeout=1.0)
        conn.execute("PRAGMA busy_timeout=5000")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS latency_test (
                id INTEGER PRIMARY KEY,
                write_ts_ns INTEGER
            )
        """)
        conn.commit()
        
        # Do warmup writes before starting replica
        for i in range(warmup):
            ts = time.time_ns()
            cursor.execute("INSERT INTO latency_test (id, write_ts_ns) VALUES (?, ?)", (i, ts))
            conn.commit()
        
        time.sleep(1)  # Let WAL sync
        
        # Start replica
        replica, replica_cwd = prepare_replica()
        time.sleep(3)  # Wait for replica to initialize and restore
        
        latencies_ms = []
        
        # Measure latencies
        for i in range(num_samples):
            sample_id = warmup + i
            write_ts = time.time_ns()
            
            cursor.execute("INSERT INTO latency_test (id, write_ts_ns) VALUES (?, ?)", 
                          (sample_id, write_ts))
            conn.commit()
            
            # Poll replica until we see the write
            start_poll = time.time()
            timeout = 5.0  # 5 second timeout
            replica_db = replica_cwd / "replica.db"
            
            while time.time() - start_poll < timeout:
                if replica_db.exists():
                    try:
                        r_conn = sqlite3.connect(str(replica_db), timeout=0.1)
                        r_cursor = r_conn.cursor()
                        r_cursor.execute("SELECT write_ts_ns FROM latency_test WHERE id = ?", (sample_id,))
                        row = r_cursor.fetchone()
                        r_conn.close()
                        
                        if row:
                            observed_ts = time.time_ns()
                            latency_ms = (observed_ts - write_ts) / 1_000_000
                            latencies_ms.append(latency_ms)
                            break
                    except Exception:
                        pass
                
                time.sleep(0.001)  # 1ms poll interval
            else:
                print(f"Warning: Sample {sample_id} timed out")
            
            if (i + 1) % 20 == 0:
                print(f"Progress: {i + 1}/{num_samples} samples")
        
        conn.close()
        replica.terminate()
        replica.wait()
        
        if not latencies_ms:
            raise RuntimeError("No latency samples collected")
        
        latencies_ms.sort()
        n = len(latencies_ms)
        
        return LatencyResult(
            sample_count=n,
            min_ms=min(latencies_ms),
            max_ms=max(latencies_ms),
            mean_ms=statistics.mean(latencies_ms),
            median_ms=statistics.median(latencies_ms),
            p90_ms=latencies_ms[int(n * 0.90)] if n > 10 else latencies_ms[-1],
            p95_ms=latencies_ms[int(n * 0.95)] if n > 20 else latencies_ms[-1],
            p99_ms=latencies_ms[int(n * 0.99)] if n > 100 else latencies_ms[-1],
            std_dev_ms=statistics.stdev(latencies_ms) if n > 1 else 0.0
        )
        
    finally:
        primary.terminate()
        primary.wait()


def print_report(result: LatencyResult):
    """Print formatted latency report."""
    print("\n" + "=" * 50)
    print("LATENCY BENCHMARK REPORT")
    print("=" * 50)
    print(f"Samples:     {result.sample_count}")
    print(f"Min:         {result.min_ms:.2f} ms")
    print(f"Max:         {result.max_ms:.2f} ms")
    print(f"Mean:        {result.mean_ms:.2f} ms")
    print(f"Median:      {result.median_ms:.2f} ms")
    print(f"P90:         {result.p90_ms:.2f} ms")
    print(f"P95:         {result.p95_ms:.2f} ms")
    print(f"P99:         {result.p99_ms:.2f} ms")
    print(f"Std Dev:     {result.std_dev_ms:.2f} ms")
    print("=" * 50)


def save_results(result: LatencyResult, output_file: str = "latency_results.json"):
    """Save results to JSON file."""
    data = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "results": {
            "sample_count": result.sample_count,
            "min_ms": result.min_ms,
            "max_ms": result.max_ms,
            "mean_ms": result.mean_ms,
            "median_ms": result.median_ms,
            "p90_ms": result.p90_ms,
            "p95_ms": result.p95_ms,
            "p99_ms": result.p99_ms,
            "std_dev_ms": result.std_dev_ms
        }
    }
    
    with open(output_file, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"\nResults saved to {output_file}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Latency benchmark for stream replication")
    parser.add_argument("-n", "--num-samples", type=int, default=100,
                        help="Number of latency samples (default: 100)")
    parser.add_argument("-w", "--warmup", type=int, default=10,
                        help="Warmup transactions (default: 10)")
    parser.add_argument("-o", "--output", type=str, default="latency_results.json",
                        help="Output JSON file (default: latency_results.json)")
    
    args = parser.parse_args()
    
    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)
    
    print(f"Running latency benchmark with {args.num_samples} samples...")
    
    result = measure_replication_latency(args.num_samples, args.warmup)
    print_report(result)
    save_results(result, args.output)


if __name__ == "__main__":
    main()
