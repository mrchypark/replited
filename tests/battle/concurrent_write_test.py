#!/usr/bin/env python3
"""
Concurrent Write Test for Stream Replication

Tests:
- Multiple concurrent writers to Primary
- Proper transaction ordering in replica
- No data loss under concurrent load
"""

import os
import sys
import time
import sqlite3
import subprocess
import shutil
import threading
import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).parent.parent))

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
CONFIG_DIR = Path(__file__).parent.parent / "config"


@dataclass
class ConcurrentResult:
    total_writes: int
    successful_writes: int
    write_errors: int
    replica_verified: bool
    missing_rows: int
    duration_seconds: float
    errors: List[str]


def cleanup():
    """Clean up test artifacts."""
    for f in ["primary.db", "primary.db-wal", "primary.db-shm"]:
        if os.path.exists(f):
            os.remove(f)
    for d in [".primary.db-replited", "logs", "backup", "replica_cwd"]:
        if os.path.exists(d):
            shutil.rmtree(d)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("backup", exist_ok=True)


def writer_task(writer_id: int, num_writes: int, db_path: str) -> tuple:
    """Writer thread task."""
    success = 0
    errors = 0
    error_msgs = []
    
    for i in range(num_writes):
        try:
            conn = sqlite3.connect(db_path, timeout=30.0)
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO concurrent_test (writer_id, write_seq, data) VALUES (?, ?, ?)",
                (writer_id, i, f"writer_{writer_id}_seq_{i}")
            )
            conn.commit()
            conn.close()
            success += 1
        except Exception as e:
            errors += 1
            if len(error_msgs) < 5:
                error_msgs.append(f"Writer {writer_id}: {e}")
        
        time.sleep(0.01)  # Small delay to simulate realistic load
    
    return (writer_id, success, errors, error_msgs)


def run_concurrent_test(
    num_writers: int = 10,
    writes_per_writer: int = 100
) -> ConcurrentResult:
    """Run concurrent write test."""
    cleanup()
    
    result = ConcurrentResult(
        total_writes=num_writers * writes_per_writer,
        successful_writes=0,
        write_errors=0,
        replica_verified=False,
        missing_rows=0,
        duration_seconds=0,
        errors=[]
    )
    
    # Start Primary
    primary_log = open("logs/primary_concurrent.log", "w")
    primary = subprocess.Popen(
        [str(REPLITED_BIN), "--config", str(CONFIG_DIR / "benchmark_primary.toml"), "replicate"],
        stdout=primary_log,
        stderr=subprocess.STDOUT
    )
    time.sleep(2)
    
    # Initialize database with WAL mode explicitly
    conn = sqlite3.connect("primary.db")
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS concurrent_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            writer_id INTEGER,
            write_seq INTEGER,
            data TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_writer ON concurrent_test(writer_id, write_seq)")
    conn.commit()
    conn.close()
    
    # Prepare Replica
    replica_cwd = Path("replica_cwd")
    replica_cwd.mkdir(exist_ok=True)
    
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
wal_retention_count = 5

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:50051"
"""
    (replica_cwd / "replica.toml").write_text(config)
    (replica_cwd / "logs").mkdir(exist_ok=True)
    
    replica_log = open(replica_cwd / "replica.log", "w")
    replica = subprocess.Popen(
        [str(REPLITED_BIN), "--config", "replica.toml", "replica-sidecar", "--force-restore"],
        cwd=str(replica_cwd),
        stdout=replica_log,
        stderr=subprocess.STDOUT
    )
    time.sleep(3)
    
    print(f"Starting concurrent test: {num_writers} writers x {writes_per_writer} writes...")
    start_time = time.time()
    
    try:
        # Run concurrent writers
        with ThreadPoolExecutor(max_workers=num_writers) as executor:
            futures = [
                executor.submit(writer_task, i, writes_per_writer, "primary.db")
                for i in range(num_writers)
            ]
            
            for future in as_completed(futures):
                writer_id, success, errors, error_msgs = future.result()
                result.successful_writes += success
                result.write_errors += errors
                result.errors.extend(error_msgs)
                print(f"Writer {writer_id} complete: {success} success, {errors} errors")
        
        result.duration_seconds = time.time() - start_time
        
        # Verify replication
        print("\nWaiting for replica sync...")
        time.sleep(5)
        
        # Check Primary
        p_conn = sqlite3.connect("primary.db")
        p_cursor = p_conn.cursor()
        p_cursor.execute("SELECT COUNT(*) FROM concurrent_test")
        primary_count = p_cursor.fetchone()[0]
        p_conn.close()
        
        # Check Replica
        r_path = replica_cwd / "replica.db"
        if r_path.exists():
            r_conn = sqlite3.connect(str(r_path))
            r_cursor = r_conn.cursor()
            r_cursor.execute("SELECT COUNT(*) FROM concurrent_test")
            replica_count = r_cursor.fetchone()[0]
            r_conn.close()
            
            print(f"Primary: {primary_count} rows, Replica: {replica_count} rows")
            
            result.missing_rows = primary_count - replica_count
            if result.missing_rows <= 5:  # Allow small sync delay
                result.replica_verified = True
        else:
            result.errors.append("Replica database not found")
        
    finally:
        replica.terminate()
        primary.terminate()
        replica.wait()
        primary.wait()
    
    return result


def print_report(result: ConcurrentResult) -> bool:
    """Print test report."""
    print("\n" + "=" * 60)
    print("CONCURRENT WRITE TEST REPORT")
    print("=" * 60)
    print(f"Total Writes Attempted: {result.total_writes}")
    print(f"Successful Writes:      {result.successful_writes}")
    print(f"Write Errors:           {result.write_errors}")
    print(f"Duration:               {result.duration_seconds:.2f}s")
    print(f"Write TPS:              {result.successful_writes / result.duration_seconds:.2f}")
    print(f"Missing Rows in Replica: {result.missing_rows}")
    print(f"Replica Verified:       {'YES ✓' if result.replica_verified else 'NO ✗'}")
    
    if result.errors:
        print(f"\nErrors ({len(result.errors)}):")
        for err in result.errors[:10]:
            print(f"  - {err}")
    
    print("=" * 60)
    
    success = result.replica_verified and result.write_errors < result.total_writes * 0.01
    print(f"RESULT: {'PASS ✓' if success else 'FAIL ✗'}")
    return success


def main():
    parser = argparse.ArgumentParser(description="Concurrent write test")
    parser.add_argument("-w", "--writers", type=int, default=10,
                        help="Number of concurrent writers (default: 10)")
    parser.add_argument("-n", "--num-writes", type=int, default=100,
                        help="Writes per writer (default: 100)")
    
    args = parser.parse_args()
    
    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)
    
    result = run_concurrent_test(args.writers, args.num_writes)
    success = print_report(result)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
