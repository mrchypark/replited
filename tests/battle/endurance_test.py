#!/usr/bin/env python3
"""
Endurance Test for Stream Replication

Long-running stability test that:
- Continuously writes data to Primary
- Monitors replica synchronization
- Tracks memory usage over time
- Verifies data integrity at intervals
"""

import os
import sys
import time
import sqlite3
import subprocess
import shutil
import hashlib
import argparse
import psutil
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
CONFIG_DIR = Path(__file__).parent.parent / "config"


@dataclass
class EnduranceStats:
    total_transactions: int
    total_bytes_written: int
    duration_seconds: float
    integrity_checks_passed: int
    integrity_checks_failed: int
    primary_max_rss_mb: float
    replica_max_rss_mb: float
    errors: list


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


def get_process_memory_mb(pid: int) -> float:
    """Get RSS memory usage in MB."""
    try:
        proc = psutil.Process(pid)
        return proc.memory_info().rss / (1024 * 1024)
    except:
        return 0.0


def compute_table_checksum(db_path: str, table: str = "endurance_test") -> Optional[str]:
    """Compute checksum of table data."""
    try:
        conn = sqlite3.connect(db_path, timeout=1.0)
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table} ORDER BY id")
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return None
        
        data = str(rows).encode()
        return hashlib.sha256(data).hexdigest()[:16]
    except Exception as e:
        return None


def run_endurance_test(duration_seconds: int, write_interval_ms: int = 100) -> EnduranceStats:
    """Run endurance test for specified duration."""
    cleanup()
    
    stats = EnduranceStats(
        total_transactions=0,
        total_bytes_written=0,
        duration_seconds=0,
        integrity_checks_passed=0,
        integrity_checks_failed=0,
        primary_max_rss_mb=0,
        replica_max_rss_mb=0,
        errors=[]
    )
    
    # Start Primary
    primary_log = open("logs/primary_endurance.log", "w")
    primary = subprocess.Popen(
        [str(REPLITED_BIN), "--config", str(CONFIG_DIR / "benchmark_primary.toml"), "replicate"],
        stdout=primary_log,
        stderr=subprocess.STDOUT
    )
    time.sleep(2)
    
    # Initialize Primary database
    conn = sqlite3.connect("primary.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS endurance_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER,
            data BLOB,
            checksum TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # Do initial writes so replica has something to restore
    payload = b'x' * 1024  # 1KB payload
    for i in range(10):
        checksum = hashlib.md5(payload).hexdigest()
        cursor.execute("INSERT INTO endurance_test (batch_id, data, checksum) VALUES (?, ?, ?)",
                      (0, payload, checksum))
    conn.commit()
    time.sleep(1)
    
    # Prepare and start Replica
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
    
    time.sleep(3)  # Wait for replica to start
    
    start_time = time.time()
    last_integrity_check = start_time
    batch_id = 1
    write_interval = write_interval_ms / 1000.0
    
    print(f"Starting endurance test for {duration_seconds} seconds...")
    print(f"Write interval: {write_interval_ms}ms")
    
    try:
        while time.time() - start_time < duration_seconds:
            # Write batch
            try:
                payload = os.urandom(1024)  # Random 1KB
                checksum = hashlib.md5(payload).hexdigest()
                cursor.execute(
                    "INSERT INTO endurance_test (batch_id, data, checksum) VALUES (?, ?, ?)",
                    (batch_id, payload, checksum)
                )
                conn.commit()
                stats.total_transactions += 1
                stats.total_bytes_written += len(payload)
            except Exception as e:
                stats.errors.append(f"Write error: {e}")
            
            # Track memory
            primary_rss = get_process_memory_mb(primary.pid)
            replica_rss = get_process_memory_mb(replica.pid)
            stats.primary_max_rss_mb = max(stats.primary_max_rss_mb, primary_rss)
            stats.replica_max_rss_mb = max(stats.replica_max_rss_mb, replica_rss)
            
            # Integrity check every 30 seconds
            if time.time() - last_integrity_check >= 30:
                primary_checksum = compute_table_checksum("primary.db")
                replica_checksum = compute_table_checksum(str(replica_cwd / "replica.db"))
                
                if primary_checksum and replica_checksum:
                    if primary_checksum == replica_checksum:
                        stats.integrity_checks_passed += 1
                    else:
                        # Give replica time to catch up, then recheck
                        time.sleep(2)
                        replica_checksum = compute_table_checksum(str(replica_cwd / "replica.db"))
                        if primary_checksum == replica_checksum:
                            stats.integrity_checks_passed += 1
                        else:
                            stats.integrity_checks_failed += 1
                            stats.errors.append(f"Checksum mismatch at batch {batch_id}")
                
                last_integrity_check = time.time()
                elapsed = time.time() - start_time
                print(f"Progress: {elapsed:.0f}s / {duration_seconds}s "
                      f"| Txns: {stats.total_transactions} "
                      f"| Primary RSS: {primary_rss:.1f}MB "
                      f"| Replica RSS: {replica_rss:.1f}MB")
            
            batch_id += 1
            time.sleep(write_interval)
        
        stats.duration_seconds = time.time() - start_time
        
    finally:
        conn.close()
        primary.terminate()
        replica.terminate()
        primary.wait()
        replica.wait()
        primary_log.close()
        replica_log.close()
    
    return stats


def print_report(stats: EnduranceStats):
    """Print endurance test report."""
    print("\n" + "=" * 60)
    print("ENDURANCE TEST REPORT")
    print("=" * 60)
    print(f"Duration:              {stats.duration_seconds:.1f} seconds")
    print(f"Total Transactions:    {stats.total_transactions}")
    print(f"Total Data Written:    {stats.total_bytes_written / (1024*1024):.2f} MB")
    print(f"Avg TPS:               {stats.total_transactions / stats.duration_seconds:.2f}")
    print(f"Integrity Checks OK:   {stats.integrity_checks_passed}")
    print(f"Integrity Checks FAIL: {stats.integrity_checks_failed}")
    print(f"Primary Max RSS:       {stats.primary_max_rss_mb:.1f} MB")
    print(f"Replica Max RSS:       {stats.replica_max_rss_mb:.1f} MB")
    print(f"Errors:                {len(stats.errors)}")
    
    if stats.errors:
        print("\nError Details:")
        for err in stats.errors[:10]:  # Show first 10 errors
            print(f"  - {err}")
    
    print("=" * 60)
    
    if stats.integrity_checks_failed == 0 and len(stats.errors) == 0:
        print("RESULT: PASS ✓")
        return True
    else:
        print("RESULT: FAIL ✗")
        return False


def main():
    parser = argparse.ArgumentParser(description="Endurance test for stream replication")
    parser.add_argument("-d", "--duration", type=int, default=300,
                        help="Test duration in seconds (default: 300 = 5 minutes)")
    parser.add_argument("-i", "--interval", type=int, default=100,
                        help="Write interval in milliseconds (default: 100)")
    
    args = parser.parse_args()
    
    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)
    
    stats = run_endurance_test(args.duration, args.interval)
    success = print_report(stats)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
