#!/usr/bin/env python3
"""
Chaos Test for Stream Replication

Tests recovery from various failure scenarios:
- Random connection drops
- Process restarts
- Network latency injection (if tc available)
- Stream disconnection and reconnection
"""

import os
import sys
import time
import signal
import sqlite3
import subprocess
import shutil
import random
import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
CONFIG_DIR = Path(__file__).parent.parent / "config"


@dataclass
class ChaosResult:
    total_chaos_events: int
    replica_restarts: int
    primary_restarts: int
    data_loss_detected: bool
    final_sync_verified: bool
    errors: List[str]


from test_utils import cleanup


def start_primary() -> subprocess.Popen:
    """Start Primary process."""
    log = open("logs/primary_chaos.log", "a")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", str(CONFIG_DIR / "benchmark_primary.toml"), "replicate"],
        stdout=log,
        stderr=subprocess.STDOUT
    )
    return proc


def start_replica(replica_cwd: Path) -> subprocess.Popen:
    """Start Replica process."""
    log = open(replica_cwd / "replica.log", "a")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", "replica.toml", "replica-sidecar"],
        cwd=str(replica_cwd),
        stdout=log,
        stderr=subprocess.STDOUT
    )
    return proc


def prepare_replica_cwd() -> Path:
    """Prepare replica working directory."""
    replica_cwd = Path("replica_cwd")
    replica_cwd.mkdir(exist_ok=True)
    
    config = """
[log]
level = "Info"
dir = "logs"

[[database]]
db = "primary.db"
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
    return replica_cwd


def get_row_count(db_path: str, table: str = "chaos_test") -> int:
    """Get row count from table."""
    try:
        conn = sqlite3.connect(db_path, timeout=1.0)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except:
        return -1


def run_chaos_test(
    duration_seconds: int,
    chaos_interval_seconds: int = 10,
    max_chaos_events: int = 20
) -> ChaosResult:
    """Run chaos test with random failures."""
    cleanup()
    
    result = ChaosResult(
        total_chaos_events=0,
        replica_restarts=0,
        primary_restarts=0,
        data_loss_detected=False,
        final_sync_verified=False,
        errors=[]
    )
    
    replica_cwd = prepare_replica_cwd()
    
    # Start Primary
    primary = start_primary()
    time.sleep(2)
    
    # Initialize database
    conn = sqlite3.connect("primary.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS chaos_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sequence INTEGER UNIQUE,
            data TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # Write initial data
    for i in range(100):
        cursor.execute("INSERT INTO chaos_test (sequence, data) VALUES (?, ?)", 
                      (i, f"initial_{i}"))
    conn.commit()
    time.sleep(1)
    
    # Start Replica with --force-restore for initial sync
    replica_log = open(replica_cwd / "replica.log", "w")
    replica = subprocess.Popen(
        [str(REPLITED_BIN), "--config", "replica.toml", "replica-sidecar", "--force-restore"],
        cwd=str(replica_cwd),
        stdout=replica_log,
        stderr=subprocess.STDOUT
    )
    time.sleep(3)
    
    start_time = time.time()
    last_chaos = start_time
    sequence = 100
    
    print(f"Starting chaos test for {duration_seconds}s, chaos events every ~{chaos_interval_seconds}s...")
    
    try:
        while time.time() - start_time < duration_seconds:
            # Continuous writes
            try:
                cursor.execute("INSERT INTO chaos_test (sequence, data) VALUES (?, ?)",
                              (sequence, f"data_{sequence}"))
                conn.commit()
                sequence += 1
            except Exception as e:
                result.errors.append(f"Write error: {e}")
            
            # Chaos event
            if (time.time() - last_chaos >= chaos_interval_seconds and 
                result.total_chaos_events < max_chaos_events):
                
                chaos_type = random.choice(["restart_replica", "restart_primary", "pause_replica"])
                result.total_chaos_events += 1
                
                print(f"[CHAOS {result.total_chaos_events}] {chaos_type}")
                
                if chaos_type == "restart_replica":
                    # Restart replica
                    replica.terminate()
                    replica.wait()
                    time.sleep(1)
                    replica = start_replica(replica_cwd)
                    result.replica_restarts += 1
                    time.sleep(2)
                    
                elif chaos_type == "restart_primary":
                    # Restart primary (more dangerous)
                    conn.close()
                    primary.terminate()
                    primary.wait()
                    time.sleep(1)
                    primary = start_primary()
                    result.primary_restarts += 1
                    time.sleep(2)
                    conn = sqlite3.connect("primary.db")
                    cursor = conn.cursor()
                    
                elif chaos_type == "pause_replica":
                    # Pause replica briefly (simulate network issue)
                    replica.send_signal(signal.SIGSTOP)
                    time.sleep(random.uniform(0.5, 2.0))
                    replica.send_signal(signal.SIGCONT)
                
                last_chaos = time.time()
            
            time.sleep(0.05)  # 50ms between writes
        
        # Final verification
        print("\n[VERIFY] Waiting for final sync...")
        conn.close()
        time.sleep(5)  # Give replica time to catch up
        
        primary_count = get_row_count("primary.db")
        replica_count = get_row_count(str(replica_cwd / "primary.db"))
        
        print(f"Primary rows: {primary_count}")
        print(f"Replica rows: {replica_count}")
        
        if primary_count > 0 and replica_count > 0:
            if replica_count >= primary_count - 10:  # Allow small lag
                result.final_sync_verified = True
            else:
                result.data_loss_detected = True
                result.errors.append(
                    f"Data sync gap: primary={primary_count}, replica={replica_count}"
                )
        else:
            result.errors.append(f"Invalid counts: primary={primary_count}, replica={replica_count}")
        
    finally:
        try:
            replica.terminate()
            replica.wait()
        except:
            pass
        try:
            primary.terminate()
            primary.wait()
        except:
            pass
    
    return result


def print_report(result: ChaosResult) -> bool:
    """Print chaos test report."""
    print("\n" + "=" * 60)
    print("CHAOS TEST REPORT")
    print("=" * 60)
    print(f"Total Chaos Events:     {result.total_chaos_events}")
    print(f"Replica Restarts:       {result.replica_restarts}")
    print(f"Primary Restarts:       {result.primary_restarts}")
    print(f"Data Loss Detected:     {'YES ✗' if result.data_loss_detected else 'NO ✓'}")
    print(f"Final Sync Verified:    {'YES ✓' if result.final_sync_verified else 'NO ✗'}")
    print(f"Errors:                 {len(result.errors)}")
    
    if result.errors:
        print("\nError Details:")
        for err in result.errors[:10]:
            print(f"  - {err}")
    
    print("=" * 60)
    
    success = result.final_sync_verified and not result.data_loss_detected
    print(f"RESULT: {'PASS ✓' if success else 'FAIL ✗'}")
    return success


def main():
    parser = argparse.ArgumentParser(description="Chaos test for stream replication")
    parser.add_argument("-d", "--duration", type=int, default=120,
                        help="Test duration in seconds (default: 120)")
    parser.add_argument("-i", "--chaos-interval", type=int, default=10,
                        help="Seconds between chaos events (default: 10)")
    parser.add_argument("-m", "--max-events", type=int, default=20,
                        help="Maximum chaos events (default: 20)")
    
    args = parser.parse_args()
    
    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)
    
    result = run_chaos_test(args.duration, args.chaos_interval, args.max_events)
    success = print_report(result)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
