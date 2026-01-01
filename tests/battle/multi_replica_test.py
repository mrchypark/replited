#!/usr/bin/env python3
"""
Multi-Replica Test for Stream Replication

Tests:
- Multiple replicas connecting to single Primary
- All replicas receiving same data
- Data consistency across all replicas
"""

import os
import sys
import time
import sqlite3
import subprocess
import shutil
import argparse
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict

sys.path.insert(0, str(Path(__file__).parent.parent))

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
CONFIG_DIR = Path(__file__).parent.parent / "config"


@dataclass
class MultiReplicaResult:
    num_replicas: int
    total_writes: int
    replica_counts: Dict[int, int]
    all_synced: bool
    sync_differences: List[str]
    duration_seconds: float


from test_utils import cleanup






def create_replica(replica_id: int, port: int = 50051) -> tuple:
    """Create and start a replica in a separate directory.
    
    Replica runs in its own directory and uses Direct Snapshot Streaming.
    """
    replica_cwd = Path(f"replica_{replica_id}")
    replica_cwd.mkdir(exist_ok=True)
    (replica_cwd / "logs").mkdir(exist_ok=True)
    
    # Replica's db name must match Primary's db name for get_restore_config lookup
    config = f"""
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
addr = "http://127.0.0.1:{port}"
"""
    (replica_cwd / "replica.toml").write_text(config)
    
    log = open(replica_cwd / "replica.log", "w")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", "replica.toml", "replica-sidecar", "--force-restore"],
        cwd=str(replica_cwd),
        stdout=log,
        stderr=subprocess.STDOUT
    )
    return (proc, replica_cwd, log)




def get_row_count(db_path: str, table: str = "multi_replica_test") -> int:
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


def get_max_sequence(db_path: str, table: str = "multi_replica_test") -> int:
    """Get max sequence from table."""
    try:
        conn = sqlite3.connect(db_path, timeout=1.0)
        cursor = conn.cursor()
        cursor.execute(f"SELECT MAX(sequence) FROM {table}")
        result = cursor.fetchone()[0]
        conn.close()
        return result if result else 0
    except:
        return -1


def run_multi_replica_test(
    num_replicas: int = 3,
    num_writes: int = 500
) -> MultiReplicaResult:
    """Run multi-replica test."""
    cleanup()
    
    result = MultiReplicaResult(
        num_replicas=num_replicas,
        total_writes=num_writes,
        replica_counts={},
        all_synced=False,
        sync_differences=[],
        duration_seconds=0
    )
    # Create Primary config (No storage backend!)
    primary_config = f"""
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
max_concurrent_snapshots = 5

[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "http://0.0.0.0:50051"
"""
    Path("primary.toml").write_text(primary_config)
    
    # Start Primary
    Path("logs").mkdir(exist_ok=True)
    primary_log = open("logs/primary_multi.log", "w")
    primary = subprocess.Popen(
        [str(REPLITED_BIN), "--config", "primary.toml", "replicate"],
        stdout=primary_log,
        stderr=subprocess.STDOUT
    )
    time.sleep(2)
    
    # Initialize database
    conn = sqlite3.connect("primary.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS multi_replica_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sequence INTEGER UNIQUE,
            data TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # Initial data
    for i in range(10):
        cursor.execute("INSERT INTO multi_replica_test (sequence, data) VALUES (?, ?)",
                      (i, f"initial_{i}"))
    conn.commit()
    conn.close()  # Close connection to allow checkpoint
    time.sleep(1)
    
    # Reopen connection for writing
    conn = sqlite3.connect("primary.db")
    cursor = conn.cursor()

    # Start replicas
    print(f"\nStarting {num_replicas} replicas...")
    replicas = []
    for i in range(num_replicas):
        proc, cwd, log = create_replica(i)
        replicas.append((proc, cwd, log))
        print(f"  Replica {i} started")
    
    time.sleep(5)  # Let replicas initialize
    
    # Write data
    print(f"\nWriting {num_writes} records to Primary...")
    start_time = time.time()
    
    for i in range(10, num_writes + 10):
        cursor.execute("INSERT INTO multi_replica_test (sequence, data) VALUES (?, ?)",
                      (i, f"data_{i}"))
        if i % 100 == 0:
            conn.commit()
            print(f"  Progress: {i - 10}/{num_writes}")
    
    conn.commit()
    conn.close()
    
    result.duration_seconds = time.time() - start_time
    
    # Wait for sync
    print("\nWaiting for replicas to sync...")
    time.sleep(30)
    
    # Verify all replicas
    primary_count = get_row_count("primary.db")
    primary_max_seq = get_max_sequence("primary.db")
    
    print(f"\nPrimary: {primary_count} rows, max_seq={primary_max_seq}")
    
    all_match = True
    for i, (proc, cwd, log) in enumerate(replicas):
        db_path = cwd / "primary.db"
        if db_path.exists():
            count = get_row_count(str(db_path))
            max_seq = get_max_sequence(str(db_path))
            result.replica_counts[i] = count
            print(f"Replica {i}: {count} rows, max_seq={max_seq}")
            
            if count != primary_count:
                all_match = False
                result.sync_differences.append(
                    f"Replica {i}: expected {primary_count}, got {count}"
                )
        else:
            result.replica_counts[i] = -1
            all_match = False
            result.sync_differences.append(f"Replica {i}: database not found")
    
    result.all_synced = all_match
    
    # Cleanup
    for proc, cwd, log in replicas:
        proc.terminate()
        proc.wait()
        log.close()
    
    primary.terminate()
    primary.wait()
    primary_log.close()
    
    return result


def print_report(result: MultiReplicaResult) -> bool:
    """Print test report."""
    print("\n" + "=" * 60)
    print("MULTI-REPLICA TEST REPORT")
    print("=" * 60)
    print(f"Number of Replicas:     {result.num_replicas}")
    print(f"Total Writes:           {result.total_writes}")
    print(f"Duration:               {result.duration_seconds:.2f}s")
    print(f"All Replicas Synced:    {'YES ✓' if result.all_synced else 'NO ✗'}")
    
    print("\nReplica Row Counts:")
    for replica_id, count in result.replica_counts.items():
        status = "✓" if count == result.total_writes + 10 else "✗"
        print(f"  Replica {replica_id}: {count} rows {status}")
    
    if result.sync_differences:
        print("\nSync Differences:")
        for diff in result.sync_differences:
            print(f"  - {diff}")
    
    print("=" * 60)
    print(f"RESULT: {'PASS ✓' if result.all_synced else 'FAIL ✗'}")
    
    return result.all_synced


def main():
    parser = argparse.ArgumentParser(description="Multi-replica test")
    parser.add_argument("-r", "--replicas", type=int, default=3,
                        help="Number of replicas (default: 3)")
    parser.add_argument("-n", "--num-writes", type=int, default=500,
                        help="Number of writes (default: 500)")
    
    args = parser.parse_args()
    
    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)
    
    result = run_multi_replica_test(args.replicas, args.num_writes)
    success = print_report(result)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
