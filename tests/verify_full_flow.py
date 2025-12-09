
import os
import shutil
import subprocess
import time
import sqlite3
import sys

def verify_full_flow():
    print("\n=== Verifying Full Replication Flow (Steps 1-4) ===")
    
    # 1. Clean up
    from test_utils import cleanup
    cleanup()
    
    # 2. Start Primary
    print("Starting Primary...")
    
    # Create primary.toml
    primary_config = """
[log]
level = "Debug"
dir = "logs"

[[database]]
db = "data.db"

[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "http://0.0.0.0:50051"
"""
    with open("primary.toml", "w") as f:
        f.write(primary_config)

    primary_log = open("logs/primary.log", "w")
    primary_proc = subprocess.Popen(
        ["../target/release/replited", "--config", "primary.toml", "replicate"],
        stdout=primary_log,
        stderr=subprocess.STDOUT
    )
    
    try:
        time.sleep(5) # Wait for start
        
        # 3. Write initial data
        print("Writing initial data to Primary...")
        conn = sqlite3.connect("data.db")
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, value TEXT)")
        cursor.execute("INSERT INTO test (value) VALUES ('initial')")
        conn.commit()
        conn.close()
        
        # Wait for WAL write
        time.sleep(1)
        
        # 4. Get generation
        gen_file = ".data.db-replited/generation"
        if not os.path.exists(gen_file):
            print("FAILURE: Generation file not found.")
            return False
            
        with open(gen_file, "r") as f:
            generation = f.read().strip()
        print(f"Generation: {generation}")
        
        # Stop Primary to release lock for checkpoint
        print("Stopping Primary for snapshot...")
        primary_proc.terminate()
        primary_proc.wait()
        
        # 5. Create snapshot (Simulated)
        # We copy current data.db as snapshot
        print("Creating simulated snapshot...")
        snapshot_dir = f"backup/data.db/generations/{generation}/snapshots"
        os.makedirs(snapshot_dir, exist_ok=True)
        snapshot_path = f"{snapshot_dir}/0000000001_0000000000.snapshot.lz4"
        
        # Checkpoint to flush WAL to DB before snapshotting
        conn = sqlite3.connect("data.db")
        cursor = conn.cursor()
        cursor.execute("PRAGMA wal_checkpoint(PASSIVE)")
        result = cursor.fetchone()
        print(f"Checkpoint result: {result}")
        if result[0] != 0:
            print("WARNING: Checkpoint failed (busy?)")
        
        # Verify content
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
        if not cursor.fetchone():
            print("FAILURE: 'test' table missing in source data.db before snapshot!")
        else:
            print("Source data.db has 'test' table.")
            
        conn.close()
        
        subprocess.check_call(["lz4", "-f", "data.db", snapshot_path])
        print(f"Created snapshot at {snapshot_path}")
        
        # Restart Primary
        print("Restarting Primary...")
        primary_log = open("logs/primary_restart.log", "w")
        primary_proc = subprocess.Popen(
            ["../target/release/replited", "--config", "primary.toml", "replicate"],
            stdout=primary_log,
            stderr=subprocess.STDOUT
        )
        time.sleep(2) # Wait for start
        
        # 6. Write MORE data (Incremental)
        print("Writing incremental data to Primary...")
        conn = sqlite3.connect("data.db")
        cursor = conn.cursor()
        cursor.execute("INSERT INTO test (value) VALUES ('incremental')")
        conn.commit()
        conn.close()
        
        # Wait for WAL write
        time.sleep(1)
        
        # 7. Prepare Replica
        os.makedirs("replica_cwd")
        # Copy backup to replica_cwd/backup (since we use local fs)
        shutil.copytree("backup", "replica_cwd/backup")
        
        replica_config = """
[log]
level = "Debug"
dir = "logs"

[[database]]
db = "data.db"
min_checkpoint_page_number = 1000
max_checkpoint_page_number = 10000
truncate_page_number = 500000
checkpoint_interval_secs = 60
wal_retention_count = 10

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:50051"
"""
        with open("replica_cwd/replica.toml", "w") as f:
            f.write(replica_config)
            
        # 8. Run Replica
        print("Starting Replica...")
        replica_log = open("replica_cwd/replica.log", "w")
        replica_proc = subprocess.Popen(
            ["../../target/debug/replited", "--config", "replica.toml", "replica-sidecar"],
            cwd="replica_cwd",
            stdout=replica_log,
            stderr=subprocess.STDOUT
        )
        
        # 9. Wait and Verify
        print("Waiting for sync...")
        start_time = time.time()
        success = False
        while time.time() - start_time < 15:
            if os.path.exists("replica_cwd/data.db"):
                try:
                    r_conn = sqlite3.connect("replica_cwd/data.db")
                    r_cursor = r_conn.cursor()
                    r_cursor.execute("SELECT value FROM test ORDER BY id")
                    rows = r_cursor.fetchall()
                    r_conn.close()
                    
                    print(f"Replica rows: {rows}")
                    if len(rows) == 2 and rows[0][0] == 'initial' and rows[1][0] == 'incremental':
                        print("SUCCESS: Replica has all data!")
                        success = True
                        break
                except Exception as e:
                    print(f"Replica DB check failed: {e}")
            time.sleep(1)
            
        replica_proc.terminate()
        replica_proc.wait()
        replica_log.close()
        
        if not success:
            print("FAILURE: Replica did not sync correctly.")
            return False
            
        return True
        
    finally:
        primary_proc.terminate()
        primary_proc.wait()
        primary_log.close()

if __name__ == "__main__":
    if not verify_full_flow():
        exit(1)
