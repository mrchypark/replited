

import os
import sys
import time
import sqlite3
import subprocess
import signal
import shutil
from pathlib import Path

# Add tests directory to path to import test_utils
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from tests.test_utils import TestEnv, get_free_port, wait_for_port

def run_simple_test():
    print("=== Starting Simple Replication Test ===")
    
    # 1. Setup Environment
    env = TestEnv("simple_replication")
    env.setup()
    
    # Allow OS to pick a free port
    PORT = get_free_port()
    print(f"Selected dynamic port: {PORT}")
    
    # 2. Setup Primary
    # Generate primary.toml with RELATIVE path and DYNAMIC PORT
    primary_config = f"""
[log]
level = "Debug"
dir = "logs"

[[database]]
db = "primary.db"
max_concurrent_snapshots = 4
wal_retention_secs = 86400
wal_retention_count = 5 
min_checkpoint_page_number = 100
max_checkpoint_page_number = 1000

[[database.replicate]]
name = "backup"
[database.replicate.params]
type = "fs"
root = "backup"

[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "127.0.0.1:{PORT}"
"""
    with open(env.primary_toml, "w") as f:
        f.write(primary_config)
    
    # Create Primary DB and Enable WAL
    print("Initializing Primary DB...")
    # Initialize using Absolute Path
    conn = sqlite3.connect(env.primary_db)
    conn.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE IF NOT EXISTS simple_test (id INTEGER PRIMARY KEY, data TEXT);
    """)
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    print(f"Tables after Init: {tables}")
    conn.close()

    # 3. Start Primary
    print("Starting Primary...")
    primary_log_path = env.root / "primary.log"
    primary_log = open(primary_log_path, "w")
    
    replited_bin = os.path.abspath("./target/release/replited")
    
    primary_proc = subprocess.Popen(
        [replited_bin, "--config", "primary.toml", "replicate"],
        cwd=env.root,
        stdout=primary_log,
        stderr=subprocess.STDOUT
    )
    
    # [Improvement] Wait for port instead of sleep
    print(f"Waiting for Primary to start on port {PORT}...")
    if not wait_for_port(PORT, timeout=10):
        print("Error: Primary failed to start / open port.")
        primary_proc.terminate()
        primary_proc.wait()
        return False

    # 4. Start Replica
    print("Starting Replica...")
    
    # Generate replica.toml with DYNAMIC PORT
    replica_config = f"""
[log]
level = "Debug"
dir = "logs"

[[database]]
db = "primary.db"

[[database.replicate]]
name = "stream-replica"
params.type = "stream"
params.addr = "http://127.0.0.1:{PORT}"
"""
    with open(env.replica_cwd / "replica.toml", "w") as f:
        f.write(replica_config)
    
    replica_log_path = env.root / "replica.log"
    replica_log = open(replica_log_path, "w")
    
    replica_proc = subprocess.Popen(
        [os.path.abspath("./target/release/replited"), "--config", "replica.toml", "replica-sidecar", "--force-restore"],
        cwd=env.replica_cwd, 
        stdout=replica_log,
        stderr=subprocess.STDOUT
    )
    
    # Wait slightly for restore to initiate connection (Replica doesn't open a port, so we wait for logic)
    # The original sleep(5) was conservative. We can keep it or poll the log for "Received valid wal frame" etc.
    # For now, 2 seconds + polling verification loop should be enough.
    time.sleep(5)

    try:
        # 5. Write to Primary
        print(f"Writing to Primary at {env.primary_db.absolute()}...")
        if not env.primary_db.exists():
             print("CRITICAL: Primary DB file missing!")
        
        conn = sqlite3.connect(str(env.primary_db))
        # Debug: List tables
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        print(f"Tables in Primary: {tables}")
        
        conn.execute("INSERT INTO simple_test (data) VALUES ('hello_world');")
        conn.commit()
        conn.close()
        print("Write committed.")

        # 6. Verify Replica
        print("Verifying Replica...")
        max_retries = 20
        
        for i in range(max_retries):
            # Replica sidecar might create folders
            wal_exists = (env.replica_db.with_suffix(".db-wal")).exists()
            db_exists = env.replica_db.exists()
            print(f"Attempt {i+1}: DB Exists: {db_exists}, WAL Exists: {wal_exists}")

            if db_exists:
                try:
                    r_conn = sqlite3.connect(env.replica_db)
                    r_conn.execute("PRAGMA busy_timeout = 1000")
                    rows = r_conn.execute("SELECT * FROM simple_test").fetchall()
                    r_conn.close()
                    if rows:
                        print(f"SUCCESS: Data found in Replica: {rows}")
                        return True
                    else:
                        print(f"Attempt {i+1}: Table exists but empty...")
                except sqlite3.OperationalError as e:
                     print(f"Attempt {i+1}: Query failed ({e})...")
            
            time.sleep(0.5)
        
        print("FAILURE: Data never appeared in Replica.")
        return False

    finally:
        print("Stopping processes...")
        # [Improvement] Safe shutdown with wait()
        if 'primary_proc' in locals():
            primary_proc.terminate()
            try:
                primary_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                primary_proc.kill()
        
        if 'replica_proc' in locals():
            replica_proc.terminate()
            try:
                replica_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                replica_proc.kill()
                
        if 'primary_log' in locals(): primary_log.close()
        if 'replica_log' in locals(): replica_log.close()

if __name__ == "__main__":
    if run_simple_test():
        sys.exit(0)
    else:
        sys.exit(1)
