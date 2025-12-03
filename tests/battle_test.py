import sqlite3
import os
import sys
import random
import string
import subprocess
import time
import shutil
import signal
import concurrent.futures
import threading
import hashlib

# Configuration
DB_NAME = "test.db"
REPLICA_DB_NAME = "replica.db"
TEST_ROOT = ".test_battle"
NUM_WRITERS = 5
NUM_RECORDS_PER_WRITER = 1000
CHAOS_INTERVAL = 5 # Seconds between chaos events
TOTAL_DURATION = 30 # Seconds to run the test

class BattleTest:
    def __init__(self, bin_path):
        self.bin_path = bin_path
        self.cwd = os.getcwd()
        self.root = os.path.join(self.cwd, TEST_ROOT)
        self.db_path = os.path.join(self.root, DB_NAME)
        self.replica_path = os.path.join(self.root, REPLICA_DB_NAME)
        self.config_file = os.path.join(self.root, "replited.toml")
        self.lock = threading.Lock()
        self.running = True
        self.replicate_proc = None
        self.restore_proc = None
        self.records = {} # In-memory source of truth: id -> (name, value)

    def setup(self):
        if os.path.exists(self.root):
            shutil.rmtree(self.root)
        os.makedirs(self.root)
        os.makedirs(os.path.join(self.root, "replited"))

        # Create initial DB
        conn = sqlite3.connect(self.db_path)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                writer_id INTEGER,
                seq_no INTEGER,
                payload TEXT,
                timestamp INTEGER
            )
        ''')
        conn.execute('PRAGMA journal_mode=WAL;')
        conn.commit()
        conn.close()

        # Generate Config
        with open(os.path.join(self.cwd, 'tests/config/fs_template.toml'), 'r') as f:
            content = f.read()
        content = content.replace('{root}', self.root)
        with open(self.config_file, 'w') as f:
            f.write(content)

    def start_replicate(self):
        cmd = [self.bin_path, "--config", self.config_file, "replicate"]
        print(f"[Replicate] Starting: {' '.join(cmd)}")
        self.replicate_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def start_restore(self):
        cmd = [self.bin_path, "--config", self.config_file, "restore", "--db", self.db_path, "--output", self.replica_path, "--follow", "--interval", "1"]
        print(f"[Restore] Starting: {' '.join(cmd)}")
        self.restore_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def stop_process(self, proc, name):
        if proc:
            print(f"[{name}] Stopping...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            print(f"[{name}] Stopped")

    def writer_task(self, writer_id):
        conn = sqlite3.connect(self.db_path, timeout=10)
        seq = 0
        while self.running and seq < NUM_RECORDS_PER_WRITER:
            try:
                payload = ''.join(random.choices(string.ascii_letters, k=50))
                ts = int(time.time() * 1000)
                cursor = conn.cursor()
                cursor.execute('INSERT INTO data (writer_id, seq_no, payload, timestamp) VALUES (?, ?, ?, ?)', 
                               (writer_id, seq, payload, ts))
                conn.commit()
                
                # Update source of truth
                with self.lock:
                    self.records[(writer_id, seq)] = (payload, ts)
                
                seq += 1
                time.sleep(random.uniform(0.01, 0.05))
            except sqlite3.OperationalError as e:
                print(f"[Writer {writer_id}] Error: {e}")
                time.sleep(0.1)
            except Exception as e:
                print(f"[Writer {writer_id}] Unexpected Error: {e}")
                break
        conn.close()
        print(f"[Writer {writer_id}] Finished. Total: {seq}")

    def chaos_monkey(self):
        while self.running:
            time.sleep(CHAOS_INTERVAL)
            if not self.running: break
            
            target = random.choice(['replicate', 'restore', 'both'])
            print(f"\n[Chaos] Killing {target}...")
            
            if target == 'replicate' or target == 'both':
                if self.replicate_proc:
                    self.replicate_proc.kill()
                    self.replicate_proc.wait()
                    self.replicate_proc = None
            
            if target == 'restore' or target == 'both':
                if self.restore_proc:
                    self.restore_proc.kill()
                    self.restore_proc.wait()
                    self.restore_proc = None
            
            time.sleep(2)
            print("[Chaos] Restarting processes...")
            
            if (target == 'replicate' or target == 'both') and self.running:
                self.start_replicate()
            
            if (target == 'restore' or target == 'both') and self.running:
                self.start_restore()

    def verify(self):
        print("\n--- Verifying Data Integrity ---")
        if not os.path.exists(self.replica_path):
            print("Replica DB not found!")
            return False

        try:
            conn = sqlite3.connect(self.replica_path)
            cursor = conn.cursor()
            
            # Check integrity
            try:
                cursor.execute('PRAGMA integrity_check')
                result = cursor.fetchone()
                print(f"Integrity Check: {result}")
                if result[0] != "ok":
                    print("Integrity check failed!")
            except Exception as e:
                print(f"Integrity check exception: {e}")

            cursor.execute('SELECT writer_id, seq_no, payload, timestamp FROM data')
            rows = cursor.fetchall()
            conn.close()
        except Exception as e:
            print(f"Failed to read replica: {e}")
            return False

        replica_data = {}
        for r in rows:
            replica_data[(r[0], r[1])] = (r[2], r[3])

        print(f"Source records: {len(self.records)}")
        print(f"Replica records: {len(replica_data)}")

        missing = 0
        mismatch = 0
        
        for key, val in self.records.items():
            if key not in replica_data:
                missing += 1
            elif replica_data[key] != val:
                mismatch += 1
        
        print(f"Missing: {missing}")
        print(f"Mismatch: {mismatch}")

        if missing == 0 and mismatch == 0:
            print("SUCCESS: Data integrity verified.")
            return True
        else:
            print("FAILURE: Data integrity check failed.")
            return False

    def run(self):
        self.setup()
        self.start_replicate()
        time.sleep(2)
        self.start_restore()
        
        # Start writers
        with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_WRITERS + 1) as executor:
            futures = [executor.submit(self.writer_task, i) for i in range(NUM_WRITERS)]
            
            # Start Chaos Monkey
            chaos_future = executor.submit(self.chaos_monkey)
            
            # Run for duration
            time.sleep(TOTAL_DURATION)
            self.running = False
            
            # Wait for writers
            for f in futures:
                f.result()
            
            # Wait for chaos
            chaos_future.result()

        print("Writers finished. Ensuring replication catches up...")
        
        # Ensure Replicate is running and give it time
        if self.replicate_proc is None:
            self.start_replicate()
        
        time.sleep(10)
        self.stop_process(self.replicate_proc, "Replicate")
        
        print("Ensuring restore catches up...")
        # Restart restore to ensure clean state and catch up
        if self.restore_proc:
            self.stop_process(self.restore_proc, "Restore")
        
        self.start_restore()
        time.sleep(10)
        self.stop_process(self.restore_proc, "Restore")

        return self.verify()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 tests/battle_test.py [replited_bin_path]")
        sys.exit(1)
        
    bin_path = sys.argv[1]
    test = BattleTest(bin_path)
    success = test.run()
    sys.exit(0 if success else 1)
