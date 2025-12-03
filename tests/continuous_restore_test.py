import sqlite3
import os, sys
import random
import string
import subprocess
import time
import shutil
import signal

class TestHelper:
    def __init__(self, root):
        self.root = root
        self.db_path = root + '/test.db'
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
    def create_table(self):
        self.conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        )
        ''') 
        self.conn.commit()

    def insert_data(self, name, value):
        self.cursor.execute('INSERT INTO users (name, value) VALUES (?, ?)', (name, value))
        self.conn.commit()
        print(f"Inserted: {name}, {value}")

    def query_all(self):
        cursor = self.conn.cursor()
        cursor.execute('SELECT name, value FROM users ORDER BY value')
        return cursor.fetchall()
    
    def checkpoint_truncate(self):
        print("Triggering PRAGMA wal_checkpoint(TRUNCATE)...")
        self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
        self.conn.commit()

class ConfigGenerator:
    def __init__(self):
        self.cwd = os.getcwd()
        self.root = self.cwd + "/.test_continuous"
        # clean test dir
        if os.path.exists(self.root):
            shutil.rmtree(self.root)
        print("root: ", self.root)
        os.makedirs(self.root)
        self.config_file = self.root + "/replited.toml"

    def generate(self):
        # create root dir of fs
        os.makedirs(self.root + "/replited")

        # generate config file
        with open(self.cwd + '/tests/config/fs_template.toml', 'r') as f:
            content = f.read()
        
        content = content.replace('{root}', self.root)
        
        with open(self.config_file, 'w+') as f:
            f.write(content)

class ProcessManager:
    def __init__(self, bin_path, config_file):
        self.bin_path = bin_path
        self.config_file = config_file
        self.replicate_proc = None
        self.restore_proc = None

    def start_replicate(self):
        cmd = [self.bin_path, "--config", self.config_file, "replicate"]
        print(f"Starting replicate: {' '.join(cmd)}")
        self.replicate_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(2) # Wait for startup

    def stop_replicate(self):
        if self.replicate_proc:
            print("Stopping replicate...")
            self.replicate_proc.terminate()
            self.replicate_proc.wait()
            self.replicate_proc = None

    def start_restore(self, db_path, output_path):
        cmd = [self.bin_path, "--config", self.config_file, "restore", "--db", db_path, "--output", output_path, "--follow", "--interval", "1"]
        print(f"Starting restore: {' '.join(cmd)}")
        self.restore_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(2) # Wait for startup

    def stop_restore(self):
        if self.restore_proc:
            print("Stopping restore...")
            self.restore_proc.terminate()
            self.restore_proc.wait()
            self.restore_proc = None

def verify_replica(replica_path, expected_data):
    if not os.path.exists(replica_path):
        print(f"Replica DB not found at {replica_path}")
        return False
    
    try:
        conn = sqlite3.connect(replica_path)
        cursor = conn.cursor()
        cursor.execute('SELECT name, value FROM users ORDER BY value')
        data = cursor.fetchall()
        conn.close()
        
        if data == expected_data:
            print("Verification PASSED")
            return True
        else:
            print(f"Verification FAILED. Expected: {expected_data}, Got: {data}")
            return False
    except Exception as e:
        print(f"Verification Error: {e}")
        return False

def run_test(bin_path):
    config_gen = ConfigGenerator()
    config_gen.generate()
    
    source_helper = TestHelper(config_gen.root)
    source_helper.create_table()
    
    proc_manager = ProcessManager(bin_path, config_gen.config_file)
    
    replica_path = config_gen.root + "/replica.db"
    
    try:
        # 1. Start Replicate
        proc_manager.start_replicate()
        
        # 2. Insert initial data
        source_helper.insert_data("Alice", 1)
        source_helper.insert_data("Bob", 2)
        time.sleep(2) # Wait for replication
        
        # 3. Start Restore (Follow)
        proc_manager.start_restore(source_helper.db_path, replica_path)
        
        # Verify initial sync
        print("\n--- Verifying Initial Sync ---")
        time.sleep(5)
        if not verify_replica(replica_path, source_helper.query_all()):
            sys.exit(1)
            
        # 4. Scenario 1: Continuous Restore
        print("\n--- Scenario 1: Continuous Restore ---")
        source_helper.insert_data("Charlie", 3)
        source_helper.insert_data("Dave", 4)
        time.sleep(5)
        if not verify_replica(replica_path, source_helper.query_all()):
            sys.exit(1)
            
        # 5. Scenario 2: Resume (Restart Restore)
        print("\n--- Scenario 2: Resume (Restart Restore) ---")
        proc_manager.stop_restore()
        source_helper.insert_data("Eve", 5)
        print("Restarting restore...")
        proc_manager.start_restore(source_helper.db_path, replica_path)
        time.sleep(5)
        if not verify_replica(replica_path, source_helper.query_all()):
            sys.exit(1)
            
        # 6. Scenario 3: Checkpoint (Generation Switch)
        print("\n--- Scenario 3: Checkpoint (Generation Switch) ---")
        source_helper.checkpoint_truncate()
        source_helper.insert_data("Frank", 6)
        time.sleep(5)
        if not verify_replica(replica_path, source_helper.query_all()):
            sys.exit(1)
            
        print("\nALL TESTS PASSED!")
        
    finally:
        proc_manager.stop_restore()
        proc_manager.stop_replicate()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 tests/continuous_restore_test.py [replited_bin_path]")
        sys.exit(1)
        
    bin_path = sys.argv[1]
    run_test(bin_path)
