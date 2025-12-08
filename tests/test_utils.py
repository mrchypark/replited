import os
import shutil
import glob
import time
import socket
from pathlib import Path

# Central artifacts directory (relative to project root)
ARTIFACTS_ROOT = Path("tests/output")

def get_free_port():
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

def wait_for_port(port, timeout=10):
    """Wait for a port to be open."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                if s.connect_ex(('127.0.0.1', port)) == 0:
                    return True
        except:
            pass
        time.sleep(0.1)
    return False

class TestEnv:
    def __init__(self, test_name: str):
        self.test_name = test_name
        self.root = ARTIFACTS_ROOT / test_name
        self.primary_db = self.root / "primary.db"
        self.replica_cwd = self.root / "replica_cwd"
        self.replica_db = self.replica_cwd / "primary.db"
        self.logs_dir = self.root / "logs"
        self.backup_dir = self.root / "backup"
        self.config_dir = self.root / "config"
        
        # Paths for config files
        self.primary_toml = self.root / "primary.toml"
        self.replica_toml = self.replica_cwd / "replica.toml"
        
    def cleanup(self):
        """Clean up the test artifacts directory."""
        if self.root.exists():
            print(f"Cleaning up {self.root}...")
            try:
                shutil.rmtree(self.root)
            except OSError as e:
                print(f"Error checking removal of {self.root}: {e}")
        
    def setup(self):
        """Create directories."""
        self.cleanup()
        print(f"Setting up test environment in: {self.root}")
        self.root.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)
        self.backup_dir.mkdir(exist_ok=True)
        self.config_dir.mkdir(exist_ok=True)
        self.replica_cwd.mkdir(exist_ok=True)
        # Also create logs dir for replica
        (self.replica_cwd / "logs").mkdir(exist_ok=True)

    def generate_primary_toml(self):
        """Generates a primary configuration file."""
        config_content = f"""
[log]
level = "Debug"
dir = "{self.logs_dir.absolute()}"

[[database]]
db = "{self.primary_db.absolute()}"
max_concurrent_snapshots = 4
wal_retention_secs = 86400
wal_retention_count = 5 
min_checkpoint_page_number = 100
max_checkpoint_page_number = 1000

[[database.replicate]]
name = "backup"
[database.replicate.params]
type = "fs"
root = "{self.backup_dir.absolute()}"

[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "127.0.0.1:50051"
"""
        with open(self.primary_toml, "w") as f:
            f.write(config_content)
        print(f"Generated {self.primary_toml}")

def cleanup():
    """Legacy cleanup for backward compatibility or global cleanup."""
    # We encourage using TestEnv now.
    pass
