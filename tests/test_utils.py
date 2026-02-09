import os
import shutil
import glob
import time
import socket
import sqlite3
import hashlib
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
    """Remove common on-disk artifacts created by legacy e2e/battle scripts.

    Newer tests should use `TestEnv` which scopes artifacts under `tests/output/`.
    Older scripts still run in their current working directory and need explicit cleanup
    to avoid leaking state between scenarios.
    """

    cwd = Path(".")

    # Common top-level files created by battle scripts.
    for name in [
        "primary.db",
        "primary.db-wal",
        "primary.db-shm",
        "data.db",
        "data.db-wal",
        "data.db-shm",
        "primary.toml",
        "replica.toml",
        "primary.log",
        "replica.log",
    ]:
        try:
            (cwd / name).unlink()
        except FileNotFoundError:
            pass
        except OSError as e:
            print(f"cleanup: failed to remove {name}: {e}")

    # Common directories created by battle scripts.
    for name in ["logs", "backup", "replica_cwd"]:
        path = cwd / name
        if path.is_dir():
            try:
                shutil.rmtree(path, ignore_errors=True)
            except OSError as e:
                print(f"cleanup: failed to remove {path}: {e}")

    # Per-replica working directories used by some battle scripts.
    for entry in cwd.iterdir():
        if entry.is_dir() and entry.name.startswith("replica_"):
            try:
                shutil.rmtree(entry, ignore_errors=True)
            except OSError as e:
                print(f"cleanup: failed to remove {entry}: {e}")

def run_integrity_check(db_path: str) -> tuple:
    """Run PRAGMA integrity_check; return (ok, detail)."""
    try:
        conn = sqlite3.connect(str(db_path), timeout=1.0)
        cursor = conn.cursor()
        cursor.execute("PRAGMA integrity_check;")
        rows = cursor.fetchall()
        conn.close()
        if len(rows) == 1 and rows[0][0] == "ok":
            return (True, "ok")
        return (False, "; ".join(str(r[0]) for r in rows))
    except Exception as e:
        return (False, str(e))

def compute_db_digest(db_path: str) -> str | None:
    """Compute deterministic digest of user tables and ordered rows."""
    if not Path(db_path).exists():
        return None
    conn = sqlite3.connect(str(db_path), timeout=1.0)
    cursor = conn.cursor()
    hasher = hashlib.sha256()
    try:
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' "
            "AND name NOT LIKE '_replited_%' "
            "ORDER BY name"
        )
        tables = [row[0] for row in cursor.fetchall()]
        for table in tables:
            hasher.update(f"table:{table}\n".encode())
            cursor.execute(f"PRAGMA table_info('{table}')")
            columns = [row[1] for row in cursor.fetchall()]
            hasher.update(("cols:" + ",".join(columns) + "\n").encode())
            if not columns:
                continue
            quoted = ", ".join([f'"{col}"' for col in columns])
            order_by = ", ".join([f'"{col}"' for col in columns])
            cursor.execute(f"SELECT {quoted} FROM \"{table}\" ORDER BY {order_by}")
            for row in cursor.fetchall():
                hasher.update((repr(row) + "\n").encode())
    finally:
        conn.close()
    return hasher.hexdigest()
