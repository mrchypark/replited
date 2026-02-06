#!/usr/bin/env python3
"""
Sidecar integration scenarios for restore/retry/recover.

Scenarios:
- restore: replica starts with stale local DB and force-restores from Primary.
- retry: replica starts before Primary and succeeds after connection retries.
- recover: replica survives Primary restart and catches up again.
"""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import TestEnv, compute_db_digest, get_free_port, run_integrity_check, wait_for_port

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
TABLE_NAME = "sidecar_case"


@dataclass
class Proc:
    proc: subprocess.Popen
    log_handle: object
    log_path: Path


@dataclass
class ScenarioResult:
    name: str
    success: bool
    errors: list[str]
    primary_digest: str | None = None
    replica_digest: str | None = None


def stop_process(proc: Proc | None) -> None:
    if proc is None:
        return
    proc.proc.terminate()
    try:
        proc.proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.proc.kill()
    proc.log_handle.close()


def write_primary_config(env: TestEnv, port: int) -> None:
    config = f"""
[log]
level = "Info"
dir = "logs"

[[database]]
db = "primary.db"
max_concurrent_snapshots = 4
wal_retention_secs = 60
wal_retention_count = 3
min_checkpoint_page_number = 10
max_checkpoint_page_number = 200

[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "127.0.0.1:{port}"
"""
    env.primary_toml.write_text(config)


def write_replica_config(replica_dir: Path, port: int) -> None:
    config = f"""
[log]
level = "Info"
dir = "logs"

[[database]]
db = "primary.db"
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 200
wal_retention_count = 2

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:{port}"
"""
    (replica_dir / "replica.toml").write_text(config)


def start_primary(env: TestEnv, port: int, log_name: str = "primary.log") -> Proc:
    write_primary_config(env, port)
    log_path = env.root / log_name
    log_handle = open(log_path, "a")
    proc = subprocess.Popen(
        [str(REPLITED_BIN.resolve()), "--config", "primary.toml", "replicate"],
        cwd=str(env.root),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return Proc(proc=proc, log_handle=log_handle, log_path=log_path)


def start_replica(
    env: TestEnv,
    name: str,
    port: int,
    force_restore: bool = False,
) -> tuple[Proc, Path]:
    replica_dir = env.root / name
    replica_dir.mkdir(parents=True, exist_ok=True)
    (replica_dir / "logs").mkdir(exist_ok=True)
    write_replica_config(replica_dir, port)
    log_path = replica_dir / "replica.log"
    log_handle = open(log_path, "a")
    cmd = [str(REPLITED_BIN.resolve()), "--config", "replica.toml", "replica-sidecar"]
    if force_restore:
        cmd.append("--force-restore")
    proc = subprocess.Popen(
        cmd,
        cwd=str(replica_dir),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return Proc(proc=proc, log_handle=log_handle, log_path=log_path), replica_dir


def init_primary_db(db_path: Path, rows: int, start_seq: int = 0) -> None:
    conn = sqlite3.connect(str(db_path), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sequence INTEGER UNIQUE,
            payload TEXT NOT NULL
        )
        """
    )
    for i in range(start_seq, start_seq + rows):
        cursor.execute(
            f"INSERT OR REPLACE INTO {TABLE_NAME} (sequence, payload) VALUES (?, ?)",
            (i, f"payload_{i}"),
        )
    conn.commit()
    conn.close()


def row_count(db_path: Path) -> int:
    if not db_path.exists():
        return -1
    try:
        conn = sqlite3.connect(str(db_path), timeout=1.0)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        count = cursor.fetchone()[0]
        conn.close()
        return int(count)
    except Exception:
        return -1


def wait_for_sync(primary_db: Path, replica_db: Path, timeout: int = 90) -> tuple[bool, str | None]:
    start = time.time()
    expected_digest = compute_db_digest(str(primary_db))
    expected_count = row_count(primary_db)
    while time.time() - start < timeout:
        if row_count(replica_db) == expected_count:
            replica_digest = compute_db_digest(str(replica_db))
            if expected_digest and replica_digest and replica_digest == expected_digest:
                return True, replica_digest
        time.sleep(1.0)
    return False, compute_db_digest(str(replica_db))


def run_restore_scenario() -> ScenarioResult:
    env = TestEnv("sidecar_restore_scenario")
    env.setup()
    result = ScenarioResult(name="restore", success=False, errors=[])
    port = get_free_port()

    primary = None
    replica = None
    replica_dir = None
    try:
        primary = start_primary(env, port)
        if not wait_for_port(port, timeout=12):
            result.errors.append("Primary failed to open stream port")
            return result

        init_primary_db(env.primary_db, rows=40)

        replica, replica_dir = start_replica(env, "replica_restore", port, force_restore=True)
        replica_db = replica_dir / "primary.db"
        synced, replica_digest = wait_for_sync(env.primary_db, replica_db, timeout=120)
        result.primary_digest = compute_db_digest(str(env.primary_db))
        result.replica_digest = replica_digest
        if not synced:
            result.errors.append("Replica failed to restore/sync to Primary digest")
            return result

        result.success = True
        return result
    finally:
        stop_process(replica)
        stop_process(primary)


def run_retry_scenario() -> ScenarioResult:
    env = TestEnv("sidecar_retry_scenario")
    env.setup()
    result = ScenarioResult(name="retry", success=False, errors=[])
    port = get_free_port()

    primary = None
    replica = None
    replica_dir = None
    try:
        replica, replica_dir = start_replica(env, "replica_retry", port, force_restore=True)
        replica_db = replica_dir / "primary.db"
        # Ensure at least one connect failure attempt before Primary starts.
        time.sleep(6)
        if replica_db.exists():
            result.errors.append("Replica DB appeared before Primary startup")
            return result

        primary = start_primary(env, port)
        if not wait_for_port(port, timeout=12):
            result.errors.append("Primary failed to open stream port")
            return result
        init_primary_db(env.primary_db, rows=30)

        synced, replica_digest = wait_for_sync(env.primary_db, replica_db, timeout=150)
        result.primary_digest = compute_db_digest(str(env.primary_db))
        result.replica_digest = replica_digest
        if not synced:
            result.errors.append("Replica failed to sync after retry window")
            return result

        result.success = True
        return result
    finally:
        stop_process(replica)
        stop_process(primary)


def run_recover_scenario() -> ScenarioResult:
    env = TestEnv("sidecar_recover_scenario")
    env.setup()
    result = ScenarioResult(name="recover", success=False, errors=[])
    port = get_free_port()

    primary = None
    replica = None
    replica_dir = None
    try:
        primary = start_primary(env, port)
        if not wait_for_port(port, timeout=12):
            result.errors.append("Primary failed to open stream port")
            return result
        init_primary_db(env.primary_db, rows=20)

        replica, replica_dir = start_replica(env, "replica_recover", port, force_restore=True)
        replica_db = replica_dir / "primary.db"
        initial_synced, _ = wait_for_sync(env.primary_db, replica_db, timeout=120)
        if not initial_synced:
            result.errors.append("Replica did not complete initial sync")
            return result
        initial_count = row_count(replica_db)
        if initial_count < 0:
            result.errors.append("Replica row count unavailable after initial sync")
            return result

        stop_process(primary)
        primary = None
        time.sleep(4)

        primary = start_primary(env, port, log_name="primary_restarted.log")
        if not wait_for_port(port, timeout=12):
            result.errors.append("Restarted Primary failed to open stream port")
            return result

        init_primary_db(env.primary_db, rows=20, start_seq=20)

        recovered, replica_digest = wait_for_sync(env.primary_db, replica_db, timeout=180)
        result.primary_digest = compute_db_digest(str(env.primary_db))
        result.replica_digest = replica_digest
        if not recovered:
            result.errors.append("Replica failed to recover after Primary restart")
            return result
        if row_count(replica_db) <= initial_count:
            result.errors.append("Replica row count did not advance after Primary restart")
            return result

        result.success = True
        return result
    finally:
        stop_process(replica)
        stop_process(primary)


def run_scenario(name: str) -> ScenarioResult:
    if name == "restore":
        return run_restore_scenario()
    if name == "retry":
        return run_retry_scenario()
    if name == "recover":
        return run_recover_scenario()
    raise ValueError(f"unknown scenario: {name}")


def print_result(result: ScenarioResult) -> bool:
    primary_ok, primary_integrity = run_integrity_check(
        str(TestEnv(f"sidecar_{result.name}_scenario").root / "primary.db")
    )
    print("\n" + "=" * 60)
    print(f"SIDEcar SCENARIO: {result.name.upper()}")
    print("=" * 60)
    print(f"Success:         {'YES' if result.success else 'NO'}")
    print(f"Primary digest:  {result.primary_digest}")
    print(f"Replica digest:  {result.replica_digest}")
    print(f"Primary integrity: {'ok' if primary_ok else primary_integrity}")
    if result.errors:
        print("Errors:")
        for err in result.errors:
            print(f"  - {err}")
    print("=" * 60)
    return result.success


def main() -> int:
    parser = argparse.ArgumentParser(description="Run sidecar restore/retry/recover scenarios")
    parser.add_argument(
        "--scenario",
        choices=["restore", "retry", "recover"],
        required=True,
        help="Scenario to run",
    )
    args = parser.parse_args()

    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        return 1

    result = run_scenario(args.scenario)
    return 0 if print_result(result) else 1


if __name__ == "__main__":
    raise SystemExit(main())
