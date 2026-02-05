#!/usr/bin/env python3
"""
Stream v2 divergence test under lag + frequent snapshots.

Verifies that a lagging replica either converges to the primary
or transitions to NeedsRestore with an explicit error code.
"""

import os
import sys
import time
import sqlite3
import subprocess
import argparse
import signal
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import TestEnv, get_free_port, wait_for_port, compute_db_digest, run_integrity_check

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"


@dataclass
class DivergenceResult:
    converged: bool
    needs_restore: bool
    error_code: Optional[str]
    primary_digest: Optional[str]
    lagging_digest: Optional[str]
    replica_digests: List[str]
    errors: List[str]
    integrity_results: List[str]


def start_primary(env: TestEnv, port: int) -> tuple:
    primary_config = f"""
[log]
level = "Info"
dir = "logs"

[[database]]
db = "primary.db"
max_concurrent_snapshots = 4
wal_retention_secs = 60
wal_retention_count = 2
min_checkpoint_page_number = 10
max_checkpoint_page_number = 200

[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "127.0.0.1:{port}"
"""
    with open(env.primary_toml, "w") as f:
        f.write(primary_config)

    primary_log_path = env.root / "primary.log"
    primary_log = open(primary_log_path, "w")
    replited_bin = REPLITED_BIN.resolve()
    primary = subprocess.Popen(
        [str(replited_bin), "--config", "primary.toml", "replicate"],
        cwd=env.root,
        stdout=primary_log,
        stderr=subprocess.STDOUT,
    )
    return primary, primary_log, primary_log_path


def start_replica(env: TestEnv, replica_name: str, port: int) -> tuple:
    replica_dir = env.root / replica_name
    replica_dir.mkdir(parents=True, exist_ok=True)
    (replica_dir / "logs").mkdir(exist_ok=True)

    replica_config = f"""
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
    (replica_dir / "replica.toml").write_text(replica_config)
    log_path = replica_dir / "replica.log"
    log = open(log_path, "a")
    replited_bin = REPLITED_BIN.resolve()
    proc = subprocess.Popen(
        [str(replited_bin), "--config", "replica.toml", "replica-sidecar", "--force-restore"],
        cwd=str(replica_dir),
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    return proc, replica_dir, log, log_path


def stop_process(proc: subprocess.Popen, log_handle) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    if log_handle:
        log_handle.close()


def wait_for_replica_db(replica_db: Path, timeout: int = 15) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        if replica_db.exists():
            try:
                conn = sqlite3.connect(str(replica_db))
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                cursor.fetchall()
                conn.close()
                return True
            except Exception:
                pass
        time.sleep(0.5)
    return False


def wait_for_table(
    db_path: Path,
    table: str,
    timeout: int = 30,
    initial_sleep: float = 0.5,
    max_sleep: float = 5.0,
) -> bool:
    start = time.time()
    sleep_secs = initial_sleep
    while time.time() - start < timeout:
        if db_path.exists():
            try:
                conn = sqlite3.connect(str(db_path), timeout=1.0)
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                )
                row = cursor.fetchone()
                conn.close()
                if row:
                    return True
            except Exception:
                pass
        time.sleep(sleep_secs)
        sleep_secs = min(sleep_secs * 1.5, max_sleep)
    return False


def scan_restore_code(log_path: Path) -> Optional[str]:
    if not log_path.exists():
        return None
    codes = ["LineageMismatch", "WalNotRetained", "SnapshotBoundaryMismatch"]
    try:
        text = log_path.read_text(errors="ignore")
    except Exception:
        return None
    for line in text.splitlines():
        if "stream error" in line:
            for code in codes:
                if code in line:
                    return code
    return None


def log_indicates_restore(log_path: Path) -> bool:
    if not log_path.exists():
        return False
    markers = [
        "NeedsRestore",
        "WAL Stuck",
        "Deleting DB to force restore",
        "LineageMismatch",
        "WalNotRetained",
        "SnapshotBoundaryMismatch",
    ]
    try:
        text = log_path.read_text(errors="ignore")
    except Exception:
        return False
    return any(marker in text for marker in markers)


def wait_for_digest(
    db_path: Path,
    expected: str,
    timeout: int = 60,
    stable_reads: int = 2,
    initial_sleep: float = 0.5,
    max_sleep: float = 5.0,
) -> Optional[str]:
    start = time.time()
    last_digest = None
    stable = 0
    sleep_secs = initial_sleep
    while time.time() - start < timeout:
        last_digest = compute_db_digest(str(db_path))
        if expected and last_digest and last_digest == expected:
            stable += 1
            if stable >= stable_reads:
                return last_digest
            sleep_secs = initial_sleep
        else:
            stable = 0
            sleep_secs = min(sleep_secs * 1.5, max_sleep)
        time.sleep(sleep_secs)
    return last_digest


def get_row_count(db_path: Path, table: str) -> int:
    try:
        conn = sqlite3.connect(str(db_path), timeout=1.0)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return -1


def wait_for_row_count(
    db_path: Path,
    table: str,
    expected: int,
    timeout: int = 60,
    stable_reads: int = 2,
    initial_sleep: float = 0.5,
    max_sleep: float = 5.0,
) -> int:
    start = time.time()
    last = -1
    stable = 0
    sleep_secs = initial_sleep
    while time.time() - start < timeout:
        last = get_row_count(db_path, table)
        if last == expected:
            stable += 1
            if stable >= stable_reads:
                return last
            sleep_secs = initial_sleep
        else:
            stable = 0
            sleep_secs = min(sleep_secs * 1.5, max_sleep)
        time.sleep(sleep_secs)
    return last


def execute_with_retry(
    cursor: sqlite3.Cursor,
    sql: str,
    params: tuple,
    retries: int = 5,
    initial_sleep: float = 0.05,
    max_sleep: float = 0.5,
) -> None:
    sleep_secs = initial_sleep
    for attempt in range(retries):
        try:
            cursor.execute(sql, params)
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "database is locked" not in message:
                raise
            if attempt == retries - 1:
                raise
            time.sleep(sleep_secs)
            sleep_secs = min(sleep_secs * 2.0, max_sleep)


def commit_with_retry(
    conn: sqlite3.Connection,
    retries: int = 5,
    initial_sleep: float = 0.05,
    max_sleep: float = 0.5,
) -> None:
    sleep_secs = initial_sleep
    for attempt in range(retries):
        try:
            conn.commit()
            return
        except sqlite3.OperationalError as exc:
            message = str(exc).lower()
            if "database is locked" not in message:
                raise
            if attempt == retries - 1:
                raise
            time.sleep(sleep_secs)
            sleep_secs = min(sleep_secs * 2.0, max_sleep)


def wait_for_replica_sync(
    db_path: Path,
    table: str,
    expected_count: int,
    expected_digest: str,
    timeout: int = 300,
    stable_reads: int = 3,
    initial_sleep: float = 0.5,
    max_sleep: float = 5.0,
) -> tuple:
    start = time.time()
    stable = 0
    sleep_secs = initial_sleep
    last_count = -1
    last_digest = None
    while time.time() - start < timeout:
        last_count = get_row_count(db_path, table)
        last_digest = compute_db_digest(str(db_path))
        if (
            last_count == expected_count
            and expected_digest
            and last_digest
            and last_digest == expected_digest
        ):
            stable += 1
            if stable >= stable_reads:
                return (True, last_count, last_digest)
            sleep_secs = initial_sleep
        else:
            stable = 0
            sleep_secs = min(sleep_secs * 1.5, max_sleep)
        time.sleep(sleep_secs)
    return (False, last_count, last_digest)


def run_test(num_writes: int, snapshot_every: int, lag_seconds: int) -> DivergenceResult:
    env = TestEnv("stream_v2_divergence")
    env.setup()

    result = DivergenceResult(
        converged=False,
        needs_restore=False,
        error_code=None,
        primary_digest=None,
        lagging_digest=None,
        replica_digests=[],
        errors=[],
        integrity_results=[],
    )

    port = get_free_port()
    primary = None
    primary_log = None
    primary_log_path = None
    replicas = {}
    lagging_name = "replica_lag"

    try:
        primary, primary_log, primary_log_path = start_primary(env, port)
        if not wait_for_port(port, timeout=10):
            result.errors.append("Primary failed to start")
            return result

        conn = sqlite3.connect(str(env.primary_db))
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA wal_autocheckpoint=20")
        conn.execute("PRAGMA busy_timeout=5000")
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS divergence_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sequence INTEGER UNIQUE,
                payload BLOB,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        commit_with_retry(conn)
        for i in range(5):
            execute_with_retry(
                cursor,
                "INSERT INTO divergence_test (sequence, payload) VALUES (?, ?)",
                (i, os.urandom(256)),
            )
        commit_with_retry(conn)

        replicas["replica_fast_0"] = start_replica(env, "replica_fast_0", port)
        replicas["replica_fast_1"] = start_replica(env, "replica_fast_1", port)
        replicas[lagging_name] = start_replica(env, lagging_name, port)

        lag_db = replicas[lagging_name][1] / "primary.db"
        wait_for_replica_db(lag_db, timeout=10)
        os.kill(replicas[lagging_name][0].pid, signal.SIGSTOP)

        print(f"Starting writes: {num_writes} rows")
        for i in range(5, num_writes + 5):
            execute_with_retry(
                cursor,
                "INSERT INTO divergence_test (sequence, payload) VALUES (?, ?)",
                (i, os.urandom(512)),
            )
            if i % 20 == 0:
                commit_with_retry(conn)
            if i % snapshot_every == 0:
                proc, rdir, log_handle, log_path = replicas["replica_fast_1"]
                stop_process(proc, log_handle)
                replicas["replica_fast_1"] = start_replica(env, "replica_fast_1", port)
                time.sleep(0.5)
            time.sleep(0.01)

        commit_with_retry(conn)
        conn.close()

        time.sleep(1)
        os.kill(replicas[lagging_name][0].pid, signal.SIGCONT)
        time.sleep(lag_seconds)

        primary_digest = compute_db_digest(str(env.primary_db))
        result.primary_digest = primary_digest
        primary_count = get_row_count(env.primary_db, "divergence_test")

        lag_log_path = replicas[lagging_name][3]
        lag_db_path = replicas[lagging_name][1] / "primary.db"

        start = time.time()
        while time.time() - start < 120:
            lagging_digest = compute_db_digest(str(lag_db_path))
            result.lagging_digest = lagging_digest
            if primary_digest and lagging_digest and primary_digest == lagging_digest:
                result.converged = True
                break
            code = scan_restore_code(lag_log_path)
            if code:
                result.needs_restore = True
                result.error_code = code
                break
            time.sleep(2)

        for name in list(replicas.keys()):
            proc, rdir, log_handle, log_path = replicas[name]
            if name == lagging_name:
                continue
            replica_db = rdir / "primary.db"
            synced = False
            count = -1
            digest = None
            for attempt in range(2):
                if not wait_for_table(replica_db, "divergence_test", timeout=180):
                    if log_indicates_restore(log_path) and attempt == 0:
                        stop_process(proc, log_handle)
                        replicas[name] = start_replica(env, name, port)
                        proc, rdir, log_handle, log_path = replicas[name]
                        replica_db = rdir / "primary.db"
                        time.sleep(1)
                        continue
                    break
                synced, count, digest = wait_for_replica_sync(
                    replica_db,
                    "divergence_test",
                    primary_count,
                    primary_digest,
                    timeout=420,
                    stable_reads=4,
                )
                if synced:
                    break
                if log_indicates_restore(log_path) and attempt == 0:
                    stop_process(proc, log_handle)
                    replicas[name] = start_replica(env, name, port)
                    proc, rdir, log_handle, log_path = replicas[name]
                    replica_db = rdir / "primary.db"
                    time.sleep(1)
                    continue
                break

            result.replica_digests.append(digest or "None")
            if not synced:
                if count != primary_count:
                    result.errors.append(
                        f"Replica {name} count mismatch: expected {primary_count}, got {count}"
                    )
                if primary_digest and digest and primary_digest != digest:
                    result.errors.append(f"Replica {name} digest mismatch")
                if digest is None:
                    result.errors.append(f"Replica {name} digest unavailable")

        if not result.converged and not result.needs_restore:
            result.errors.append("Lagging replica did not converge or emit NeedsRestore code")

    finally:
        if lagging_name in replicas:
            try:
                os.kill(replicas[lagging_name][0].pid, signal.SIGCONT)
            except Exception:
                pass
        for proc, rdir, log_handle, log_path in replicas.values():
            stop_process(proc, log_handle)
        if primary is not None:
            stop_process(primary, primary_log)

        dbs = [("primary", env.primary_db)]
        for name, (_, rdir, _, _) in replicas.items():
            dbs.append((name, rdir / "primary.db"))
        for name, db_path in dbs:
            ok, detail = run_integrity_check(str(db_path))
            result.integrity_results.append(f"{name}: {detail}")
            if not ok:
                result.errors.append(f"Integrity check failed for {name}: {detail}")

    return result


def print_report(result: DivergenceResult) -> bool:
    print("\n" + "=" * 60)
    print("STREAM V2 DIVERGENCE TEST REPORT")
    print("=" * 60)
    print(f"Converged:              {'YES' if result.converged else 'NO'}")
    print(f"NeedsRestore:           {'YES' if result.needs_restore else 'NO'}")
    if result.error_code:
        print(f"NeedsRestore Code:      {result.error_code}")
    print(f"Primary Digest:         {result.primary_digest}")
    print(f"Lagging Digest:         {result.lagging_digest}")
    print("Replica Digests:")
    for digest in result.replica_digests:
        print(f"  - {digest}")
    print("Integrity Checks:")
    for detail in result.integrity_results:
        print(f"  - {detail}")
    if result.errors:
        print("\nErrors:")
        for err in result.errors[:10]:
            print(f"  - {err}")
    print("=" * 60)

    success = (
        (result.converged or (result.needs_restore and bool(result.error_code)))
        and not result.errors
    )
    print(f"RESULT: {'PASS' if success else 'FAIL'}")
    return success


def main():
    parser = argparse.ArgumentParser(description="Stream v2 divergence test")
    parser.add_argument("-n", "--num-writes", type=int, default=400)
    parser.add_argument("-s", "--snapshot-every", type=int, default=80)
    parser.add_argument("-l", "--lag-seconds", type=int, default=8)

    args = parser.parse_args()

    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)

    result = run_test(args.num_writes, args.snapshot_every, args.lag_seconds)
    success = print_report(result)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
