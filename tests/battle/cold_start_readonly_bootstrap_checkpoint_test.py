#!/usr/bin/env python3
"""
Cold-start sidecar regression for readonly app bootstrap and same-index WAL chunks.

This scenario reproduces the production class where the replica starts with an
empty local DB, bootstraps via `replica-sidecar --force-restore --exec`, then
receives a WAL stream large enough to be split into multiple chunks in the same
WAL index. A regression here typically appears as InvalidLsn, disk I/O errors,
or a reader app starting before the DB exists.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import TestEnv, compute_db_digest, get_free_port, run_integrity_check, wait_for_port

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
TABLE_NAME = "cold_start_rows"

LOG_DENYLIST = [
    "InvalidLsn",
    "ack regression",
    "database disk image is malformed",
    "disk I/O error",
    "SnapshotBoundaryMismatch",
    "LineageMismatch",
    "WAL Stuck",
]


@dataclass
class Proc:
    proc: subprocess.Popen
    log_handle: object
    log_path: Path


@dataclass
class ScenarioResult:
    success: bool
    errors: list[str]
    primary_digest: str | None = None
    replica_digest: str | None = None
    diagnostics: list[str] | None = None


def stop_process(proc: Proc | None) -> None:
    if proc is None:
        return
    proc.proc.terminate()
    try:
        proc.proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.proc.kill()
        proc.proc.wait(timeout=5)
    proc.log_handle.close()


def write_primary_config(env: TestEnv, port: int) -> None:
    env.primary_toml.write_text(
        f"""
[log]
level = "Info"
dir = "logs"

[[database]]
db = "primary.db"
cache_root = "cache"
max_concurrent_snapshots = 4
wal_retention_secs = 120
wal_retention_count = 8
min_checkpoint_page_number = 1000
max_checkpoint_page_number = 10000
checkpoint_interval_secs = 5

[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "127.0.0.1:{port}"
""".strip()
        + "\n"
    )


def write_replica_config(replica_dir: Path, port: int) -> None:
    (replica_dir / "replica.toml").write_text(
        f"""
[log]
level = "Debug"
dir = "logs"

[[database]]
db = "primary.db"
cache_root = "cache"
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100
wal_retention_count = 4

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:{port}"
""".strip()
        + "\n"
    )


def write_reader_app(replica_dir: Path) -> Path:
    script = replica_dir / "readonly_reader.py"
    script.write_text(
        f"""
from __future__ import annotations

import sqlite3
import sys
import time
from pathlib import Path

db_path = Path("primary.db")
ready_path = Path("reader.ready")
error_path = Path("reader.error")

if not db_path.exists():
    error_path.write_text("reader started before primary.db existed\\n")
    sys.exit(1)

uri = f"file:{{db_path}}?mode=ro"
try:
    conn = sqlite3.connect(uri, uri=True, timeout=1.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA query_only = ON")
    cursor.execute("SELECT COUNT(*) FROM {TABLE_NAME}")
    cursor.fetchone()
    ready_path.write_text("ready\\n")
    while True:
        cursor.execute("SELECT COUNT(*) FROM {TABLE_NAME}")
        cursor.fetchone()
        time.sleep(0.2)
except Exception as exc:
    error_path.write_text(str(exc) + "\\n")
    sys.exit(1)
""".lstrip()
    )
    return script


def start_primary(env: TestEnv, port: int) -> Proc:
    write_primary_config(env, port)
    log_path = env.root / "primary.log"
    log_handle = open(log_path, "a")
    proc = subprocess.Popen(
        [str(REPLITED_BIN.resolve()), "--config", "primary.toml", "replicate"],
        cwd=str(env.root),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return Proc(proc=proc, log_handle=log_handle, log_path=log_path)


def start_replica_with_reader(env: TestEnv, port: int) -> tuple[Proc, Path]:
    replica_dir = env.root / "replica_readonly"
    replica_dir.mkdir(parents=True, exist_ok=True)
    (replica_dir / "logs").mkdir(exist_ok=True)
    write_replica_config(replica_dir, port)
    reader_script = write_reader_app(replica_dir)
    log_path = replica_dir / "replica.log"
    log_handle = open(log_path, "a")
    cmd = [
        str(REPLITED_BIN.resolve()),
        "--config",
        "replica.toml",
        "replica-sidecar",
        "--force-restore",
        "--exec",
        f"{sys.executable} {reader_script.name}",
    ]
    proc = subprocess.Popen(
        cmd,
        cwd=str(replica_dir),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return Proc(proc=proc, log_handle=log_handle, log_path=log_path), replica_dir


def initialize_primary(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            sequence INTEGER PRIMARY KEY,
            payload TEXT NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def write_rows(db_path: Path, start: int, count: int, payload_size: int) -> None:
    payload = "x" * payload_size
    conn = sqlite3.connect(str(db_path), timeout=5.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    for sequence in range(start, start + count):
        cursor.execute(
            f"INSERT OR REPLACE INTO {TABLE_NAME} (sequence, payload) VALUES (?, ?)",
            (sequence, f"{sequence}:{payload}"),
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
        count = int(cursor.fetchone()[0])
        conn.close()
        return count
    except Exception:
        return -1


def wait_for_digest_sync(
    primary_db: Path,
    replica_db: Path,
    timeout: int = 180,
) -> tuple[bool, str | None]:
    expected_count = row_count(primary_db)
    expected_digest = compute_db_digest(str(primary_db))
    start = time.time()
    while time.time() - start < timeout:
        if row_count(replica_db) == expected_count:
            replica_digest = compute_db_digest(str(replica_db))
            if expected_digest and replica_digest == expected_digest:
                return True, replica_digest
        time.sleep(0.5)
    return False, compute_db_digest(str(replica_db))


def wait_for_reader_ready(replica_dir: Path, timeout: int = 60) -> bool:
    ready_path = replica_dir / "reader.ready"
    error_path = replica_dir / "reader.error"
    start = time.time()
    while time.time() - start < timeout:
        if error_path.exists():
            return False
        if ready_path.exists():
            return True
        time.sleep(0.2)
    return False


def scan_log_errors(log_paths: list[Path]) -> list[str]:
    errors: list[str] = []
    for log_path in log_paths:
        if not log_path.exists():
            continue
        text = log_path.read_text(errors="ignore")
        for marker in LOG_DENYLIST:
            if marker in text:
                errors.append(f"{log_path}: found denied marker {marker!r}")
    return errors


DIAGNOSTIC_MARKERS = [
    "WAL checkpoint",
    "WAL index advanced",
    "WAL-index",
    "Stale WAL-index",
    "ProcessManager",
    "InvalidLsn",
    "ack regression",
    "disk I/O error",
    "SnapshotBoundaryMismatch",
    "LineageMismatch",
    "WAL Stuck",
    "ReplicaSidecar error",
]


def diagnostic_log_excerpt(log_path: Path, max_lines: int = 80) -> str | None:
    if not log_path.exists():
        return None
    lines = log_path.read_text(errors="ignore").splitlines()
    matches: list[str] = []
    for lineno, line in enumerate(lines, start=1):
        if any(marker in line for marker in DIAGNOSTIC_MARKERS):
            matches.append(f"{log_path}:{lineno}: {line}")
    if len(matches) > max_lines:
        matches = matches[-max_lines:]
    if not matches:
        tail = lines[-min(40, len(lines)) :]
        matches = [f"{log_path}:tail: {line}" for line in tail]
    return "\n".join(matches)


def collect_diagnostics(log_paths: list[Path]) -> list[str]:
    excerpts: list[str] = []
    for log_path in log_paths:
        excerpt = diagnostic_log_excerpt(log_path)
        if excerpt:
            excerpts.append(excerpt)
    return excerpts


def run_scenario() -> ScenarioResult:
    env = TestEnv("cold_start_readonly_bootstrap_checkpoint")
    env.setup()
    result = ScenarioResult(success=False, errors=[], diagnostics=[])
    port = get_free_port()

    primary: Proc | None = None
    replica: Proc | None = None
    replica_dir: Path | None = None
    try:
        initialize_primary(env.primary_db)
        write_rows(env.primary_db, start=0, count=20, payload_size=256)

        primary = start_primary(env, port)
        if not wait_for_port(port, timeout=12):
            result.errors.append("Primary failed to open stream port")
            return result

        replica, replica_dir = start_replica_with_reader(env, port)
        replica_db = replica_dir / "primary.db"
        initial_synced, _ = wait_for_digest_sync(env.primary_db, replica_db, timeout=180)
        if not initial_synced:
            result.errors.append("Replica failed cold-start bootstrap sync")
            return result

        if not wait_for_reader_ready(replica_dir, timeout=60):
            reader_error = replica_dir / "reader.error"
            detail = reader_error.read_text(errors="ignore").strip() if reader_error.exists() else "timeout"
            result.errors.append(f"Readonly reader did not start cleanly after bootstrap: {detail}")
            return result

        # Force a WAL stream that exceeds the server chunk size while staying in
        # a single WAL index. Old sidecar logic checkpointed after the first
        # chunk, removed the local WAL, and failed the next non-zero offset chunk.
        write_rows(env.primary_db, start=20, count=220, payload_size=4096)
        final_synced, replica_digest = wait_for_digest_sync(env.primary_db, replica_db, timeout=240)
        result.primary_digest = compute_db_digest(str(env.primary_db))
        result.replica_digest = replica_digest
        if not final_synced:
            result.errors.append("Replica failed to apply large same-index WAL stream")

        primary_ok, primary_integrity = run_integrity_check(str(env.primary_db))
        replica_ok, replica_integrity = run_integrity_check(str(replica_db))
        if not primary_ok:
            result.errors.append(f"Primary integrity_check failed: {primary_integrity}")
        if not replica_ok:
            result.errors.append(f"Replica integrity_check failed: {replica_integrity}")

        time.sleep(2)
        log_paths = [
            env.root / "primary.log",
            replica.log_path,
            replica_dir / "logs" / "replited.log",
            replica_dir / "reader.error",
        ]
        result.errors.extend(scan_log_errors(log_paths))
        result.success = not result.errors
        if not result.success:
            result.diagnostics = collect_diagnostics(log_paths)
        return result
    finally:
        stop_process(replica)
        stop_process(primary)


def print_result(result: ScenarioResult) -> bool:
    print("\n" + "=" * 72)
    print("SIDECAR COLD-START READONLY BOOTSTRAP CHECKPOINT")
    print("=" * 72)
    print(f"Success:        {'YES' if result.success else 'NO'}")
    print(f"Primary digest: {result.primary_digest}")
    print(f"Replica digest: {result.replica_digest}")
    if result.errors:
        print("Errors:")
        for error in result.errors:
            print(f"  - {error}")
    if result.diagnostics:
        print("Diagnostics:")
        for excerpt in result.diagnostics:
            print(excerpt)
    print("=" * 72)
    return result.success


def main() -> int:
    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        return 1
    return 0 if print_result(run_scenario()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
