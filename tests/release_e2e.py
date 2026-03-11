#!/usr/bin/env python3
"""Canonical high-trust release-binary E2E scenarios for replited."""

from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from test_utils import TestEnv, compute_db_digest, get_free_port, run_integrity_check, wait_for_port

REPLITED_BIN = Path(__file__).resolve().parent.parent / "target" / "release" / "replited"
TABLE_NAME = "release_e2e"


@dataclass
class Proc:
    proc: subprocess.Popen
    log_path: Path
    log_handle: object


@dataclass
class ScenarioResult:
    name: str
    success: bool
    errors: list[str]
    primary_digest: str | None = None
    replica_digest: str | None = None


def ensure_release_binary() -> None:
    if REPLITED_BIN.exists():
        return
    raise FileNotFoundError(
        f"release binary not found at {REPLITED_BIN}. Run `cargo build --release` first."
    )


def stop_process(proc: Proc | None) -> int | None:
    if proc is None:
        return None
    proc.proc.terminate()
    try:
        return_code = proc.proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.proc.kill()
        return_code = proc.proc.wait(timeout=5)
    proc.log_handle.close()
    return return_code


def write_primary_config(env: TestEnv, port: int, log_level: str = "Info") -> None:
    env.primary_toml.write_text(
        f"""
[log]
level = "{log_level}"
dir = "logs"

[[database]]
db = "primary.db"
cache_root = "cache"
max_concurrent_snapshots = 4
wal_retention_count = 5
min_checkpoint_page_number = 10
max_checkpoint_page_number = 200

[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "127.0.0.1:{port}"
""".strip()
        + "\n"
    )


def write_replica_config(
    replica_dir: Path,
    port: int,
    db_name: str = "primary.db",
    log_level: str = "Debug",
) -> None:
    (replica_dir / "replica.toml").write_text(
        f"""
[log]
level = "{log_level}"
dir = "logs"

[[database]]
db = "{db_name}"
cache_root = "cache"
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 200
wal_retention_count = 2

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:{port}"
""".strip()
        + "\n"
    )


def start_primary(env: TestEnv, port: int, log_name: str = "primary.log") -> Proc:
    write_primary_config(env, port)
    log_path = env.root / log_name
    log_handle = open(log_path, "a")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", "primary.toml", "replicate"],
        cwd=str(env.root),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return Proc(proc=proc, log_path=log_path, log_handle=log_handle)


def start_replica(
    env: TestEnv,
    replica_name: str,
    port: int,
    *,
    force_restore: bool,
    db_name: str = "primary.db",
    log_name: str = "replica.log",
    log_level: str = "Debug",
) -> tuple[Proc, Path]:
    replica_dir = env.root / replica_name
    replica_dir.mkdir(parents=True, exist_ok=True)
    (replica_dir / "logs").mkdir(exist_ok=True)
    write_replica_config(replica_dir, port, db_name=db_name, log_level=log_level)
    log_path = replica_dir / log_name
    log_handle = open(log_path, "a")
    cmd = [str(REPLITED_BIN), "--config", "replica.toml", "replica-sidecar"]
    if force_restore:
        cmd.append("--force-restore")
    proc = subprocess.Popen(
        cmd,
        cwd=str(replica_dir),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return Proc(proc=proc, log_path=log_path, log_handle=log_handle), replica_dir


def initialize_db(db_path: Path) -> None:
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
    conn.commit()
    conn.close()


def write_rows(db_path: Path, start_seq: int, count: int) -> None:
    conn = sqlite3.connect(str(db_path), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    for sequence in range(start_seq, start_seq + count):
        cursor.execute(
            f"INSERT OR REPLACE INTO {TABLE_NAME} (sequence, payload) VALUES (?, ?)",
            (sequence, f"payload_{sequence}"),
        )
    conn.commit()
    conn.close()


def row_count(db_path: Path) -> int:
    if not db_path.exists() or db_path.is_dir():
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


def wait_for_digest_sync(primary_db: Path, replica_db: Path, timeout: int = 120) -> tuple[bool, str | None]:
    start = time.time()
    expected_count = row_count(primary_db)
    expected_digest = compute_db_digest(str(primary_db))
    while time.time() - start < timeout:
        if row_count(replica_db) == expected_count:
            replica_digest = compute_db_digest(str(replica_db))
            if expected_digest and replica_digest and replica_digest == expected_digest:
                return True, replica_digest
        time.sleep(1.0)
    return False, compute_db_digest(str(replica_db))


def meta_dir(replica_dir: Path, db_name: str = "primary.db") -> Path:
    return replica_dir / f".{db_name}-replited"


def replited_log_path(work_dir: Path) -> Path:
    return work_dir / "logs" / "replited.log"


def assert_integrity(db_path: Path, result: ScenarioResult, label: str) -> None:
    ok, detail = run_integrity_check(str(db_path))
    if not ok:
        result.errors.append(f"{label} integrity_check failed: {detail}")


def scenario_fresh_bootstrap() -> ScenarioResult:
    env = TestEnv("release_e2e_fresh_bootstrap")
    env.setup()
    result = ScenarioResult(name="fresh-bootstrap", success=False, errors=[])
    port = get_free_port()

    primary = None
    replica = None
    try:
        initialize_db(env.primary_db)
        write_rows(env.primary_db, 0, 40)

        primary = start_primary(env, port)
        if not wait_for_port(port, timeout=12):
            result.errors.append("Primary failed to open stream port")
            return result

        replica, replica_dir = start_replica(
            env,
            "replica_bootstrap",
            port,
            force_restore=True,
        )
        replica_db = replica_dir / "primary.db"
        synced, replica_digest = wait_for_digest_sync(env.primary_db, replica_db, timeout=150)
        if not synced:
            result.errors.append("Replica failed to complete initial bootstrap sync")
            return result

        write_rows(env.primary_db, 40, 20)
        synced_after_write, replica_digest = wait_for_digest_sync(
            env.primary_db,
            replica_db,
            timeout=120,
        )
        result.primary_digest = compute_db_digest(str(env.primary_db))
        result.replica_digest = replica_digest
        if not synced_after_write:
            result.errors.append("Replica failed to catch writes after bootstrap")
            return result

        assert_integrity(env.primary_db, result, "primary")
        assert_integrity(replica_db, result, "replica")
        result.success = not result.errors
        return result
    finally:
        stop_process(replica)
        stop_process(primary)


def scenario_resume_without_restore() -> ScenarioResult:
    env = TestEnv("release_e2e_resume")
    env.setup()
    result = ScenarioResult(name="resume-without-restore", success=False, errors=[])
    port = get_free_port()

    primary = None
    replica = None
    restarted_replica = None
    try:
        initialize_db(env.primary_db)
        write_rows(env.primary_db, 0, 25)

        primary = start_primary(env, port)
        if not wait_for_port(port, timeout=12):
            result.errors.append("Primary failed to open stream port")
            return result

        replica, replica_dir = start_replica(
            env,
            "replica_resume",
            port,
            force_restore=True,
            log_name="replica_initial.log",
        )
        replica_db = replica_dir / "primary.db"
        synced, _ = wait_for_digest_sync(env.primary_db, replica_db, timeout=150)
        if not synced:
            result.errors.append("Replica failed initial bootstrap before restart")
            return result

        stop_process(replica)
        replica = None

        lsn_file = meta_dir(replica_dir) / "last_applied_lsn"
        if not lsn_file.exists():
            result.errors.append("Replica did not persist last_applied_lsn before restart")
            return result

        write_rows(env.primary_db, 25, 15)
        replited_log = replited_log_path(replica_dir)
        restart_log_offset = replited_log.stat().st_size if replited_log.exists() else 0

        restarted_replica, _ = start_replica(
            env,
            "replica_resume",
            port,
            force_restore=False,
            log_name="replica_restart.log",
            log_level="Debug",
        )
        resumed, replica_digest = wait_for_digest_sync(env.primary_db, replica_db, timeout=150)
        result.primary_digest = compute_db_digest(str(env.primary_db))
        result.replica_digest = replica_digest
        if not resumed:
            result.errors.append("Replica failed to resume and catch up after restart")
            return result

        restart_log = ""
        if replited_log.exists():
            with replited_log.open("r") as handle:
                handle.seek(restart_log_offset)
                restart_log = handle.read()
        if "loop state: CatchingUp" not in restart_log:
            result.errors.append("Restarted replica did not enter CatchingUp state")
        if "loop state: Bootstrapping" in restart_log:
            result.errors.append("Restarted replica unexpectedly re-entered Bootstrapping")

        assert_integrity(replica_db, result, "replica")
        result.success = not result.errors
        return result
    finally:
        stop_process(restarted_replica)
        stop_process(replica)
        stop_process(primary)


SCENARIOS = {
    "fresh-bootstrap": scenario_fresh_bootstrap,
    "resume-without-restore": scenario_resume_without_restore,
}


def run_scenarios(names: list[str]) -> list[ScenarioResult]:
    return [SCENARIOS[name]() for name in names]


def print_report(results: list[ScenarioResult]) -> bool:
    print("\n=== Release E2E Report ===")
    all_ok = True
    for result in results:
        status = "PASS" if result.success else "FAIL"
        print(f"\n[{status}] {result.name}")
        if result.primary_digest:
            print(f"  primary_digest={result.primary_digest}")
        if result.replica_digest:
            print(f"  replica_digest={result.replica_digest}")
        if result.errors:
            all_ok = False
            for error in result.errors:
                print(f"  - {error}")
    return all_ok


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=sorted(SCENARIOS),
        help="Run only the named scenario. May be passed multiple times.",
    )
    return parser.parse_args()


def main() -> int:
    ensure_release_binary()
    args = parse_args()
    scenario_names = args.scenario or list(SCENARIOS)
    results = run_scenarios(scenario_names)
    ok = print_report(results)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
