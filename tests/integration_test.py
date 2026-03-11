#!/usr/bin/env python3
"""FS manifest archival/restore integration test used by GitHub Actions."""

from __future__ import annotations

import argparse
import re
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from test_utils import TestEnv, compute_db_digest, run_integrity_check

TABLE_NAME = "integration_rows"
RESTORE_SUMMARY_RE = re.compile(
    r"restore_request_cost path=(?P<path>\w+) "
    r"latest_pointer_gets=(?P<latest_pointer_gets>\d+) "
    r"generation_manifest_gets=(?P<generation_manifest_gets>\d+) "
    r"object_gets=(?P<object_gets>\d+) "
    r"list_calls=(?P<list_calls>\d+)"
)


@dataclass
class Proc:
    proc: subprocess.Popen
    log_path: Path
    log_handle: object


@dataclass
class RestoreDiagnostics:
    path: str
    latest_pointer_gets: int
    generation_manifest_gets: int
    object_gets: int
    list_calls: int
    raw_line: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("timeout", type=int, help="timeout in seconds or milliseconds")
    parser.add_argument("binary")
    return parser.parse_args()


def effective_timeout(raw: int) -> int:
    if raw >= 1000:
        return max(30, raw // 1000)
    return max(30, raw)


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


def initialize_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
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


def render_config(env: TestEnv, db_path: Path) -> str:
    root = str(env.root.resolve())
    shared = f"""
[log]
level = "Debug"
dir = "{root}/logs"

[[database]]
db = "{db_path.resolve()}"
min_checkpoint_page_number = 10
max_checkpoint_page_number = 200
truncate_page_number = 1000
checkpoint_interval_secs = 1
monitor_interval_ms = 200
wal_retention_count = 5
max_concurrent_snapshots = 2
""".strip()

    replicate = f"""
[[database.replicate]]
name = "fs-backup"
[database.replicate.params]
type = "fs"
root = "{env.backup_dir.resolve()}"
""".strip()

    return shared + "\n\n" + replicate + "\n"


def start_primary(binary: Path, config_path: Path, cwd: Path, log_path: Path) -> Proc:
    log_handle = open(log_path, "a")
    proc = subprocess.Popen(
        [str(binary), "--config", str(config_path.resolve()), "replicate"],
        cwd=str(cwd),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    return Proc(proc=proc, log_path=log_path, log_handle=log_handle)


def remove_restore_output(output_db: Path) -> None:
    for candidate in [
        output_db,
        output_db.with_name(output_db.name + "-wal"),
        output_db.with_name(output_db.name + "-shm"),
    ]:
        try:
            candidate.unlink()
        except FileNotFoundError:
            pass


def replited_log_path(env: TestEnv) -> Path:
    return env.logs_dir / "replited.log"


def read_log_delta(log_path: Path, offset: int) -> tuple[int, str]:
    if not log_path.exists():
        return 0, ""
    with log_path.open("r", errors="replace") as handle:
        handle.seek(offset)
        delta = handle.read()
        return handle.tell(), delta


def parse_restore_diagnostics(log_text: str) -> RestoreDiagnostics | None:
    matches = list(RESTORE_SUMMARY_RE.finditer(log_text))
    if not matches:
        return None
    match = matches[-1]
    return RestoreDiagnostics(
        path=match.group("path"),
        latest_pointer_gets=int(match.group("latest_pointer_gets")),
        generation_manifest_gets=int(match.group("generation_manifest_gets")),
        object_gets=int(match.group("object_gets")),
        list_calls=int(match.group("list_calls")),
        raw_line=match.group(0),
    )


def run_restore(
    binary: Path,
    config_path: Path,
    db_path: Path,
    output_db: Path,
    log_path: Path,
) -> tuple[subprocess.CompletedProcess, RestoreDiagnostics | None]:
    remove_restore_output(output_db)
    log_offset = log_path.stat().st_size if log_path.exists() else 0
    result = subprocess.run(
        [
            str(binary),
            "--config",
            str(config_path.resolve()),
            "restore",
            "--db",
            str(db_path.resolve()),
            "--output",
            str(output_db),
        ],
        capture_output=True,
        text=True,
    )
    _, log_delta = read_log_delta(log_path, log_offset)
    return result, parse_restore_diagnostics(log_delta)


def restore_until_digest(
    binary: Path,
    config_path: Path,
    db_path: Path,
    output_db: Path,
    log_path: Path,
    expected_digest: str,
    deadline: float,
) -> tuple[bool, str, RestoreDiagnostics | None]:
    last_error = "restore never attempted"
    last_diagnostics = None
    while time.time() < deadline:
        result, diagnostics = run_restore(binary, config_path, db_path, output_db, log_path)
        last_diagnostics = diagnostics
        if result.returncode == 0:
            if diagnostics is None:
                last_error = "restore succeeded but request-cost summary line was missing"
            else:
                restored_digest = compute_db_digest(str(output_db))
                if restored_digest == expected_digest:
                    ok, detail = run_integrity_check(str(output_db))
                    if not ok:
                        last_error = (
                            f"restore matched digest but integrity_check failed: {detail}"
                        )
                    elif diagnostics.path != "manifest":
                        last_error = (
                            f"fs restore expected manifest path but saw {diagnostics.path}"
                        )
                    elif diagnostics.list_calls != 0:
                        last_error = (
                            "fs manifest restore expected list_calls=0 but saw "
                            f"{diagnostics.list_calls}"
                        )
                    else:
                        return True, detail, diagnostics
                else:
                    last_error = (
                        "restore succeeded but digest mismatched "
                        f"(expected={expected_digest}, actual={restored_digest})"
                    )
        else:
            stderr = result.stderr.strip() or result.stdout.strip()
            last_error = stderr or f"restore exited with {result.returncode}"
        time.sleep(1.0)
    return False, last_error, last_diagnostics


def tail(path: Path, lines: int = 40) -> str:
    if not path.exists():
        return ""
    text = path.read_text(errors="replace")
    return "\n".join(text.splitlines()[-lines:])


def print_restore_diagnostics(backend: str, diagnostics: RestoreDiagnostics) -> None:
    print(f"restore diagnostics ({backend}): {diagnostics.raw_line}")


def verify_archival_restore(binary: Path, timeout_sec: int) -> int:
    backend = "fs"
    env = TestEnv("fs_integration")
    env.setup()

    db_path = env.root / "test.db"
    config_path = env.root / "primary.toml"
    restore_output = env.root / "restored.db"
    restore_log = replited_log_path(env)
    primary_log = env.root / "primary.log"
    primary = None

    try:
        initialize_db(db_path)
        config_path.write_text(render_config(env, db_path))

        primary = start_primary(binary, config_path, env.root, primary_log)
        time.sleep(2.0)
        if primary.proc.poll() is not None:
            print("FAIL: primary process exited early")
            print(tail(primary_log))
            return 1

        write_rows(db_path, 0, 64)
        deadline = time.time() + timeout_sec
        expected_digest = compute_db_digest(str(db_path))
        ok, detail, diagnostics = restore_until_digest(
            binary,
            config_path,
            db_path,
            restore_output,
            restore_log,
            expected_digest,
            deadline,
        )
        if not ok:
            print(f"FAIL: initial restore validation failed for {backend}: {detail}")
            if diagnostics is not None:
                print_restore_diagnostics(backend, diagnostics)
            print("--- primary log tail ---")
            print(tail(primary_log))
            return 1
        if diagnostics is not None:
            print_restore_diagnostics(backend, diagnostics)

        write_rows(db_path, 64, 64)
        expected_digest = compute_db_digest(str(db_path))
        ok, detail, diagnostics = restore_until_digest(
            binary,
            config_path,
            db_path,
            restore_output,
            restore_log,
            expected_digest,
            deadline,
        )
        if not ok:
            print(f"FAIL: incremental restore validation failed for {backend}: {detail}")
            if diagnostics is not None:
                print_restore_diagnostics(backend, diagnostics)
            print("--- primary log tail ---")
            print(tail(primary_log))
            return 1
        if diagnostics is not None:
            print_restore_diagnostics(backend, diagnostics)

        print(f"PASS: backend {backend} archived and restored successfully")
        print(f"primary digest: {expected_digest}")
        print(f"restored digest: {compute_db_digest(str(restore_output))}")
        return 0
    finally:
        stop_process(primary)


def main() -> int:
    args = parse_args()
    binary = Path(args.binary).resolve()
    if not binary.exists():
        print(f"binary not found: {binary}")
        return 2

    timeout_sec = effective_timeout(args.timeout)
    return verify_archival_restore(binary, timeout_sec)


if __name__ == "__main__":
    sys.exit(main())
