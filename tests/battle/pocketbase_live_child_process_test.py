#!/usr/bin/env python3
"""
Manual PocketBase child-process regression for externally-applied replica WAL.

This is intentionally gated behind RUN_POCKETBASE_LIVE=1 because it downloads or
uses a real PocketBase binary, starts HTTP listeners, and can take more than a
minute. The protected behavior is that the maintained live demo reaches replica
health and observes a primary-created record through the long-lived replica
PocketBase API.
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import get_free_port

ROOT = Path(__file__).resolve().parents[2]
DEMO = ROOT / "example" / "run_pocketbase_live_demo.sh"
REPLITED_BIN = ROOT / "target" / "release" / "replited"
RUNTIME_DIR = ROOT / "example" / "runtime"
LOG_DIR = RUNTIME_DIR / "logs"
REPLICA_DB = RUNTIME_DIR / "replica" / "data.db"

DIAGNOSTIC_MARKERS = [
    "database disk image is malformed",
    "disk I/O error",
    "SnapshotBoundaryMismatch",
    "LineageMismatch",
    "WAL checkpoint",
    "seeded WAL prefix",
    "WAL index advanced",
    "ProcessManager",
    "ReplicaSidecar error",
]


def skip_if_not_enabled() -> None:
    if os.environ.get("RUN_POCKETBASE_LIVE") == "1":
        return
    print("SKIP: set RUN_POCKETBASE_LIVE=1 to run the real PocketBase live regression")
    raise SystemExit(0)


def require_replited_binary() -> None:
    if REPLITED_BIN.exists():
        return
    print(f"missing replited binary at {REPLITED_BIN}", file=sys.stderr)
    print("run: cargo build --release", file=sys.stderr)
    raise SystemExit(1)


def sqlite_quick_check(db_path: Path) -> str:
    if not db_path.exists():
        return f"{db_path} does not exist"
    try:
        conn = sqlite3.connect(str(db_path), timeout=1.0)
        rows = conn.execute("PRAGMA quick_check").fetchall()
        conn.close()
        return "; ".join(str(row[0]) for row in rows)
    except Exception as exc:
        return f"quick_check failed: {exc}"


def extract_log_markers(log_path: Path, max_lines: int = 80) -> list[str]:
    if not log_path.exists():
        return [f"{log_path}: missing"]
    lines = log_path.read_text(errors="ignore").splitlines()
    matches = [
        f"{log_path}:{lineno}: {line}"
        for lineno, line in enumerate(lines, start=1)
        if any(marker in line for marker in DIAGNOSTIC_MARKERS)
    ]
    if matches:
        return matches[-max_lines:]
    return [f"{log_path}:tail: {line}" for line in lines[-min(max_lines, len(lines)) :]]


def print_diagnostics(result: subprocess.CompletedProcess[str]) -> None:
    print("\n" + "=" * 72)
    print("POCKETBASE LIVE CHILD-PROCESS DIAGNOSTICS")
    print("=" * 72)
    print(f"exit_code: {result.returncode}")
    print(f"replica_quick_check: {sqlite_quick_check(REPLICA_DB)}")
    if result.stdout:
        print("stdout tail:")
        print("\n".join(result.stdout.splitlines()[-80:]))
    if result.stderr:
        print("stderr tail:")
        print("\n".join(result.stderr.splitlines()[-80:]))
    for log_name in [
        "primary-pocketbase.log",
        "primary-replited.log",
        "replica-sidecar.log",
    ]:
        for line in extract_log_markers(LOG_DIR / log_name):
            print(line)
    print("=" * 72)


def run_demo() -> subprocess.CompletedProcess[str]:
    primary_port = get_free_port()
    replica_port = get_free_port()
    env = os.environ.copy()
    env.update(
        {
            "DEMO_HOLD": "0",
            "PRIMARY_HTTP": f"127.0.0.1:{primary_port}",
            "REPLICA_HTTP": f"127.0.0.1:{replica_port}",
            "PRIMARY_URL": f"http://127.0.0.1:{primary_port}",
            "REPLICA_URL": f"http://127.0.0.1:{replica_port}",
            "REPLITED_BIN": str(REPLITED_BIN),
        }
    )
    env.setdefault("POCKETBASE_ADMIN_EMAIL", "test@example.com")
    env.setdefault("POCKETBASE_ADMIN_PASS", "password123456")
    env.setdefault("POCKETBASE_VERSION", "0.36.9")

    shutil.rmtree(RUNTIME_DIR, ignore_errors=True)
    return subprocess.run(
        ["bash", str(DEMO)],
        cwd=str(ROOT),
        env=env,
        text=True,
        capture_output=True,
        timeout=180,
        check=False,
    )


def main() -> int:
    skip_if_not_enabled()
    require_replited_binary()
    start = time.time()
    result = run_demo()
    elapsed = time.time() - start
    if result.returncode == 0:
        print(f"PASS: PocketBase live demo completed in {elapsed:.1f}s")
        return 0
    print_diagnostics(result)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
