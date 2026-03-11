#!/usr/bin/env python3
"""
Archival restore benchmark for manifest-first archival.

Initial scope is the canonical fs archival path to keep setup simple and
measure something immediately useful:
- time until archival state becomes restorable
- successful restore wall time
- effective restored MiB/sec
- restore request-cost diagnostics
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import compute_db_digest, run_integrity_check

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
ARTIFACT_ROOT = Path(__file__).parent.parent / "output" / "benchmarks" / "archival"
RESTORE_SUMMARY_RE = re.compile(
    r"restore_request_cost path=(?P<path>\w+) "
    r"latest_pointer_gets=(?P<latest_pointer_gets>\d+) "
    r"generation_manifest_gets=(?P<generation_manifest_gets>\d+) "
    r"object_gets=(?P<object_gets>\d+) "
    r"list_calls=(?P<list_calls>\d+) "
    r"cache_hits=(?P<cache_hits>\d+) "
    r"cache_misses=(?P<cache_misses>\d+) "
    r"cache_write_failures=(?P<cache_write_failures>\d+)"
)


@dataclass
class ArchivalBenchmarkResult:
    backend: str
    total_rows: int
    payload_size: int
    archive_ready_seconds: float
    restore_wall_seconds: float
    restored_mib_per_second: float
    restore_path: str
    latest_pointer_gets: int
    generation_manifest_gets: int
    object_gets: int
    list_calls: int
    cache_hits: int
    cache_misses: int
    cache_write_failures: int
    primary_digest: str
    restored_digest: str


def cleanup() -> None:
    if ARTIFACT_ROOT.exists():
        shutil.rmtree(ARTIFACT_ROOT)
    (ARTIFACT_ROOT / "logs").mkdir(parents=True, exist_ok=True)
    (ARTIFACT_ROOT / "backup").mkdir(exist_ok=True)
    (ARTIFACT_ROOT / "cache").mkdir(exist_ok=True)


def write_primary_config() -> None:
    config = """
[log]
level = "Info"
dir = "logs"

[[database]]
db = "primary.db"
cache_root = "cache"
min_checkpoint_page_number = 500
max_checkpoint_page_number = 5000
truncate_page_number = 200000
checkpoint_interval_secs = 60
monitor_interval_ms = 50
wal_retention_count = 20

[[database.replicate]]
name = "backup"
[database.replicate.params]
type = "fs"
root = "./backup"
""".strip()
    (ARTIFACT_ROOT / "primary.toml").write_text(config + "\n")


def start_primary() -> subprocess.Popen:
    log = open(ARTIFACT_ROOT / "logs" / "primary_archival.log", "w")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", str(ARTIFACT_ROOT / "primary.toml"), "replicate"],
        cwd=str(ARTIFACT_ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    time.sleep(3)
    return proc


def initialize_primary_db() -> None:
    conn = sqlite3.connect(str(ARTIFACT_ROOT / "primary.db"), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS archival_bench (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload BLOB NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def write_rows(total_rows: int, payload_size: int, commit_batch_size: int) -> None:
    payload = b"x" * payload_size
    conn = sqlite3.connect(str(ARTIFACT_ROOT / "primary.db"), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA busy_timeout=5000")
    for offset in range(total_rows):
        cursor.execute(
            "INSERT INTO archival_bench (id, payload) VALUES (?, ?)",
            (offset + 1, payload),
        )
        if (offset + 1) % commit_batch_size == 0:
            conn.commit()
    conn.commit()
    conn.close()


def parse_restore_diagnostics(log_text: str) -> dict[str, int | str] | None:
    matches = list(RESTORE_SUMMARY_RE.finditer(log_text))
    if not matches:
        return None
    match = matches[-1]
    return {
        "restore_path": match.group("path"),
        "latest_pointer_gets": int(match.group("latest_pointer_gets")),
        "generation_manifest_gets": int(match.group("generation_manifest_gets")),
        "object_gets": int(match.group("object_gets")),
        "list_calls": int(match.group("list_calls")),
        "cache_hits": int(match.group("cache_hits")),
        "cache_misses": int(match.group("cache_misses")),
        "cache_write_failures": int(match.group("cache_write_failures")),
    }


def read_log_delta(log_path: Path, offset: int) -> str:
    if not log_path.exists():
        return ""
    with open(log_path, "r", encoding="utf-8") as handle:
        handle.seek(offset)
        return handle.read()


def remove_restore_output(output_db: Path) -> None:
    for candidate in [
        output_db,
        output_db.with_name(output_db.name + "-wal"),
        output_db.with_name(output_db.name + "-shm"),
    ]:
        if candidate.exists():
            candidate.unlink()


def try_restore(output_db: Path, backend_name: str) -> tuple[subprocess.CompletedProcess[str], dict[str, int | str] | None]:
    remove_restore_output(output_db)
    log_path = ARTIFACT_ROOT / "logs" / "replited.log"
    log_offset = log_path.stat().st_size if log_path.exists() else 0
    result = subprocess.run(
        [
            str(REPLITED_BIN),
            "--config",
            str((ARTIFACT_ROOT / "primary.toml").resolve()),
            "restore",
            "--db",
            "primary.db",
            "--output",
            str(output_db.resolve()),
            "--truth-source",
            backend_name,
        ],
        cwd=str(ARTIFACT_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    log_delta = read_log_delta(log_path, log_offset)
    diagnostics = parse_restore_diagnostics("\n".join([result.stdout, result.stderr, log_delta]))
    return result, diagnostics


def measure_archival_restore(total_rows: int, payload_size: int, commit_batch_size: int) -> ArchivalBenchmarkResult:
    cleanup()
    write_primary_config()

    primary = start_primary()
    try:
        initialize_primary_db()
        write_rows(total_rows, payload_size, commit_batch_size)

        primary_digest = compute_db_digest(str(ARTIFACT_ROOT / "primary.db"))
        restore_output = ARTIFACT_ROOT / "restored.db"

        ready_start = time.perf_counter()
        deadline = time.time() + 60
        last_result: subprocess.CompletedProcess[str] | None = None
        last_diagnostics: dict[str, int | str] | None = None
        restore_wall_seconds = 0.0
        while time.time() < deadline:
            restore_start = time.perf_counter()
            result, diagnostics = try_restore(restore_output, "backup")
            restore_wall_seconds = time.perf_counter() - restore_start
            last_result = result
            last_diagnostics = diagnostics
            if result.returncode == 0:
                restored_digest = compute_db_digest(str(restore_output))
                if restored_digest == primary_digest:
                    archive_ready_seconds = time.perf_counter() - ready_start
                    primary_ok, primary_detail = run_integrity_check(str(ARTIFACT_ROOT / "primary.db"))
                    restored_ok, restored_detail = run_integrity_check(str(restore_output))
                    if not primary_ok:
                        raise RuntimeError(f"primary integrity_check failed: {primary_detail}")
                    if not restored_ok:
                        raise RuntimeError(f"restored integrity_check failed: {restored_detail}")
                    if last_diagnostics is None:
                        _, last_diagnostics = try_restore(restore_output, "backup")
                    mib = (payload_size * total_rows) / (1024 * 1024)
                    stats = last_diagnostics or {
                        "restore_path": "unknown",
                        "latest_pointer_gets": 0,
                        "generation_manifest_gets": 0,
                        "object_gets": 0,
                        "list_calls": 0,
                        "cache_hits": 0,
                        "cache_misses": 0,
                        "cache_write_failures": 0,
                    }
                    return ArchivalBenchmarkResult(
                        backend="fs",
                        total_rows=total_rows,
                        payload_size=payload_size,
                        archive_ready_seconds=archive_ready_seconds,
                        restore_wall_seconds=restore_wall_seconds,
                        restored_mib_per_second=mib / max(restore_wall_seconds, 0.000001),
                        restore_path=str(stats["restore_path"]),
                        latest_pointer_gets=int(stats["latest_pointer_gets"]),
                        generation_manifest_gets=int(stats["generation_manifest_gets"]),
                        object_gets=int(stats["object_gets"]),
                        list_calls=int(stats["list_calls"]),
                        cache_hits=int(stats["cache_hits"]),
                        cache_misses=int(stats["cache_misses"]),
                        cache_write_failures=int(stats["cache_write_failures"]),
                        primary_digest=primary_digest or "",
                        restored_digest=restored_digest or "",
                    )
            time.sleep(0.25)

        detail = ""
        if last_result is not None:
            detail = (last_result.stderr or last_result.stdout or "").strip()
        raise RuntimeError(f"archival restore did not succeed before timeout: {detail}")
    finally:
        primary.terminate()
        primary.wait(timeout=10)


def print_report(result: ArchivalBenchmarkResult) -> None:
    print("\n" + "=" * 72)
    print("ARCHIVAL RESTORE BENCHMARK REPORT")
    print("=" * 72)
    print(f"Backend:            {result.backend}")
    print(f"Rows:               {result.total_rows}")
    print(f"Payload size:       {result.payload_size} bytes")
    print(f"Archive ready:      {result.archive_ready_seconds:.2f}s")
    print(f"Restore wall time:  {result.restore_wall_seconds:.2f}s")
    print(f"Restore MiB/s:      {result.restored_mib_per_second:.2f}")
    print(f"Restore path:       {result.restore_path}")
    print(f"latest_pointer_gets {result.latest_pointer_gets}")
    print(f"generation_manifest_gets {result.generation_manifest_gets}")
    print(f"object_gets         {result.object_gets}")
    print(f"list_calls          {result.list_calls}")
    print(f"cache_hits          {result.cache_hits}")
    print(f"cache_misses        {result.cache_misses}")
    print(f"cache_write_failures {result.cache_write_failures}")
    print("=" * 72)


def save_results(result: ArchivalBenchmarkResult, output_file: str) -> None:
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "result": asdict(result),
    }
    with open(output_file, "w") as handle:
        json.dump(output, handle, indent=2)
    print(f"\nResults saved to {output_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Archival restore benchmark")
    parser.add_argument("-n", "--num-rows", type=int, default=2000)
    parser.add_argument("-p", "--payload-size", type=int, default=4096)
    parser.add_argument("-b", "--commit-batch-size", type=int, default=100)
    parser.add_argument("-o", "--output", type=str, default="archival_results.json")
    args = parser.parse_args()

    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)

    print(
        f"Running archival benchmark with rows={args.num_rows}, "
        f"payload={args.payload_size}, batch={args.commit_batch_size}"
    )
    result = measure_archival_restore(args.num_rows, args.payload_size, args.commit_batch_size)
    print_report(result)
    save_results(result, args.output)


if __name__ == "__main__":
    main()
