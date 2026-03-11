#!/usr/bin/env python3
"""
End-to-end throughput benchmark for stream replication.

Measures:
- initial replica bootstrap time
- steady-state catch-up wall time after primary writes
- effective replicated rows/sec
- effective replicated MiB/sec
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import statistics
import subprocess
import sys
import time
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import compute_db_digest, run_integrity_check

REPLITED_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "replited"
CONFIG_DIR = Path(__file__).parent.parent / "config"
ARTIFACT_ROOT = Path(__file__).parent.parent / "output" / "benchmarks" / "throughput"


@dataclass
class BenchmarkResult:
    payload_size: int
    total_transactions: int
    commit_batch_size: int
    initial_restore_seconds: float
    write_phase_seconds: float
    catchup_seconds: float
    end_to_end_rows_per_second: float
    end_to_end_mib_per_second: float
    catchup_rows_per_second: float
    catchup_mib_per_second: float
    primary_digest: str
    replica_digest: str


def cleanup() -> None:
    if ARTIFACT_ROOT.exists():
        shutil.rmtree(ARTIFACT_ROOT)
    (ARTIFACT_ROOT / "logs").mkdir(parents=True, exist_ok=True)
    (ARTIFACT_ROOT / "backup").mkdir(exist_ok=True)


def start_primary() -> subprocess.Popen:
    log = open(ARTIFACT_ROOT / "logs" / "primary_throughput.log", "w")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", str(CONFIG_DIR / "benchmark_primary.toml"), "replicate"],
        cwd=str(ARTIFACT_ROOT),
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    time.sleep(3)
    return proc


def prepare_replica() -> tuple[subprocess.Popen, Path]:
    replica_cwd = ARTIFACT_ROOT / "replica_cwd"
    replica_cwd.mkdir(exist_ok=True)
    config = """
[log]
level = "Info"
dir = "logs"

[[database]]
db = "replica.db"
cache_root = "cache"
min_checkpoint_page_number = 100
max_checkpoint_page_number = 1000
truncate_page_number = 50000
checkpoint_interval_secs = 30
monitor_interval_ms = 50
apply_checkpoint_frame_interval = 20
apply_checkpoint_interval_ms = 200
wal_retention_count = 20

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:50051"
remote_db_name = "primary.db"
""".strip()
    (replica_cwd / "replica.toml").write_text(config + "\n")
    (replica_cwd / "logs").mkdir(exist_ok=True)
    log = open(replica_cwd / "replica.log", "w")
    proc = subprocess.Popen(
        [str(REPLITED_BIN), "--config", "replica.toml", "replica-sidecar", "--force-restore"],
        cwd=str(replica_cwd),
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    return proc, replica_cwd


def initialize_primary_db() -> None:
    conn = sqlite3.connect(str(ARTIFACT_ROOT / "primary.db"), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bench (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload BLOB NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def write_rows(start_seq: int, total_transactions: int, payload_size: int, commit_batch_size: int) -> float:
    payload = b"x" * payload_size
    conn = sqlite3.connect(str(ARTIFACT_ROOT / "primary.db"), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA busy_timeout=5000")
    start = time.perf_counter()
    for offset in range(total_transactions):
        cursor.execute(
            "INSERT INTO bench (id, payload) VALUES (?, ?)",
            (start_seq + offset, payload),
        )
        if (offset + 1) % commit_batch_size == 0:
            conn.commit()
    conn.commit()
    end = time.perf_counter()
    conn.close()
    return end - start


def current_row_count(db_path: Path) -> int | None:
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=0.2)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM bench")
        count = cursor.fetchone()[0]
        conn.close()
        return count
    except Exception:
        return None


def wait_for_replica_digest(replica_db: Path, expected_rows: int, expected_digest: str, timeout_seconds: int) -> float:
    start = time.perf_counter()
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        count = current_row_count(replica_db)
        if count == expected_rows:
            digest = compute_db_digest(str(replica_db))
            if digest == expected_digest:
                return time.perf_counter() - start
        time.sleep(0.01)
    raise RuntimeError("replica did not catch up before timeout")


def run_throughput_test(payload_size: int, total_transactions: int, commit_batch_size: int, warmup: int) -> BenchmarkResult:
    cleanup()
    primary = start_primary()
    replica = None
    try:
        initialize_primary_db()
        if warmup > 0:
            write_rows(1, warmup, payload_size, commit_batch_size)

        replica, replica_cwd = prepare_replica()
        replica_db = replica_cwd / "replica.db"

        warmup_digest = compute_db_digest(str(ARTIFACT_ROOT / "primary.db"))
        warmup_restore_seconds = wait_for_replica_digest(
            replica_db,
            warmup,
            warmup_digest,
            timeout_seconds=30,
        )

        start_id = warmup + 1
        write_phase_seconds = write_rows(start_id, total_transactions, payload_size, commit_batch_size)
        final_digest = compute_db_digest(str(ARTIFACT_ROOT / "primary.db"))
        catchup_seconds = wait_for_replica_digest(
            replica_db,
            warmup + total_transactions,
            final_digest,
            timeout_seconds=60,
        )

        primary_ok, primary_detail = run_integrity_check(str(ARTIFACT_ROOT / "primary.db"))
        replica_ok, replica_detail = run_integrity_check(str(replica_db))
        if not primary_ok:
            raise RuntimeError(f"primary integrity_check failed: {primary_detail}")
        if not replica_ok:
            raise RuntimeError(f"replica integrity_check failed: {replica_detail}")

        mib = (payload_size * total_transactions) / (1024 * 1024)
        total_replication_window = max(write_phase_seconds + catchup_seconds, 0.000001)
        catchup_window = max(catchup_seconds, 0.000001)
        replica_digest = compute_db_digest(str(replica_db))
        return BenchmarkResult(
            payload_size=payload_size,
            total_transactions=total_transactions,
            commit_batch_size=commit_batch_size,
            initial_restore_seconds=warmup_restore_seconds,
            write_phase_seconds=write_phase_seconds,
            catchup_seconds=catchup_seconds,
            end_to_end_rows_per_second=total_transactions / total_replication_window,
            end_to_end_mib_per_second=mib / total_replication_window,
            catchup_rows_per_second=total_transactions / catchup_window,
            catchup_mib_per_second=mib / catchup_window,
            primary_digest=final_digest,
            replica_digest=replica_digest or "",
        )
    finally:
        if replica is not None:
            replica.terminate()
            replica.wait(timeout=10)
        primary.terminate()
        primary.wait(timeout=10)


def run_all_benchmarks(payload_sizes: list[int], total_transactions: int, commit_batch_size: int, warmup: int) -> list[BenchmarkResult]:
    results: list[BenchmarkResult] = []
    for payload_size in payload_sizes:
        print(f"\n=== Stream catch-up payload={payload_size} bytes ===")
        result = run_throughput_test(payload_size, total_transactions, commit_batch_size, warmup)
        results.append(result)
        print(f"Initial restore: {result.initial_restore_seconds:.2f}s")
        print(f"Write phase:     {result.write_phase_seconds:.2f}s")
        print(f"Catch-up wait:   {result.catchup_seconds:.2f}s")
        print(f"End-to-end rows/s: {result.end_to_end_rows_per_second:.2f}")
        print(f"Catch-up rows/s:   {result.catchup_rows_per_second:.2f}")
        print(f"End-to-end MiB/s:  {result.end_to_end_mib_per_second:.2f}")
        print(f"Catch-up MiB/s:    {result.catchup_mib_per_second:.2f}")
    return results


def print_report(results: list[BenchmarkResult]) -> None:
    print("\n" + "=" * 84)
    print("STREAM CATCH-UP THROUGHPUT BENCHMARK REPORT")
    print("=" * 84)
    print(
        f"{'Payload':>10} | {'Restore':>8} | {'Write':>8} | {'Catch-up':>8} | {'E2E Rows/s':>12} | {'Catch Rows/s':>12}"
    )
    print("-" * 84)
    for result in results:
        payload_str = f"{result.payload_size}B" if result.payload_size < 1024 else f"{result.payload_size // 1024}KB"
        print(
            f"{payload_str:>10} | {result.initial_restore_seconds:>8.2f} | {result.write_phase_seconds:>8.2f} | "
            f"{result.catchup_seconds:>8.2f} | {result.end_to_end_rows_per_second:>12.2f} | {result.catchup_rows_per_second:>12.2f}"
        )
    if len(results) > 1:
        print("-" * 84)
        print(
            f"{'Median':>10} | {'-':>8} | {'-':>8} | {'-':>8} | "
            f"{statistics.median(r.end_to_end_rows_per_second for r in results):>12.2f} | "
            f"{statistics.median(r.catchup_rows_per_second for r in results):>12.2f}"
        )
    print("=" * 84)


def save_results(results: list[BenchmarkResult], output_file: str) -> None:
    output = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "results": [asdict(result) for result in results],
    }
    with open(output_file, "w") as handle:
        json.dump(output, handle, indent=2)
    print(f"\nResults saved to {output_file}")


def parse_payload_sizes(raw: str) -> list[int]:
    payload_sizes = []
    for token in raw.split(","):
        token = token.strip()
        if token:
            payload_sizes.append(int(token))
    if not payload_sizes:
        raise argparse.ArgumentTypeError("payload size list must not be empty")
    return payload_sizes


def main() -> None:
    parser = argparse.ArgumentParser(description="End-to-end throughput benchmark for stream replication")
    parser.add_argument("-n", "--num-transactions", type=int, default=500)
    parser.add_argument("-b", "--commit-batch-size", type=int, default=100)
    parser.add_argument("-w", "--warmup", type=int, default=50)
    parser.add_argument(
        "-p",
        "--payload-sizes",
        type=parse_payload_sizes,
        default=parse_payload_sizes("1024,10240"),
        help="Comma-separated payload sizes in bytes (default: 1024,10240)",
    )
    parser.add_argument("-o", "--output", type=str, default="throughput_results.json")
    args = parser.parse_args()

    if not REPLITED_BIN.exists():
        print(f"Error: replited binary not found at {REPLITED_BIN}")
        print("Please run: cargo build --release")
        sys.exit(1)

    print(
        f"Running stream throughput benchmark with payloads={args.payload_sizes}, "
        f"transactions={args.num_transactions}, batch={args.commit_batch_size}, warmup={args.warmup}"
    )
    results = run_all_benchmarks(args.payload_sizes, args.num_transactions, args.commit_batch_size, args.warmup)
    print_report(results)
    save_results(results, args.output)


if __name__ == "__main__":
    main()
