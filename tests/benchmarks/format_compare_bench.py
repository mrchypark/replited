#!/usr/bin/env python3
"""
Snapshot-format comparison benchmark for manifest-vs-LTX experiments.

This intentionally benchmarks only the base snapshot representation:
- current manifest-style compressed SQLite snapshot
- experimental LTX snapshot

Incremental LTX is deferred until the snapshot experiment proves worthwhile.
"""

from __future__ import annotations

import argparse
import json
import random
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import compute_db_digest, run_integrity_check

LTX_PROBE_BIN = Path(__file__).parent.parent.parent / "target" / "release" / "ltx_probe"
ARTIFACT_ROOT = Path(__file__).parent.parent / "output" / "benchmarks" / "format_compare"


@dataclass
class SnapshotFormatResult:
    format: str
    payload_mode: str
    total_rows: int
    payload_size: int
    source_db_bytes: int
    artifact_bytes: int
    encode_seconds: float
    decode_seconds: float
    artifact_ratio: float
    source_digest: str
    restored_digest: str
    integrity_ok: bool
    integrity_detail: str


def cleanup() -> None:
    if ARTIFACT_ROOT.exists():
        subprocess.run(["rm", "-rf", str(ARTIFACT_ROOT)], check=True)
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)


def create_source_db(total_rows: int, payload_size: int, payload_mode: str) -> Path:
    mode_root = ARTIFACT_ROOT / payload_mode
    mode_root.mkdir(parents=True, exist_ok=True)
    db_path = mode_root / "source.db"
    conn = sqlite3.connect(str(db_path), timeout=2.0)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS bench (
            id INTEGER PRIMARY KEY,
            payload BLOB NOT NULL
        )
        """
    )
    rng = random.Random(0)
    for idx in range(total_rows):
        if payload_mode == "repeat":
            payload = b"x" * payload_size
        elif payload_mode == "random":
            payload = bytes(rng.getrandbits(8) for _ in range(payload_size))
        else:
            raise ValueError(f"unsupported payload_mode: {payload_mode}")
        cursor.execute(
            "INSERT INTO bench (id, payload) VALUES (?, ?)",
            (idx + 1, payload),
        )
    conn.commit()
    conn.close()
    return db_path


def run_probe(*args: str) -> float:
    started = time.perf_counter()
    subprocess.run(
        [str(LTX_PROBE_BIN), *args],
        check=True,
        cwd=str(ARTIFACT_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    return time.perf_counter() - started


def benchmark_format(fmt: str, payload_mode: str, source_db: Path) -> SnapshotFormatResult:
    mode_root = ARTIFACT_ROOT / payload_mode
    artifact_path = mode_root / f"{fmt}.snapshot"
    restored_db = mode_root / f"restored-{fmt}.db"
    if artifact_path.exists():
        artifact_path.unlink()
    if restored_db.exists():
        restored_db.unlink()

    encode_seconds = run_probe(
        "encode-snapshot",
        "--format",
        fmt,
        "--db",
        str(source_db),
        "--out",
        str(artifact_path),
    )
    decode_seconds = run_probe(
        "decode-snapshot",
        "--format",
        fmt,
        "--input",
        str(artifact_path),
        "--output",
        str(restored_db),
    )

    source_digest = compute_db_digest(str(source_db))
    restored_digest = compute_db_digest(str(restored_db))
    integrity_ok, integrity_detail = run_integrity_check(str(restored_db))
    source_bytes = source_db.stat().st_size
    artifact_bytes = artifact_path.stat().st_size

    return SnapshotFormatResult(
        format=fmt,
        payload_mode=payload_mode,
        total_rows=count_rows(source_db),
        payload_size=payload_size(source_db),
        source_db_bytes=source_bytes,
        artifact_bytes=artifact_bytes,
        encode_seconds=encode_seconds,
        decode_seconds=decode_seconds,
        artifact_ratio=artifact_bytes / max(source_bytes, 1),
        source_digest=source_digest or "",
        restored_digest=restored_digest or "",
        integrity_ok=integrity_ok,
        integrity_detail=integrity_detail,
    )


def count_rows(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path), timeout=2.0)
    try:
        return int(conn.execute("SELECT COUNT(*) FROM bench").fetchone()[0])
    finally:
        conn.close()


def payload_size(db_path: Path) -> int:
    conn = sqlite3.connect(str(db_path), timeout=2.0)
    try:
        row = conn.execute("SELECT LENGTH(payload) FROM bench LIMIT 1").fetchone()
        return int(row[0]) if row is not None else 0
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare manifest snapshot and LTX snapshot formats")
    parser.add_argument("-n", "--num-rows", type=int, default=2000)
    parser.add_argument("-p", "--payload-size", type=int, default=4096)
    parser.add_argument(
        "--payload-modes",
        type=str,
        default="repeat,random",
        help="comma-separated payload modes: repeat,random",
    )
    parser.add_argument("-o", "--output", type=str, default="format_compare_results.json")
    args = parser.parse_args()

    cleanup()
    payload_modes = [mode.strip() for mode in args.payload_modes.split(",") if mode.strip()]
    results = []
    for payload_mode in payload_modes:
        source_db = create_source_db(args.num_rows, args.payload_size, payload_mode)
        results.append(asdict(benchmark_format("manifest", payload_mode, source_db)))
        results.append(asdict(benchmark_format("ltx", payload_mode, source_db)))

    for result in results:
        if result["source_digest"] != result["restored_digest"]:
            raise RuntimeError(
                f"{result['format']} digest mismatch: {result['source_digest']} != {result['restored_digest']}"
            )
        if not result["integrity_ok"]:
            raise RuntimeError(
                f"{result['format']} integrity_check failed: {result['integrity_detail']}"
            )

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(results, indent=2) + "\n")

    print(f"Snapshot format benchmark complete. Results written to {output_path}")
    for result in results:
        print(
            f"{result['payload_mode']:>6} | {result['format']:>8} "
            f"| encode={result['encode_seconds']:.4f}s "
            f"| decode={result['decode_seconds']:.4f}s "
            f"| artifact={result['artifact_bytes']} bytes "
            f"| ratio={result['artifact_ratio']:.4f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
