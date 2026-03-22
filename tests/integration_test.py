#!/usr/bin/env python3
"""Manifest archival/restore integration test used by GitHub Actions."""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import subprocess
import sys
import time
import unittest
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from test_utils import TestEnv, compute_db_digest, run_integrity_check

TABLE_NAME = "integration_rows"
DEFAULT_S3_ENDPOINT = "http://127.0.0.1:4566"
DEFAULT_S3_BUCKET = "replited-itest"
DEFAULT_S3_REGION = "us-east-1"
DEFAULT_S3_ACCESS_KEY_ID = "test"
DEFAULT_S3_SECRET_ACCESS_KEY = "test"
DEFAULT_S3_ROOT = "/replited"
DEFAULT_GCS_ENDPOINT = "http://127.0.0.1:4443"
DEFAULT_GCS_BUCKET = "replited-itest"
DEFAULT_GCS_ROOT = "/replited"
DEFAULT_AZB_ENDPOINT = "http://127.0.0.1:10000/devstoreaccount1"
DEFAULT_AZB_CONTAINER = "replited-itest"
DEFAULT_AZB_ACCOUNT_NAME = "devstoreaccount1"
DEFAULT_AZB_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
DEFAULT_AZB_ROOT = "/replited"
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
    cache_hits: int
    cache_misses: int
    cache_write_failures: int
    raw_line: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("timeout", type=int, help="timeout in seconds or milliseconds")
    parser.add_argument("binary")
    parser.add_argument(
        "--backend",
        choices=["fs", "s3", "gcs", "azb"],
        default="fs",
        help="archival backend to validate",
    )
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


def s3_settings() -> dict[str, str]:
    return {
        "endpoint": os.environ.get("REPLITED_TEST_S3_ENDPOINT", DEFAULT_S3_ENDPOINT),
        "bucket": os.environ.get("REPLITED_TEST_S3_BUCKET", DEFAULT_S3_BUCKET),
        "region": os.environ.get("REPLITED_TEST_S3_REGION", DEFAULT_S3_REGION),
        "access_key_id": os.environ.get(
            "REPLITED_TEST_S3_ACCESS_KEY_ID", DEFAULT_S3_ACCESS_KEY_ID
        ),
        "secret_access_key": os.environ.get(
            "REPLITED_TEST_S3_SECRET_ACCESS_KEY", DEFAULT_S3_SECRET_ACCESS_KEY
        ),
        "root": os.environ.get("REPLITED_TEST_S3_ROOT", DEFAULT_S3_ROOT),
    }


def gcs_settings() -> dict[str, str]:
    return {
        "endpoint": os.environ.get("REPLITED_TEST_GCS_ENDPOINT", DEFAULT_GCS_ENDPOINT),
        "bucket": os.environ.get("REPLITED_TEST_GCS_BUCKET", DEFAULT_GCS_BUCKET),
        "root": os.environ.get("REPLITED_TEST_GCS_ROOT", DEFAULT_GCS_ROOT),
        "credential": os.environ.get("REPLITED_TEST_GCS_CREDENTIAL", ""),
    }


def azb_settings() -> dict[str, str]:
    return {
        "endpoint": os.environ.get("REPLITED_TEST_AZB_ENDPOINT", DEFAULT_AZB_ENDPOINT),
        "container": os.environ.get("REPLITED_TEST_AZB_CONTAINER", DEFAULT_AZB_CONTAINER),
        "account_name": os.environ.get(
            "REPLITED_TEST_AZB_ACCOUNT_NAME", DEFAULT_AZB_ACCOUNT_NAME
        ),
        "account_key": os.environ.get(
            "REPLITED_TEST_AZB_ACCOUNT_KEY", DEFAULT_AZB_ACCOUNT_KEY
        ),
        "root": os.environ.get("REPLITED_TEST_AZB_ROOT", DEFAULT_AZB_ROOT),
    }


def render_config(env: TestEnv, db_path: Path, backend: str) -> str:
    root = str(env.root.resolve())
    shared = f"""
[log]
level = "Debug"
dir = "{root}/logs"

[[database]]
db = "{db_path.resolve()}"
cache_root = "{(env.root / 'cache').resolve()}"
min_checkpoint_page_number = 10
max_checkpoint_page_number = 200
truncate_page_number = 1000
checkpoint_interval_secs = 1
monitor_interval_ms = 200
wal_retention_count = 5
max_concurrent_snapshots = 2
""".strip()

    if backend == "fs":
        replicate = f"""
[[database.replicate]]
name = "fs-backup"
[database.replicate.params]
type = "fs"
root = "{env.backup_dir.resolve()}"
""".strip()
    elif backend == "s3":
        settings = s3_settings()
        replicate = f"""
[[database.replicate]]
name = "s3-backup"
[database.replicate.params]
type = "s3"
endpoint = "{settings['endpoint']}"
bucket = "{settings['bucket']}"
region = "{settings['region']}"
root = "{settings['root']}"
access_key_id = "{settings['access_key_id']}"
secret_access_key = "{settings['secret_access_key']}"
""".strip()
    elif backend == "gcs":
        settings = gcs_settings()
        replicate = f"""
[[database.replicate]]
name = "gcs-backup"
[database.replicate.params]
type = "gcs"
endpoint = "{settings['endpoint']}"
bucket = "{settings['bucket']}"
root = "{settings['root']}"
credential = "{settings['credential']}"
allow_anonymous = true
disable_vm_metadata = true
""".strip()
    elif backend == "azb":
        settings = azb_settings()
        replicate = f"""
[[database.replicate]]
name = "azb-backup"
[database.replicate.params]
type = "azb"
endpoint = "{settings['endpoint']}"
container = "{settings['container']}"
account_name = "{settings['account_name']}"
account_key = "{settings['account_key']}"
root = "{settings['root']}"
""".strip()
    else:
        raise ValueError(f"unsupported backend: {backend}")

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
        cache_hits=int(match.group("cache_hits")),
        cache_misses=int(match.group("cache_misses")),
        cache_write_failures=int(match.group("cache_write_failures")),
        raw_line=match.group(0),
    )


def run_restore(
    binary: Path,
    backend: str,
    config_path: Path,
    db_path: Path,
    output_db: Path,
    log_path: Path,
) -> tuple[subprocess.CompletedProcess, RestoreDiagnostics | None]:
    remove_restore_output(output_db)
    log_offset = log_path.stat().st_size if log_path.exists() else 0
    restore_timeout = 30 if backend == "fs" else 60
    try:
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
                "--truth-source",
                f"{backend}-backup",
            ],
            capture_output=True,
            text=True,
            timeout=restore_timeout,
        )
    except subprocess.TimeoutExpired as exc:
        result = subprocess.CompletedProcess(
            exc.cmd,
            124,
            stdout=exc.stdout or "",
            stderr=f"restore timed out after {restore_timeout}s",
        )
    _, log_delta = read_log_delta(log_path, log_offset)
    diagnostics_text = "\n".join(
        chunk for chunk in [result.stdout, result.stderr, log_delta] if chunk
    )
    return result, parse_restore_diagnostics(diagnostics_text)


def restore_until_digest(
    binary: Path,
    backend: str,
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
        result, diagnostics = run_restore(
            binary, backend, config_path, db_path, output_db, log_path
        )
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
                            f"{backend} restore expected manifest path but saw "
                            f"{diagnostics.path}"
                        )
                    elif diagnostics.list_calls != 0:
                        last_error = (
                            f"{backend} manifest restore expected list_calls=0 but saw "
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


def verify_archival_restore(binary: Path, backend: str, timeout_sec: int) -> int:
    env = TestEnv(f"{backend}_integration")
    env.setup()

    db_path = env.root / "test.db"
    config_path = env.root / "primary.toml"
    restore_output = env.root / "restored.db"
    restore_log = replited_log_path(env)
    primary_log = restore_log
    primary = None

    try:
        initialize_db(db_path)
        config_path.write_text(render_config(env, db_path, backend))

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
            backend,
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
            backend,
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
    return verify_archival_restore(binary, args.backend, timeout_sec)


class IntegrationHarnessTests(unittest.TestCase):
    def test_parse_restore_diagnostics_accepts_expanded_cache_counters(self) -> None:
        diagnostics = parse_restore_diagnostics(
            "restore_request_cost path=manifest "
            "latest_pointer_gets=1 generation_manifest_gets=2 object_gets=3 list_calls=0 "
            "cache_hits=4 cache_misses=5 cache_write_failures=6"
        )

        self.assertIsNotNone(diagnostics)
        assert diagnostics is not None
        self.assertEqual(diagnostics.path, "manifest")
        self.assertEqual(diagnostics.latest_pointer_gets, 1)
        self.assertEqual(diagnostics.generation_manifest_gets, 2)
        self.assertEqual(diagnostics.object_gets, 3)
        self.assertEqual(diagnostics.list_calls, 0)
        self.assertEqual(diagnostics.cache_hits, 4)
        self.assertEqual(diagnostics.cache_misses, 5)
        self.assertEqual(diagnostics.cache_write_failures, 6)

    def test_render_config_supports_s3_backend(self) -> None:
        env = TestEnv("integration_config_s3")
        db_path = env.root / "db.sqlite"

        config = render_config(env, db_path, "s3")

        self.assertIn('type = "s3"', config)
        self.assertIn(f'endpoint = "{DEFAULT_S3_ENDPOINT}"', config)
        self.assertIn(f'bucket = "{DEFAULT_S3_BUCKET}"', config)
        self.assertIn('cache_root = "', config)

    def test_render_config_supports_gcs_backend(self) -> None:
        env = TestEnv("integration_config_gcs")
        db_path = env.root / "db.sqlite"

        config = render_config(env, db_path, "gcs")

        self.assertIn('type = "gcs"', config)
        self.assertIn(f'endpoint = "{DEFAULT_GCS_ENDPOINT}"', config)
        self.assertIn(f'bucket = "{DEFAULT_GCS_BUCKET}"', config)
        self.assertIn("allow_anonymous = true", config)
        self.assertIn("disable_vm_metadata = true", config)

    def test_render_config_supports_azb_backend(self) -> None:
        env = TestEnv("integration_config_azb")
        db_path = env.root / "db.sqlite"

        config = render_config(env, db_path, "azb")

        self.assertIn('type = "azb"', config)
        self.assertIn(f'endpoint = "{DEFAULT_AZB_ENDPOINT}"', config)
        self.assertIn(f'container = "{DEFAULT_AZB_CONTAINER}"', config)


if __name__ == "__main__":
    sys.exit(main())
