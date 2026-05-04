#!/usr/bin/env python3
"""
Gated latest-state read-only child-process regression.

This reproduces the tg900 deployment shape more closely than the stock
PocketBase harness:

- latest-state writer runs from the published container image.
- replited primary starts after writer bootstrap and before the test write.
- latest-state read-only child runs from the same image under
  `replica-sidecar --exec`.
- the replica uses tg900-like checkpoint settings.

The protected behavior is that a write committed on the primary DB becomes
visible through the long-lived read-only latest-state API, not only in the
replica SQLite files. The test is gated because it requires Docker and runs
real containers.
"""

from __future__ import annotations

import json
import hashlib
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from test_utils import TestEnv, get_free_port, wait_for_port

ROOT = Path(__file__).resolve().parents[2]
REPLITED_BIN = Path(
    os.environ.get("REPLITED_BIN", str(ROOT / "target" / "release" / "replited"))
)
LATEST_STATE_IMAGE = os.environ.get(
    "LATEST_STATE_IMAGE",
    "asia-northeast3-docker.pkg.dev/patch2-the-new-era/conalog/latest-state:20260503-1e6471d",
)
RUN_ENV = "RUN_LATEST_STATE_LIVE"

LOG_DENYLIST = [
    "database disk image is malformed",
    "disk I/O error",
    "SnapshotBoundaryMismatch",
    "LineageMismatch",
    "panic:",
    "ReplicaSidecar error",
]


def process_exit_error(proc: Proc | None) -> str | None:
    if proc is None:
        return None
    returncode = proc.proc.poll()
    if returncode is None:
        return None
    return f"{proc.name} exited unexpectedly with return code {returncode}"


@dataclass
class Proc:
    proc: subprocess.Popen[str]
    log_handle: object
    log_path: Path
    name: str


def skip_if_not_enabled() -> None:
    if os.environ.get(RUN_ENV) == "1":
        return
    print(f"SKIP: set {RUN_ENV}=1 to run the latest-state live regression")
    raise SystemExit(0)


def require_binary(path: Path, label: str) -> None:
    if path.exists():
        return
    print(f"missing {label} at {path}", file=sys.stderr)
    raise SystemExit(1)


def require_cmd(cmd: str) -> None:
    if shutil.which(cmd):
        return
    print(f"missing required command: {cmd}", file=sys.stderr)
    raise SystemExit(1)


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


def docker_rm(name: str) -> None:
    subprocess.run(
        ["docker", "rm", "-f", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )


def http_get(url: str, timeout: float = 2.0) -> tuple[int, str]:
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as response:
        body = response.read().decode("utf-8", errors="replace")
        return response.status, body


def wait_for_http_ok(url: str, timeout: int = 120) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = http_get(url)
            if status == 200 and body.strip() == "OK":
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def wait_for_http_ok_while_processes_live(
    url: str,
    processes: list[Proc | None],
    errors: list[str],
    timeout: int = 120,
) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        for proc in processes:
            if error := process_exit_error(proc):
                errors.append(error)
                return False
        try:
            status, body = http_get(url)
            if status == 200 and body.strip() == "OK":
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def start_logged_process(
    cmd: list[str],
    cwd: Path,
    log_path: Path,
    name: str,
) -> Proc:
    log_handle = open(log_path, "a")
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    return Proc(proc=proc, log_handle=log_handle, log_path=log_path, name=name)


def start_latest_state_container(
    *,
    name: str,
    pb_dir: Path,
    data_dir: Path,
    port: int,
    log_path: Path,
    read_only: bool,
) -> Proc:
    docker_rm(name)
    env = [
        "-e",
        "DEBUG_MODE=true",
        "-e",
        "BATCH_ENABLED=false",
        "-e",
        "DEVICE_CACHE_FLUSH_INTERVAL=1s",
        "-e",
        "USE_INVERTER_LATEST_STATE=true",
        "-e",
        "DEVICE_ID_LIST_FILE=/data/device_list.csv",
        "-e",
        "REGISTRY_ENDPOINT=http://127.0.0.1:1",
        "-e",
        "REGISTRY_CREDENTIAL_ID=dummy",
        "-e",
        "REGISTRY_CREDENTIAL_PW=dummy",
        "-e",
        "ADMIN_EMAILS=test@test.com",
        "-e",
        "ADMIN_PASSWORDS=qwer@12345",
        "-e",
        "OTEL_EXPORTER_OTLP_ENDPOINT=",
    ]
    if read_only:
        env.extend(["-e", "POCKETBASE_DB_READONLY=true"])

    cmd = [
        "docker",
        "run",
        "--rm",
        "--platform=linux/amd64",
        "--name",
        name,
        "-p",
        f"127.0.0.1:{port}:8090",
        "-v",
        f"{pb_dir.resolve()}:/pb_data",
        "-v",
        f"{data_dir.resolve()}:/data",
        *env,
        LATEST_STATE_IMAGE,
        "serve",
        "--http",
        "0.0.0.0:8090",
        "--dir",
        "/pb_data",
    ]
    return start_logged_process(cmd, cwd=ROOT, log_path=log_path, name=name)


def latest_state_replica_exec_cmd(
    *,
    name: str,
    replica_dir: Path,
    data_dir: Path,
    port: int,
) -> str:
    docker_args = [
        "docker",
        "run",
        "--rm",
        "--platform=linux/amd64",
        "--name",
        name,
        "-p",
        f"127.0.0.1:{port}:8090",
        "-v",
        f"{replica_dir.resolve()}:/pb_data",
        "-v",
        f"{data_dir.resolve()}:/data",
        "-e",
        "DEBUG_MODE=true",
        "-e",
        "BATCH_ENABLED=false",
        "-e",
        "DEVICE_CACHE_FLUSH_INTERVAL=1s",
        "-e",
        "USE_INVERTER_LATEST_STATE=true",
        "-e",
        "DEVICE_ID_LIST_FILE=/data/device_list.csv",
        "-e",
        "REGISTRY_ENDPOINT=http://127.0.0.1:1",
        "-e",
        "REGISTRY_CREDENTIAL_ID=dummy",
        "-e",
        "REGISTRY_CREDENTIAL_PW=dummy",
        "-e",
        "ADMIN_EMAILS=test@test.com",
        "-e",
        "ADMIN_PASSWORDS=qwer@12345",
        "-e",
        "POCKETBASE_DB_READONLY=true",
        LATEST_STATE_IMAGE,
        "serve",
        "--http",
        "0.0.0.0:8090",
        "--dir",
        "/pb_data",
    ]
    quoted = " ".join(sh_quote(arg) for arg in docker_args)
    return (
        "sh -lc "
        + sh_quote(
            "set -e; "
            f"name={sh_quote(name)}; "
            'cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }; '
            "trap cleanup INT TERM EXIT; "
            "cleanup; "
            f"{quoted} & "
            "pid=$!; wait $pid"
        )
    )


def latest_state_managed_proxy_child_template(
    *,
    name_prefix: str,
    data_dir: Path,
) -> str:
    name = f"{name_prefix}-{{port}}"
    docker_args = [
        "docker",
        "run",
        "--rm",
        "--platform=linux/amd64",
        "--name",
        name,
        "-p",
        "127.0.0.1:{port}:8090",
        "-v",
        "{dir}:/pb_data",
        "-v",
        f"{data_dir.resolve()}:/data",
        "-e",
        "DEBUG_MODE=true",
        "-e",
        "BATCH_ENABLED=false",
        "-e",
        "DEVICE_CACHE_FLUSH_INTERVAL=1s",
        "-e",
        "USE_INVERTER_LATEST_STATE=true",
        "-e",
        "DEVICE_ID_LIST_FILE=/data/device_list.csv",
        "-e",
        "REGISTRY_ENDPOINT=http://127.0.0.1:1",
        "-e",
        "REGISTRY_CREDENTIAL_ID=dummy",
        "-e",
        "REGISTRY_CREDENTIAL_PW=dummy",
        "-e",
        "ADMIN_EMAILS=test@test.com",
        "-e",
        "ADMIN_PASSWORDS=qwer@12345",
        "-e",
        "POCKETBASE_DB_READONLY=true",
        LATEST_STATE_IMAGE,
        "serve",
        "--http",
        "0.0.0.0:8090",
        "--dir",
        "/pb_data",
    ]
    quoted = " ".join(sh_quote(arg) for arg in docker_args)
    return (
        "sh -lc "
        + sh_quote(
            "set -e; "
            f"name={sh_quote(name)}; "
            'cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }; '
            "trap cleanup INT TERM EXIT; "
            "cleanup; "
            f"{quoted} & "
            "pid=$!; wait $pid"
        )
    )


def sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def write_primary_config(env: TestEnv, stream_port: int, primary_dir: Path) -> Path:
    config = env.root / "primary.toml"
    config.write_text(
        f"""
[log]
level = "Info"
dir = "{(env.root / "logs").absolute()}"

[[database]]
db = "{(primary_dir / "data.db").absolute()}"
cache_root = "{(primary_dir / "cache").absolute()}"
monitor_interval_ms = 100
checkpoint_interval_secs = 300
wal_retention_count = 120
max_concurrent_snapshots = 3

[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "127.0.0.1:{stream_port}"
""".strip()
        + "\n"
    )
    return config


def write_replica_config(
    env: TestEnv,
    stream_port: int,
    primary_dir: Path,
    replica_dir: Path,
) -> Path:
    config = replica_dir / "replica.toml"
    config.write_text(
        f"""
[log]
level = "Info"
dir = "{(replica_dir / "logs").absolute()}"

[[database]]
db = "{(replica_dir / "data.db").absolute()}"
cache_root = "{(replica_dir / "cache").absolute()}"
apply_checkpoint_frame_interval = 10000
apply_checkpoint_interval_ms = 60000

[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:{stream_port}"
remote_db_name = "{(primary_dir / "data.db").absolute()}"
""".strip()
        + "\n"
    )
    return config


def insert_primary_device_state(db_path: Path, device_id: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    record_id = "r" + hashlib.sha1(device_id.encode("utf-8")).hexdigest()[:14]
    conn = sqlite3.connect(str(db_path), timeout=10.0)
    try:
        cursor = conn.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA busy_timeout=10000")
        cursor.execute(
            """
            INSERT OR REPLACE INTO device_states (
                id,
                application_id,
                created,
                data,
                device_id,
                panel_position,
                plant_id,
                timestamp,
                updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record_id,
                "codex",
                timestamp,
                json.dumps({"source": "latest-state-live-regression"}),
                device_id,
                "codex",
                "codex",
                timestamp,
                timestamp,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def wait_for_replica_db_record(replica_db: Path, device_id: str, timeout: int = 180) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = sqlite3.connect(str(replica_db), timeout=2.0)
            count = conn.execute(
                "SELECT COUNT(*) FROM device_states WHERE device_id = ?",
                (device_id,),
            ).fetchone()[0]
            conn.close()
            if int(count) == 1:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def wait_for_replica_db_records(
    replica_db: Path,
    device_ids: list[str],
    timeout: int = 180,
) -> list[str]:
    missing = set(device_ids)
    deadline = time.time() + timeout
    while time.time() < deadline and missing:
        try:
            conn = sqlite3.connect(str(replica_db), timeout=2.0)
            placeholders = ",".join("?" for _ in missing)
            rows = conn.execute(
                f"SELECT device_id FROM device_states WHERE device_id IN ({placeholders})",
                tuple(missing),
            ).fetchall()
            conn.close()
            for (device_id,) in rows:
                missing.discard(device_id)
        except Exception:
            pass
        if missing:
            time.sleep(0.5)
    return sorted(missing)


def wait_for_primary_data_ready(primary_db: Path, timeout: int = 60) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = sqlite3.connect(str(primary_db), timeout=2.0)
            conn.execute("PRAGMA busy_timeout=10000")
            migration_count = conn.execute("SELECT COUNT(*) FROM _migrations").fetchone()[0]
            collection_count = conn.execute(
                "SELECT COUNT(*) FROM _collections WHERE name = ?",
                ("device_states",),
            ).fetchone()[0]
            conn.close()
            if int(migration_count) > 0 and int(collection_count) == 1:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def wait_for_replica_api_record(replica_url: str, device_id: str, timeout: int = 180) -> bool:
    quoted = urllib.parse.quote(f'device_id="{device_id}"', safe="")
    url = f"{replica_url}/api/collections/device_states/records?filter=({quoted})"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, body = http_get(url)
            if status == 200 and int(json.loads(body).get("totalItems", 0)) == 1:
                return True
        except Exception:
            pass
        time.sleep(1.0)
    return False


def wait_for_replica_api_records(
    replica_url: str,
    device_ids: list[str],
    timeout: int = 180,
) -> list[str]:
    missing = set(device_ids)
    deadline = time.time() + timeout
    while time.time() < deadline and missing:
        observed = []
        for device_id in list(missing):
            if wait_for_replica_api_record(replica_url, device_id, timeout=1):
                observed.append(device_id)
        for device_id in observed:
            missing.remove(device_id)
        if missing:
            time.sleep(1.0)
    return sorted(missing)


def copy_ready_auxiliary_db(primary_dir: Path, replica_dir: Path, timeout: int = 30) -> bool:
    source = primary_dir / "auxiliary.db"
    destination = replica_dir / "auxiliary.db"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = sqlite3.connect(str(source), timeout=5.0)
            conn.execute("PRAGMA busy_timeout=10000")
            has_logs_table = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = '_logs'",
            ).fetchone()[0]
            if int(has_logs_table) == 1:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
                conn.close()
                shutil.copy2(source, destination)
                for suffix in ("-wal", "-shm"):
                    sidecar = source.with_name(source.name + suffix)
                    sidecar_destination = destination.with_name(destination.name + suffix)
                    if sidecar.exists():
                        shutil.copy2(sidecar, sidecar_destination)
                    elif sidecar_destination.exists():
                        sidecar_destination.unlink()
                return True
            conn.close()
        except Exception:
            pass
        time.sleep(0.5)
    return False


def scan_logs(log_paths: list[Path]) -> list[str]:
    errors: list[str] = []
    for log_path in log_paths:
        if not log_path.exists():
            errors.append(f"{log_path}: missing")
            continue
        text = log_path.read_text(errors="ignore")
        for marker in LOG_DENYLIST:
            if marker in text:
                errors.append(f"{log_path}: found denied marker {marker!r}")
    return errors


def print_log_tails(log_paths: list[Path]) -> None:
    for log_path in log_paths:
        print(f"\n--- {log_path} tail ---")
        if log_path.exists():
            print("\n".join(log_path.read_text(errors="ignore").splitlines()[-80:]))
        else:
            print("missing")


def run_scenario() -> int:
    env = TestEnv("latest_state_readonly_child_process")
    env.setup()
    logs_dir = env.root / "logs"
    logs_dir.mkdir(exist_ok=True)
    primary_dir = env.root / "primary"
    replica_dir = env.root / "replica"
    data_dir = env.root / "data"
    for path in [primary_dir, replica_dir, data_dir, replica_dir / "logs"]:
        path.mkdir(parents=True, exist_ok=True)
    (data_dir / "device_list.csv").write_text("codex-device\n")

    stream_port = get_free_port()
    primary_port = get_free_port()
    replica_port = get_free_port()
    primary_url = f"http://127.0.0.1:{primary_port}"
    replica_url = f"http://127.0.0.1:{replica_port}"
    primary_name = f"replited-latest-state-primary-{os.getpid()}"
    replica_name = f"replited-latest-state-read-{os.getpid()}"

    primary_app: Proc | None = None
    primary_replited: Proc | None = None
    replica_sidecar: Proc | None = None
    errors: list[str] = []
    try:
        primary_config = write_primary_config(env, stream_port, primary_dir)
        primary_replited = start_logged_process(
            [
                str(REPLITED_BIN.resolve()),
                "--config",
                str(primary_config.resolve()),
                "replicate",
            ],
            cwd=env.root,
            log_path=logs_dir / "primary-replited.log",
            name="primary-replited",
        )
        if not wait_for_port(stream_port, timeout=20):
            errors.append("primary replited stream port did not open")
            return fail(errors, [primary_replited.log_path])

        primary_app = start_latest_state_container(
            name=primary_name,
            pb_dir=primary_dir,
            data_dir=data_dir,
            port=primary_port,
            log_path=logs_dir / "primary-latest-state.log",
            read_only=False,
        )
        if not wait_for_http_ok_while_processes_live(
            f"{primary_url}/health",
            [primary_replited],
            errors,
            timeout=120,
        ):
            errors.append("latest-state writer did not become healthy")
            return fail(errors, [primary_app.log_path, primary_replited.log_path])

        if not copy_ready_auxiliary_db(primary_dir, replica_dir):
            errors.append("latest-state writer did not create a ready auxiliary.db")
            return fail(errors, [primary_app.log_path, primary_replited.log_path])
        if not wait_for_primary_data_ready(primary_dir / "data.db"):
            errors.append("latest-state writer did not create a ready data.db")
            return fail(errors, [primary_app.log_path, primary_replited.log_path])
        if error := process_exit_error(primary_replited):
            errors.append(error)
            return fail(errors, [primary_app.log_path, primary_replited.log_path])
        if not wait_for_port(stream_port, timeout=5):
            errors.append("primary replited stream port closed before replica start")
            return fail(errors, [primary_app.log_path, primary_replited.log_path])

        pre_replica_device_ids = [
            f"codex-live-before-replica-{int(time.time())}-{index}"
            for index in range(3)
        ]
        for device_id in pre_replica_device_ids:
            insert_primary_device_state(primary_dir / "data.db", device_id)

        replica_config = write_replica_config(env, stream_port, primary_dir, replica_dir)
        replica_args = [
            str(REPLITED_BIN.resolve()),
            "--config",
            str(replica_config.resolve()),
            "replica-sidecar",
            "--force-restore",
        ]
        if os.environ.get("REPLITED_LATEST_STATE_MANAGED_PROXY") == "1":
            replica_args.extend(
                [
                    "--exec-managed-proxy",
                    f"127.0.0.1:{replica_port}",
                    "--exec-child-template",
                    latest_state_managed_proxy_child_template(
                        name_prefix=replica_name,
                        data_dir=data_dir,
                    ),
                    "--exec-generation-root",
                    str((replica_dir / "managed-readers").resolve()),
                ]
            )
        else:
            exec_cmd = latest_state_replica_exec_cmd(
                name=replica_name,
                replica_dir=replica_dir,
                data_dir=data_dir,
                port=replica_port,
            )
            replica_args.extend(["--exec", exec_cmd])
        replica_sidecar = start_logged_process(
            replica_args,
            cwd=replica_dir,
            log_path=logs_dir / "replica-sidecar.log",
            name="replica-sidecar",
        )
        if not wait_for_http_ok_while_processes_live(
            f"{replica_url}/health",
            [primary_replited, replica_sidecar],
            errors,
            timeout=180,
        ):
            errors.append("latest-state read-only replica did not become healthy")
            return fail(errors, [primary_app.log_path, primary_replited.log_path, replica_sidecar.log_path])

        missing_pre_replica_db = wait_for_replica_db_records(
            replica_dir / "data.db",
            pre_replica_device_ids,
            timeout=180,
        )
        if missing_pre_replica_db:
            errors.append(
                "replica SQLite files did not receive pre-replica writes: "
                + ", ".join(missing_pre_replica_db)
            )
        missing_pre_replica = wait_for_replica_api_records(
            replica_url,
            pre_replica_device_ids,
            timeout=180,
        )
        if missing_pre_replica:
            errors.append(
                "read-only latest-state API did not observe pre-replica writes: "
                + ", ".join(missing_pre_replica)
            )

        post_health_device_ids = [
            f"codex-live-after-health-{int(time.time())}-{index}"
            for index in range(3)
        ]
        for device_id in post_health_device_ids:
            insert_primary_device_state(primary_dir / "data.db", device_id)
        missing_post_health_db = wait_for_replica_db_records(
            replica_dir / "data.db",
            post_health_device_ids,
            timeout=180,
        )
        if missing_post_health_db:
            errors.append(
                "replica SQLite files did not receive post-health writes: "
                + ", ".join(missing_post_health_db)
            )
        missing_post_health = wait_for_replica_api_records(
            replica_url,
            post_health_device_ids,
            timeout=180,
        )
        if missing_post_health:
            errors.append(
                "read-only latest-state API did not observe post-health writes: "
                + ", ".join(missing_post_health)
            )

        log_paths = [primary_app.log_path, primary_replited.log_path, replica_sidecar.log_path]
        errors.extend(scan_logs(log_paths))
        if errors:
            return fail(errors, log_paths)

        print("PASS: latest-state read-only child-process observed replicated write through API")
        return 0
    finally:
        stop_process(replica_sidecar)
        stop_process(primary_replited)
        stop_process(primary_app)
        docker_rm(replica_name)
        docker_rm(primary_name)


def fail(errors: list[str], log_paths: list[Path]) -> int:
    print("\nLATEST-STATE READONLY CHILD-PROCESS REGRESSION FAILED")
    for error in errors:
        print(f"  - {error}")
    print_log_tails(log_paths)
    return 1


def main() -> int:
    skip_if_not_enabled()
    require_binary(REPLITED_BIN, "replited binary")
    require_cmd("docker")
    require_cmd("sqlite3")
    return run_scenario()


if __name__ == "__main__":
    raise SystemExit(main())
