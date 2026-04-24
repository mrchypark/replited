#!/usr/bin/env python3

import json
import os
import random
import string
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

PRIMARY_URL = os.environ.get(
    "PRIMARY_URL", "http://primary-api.replited-chaos.svc.cluster.local:8090"
)
REPLICA_SERVICE_URL = os.environ.get(
    "REPLICA_SERVICE_URL", "http://replica-read.replited-chaos.svc.cluster.local:8090"
)
REPLICA_POD_TEMPLATE = os.environ.get(
    "REPLICA_POD_TEMPLATE",
    "http://replica-pocketbase-{index}.replica-headless.replited-chaos.svc.cluster.local:8090",
)
REPLICA_COUNT = int(os.environ.get("REPLICA_COUNT", "3"))
ADMIN_EMAIL = os.environ.get("POCKETBASE_ADMIN_EMAIL", "test@example.com")
ADMIN_PASS = os.environ.get("POCKETBASE_ADMIN_PASS", "password123456")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME", "chaos_notes")
DURATION_SECS = int(os.environ.get("DURATION_SECS", "180"))
WRITE_INTERVAL_SECS = float(os.environ.get("WRITE_INTERVAL_SECS", "0.1"))
READ_INTERVAL_SECS = float(os.environ.get("READ_INTERVAL_SECS", "0.05"))
FINAL_SYNC_TIMEOUT_SECS = int(os.environ.get("FINAL_SYNC_TIMEOUT_SECS", "180"))
SEED_WAIT_TIMEOUT_SECS = int(os.environ.get("SEED_WAIT_TIMEOUT_SECS", "120"))
PAYLOAD_SIZE = int(os.environ.get("PAYLOAD_SIZE", "1024"))


def request(method, url, data=None, headers=None, timeout=5):
    if headers is None:
        headers = {}
    body = None
    if data is not None:
        body = json.dumps(data).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            text = response.read().decode("utf-8")
            payload = {}
            if text:
                try:
                    payload = json.loads(text)
                except Exception:
                    payload = {"raw": text}
            return {"status": response.status, "json": payload, "text": text}
    except urllib.error.HTTPError as err:
        text = err.read().decode("utf-8")
        payload = {}
        if text:
            try:
                payload = json.loads(text)
            except Exception:
                payload = {"raw": text}
        return {"status": err.code, "json": payload, "text": text}
    except Exception as err:
        return {"status": -1, "error": str(err)}


def wait_for_health(url, timeout_secs):
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        res = request("GET", f"{url}/api/health", timeout=2)
        if res["status"] == 200:
            return True
        time.sleep(1)
    return False


def login_primary():
    res = request(
        "POST",
        f"{PRIMARY_URL}/api/collections/_superusers/auth-with-password",
        data={"identity": ADMIN_EMAIL, "password": ADMIN_PASS},
        timeout=10,
    )
    if res["status"] != 200:
        raise RuntimeError(f"primary login failed: {res}")
    return res["json"]["token"]


def ensure_collection(headers):
    schema = [
        {"name": "title", "type": "text", "required": True},
        {"name": "content", "type": "text"},
        {"name": "sequence", "type": "number"},
    ]
    data = {
        "name": COLLECTION_NAME,
        "type": "base",
        "schema": schema,
        "listRule": "",
        "viewRule": "",
    }
    res = request(
        "POST", f"{PRIMARY_URL}/api/collections", data=data, headers=headers, timeout=10
    )
    if res["status"] in (200, 400):
        return
    raise RuntimeError(f"collection setup failed: {res}")


def create_record(headers, sequence, title):
    payload = {
        "title": title,
        "content": "".join(random.choices(string.ascii_letters + string.digits, k=PAYLOAD_SIZE)),
        "sequence": sequence,
    }
    res = request(
        "POST",
        f"{PRIMARY_URL}/api/collections/{COLLECTION_NAME}/records",
        data=payload,
        headers=headers,
        timeout=10,
    )
    if res["status"] != 200:
        raise RuntimeError(f"create record failed: {res}")
    return res["json"]


def wait_for_seed(seed_id):
    deadline = time.time() + SEED_WAIT_TIMEOUT_SECS
    while time.time() < deadline:
        all_ready = True
        for index in range(REPLICA_COUNT):
            url = REPLICA_POD_TEMPLATE.format(index=index)
            res = request(
                "GET",
                f"{url}/api/collections/{COLLECTION_NAME}/records/{seed_id}",
                timeout=2,
            )
            if res["status"] != 200:
                all_ready = False
                break
        if all_ready:
            return
        time.sleep(1)
    raise RuntimeError("seed record did not converge to all replicas before chaos run")


def list_total_items(base_url):
    query = urllib.parse.urlencode({"page": 1, "perPage": 1})
    res = request(
        "GET",
        f"{base_url}/api/collections/{COLLECTION_NAME}/records?{query}",
        timeout=5,
    )
    if res["status"] != 200:
        return None, res
    return int(res["json"].get("totalItems", 0)), res


class ChaosState:
    def __init__(self):
        self.stop = False
        self.write_errors = []
        self.read_errors = []
        self.total_writes = 0
        self.total_reads = 0
        self.last_write_sequence = -1


def writer_loop(state, headers, start_sequence):
    sequence = start_sequence
    while not state.stop:
        try:
            create_record(headers, sequence, f"chaos-write-{sequence}")
            state.total_writes += 1
            state.last_write_sequence = sequence
            sequence += 1
        except Exception as err:
            state.write_errors.append(str(err))
        time.sleep(WRITE_INTERVAL_SECS)


def reader_loop(state, seed_id):
    while not state.stop:
        res = request(
            "GET",
            f"{REPLICA_SERVICE_URL}/api/collections/{COLLECTION_NAME}/records/{seed_id}",
            timeout=2,
        )
        state.total_reads += 1
        if res["status"] != 200:
            state.read_errors.append(res)
        time.sleep(READ_INTERVAL_SECS)


def wait_for_final_sync(expected_total):
    deadline = time.time() + FINAL_SYNC_TIMEOUT_SECS
    final_counts = {}
    while time.time() < deadline:
        final_counts = {}
        all_ready = True
        for index in range(REPLICA_COUNT):
            base_url = REPLICA_POD_TEMPLATE.format(index=index)
            total, res = list_total_items(base_url)
            if total is None:
                all_ready = False
                final_counts[f"replica-{index}"] = {"status": res["status"], "body": res}
                break
            final_counts[f"replica-{index}"] = total
            if total < expected_total:
                all_ready = False
        if all_ready:
            return True, final_counts
        time.sleep(2)
    return False, final_counts


def main():
    if not wait_for_health(PRIMARY_URL, 120):
        raise RuntimeError("primary PocketBase never became healthy")
    if not wait_for_health(REPLICA_SERVICE_URL, 180):
        raise RuntimeError("replica read service never became healthy")

    token = login_primary()
    headers = {"Authorization": token}
    ensure_collection(headers)

    seed_record = create_record(headers, 0, "seed-record")
    seed_id = seed_record["id"]

    wait_for_seed(seed_id)

    state = ChaosState()
    writer = threading.Thread(target=writer_loop, args=(state, headers, 1), daemon=True)
    reader = threading.Thread(target=reader_loop, args=(state, seed_id), daemon=True)

    writer.start()
    reader.start()

    start = time.time()
    while time.time() - start < DURATION_SECS:
        time.sleep(1)

    state.stop = True
    writer.join(timeout=10)
    reader.join(timeout=10)

    expected_total = state.total_writes + 1
    synced, final_counts = wait_for_final_sync(expected_total)

    service_failures = []
    for err in state.read_errors:
        status = err.get("status", -1)
        if status == -1 or status >= 500 or status == 404:
            service_failures.append(err)

    summary = {
        "duration_secs": DURATION_SECS,
        "total_writes": state.total_writes,
        "total_reads": state.total_reads,
        "write_errors": state.write_errors,
        "service_read_errors": state.read_errors,
        "service_failure_count": len(service_failures),
        "final_sync_complete": synced,
        "final_counts": final_counts,
        "expected_total": expected_total,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))

    if state.write_errors:
        print("writer encountered errors", file=sys.stderr)
        return 1
    if service_failures:
        print("replica read service returned failures during chaos", file=sys.stderr)
        return 1
    if not synced:
        print("replicas did not converge before timeout", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
