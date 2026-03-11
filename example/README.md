# PocketBase Live Example

This example runs a real PocketBase primary and replica on the host machine and uses `replited` streaming replication between them.

## Topology

- Primary PocketBase: `127.0.0.1:8090`
- Primary `replited`: stream server on `127.0.0.1:50051`
- Replica `replited`: `replica-sidecar --exec "...pocketbase serve..."`
- Replica PocketBase: `127.0.0.1:8091`

The replica PocketBase process is supervised by `replica-sidecar`, which allows `replited` to gate PocketBase during restore and recovery.

## Prerequisites

- `cargo build --release`
- `curl`
- `unzip`
- `python3`

The demo script auto-downloads the official PocketBase binary into `example/bin/` if it is missing.

## Canonical Path

The host-based script is the canonical example for this repository.

- Use [run_pocketbase_live_demo.sh](/Users/cypark/Documents/project/replited/example/run_pocketbase_live_demo.sh) for the supported live demo.
- The Docker Compose files in this folder are retained as legacy reference material and are not the primary maintained path.

## Run

```bash
bash example/run_pocketbase_live_demo.sh
```

The script:

1. Creates runtime directories under `example/runtime/`
2. Starts PocketBase primary on the host
3. Starts `replited replicate`
4. Creates a PocketBase superuser on primary
5. Starts `replica-sidecar --force-restore --exec "...pocketbase serve..."`
6. Waits for the replica PocketBase health check
7. Runs `example/verify_pocketbase.py`

## What You Should See

- Primary Admin UI: [http://127.0.0.1:8090/_/](http://127.0.0.1:8090/_/)
- Replica Admin UI: [http://127.0.0.1:8091/_/](http://127.0.0.1:8091/_/)

`verify_pocketbase.py` logs into the primary, creates a public `notes` collection, inserts a record, and confirms that the record appears on the replica.

## Non-Interactive Run

To run the verification and exit immediately after success:

```bash
DEMO_HOLD=0 bash example/run_pocketbase_live_demo.sh
```

## Runtime Logs

- `example/runtime/logs/primary-pocketbase.log`
- `example/runtime/logs/primary-replited.log`
- `example/runtime/logs/replica-sidecar.log`

## Important Notes

- The replica database is read-only from the application perspective.
- The primary and replica each use distinct `cache_root` directories.
- This example is host-based on purpose so it avoids Docker image/build instability and shows the actual release binary behavior.
