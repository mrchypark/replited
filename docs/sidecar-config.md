# Replica Sidecar Config Guide

This document explains how to run and tune `replica-sidecar`.

It focuses on:

- Sidecar command options (`--force-restore`, `--exec`)
- Sidecar-related config keys
- Safe usage patterns for real applications (PocketBase, TrailBase, custom apps)

Stream server/client configuration is documented separately in:
- `docs/stream-copy-config.md`

## 1. Command Usage

Basic:

```bash
replited --config replica.toml replica-sidecar
```

Force bootstrap restore:

```bash
replited --config replica.toml replica-sidecar --force-restore
```

Child process mode:

```bash
replited --config replica.toml replica-sidecar \
  --exec "pocketbase serve --http=127.0.0.1:8090 --dir=/pb_data"
```

## 2. Sidecar Runtime Behavior

Sidecar runs a per-DB state machine:

1. `Bootstrapping`
2. `CatchingUp`
3. `Streaming`
4. `NeedsRestore` (when non-recoverable stream errors occur)

During bootstrap:

- With `--force-restore`, sidecar deletes local `db`, `db-wal`, `db-shm` first.
- Sidecar downloads snapshot from Primary stream and restores local DB.

During streaming:

- Sidecar applies WAL chunks to local replica DB.
- Sidecar periodically checkpoint-refreshes WAL index using:
  - frame threshold (`apply_checkpoint_frame_interval`)
  - time threshold (`apply_checkpoint_interval_ms`)

Automatic restore guard:

- Sidecar limits automatic restore loops (backs off after repeated failures).

## 3. Sidecar Config Keys

Configure in `[[database]]` inside replica config:

| Key | Default | Required | Sidecar usage |
| --- | --- | --- | --- |
| `db` | none | yes | Local replica SQLite path |
| `apply_checkpoint_frame_interval` | `128` | no | WAL frames before forced refresh checkpoint |
| `apply_checkpoint_interval_ms` | `2000` | no | Max ms between refresh checkpoints |
| `replicate` | none | yes | Must include one stream target |

Validation constraints:

- `apply_checkpoint_frame_interval` must be `> 0`.
- `replicate` must not be empty.

Stream target keys sidecar reads:

| Key | Required | Meaning |
| --- | --- | --- |
| `params.type = "stream"` | yes | Enables stream snapshot/WAL flow |
| `params.addr` | yes | Primary stream endpoint (`http://...`) |
| `params.remote_db_name` | recommended | Primary DB identity |

Important:

- Sidecar uses the first stream target it finds in `replicate`.

## 4. `--exec` Child Process Mode

`--exec` lets sidecar supervise your app process.

Example:

```bash
replited --config replica.toml replica-sidecar \
  --exec "/usr/local/bin/my-app --config /etc/my-app.yml"
```

Behavior:

- Sidecar starts the child process.
- During sensitive restore/recovery sections, sidecar can temporarily stop the child process.
- After blocker sections complete, sidecar starts it again.

Why use this:

- Prevents reader processes from holding stale WAL/SHM state through restore boundaries.
- Reduces "read process pinned old state" problems in real workloads.

## 5. PocketBase Example (Recommended)

Replica config:

```toml
[log]
level = "Info"
dir = "./logs"

[[database]]
db = "/pb_data/replica/data.db"
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100

[[database.replicate]]
name = "stream-primary"
[database.replicate.params]
type = "stream"
addr = "http://primary-replited:50051"
remote_db_name = "/pb_data/data.db"
```

Run:

```bash
replited --config replica.toml replica-sidecar \
  --force-restore \
  --exec "pocketbase serve --http=0.0.0.0:8090 --dir=/pb_data/replica"
```

## 6. Operational Recommendations

First startup:

- Use `--force-restore` once for clean bootstrap.

Steady-state restarts:

- Restart without `--force-restore` to resume from last applied LSN.

When to force restore again:

- Local replica DB is suspected corrupted/diverged.
- Sidecar repeatedly enters restore-required path and cannot converge.

Tune checkpoint values by workload:

- Write-heavy + low-latency reads: `10~50` frames, `100~500ms`.
- Throughput-oriented: larger values to reduce checkpoint frequency.

## 7. Troubleshooting

### Sidecar loops in restore/retry

- Check stream connectivity (`addr`) and DB identity (`remote_db_name`).
- Verify Primary is healthy and serving snapshot/WAL stream.

### Replica updates are slow to appear

- Decrease `apply_checkpoint_frame_interval`.
- Decrease `apply_checkpoint_interval_ms`.

### App sees stale data after replica transitions

- Use `--exec` mode so sidecar can safely gate the app during recovery/restore.

### Sidecar cannot find stream config

- Ensure `[[database.replicate]]` contains `params.type = "stream"` for each DB entry.
