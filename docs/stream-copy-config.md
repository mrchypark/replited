# Streaming Copy Config Guide

This document explains how to configure stream replication (`replicate` command) in `replited`.

## 1. What This Covers

- Primary stream server config
- Replica stream client config
- Required and optional keys with defaults
- Address/path rules that commonly cause mismatches

`replica-sidecar` runtime options are documented separately in:
- `docs/sidecar-config.md`

## 2. Minimal Working Config

### Primary (`replicate`)

```toml
[log]
level = "Info"
dir = "./logs"

[[database]]
db = "/data/primary.db"
min_checkpoint_page_number = 1000
max_checkpoint_page_number = 10000
truncate_page_number = 500000
checkpoint_interval_secs = 60
wal_retention_count = 10
max_concurrent_snapshots = 5

[[database.replicate]]
name = "stream-primary"
[database.replicate.params]
type = "stream"
addr = "127.0.0.1:50051"
```

Start command:

```bash
replited --config primary.toml replicate
```

### Replica (`replica-sidecar` consumes this)

```toml
[log]
level = "Info"
dir = "./logs"

[[database]]
db = "/data/replica.db"
apply_checkpoint_frame_interval = 128
apply_checkpoint_interval_ms = 2000

[[database.replicate]]
name = "stream-primary"
[database.replicate.params]
type = "stream"
addr = "http://127.0.0.1:50051"
remote_db_name = "/data/primary.db"
```

Notes:

- Replica `addr` must include scheme (`http://...`) because gRPC client connect requires it.
- `remote_db_name` should point to the Primary DB identity (usually Primary `db` path).
- If `remote_db_name` is omitted, sidecar uses local `db` value, which often mismatches in real deployments.

## 3. Stream Config Reference

## `[[database]]` common keys

| Key | Default | Required | Meaning |
| --- | --- | --- | --- |
| `db` | none | yes | SQLite file path for this database entry |
| `replicate` | none | yes | Replication targets; must include at least one item |
| `min_checkpoint_page_number` | `1000` | no | Passive checkpoint threshold (pages) |
| `max_checkpoint_page_number` | `10000` | no | Forced checkpoint threshold (pages) |
| `truncate_page_number` | `500000` | no | Forced truncate checkpoint threshold (pages) |
| `checkpoint_interval_secs` | `60` | no | Periodic checkpoint cadence |
| `wal_retention_count` | `10` | no | WAL segments retained locally for gap fill |
| `max_concurrent_snapshots` | `5` | no | Max concurrent snapshot streams from Primary |

Validation constraints:

- `replicate` must not be empty.
- `min_checkpoint_page_number` must be `> 0`.
- `min_checkpoint_page_number <= max_checkpoint_page_number`.

## `[[database.replicate]]` stream keys

| Key | Default | Required | Meaning |
| --- | --- | --- | --- |
| `name` | none | yes | Logical target name |
| `params.type` | none | yes | Must be `"stream"` for stream replication |
| `params.addr` | `"http://127.0.0.1:50051"` | yes | Stream endpoint |
| `params.remote_db_name` | `null` | no | Primary DB identity override (recommended) |

Address rule by role:

- Primary server bind: `addr` may be `127.0.0.1:50051` or `http://127.0.0.1:50051`.
- Replica client connect: use `http://...` explicitly.

## 4. Recommended Profiles

### Low-latency replica visibility

```toml
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100
```

### Balanced throughput

```toml
apply_checkpoint_frame_interval = 50
apply_checkpoint_interval_ms = 500
```

### Conservative I/O

```toml
apply_checkpoint_frame_interval = 128
apply_checkpoint_interval_ms = 2000
```

Tradeoff:

- Lower values: faster visibility, more checkpoint I/O.
- Higher values: lower I/O, slower visibility.

## 5. Multi-DB Pattern

If one Primary serves multiple DBs, add one `[[database]]` entry per DB, and keep DB identity explicit:

```toml
[[database]]
db = "/data/a.db"
[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "http://primary:50051"
remote_db_name = "/data/a.db"

[[database]]
db = "/data/b.db"
[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "http://primary:50051"
remote_db_name = "/data/b.db"
```

## 6. Common Misconfigurations

### Replica cannot connect to stream

- Symptom: `Failed to connect to stream`
- Check: Replica `addr` includes `http://` and port is reachable.

### Snapshot or WAL stream reports DB not found

- Cause: DB identity mismatch
- Fix: set `remote_db_name` to Primary `db` value exactly.

### Replica lags too much

- Reduce `apply_checkpoint_frame_interval` and/or `apply_checkpoint_interval_ms`.

### Primary overloaded by many bootstraps

- Reduce concurrent startup and tune `max_concurrent_snapshots`.
