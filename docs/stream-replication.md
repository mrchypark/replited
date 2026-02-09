# Stream Replication Guide

Stream replication enables real-time WAL (Write-Ahead Log) synchronization between Primary and Replica databases over gRPC.

## Architecture Overview

```
┌─────────────┐     gRPC Stream     ┌─────────────┐
│   Primary   │ ──────────────────► │   Replica   │
│  replited   │     WAL Frames      │  replited   │
└─────────────┘                     └─────────────┘
       │                                   │
       └──────── Snapshot (zstd) ─────────►│
                                           ▼
                                    ┌─────────────┐
                                    │  Restored   │
                                    │     DB      │
                                    └─────────────┘
```

## ⚠️ Requirements

> [!IMPORTANT]
> Stream replication does **not** require a separate storage backend (fs, s3, etc.) for bootstrapping.
> The replica can stream the initial snapshot directly from the Primary (compressed with `zstd`) and then switch to WAL streaming.

### Primary Configuration

```toml
[[database]]
db = "primary.db"

# Stream server
[[database.replicate]]
name = "stream-server"
[database.replicate.params]
type = "stream"
addr = "http://0.0.0.0:50051"
```

### Replica Configuration

```toml
[[database]]
db = "replica.db"  # Local path can differ from Primary

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://PRIMARY_IP:50051"
remote_db_name = "primary.db" # Primary DB identity (usually Primary `db` value)
```

Notes:

- Primary DB identity is the `db` string from the Primary config. If the replica `db` path differs, set `remote_db_name` explicitly.
- For a full config reference, see `docs/stream-copy-config.md` and `docs/sidecar-config.md`.

## Initial Restore Flow

1. **Replica** starts with `--force-restore` flag
2. **Replica** connects to Primary and calls `stream_snapshot_v2`
3. **Primary** generates/serves a compressed snapshot stream (`zstd`) plus a snapshot boundary LSN
4. **Replica** restores the snapshot locally and records the snapshot boundary LSN
5. **Replica** switches to `stream_wal_v2` to apply real-time WAL updates

## v2 LSN Contract

An LSN (Log Sequence Number) is the authoritative position of a replica in the WAL stream.
It is defined as a 3-tuple: `LSN = (generation, index, offset)`.

- `generation`: The replication generation name written by the primary. A generation change resets WAL history.
- `index`: The WAL segment index within the generation.
- `offset`: The byte offset within the WAL segment. `offset = 0` points to the WAL header.

### Alignment Rule

`offset` must always be aligned to a WAL frame boundary. Use SQLite's alignment helper:

```
offset == align_frame(page_size, offset)
```

If an offset is not aligned, the LSN is invalid and must not be used for streaming.

### Forbidden Resume Sources

v2 replication must never derive an LSN from the live `-wal` file size. Local WAL files can be
truncated, reset, or contain partial frames and are not authoritative. The replica must only
resume from an explicit LSN recorded by the replication system (e.g. last applied or snapshot
boundary).

## v2 Error Taxonomy

Stream replication uses explicit error codes to drive replica state transitions.

| Code | Meaning | Replica action |
| --- | --- | --- |
| `LINEAGE_MISMATCH` | Replica generation does not match the primary's lineage. | `NeedsRestore` |
| `WAL_NOT_RETAINED` | Requested LSN is older than retained WAL segments. | `NeedsRestore` |
| `SNAPSHOT_BOUNDARY_MISMATCH` | Requested LSN does not match the snapshot boundary used to seed the replica. | `NeedsRestore` |
| `INVALID_LSN` | LSN is malformed or violates alignment/ordering rules. | `Retry` (recompute LSN) |

If `INVALID_LSN` repeats after recomputation, the replica must fall back to `NeedsRestore`.

## Commands

### Start Primary
```bash
replited --config primary.toml replicate
```

### Start Replica
```bash
# First time (restore from snapshot)
replited --config replica.toml replica-sidecar --force-restore

# Subsequent (resume from existing WAL)
replited --config replica.toml replica-sidecar
```

## Troubleshooting

### "Database not found" on Primary
Replica is requesting a different DB identity. Ensure `remote_db_name` matches the Primary `db` value exactly.

### "NoSnapshotError"
Primary could not provide a usable snapshot yet. Ensure:
1. Primary is running `replited ... replicate` with a `type = "stream"` target configured
2. The database has had initial data written and a checkpoint has occurred
3. Retry after a short delay (snapshot generation is retried internally)

### "Invalid stream address"
Address should be in format `http://IP:PORT`. The scheme is required for gRPC client but stripped for server binding.
