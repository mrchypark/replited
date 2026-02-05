# Stream Replication Guide

Stream replication enables real-time WAL (Write-Ahead Log) synchronization between Primary and Replica databases over gRPC.

## Architecture Overview

```
┌─────────────┐     gRPC Stream     ┌─────────────┐
│   Primary   │ ──────────────────► │   Replica   │
│  replited   │     WAL Frames      │  replited   │
└─────────────┘                     └─────────────┘
       │                                   │
       ▼                                   ▼
┌─────────────┐                     ┌─────────────┐
│  Storage    │ ───── Snapshot ───► │  Restored   │
│  Backend    │    (Initial Only)   │     DB      │
└─────────────┘                     └─────────────┘
```

## ⚠️ Requirements

> [!IMPORTANT]
> Stream replication requires a **storage backend** (fs, s3, etc.) for initial snapshot restore.
> Streaming alone only synchronizes WAL frames - it cannot create the initial database.

### Primary Configuration

```toml
[[database]]
db = "primary.db"

# Required: Storage backend for snapshots
[[database.replicate]]
name = "backup"
[database.replicate.params]
type = "fs"
root = "./backup"

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
db = "primary.db"  # Must match Primary's db name

[[database.replicate]]
name = "stream-client"
[database.replicate.params]
type = "stream"
addr = "http://PRIMARY_IP:50051"
```

## Initial Restore Flow

1. **Replica** starts with `--force-restore` flag
2. **Replica** connects to Primary and calls `get_restore_config()`
3. **Primary** returns its `DbConfig` including storage settings
4. **Replica** uses `StorageClient` to download snapshot from storage backend
5. **Replica** applies snapshot to create initial database
6. **Replica** starts streaming WAL frames for ongoing sync

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
Replica is requesting a different database name. Ensure `db` field matches between Primary and Replica configs.

### "NoSnapshotError"
No snapshot available in storage backend. Ensure:
1. Primary has a storage backend configured (fs, s3, etc.)
2. Initial data has been written and checkpointed
3. Snapshot file exists in the storage path

### "Invalid stream address"
Address should be in format `http://IP:PORT`. The scheme is required for gRPC client but stripped for server binding.
