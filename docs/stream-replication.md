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
