<!-- MarkdownTOC autolink="true" -->
- [Overview](#overview)
- [Log config](#log-config)
- [Database config](#database-config)
	- [Replicate Config](#replicate-config)
		- [Azure blob Params](#azure-blob-params)
		- [File System Params](#file-system-params)
		- [Ftp Params](#ftp-params) 
		- [Gcs Params](#gcs-params) 
		- [S3 Params](#s3-params)
		- [Stream Params](#stream-params)
	- [⚠️ Replica Read-Only Warning](#-replica-read-only-warning)
	- [Checkpoint Tuning Guide](#checkpoint-tuning-guide)
  
  <!-- /MarkdownTOC -->

## Overview

replited use `toml` as its config file format, the structure of config is:

* Log config;
* One or more database configs:
  * sqlite database file path;
  * one or more database replicate backend.

See config sample in [sample.toml](./etc/sample.toml)

## Log Config

| item  |  value    |
| :---- | ---- |
| level |  Trace/Debug/Info/Warn/Error    |
| dir   |  log files directory    |

## Database Config
| item  |  value    |
| :---- | ---- |
| db | sqlite database file path |
| replicate | one or more database replicate backend |
| min_checkpoint_page_number | passive checkpoint threshold (pages) |
| max_checkpoint_page_number | forced checkpoint threshold (pages) |
| truncate_page_number | forced truncation checkpoint threshold (pages) |
| checkpoint_interval_secs | seconds between background checkpoints on primary |
| monitor_interval_ms | milliseconds between primary WAL shadow sync polls (default 1000) |
| apply_checkpoint_frame_interval | **replica-side**: WAL frames to buffer before checkpoint (default 128) |
| apply_checkpoint_interval_ms | **replica-side**: max milliseconds between checkpoints (default 2000) |
| wal_retention_count | number of shadow WAL segments retained locally on primary for gap fill (default 10) |
| max_concurrent_snapshots | max concurrent snapshot streams allowed (default 5) |

### Replicate Config
| item  |  value    |
| :---- | ---- |
| name | replicate backend config name, cannot duplicate |
| params | params of backend, see below |

#### Azure blob Params
| item  |  value    |
| :---- | ---- |
| params.type | `"azb"` (alias: `"Azb"`) |
| params.root | root of Azblob service backend. |
| params.container | container name of Azblob service backend. |
| params.endpoint | endpoint of Azblob service backend. |
| params.account_name | account name of Azblob service backend. |
| params.account_key | account key of Azblob service backend. |

#### File System Params
| item  |  value    |
| :---- | ---- |
| params.type | `"fs"` (alias: `"Fs"`) |
| params.root | root directory of file system backend |

#### Ftp Params
| item  |  value    |
| :---- | ---- |
| params.type | `"ftp"` (alias: `"Ftp"`) |
| params.endpoint | Endpoint of this ftp, use "ftps://127.0.0.1" by default. |
| params.root | root directory of file system backend, use "/" by default. |
| params.username | username of ftp backend. |
| params.password | password of ftp backend. |

#### Gcs Params
| item  |  value    |
| :---- | ---- |
| params.type | `"gcs"` (alias: `"Gcs"`) |
| params.endpoint | Endpoint of this backend, must be full uri, use "https://storage.googleapis.com" by default. |
| params.root | Root URI of gcs operations. |
| params.bucket | Bucket name of this backend. |
| params.credential | Credentials string for GCS service OAuth2 authentication. |


#### S3 Params
| item  |  value    |
| :---- | ---- |
| params.type | `"s3"` (alias: `"S3"`) |
| params.endpoint | Endpoint of this backend, must be full uri, use "https://s3.amazonaws.com" by default. |
| params.region | Region represent the signing region of this endpoint.If `region` is empty, use env value `AWS_REGION` if it is set, or else use `us-east-1` by default. |
| params.bucket | Bucket name of this backend. |
| params.access_key_id | access_key_id of this backend. |
| params.secret_access_key | secret_access_key of this backend. |
| params.root | root of this backend. |

#### Stream Params
| item  |  value    |
| :---- | ---- |
| params.type | `"stream"` |
| params.addr | gRPC server address (e.g., "0.0.0.0:50051" for server, "http://127.0.0.1:50051" for client) |
| params.remote_db_name | **(Recommended for replica)** Primary database name/path to look up in primary's config (defaults to local `db` if omitted) |

**Note**: Stream replication does not require a storage backend (fs, s3, etc.) for initial snapshot restore. The replica can stream the snapshot directly from the primary.

---

## ⚠️ Replica Read-Only Warning

**The replica database is read-only by design.** SQLite replication follows a **single-writer, multiple-reader** pattern:

- **Primary**: The only instance that can write to the database
- **Replica**: Receives WAL updates from primary and applies them to a local copy

### Why Read-Only?

1. **Data Consistency**: Replica writes would create divergence that the replication stream cannot resolve
2. **WAL Integrity**: SQLite uses checksums and salt values in WAL headers. Writes on replica would invalidate these checksums
3. **Replication Failure**: WAL checksums will mismatch, causing replication to stop with errors

### What Happens If You Write to Replica?

```
WAL Stuck detected - data divergence may have occurred
Auto-restore triggered: deleting corrupt database
```

The sidecar will detect the mismatch and attempt auto-restore, but this may result in **data loss** if writes were significant.

### Safe Architecture Pattern

```
                    ┌─────────────────┐
                    │   Application   │
                    │   (reads/writes)│
                    └────────┬────────┘
                             │ writes
                             ▼
                    ┌─────────────────┐
                    │     PRIMARY     │ ◄── Write here
                    │  (replited)     │
                    └────────┬────────┘
                             │ stream WAL
                             ▼
              ┌──────────────┴──────────────┐
              │                              │
              ▼                              ▼
     ┌─────────────────┐          ┌─────────────────┐
     │   REPLICA 1     │          │   REPLICA 2     │
     │   (read-only)   │          │   (read-only)   │
     └─────────────────┘          └─────────────────┘
              │                              │
              └──────────┬───────────────────┘
                         │ reads
                         ▼
                 ┌─────────────────┐
                 │   Application   │ ◄── Read replicas
                 │   (reads only)  │     for scale
                 └─────────────────┘
```

### Child Process Mode (Recommended)

Use the `--exec` flag to automatically manage your application:

```bash
replited replica-sidecar \
  --exec "/path/to/your-app serve --http=0.0.0.0:8080" \
  --config replica.toml
```

**Benefits:**
- Sidecar controls application lifecycle
- Application only starts when replica is ready
- Automatic restart on schema changes or WAL stuck

---

## Checkpoint Tuning Guide

Checkpoint tuning is **critical** for:
- **Data visibility**: How quickly new data appears on replicas
- **I/O performance**: How much work the replica does
- **Replication lag**: How far behind replicas can fall

### Understanding Checkpoints

SQLite WAL mode works by:
1. **Writes** go to WAL (Write-Ahead Log) file first
2. **Readers** can read from either main DB or WAL
3. **Checkpoint** moves WAL changes into the main DB file

Without checkpoints:
- WAL file grows indefinitely
- Readers eventually see stale data
- On replica: new data never becomes visible to your application

### Configuration Options

#### `apply_checkpoint_frame_interval` (Replica-side)
- **Default**: 128
- **Range**: 1 - 10000
- **Effect**: Trigger checkpoint after every N frames written

#### `apply_checkpoint_interval_ms` (Replica-side)
- **Default**: 2000 (2 seconds)
- **Range**: 10 - 60000
- **Effect**: Maximum time between checkpoints

### Tuning Guidelines

| Workload Type | frame_interval | interval_ms | Rationale |
|---------------|----------------|-------------|-----------|
| **Low write** (few ops/sec) | 128 | 2000 | Default is fine |
| **Moderate write** (10-100 ops/sec) | 50 | 500 | Balance I/O and visibility |
| **High write** (100+ ops/sec) | 10-20 | 100-200 | Prioritize visibility |
| **Real-time** (sub-second latency needed) | 5-10 | 50-100 | Minimize latency |
| **Bulk load** (batch inserts) | 1000 | 5000 | Minimize I/O during load |

### Example: High-Throughput Replica

For applications requiring sub-second data visibility:

```toml
[[database]]
db = "/var/lib/app/replica.db"

# Aggressive checkpoint for real-time visibility
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100
max_concurrent_snapshots = 5

[[database.replicate]]
name = "primary-stream"
[database.replicate.params]
type = "stream"
addr = "http://primary:50051"
remote_db_name = "/var/lib/app/primary.db"
```

### Monitoring Checkpoints

Check replica logs for checkpoint metrics:

```bash
# Look for checkpoint activity
grep -i "checkpoint" /var/log/replited/replica.log

# Example output:
WalWriter: Checkpoint result: (0, 128, 128)  # (busy, logged, checkpointed frames)
```

### Troubleshooting

| Symptom | Likely Cause | Solution |
|---------|--------------|----------|
| Replica never sees new data | Checkpoint interval too high | Decrease `apply_checkpoint_interval_ms` |
| High I/O on replica | Checkpoint too aggressive | Increase `apply_checkpoint_frame_interval` |
| WAL file growing huge | Checkpoints not running | Check `apply_checkpoint_interval_ms` |
| Replication lag > 10s | Not enough checkpoint frequency | Lower both interval settings |

---

## Framework Integration Guide

### PocketBase

PocketBase is a Go-based backend with embedded SQLite. replited integrates seamlessly using Child Process Mode.

**Key Configuration**:
- Default connection pool: `DataMaxOpenConns: 10`, `DataMaxIdleConns: 5`
- Busy timeout: 10 seconds
- Uses `?_pragma=journal_mode(WAL)` for WAL mode

**Primary Setup**:
```toml
[log]
level = "Info"
dir = "/var/log/replited"

[[database]]
db = "/pb_data/data.db"

[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "0.0.0.0:50051"
```

**Replica Setup**:
```toml
[log]
level = "Info"
dir = "/var/log/replited"

[[database]]
db = "/pb_data/replica/data.db"
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100

[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "http://primary:50051"
remote_db_name = "/pb_data/data.db"
```

**Running Commands**:
```bash
# Primary
replited --config primary.toml replicate

# Replica with Child Process Mode
replited --config replica.toml replica-sidecar \
  --exec "pocketbase serve --http=0.0.0.0:8090 --dir=/pb_data/replica"
```

**Known Issues**:
- "database is locked (5)" may occur under high concurrency ([Issue #875](https://github.com/pocketbase/pocketbase/issues/875))
- Solution: Ensure proper shutdown order using Child Process Mode

### TrailBase

TrailBase is a Rust-based application server with built-in SQLite support and V8 JavaScript runtime.

**Key Configuration**:
- Uses `config.textproto` format
- Supports multiple independent databases
- Uses `deadpool-sqlite` for connection pooling
- WAL mode is explicitly enabled

**Primary Setup** (`config.textproto`):
```protobuf
databases {
  name: "main"
  path: "data/main.db"
}
```

**replited Primary Config** (`primary.toml`):
```toml
[log]
level = "Info"
dir = "/var/log/replited"

[[database]]
db = "/path/to/trailbase/data/main.db"

[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "0.0.0.0:50051"
```

**replited Replica Config** (`replica.toml`):
```toml
[log]
level = "Info"
dir = "/var/log/replited"

[[database]]
db = "/path/to/replica/data/main.db"
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100

[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "http://primary:50051"
remote_db_name = "/path/to/trailbase/data/main.db"
```

**Running Commands**:
```bash
# Primary
replited --config primary.toml replicate

# Replica with Child Process Mode
replited --config replica.toml replica-sidecar \
  --exec "trailbase serve --config /etc/trailbase/config.textproto"
```
