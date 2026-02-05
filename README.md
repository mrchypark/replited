# replited(Replicate SQLITE Daemon)

[![GitHub stars](https://img.shields.io/github/stars/mrchypark/replited?label=Stars&logo=github)](https://github.com/mrchypark/replited)
[![GitHub forks](https://img.shields.io/github/forks/mrchypark/replited?label=Forks&logo=github)](https://github.com/mrchypark/replited)

<!-- MarkdownTOC autolink="true" -->
- [Introduction](#introduction)
- [Why replited](#why-replited)
- [Support Backend](#support-backend)
- [Quick Start](#quick-start)
- [Config](#config)
- [Sub commands](#sub-commands)
	- [Replicate](#replicate)
  	- [Restore](#restore)
	- [Stream (primary + replica)](#stream-primary--replica)
		- [⚠️ Important: Replica is Read-Only](#-important-replica-is-read-only)
		- [Replica Checkpoint Tuning](#replica-checkpoint-tuning)
  <!-- /MarkdownTOC -->

## Introduction

Inspired by [Litestream](https://litestream.io/), with the power of [Rust](https://www.rust-lang.org/) and [OpenDAL](https://opendal.apache.org/), replited target to replicate sqlite to everywhere(file system,s3,ftp,google drive,dropbox,etc).

## Why replited
* Using sqlite's [WAL](https://sqlite.org/wal.html) mechanism, instead of backing up full data every time, do incremental backup of data to reduce the amount of synchronised data;
* Support for multiple types of storage backends,such as s3,gcs,ftp,local file system,etc.

## Support Backend

| Type                       | Services                                                     |
| -------------------------- | ------------------------------------------------------------ |
| Standard Storage Protocols | ftp![CI](https://github.com/mrchypark/replited/actions/workflows/ftp_integration_test.yml/badge.svg) |
| Object Storage Services    | [azblob] [gcs] <br> [s3]![CI](https://github.com/mrchypark/replited/actions/workflows/s3_integration_test.yml/badge.svg) |
| File Storage Services      | fs![CI](https://github.com/mrchypark/replited/actions/workflows/fs_integration_test.yml/badge.svg) |

[azblob]: https://azure.microsoft.com/en-us/services/storage/blobs/
[gcs]: https://cloud.google.com/storage
[s3]: https://aws.amazon.com/s3/



## Quick Start

Start a daemon to replicate sqlite:

```shell
replited --config {config file} replicate 
```

Restore sqlite from backend:

```shell
replited --config {config file} restore --db {db in config file} --output {output sqlite db file path}
```

## Config

See [config.md](./config.md)

Replica-side WAL checkpoint tuning (new)
- `apply_checkpoint_frame_interval` (default: 128): number of WAL frames to buffer before checkpointing. Lower values surface new schema/data faster at the cost of more I/O.
- `apply_checkpoint_interval_ms` (default: 2000): max milliseconds between checkpoints even if the frame threshold is not reached.
- DDL (page 1) followed by a commit triggers an immediate checkpoint and SHM rebuild so schema changes are visible without restarting the sidecar.


## Sub commands
### Replicate
`repicate` sub command will run a background process to replicate db to replicates in config periodically, example:
```
replited  --config ./etc/sample.toml  replicate
```

### Restore
`restore` sub command will restore db from replicates in config, example:
```
replited  --config ./etc/sample.toml restore --db /Users/codedump/local/sqlite/test.db --output ./test.db
```

command options:
* `db`: which db will be restore from config
* `output`: which path will restored db saved

### Stream (primary + replica)
- **Primary**: Add a stream replicate target in config (`params.type = "stream"`, `addr = "0.0.0.0:50051"`).
  ```bash
  replited --config primary.toml replicate
  ```
- **Replica**: Point to the primary stream endpoint in the replica config (`params.type = "stream"`, `addr = "http://127.0.0.1:50051"`).
  ```bash
  replited --config replica.toml replica-sidecar --force-restore
  ```
  The sidecar will automatically:
  1. Download the latest snapshot directly from the Primary (Direct Snapshot Streaming).
  2. Switch to WAL streaming mode to apply real-time updates.

**Note**: Direct Snapshot Streaming uses `zstd` for compression and does not require a shared storage backend.

#### ⚠️ Important: Replica is Read-Only

**The replica database is read-only by design.** Writing to the replica will cause:
- **Data divergence**: Replica will have writes that primary doesn't know about
- **Replication failure**: WAL checksums will mismatch, causing replication to stop
- **Data corruption**: In extreme cases, the entire replication stream may become corrupted

**Do NOT write to the replica database.** If you need to write, write to the primary instead.

For automatic write blocking, use **Child Process Mode** with `--exec` flag:
```bash
replited replica-sidecar --exec "/path/to/your-app serve"
```
This ensures your application only connects to the replica when it's safe to read.

#### Replica Checkpoint Tuning

WAL checkpoint tuning is **critical for performance** and **data visibility on replicas**. Proper tuning ensures:

- New schema changes are visible to readers without restarting
- Write-heavy workloads don't overwhelm the replica
- Data propagates to readers in a timely manner

| Setting | Default | Recommended | Description |
|---------|---------|-------------|-------------|
| `apply_checkpoint_frame_interval` | 128 | 10-50 | Frames to buffer before checkpoint |
| `apply_checkpoint_interval_ms` | 2000 | 100-500 | Max milliseconds between checkpoints |

**Example Production Configuration**:
```toml
[[database]]
db = "/data/replica.db"

# Aggressive checkpoint for low-latency replication
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100

[[database.replicate]]
name = "stream"
[database.replicate.params]
type = "stream"
addr = "http://primary:50051"
remote_db_name = "/data/primary.db"
```

**Trade-offs**:
- Lower values = faster data visibility, higher I/O
- Higher values = better throughput, slower propagation
- For high-write workloads: start with `frame_interval=50`, `interval_ms=500`
- For read-heavy workloads: use defaults or higher values

---

## Integration with Frameworks

### PocketBase Integration

replited is fully compatible with PocketBase using the **Child Process Mode**:

```bash
# Primary: Run replited replicate alongside PocketBase
replited --config primary.toml replicate

# Replica: Use --exec to run PocketBase under replited supervision
replited --config replica.toml replica-sidecar \
  --exec "pocketbase serve --http=0.0.0.0:8090 --dir=/pb_data"
```

**PocketBase Configuration Notes**:
- PocketBase uses WAL mode by default with `?_pragma=journal_mode(WAL)`
- Connection pool: `DataMaxOpenConns: 10`, `DataMaxIdleConns: 5`
- Busy timeout: 10 seconds (`?_pragma=busy_timeout(10000)`)

**Example PocketBase Primary Config**:
```toml
[log]
level = "Info"
dir = "/var/log/replited"

[[database]]
db = "/pb_data/data.db"

[[database.replicate]]
name = "stream-primary"
[database.replicate.params]
type = "stream"
addr = "0.0.0.0:50051"
```

**Example PocketBase Replica Config**:
```toml
[log]
level = "Info"
dir = "/var/log/replited"

[[database]]
db = "/pb_data/replica/data.db"

# Checkpoint tuning for responsive replicas
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100

[[database.replicate]]
name = "stream-primary"
[database.replicate.params]
type = "stream"
addr = "http://primary-replited:50051"
remote_db_name = "/pb_data/data.db"
```

**Known Issues**:
- PocketBase may report "database is locked" under high concurrent load (see [Issue #875](https://github.com/pocketbase/pocketbase/issues/875))
- Solution: Use Child Process Mode to ensure proper shutdown sequencing

### TrailBase Integration

TrailBase is built on Rust + SQLite + V8, with explicit WAL mode support:

```bash
# Primary: Run replited replicate alongside TrailBase
replited --config primary.toml replicate

# Replica: Use --exec to run TrailBase under replited supervision
replited --config replica.toml replica-sidecar \
  --exec "trailbase serve --config /etc/trailbase/config.textproto"
```

**TrailBase Configuration Notes**:
- Uses `config.textproto` for configuration
- Supports multiple independent SQLite databases
- Uses `deadpool-sqlite` for connection pooling
- WAL mode is explicitly enabled

**Example TrailBase Primary Config** (`config.textproto`):
```protobuf
databases {
  name: "main"
  path: "data/main.db"
}

server {
  addr: "0.0.0.0:4000"
}
```

**Example replited Primary Config** (`primary.toml`):
```toml
[log]
level = "Info"
dir = "/var/log/replited"

[[database]]
db = "/path/to/trailbase/data/main.db"

[[database.replicate]]
name = "stream-primary"
[database.replicate.params]
type = "stream"
addr = "0.0.0.0:50051"
```

**Example replited Replica Config** (`replica.toml`):
```toml
[log]
level = "Info"
dir = "/var/log/replited"

[[database]]
db = "/path/to/replica/data/main.db"

# Checkpoint tuning for responsive replicas
apply_checkpoint_frame_interval = 10
apply_checkpoint_interval_ms = 100

[[database.replicate]]
name = "stream-primary"
[database.replicate.params]
type = "stream"
addr = "http://primary-replited:50051"
remote_db_name = "/path/to/trailbase/data/main.db"
```

---

## Troubleshooting

### Common Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| "database is locked" | High concurrent access | Increase `busy_timeout`, use connection pooling |
| Replication lag > 30s | Checkpoint too infrequent | Decrease `apply_checkpoint_interval_ms` |
| WAL file growing huge | Checkpoints not running | Check checkpoint settings, force manual checkpoint |
| Schema changes not visible | SHM not rebuilt | Restart replica or wait for DDL-triggered checkpoint |

## Stargazers over time
[![Stargazers over time](https://starchart.cc/mrchypark/replited.svg?variant=adaptive)](https://starchart.cc/mrchypark/replited)

​                    
