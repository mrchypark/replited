# replited(Replicate SQLITE Daemon)

[![GitHub stars](https://img.shields.io/github/stars/lichuang/replited?label=Stars&logo=github)](https://github.com/lichuang/replited)
[![GitHub forks](https://img.shields.io/github/forks/lichuang/replited?label=Forks&logo=github)](https://github.com/lichuang/replited)

<!-- MarkdownTOC autolink="true" -->
- [Introduction](#introduction)
- [Why replited](#why-replited)
- [Support Backend](#support-backend)
- [Quick Start](#quick-start)
- [Config](#config)
- [Sub commands](#sub-commands)
	- [Replicate](#replicate)
  - [Restore](#restore)
  <!-- /MarkdownTOC -->

## Introduction

Inspired by [Litestream](https://litestream.io/), with the power of [Rust](https://www.rust-lang.org/) and [OpenDAL](https://opendal.apache.org/), replited target to replicate sqlite to everywhere(file system,s3,ftp,google drive,dropbox,etc).

## Why replited
* Using sqlite's [WAL](https://sqlite.org/wal.html) mechanism, instead of backing up full data every time, do incremental backup of data to reduce the amount of synchronised data;
* Support for multiple types of storage backends,such as s3,gcs,ftp,local file system,etc.

## Support Backend

| Type                       | Services                                                     |
| -------------------------- | ------------------------------------------------------------ |
| Standard Storage Protocols | ftp![CI](https://github.com/lichuang/replited/actions/workflows/ftp_integration_test.yml/badge.svg)                                    |
| Object Storage Services    | [azblob] [gcs] <br> [s3]![CI](https://github.com/lichuang/replited/actions/workflows/s3_integration_test.yml/badge.svg) |
| File Storage Services      | fs![CI](https://github.com/lichuang/replited/actions/workflows/fs_integration_test.yml/badge.svg)                                                          |

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

## Stargazers over time
[![Stargazers over time](https://starchart.cc/lichuang/replited.svg?variant=adaptive)](https://starchart.cc/lichuang/replited)

​                    
