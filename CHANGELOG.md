# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

<!-- Release notes generated using configuration in .github/release.yml at main -->

## [v0.3.3] - 2026-02-06

### Fixes
* fix: resolve `restore --follow` WAL apply early-return that could skip later WAL indexes.
* fix: correct generation handoff in follow mode (`offset` used as `index`) to prevent skipped replay.
* fix: harden sidecar blocker lifecycle to ensure managed child process is always unblocked on errors.
* fix: improve rewind-floor boundary handling in stream apply path to avoid false `SnapshotBoundaryMismatch`.
* fix: select latest snapshot by `(index, offset)` instead of only `index`.

### Configuration / Behavior
* config: allow `max_checkpoint_page_number = 0` to disable forced checkpoints as documented.
* config: add `monitor_interval_ms` (default `1000`) to control primary WAL shadow sync polling interval.

### Performance
* perf: remove fixed 1s idle delay in replica streaming loop; use short idle backoff only when no progress.
* perf: lower E2E replication latency in benchmark profile via `monitor_interval_ms` and checkpoint tuning updates.

### Logging
* chore: downgrade expected cold-start WAL rewind log to `INFO`.
* chore: suppress noisy startup `journal_mode=WAL` busy/locked warning for pinned primary connection.
* chore: remove release build warnings from debug-only parameter naming.

## [v0.3.0] - 2025-12-05

### Features
* feat: Implement Direct Snapshot Streaming. Replicas can now restore directly from the Primary via gRPC without a shared storage backend.
* feat: Internalize compression using `zstd`. Replaced external `zstd` binary dependency with Rust `zstd` crate.
* feat: Add concurrency control for snapshot streaming with `max_concurrent_snapshots` configuration.

### Configuration
* config: Stream replication no longer requires a storage backend (fs, s3, etc.) for initial snapshot restore.
* config: Added `max_concurrent_snapshots` to `[[database]]` config (default: 10).

### Fixes
* fix: Resolve stack overflow issue in Primary server by moving large buffer allocations to heap.
* fix: Remove validation that enforced storage backend presence for stream replication.

## [v0.2.0] - 2025-12-05

### Features
* feat: Implement gRPC-based streaming replication with new `replica_sidecar` command.
* feat: Implement continuous restore for SQLite databases with WAL segment filtering by creation time.
* feat: Enhance snapshot restoration with integrity checks and improved latest WAL segment selection.
* feat: Add battle test for replication and enhance restore command with follow and timestamp options.
* feat: Improve streaming WAL replication and replica checkpoint tuning.

### CI
* ci: Add release workflow with multi-platform builds (linux/darwin/windows) and Docker image.
* ci: Add tag workflow for automatic version tagging.

### Chores
* chore: Upgrade Rust toolchain to 1.88.
* chore: Upgrade logforth to 0.28.1.
* chore: Use `codegen-units=1` in release profile.
* chore: Update toolchain to 1.82.0.
* chore: Bump suppaftp version to 6.0.1.

---

## [v0.1.0] - 2024-10-15

### Feature
* feat: add replicate/restore sub commands.
* feat: add fs/ftp/azure blob/gcs/s3 backend support.
### Docs
* docs: add config.md about config fotmat.
### CI
* ci: add integration test of ftp/s3/fs.

