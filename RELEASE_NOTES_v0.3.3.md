# replited v0.3.3

Release date: 2026-02-06

## Highlights

- Fixed multiple stream/restore edge cases that could cause replay gaps or repeated restore loops.
- Added `monitor_interval_ms` to tune primary-side WAL shadow sync polling interval.
- Reduced E2E replication latency by removing fixed 1-second idle delay in replica streaming.
- Improved log quality by reducing expected startup warning noise.

## What's Changed

### Stability

- Fixed `restore --follow` early return path in WAL apply loop.
- Fixed follow-mode generation transition to use correct start index.
- Fixed blocker/unblock lifecycle in sidecar managed-process WAL reset path.
- Improved rewind-floor handling to accept valid next-index boundary advance.
- Improved latest snapshot selection to compare `(index, offset)`.

### Configuration

- `max_checkpoint_page_number = 0` is now accepted to disable forced checkpointing.
- New `monitor_interval_ms` (default: `1000`) in `[[database]]`:
  - controls primary-side WAL shadow sync poll interval
  - lower values reduce replication lag with higher polling overhead

### Logging

- Startup WAL-mode busy/locked condition on pinned primary connection is now debug-level.
- Cold-start `WAL header missing ... rewinding` is now info-level.
- Removed release-build warnings from debug-only function arguments.

## Benchmark Snapshot (E2E)

Using `tests/benchmarks/run_all_benchmarks.sh` with benchmark config tuning:

- Latency: mean `79.30 ms`, p90 `104.45 ms`, p99 `123.29 ms`
- Warning summary during benchmark run:
  - Primary WARN/ERROR: `0/0`
  - Replica WARN/ERROR: `0/0`
  - `SnapshotBoundaryMismatch`: `0`

## Verification

- `cargo test -q`
- `cargo clippy --all-targets --all-features -- -D warnings`
- E2E benchmark script:
  - `tests/benchmarks/run_all_benchmarks.sh`
