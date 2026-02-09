# replited v0.3.4

Release date: 2026-02-09

## Highlights

- `restore` now consistently outputs a `DB + WAL + SHM` set in non-follow mode.
- Replica sidecar streaming is more robust against transient WAL truncation/rotation edge cases.
- Storage listing ignores transient `*.tmp` snapshot/WAL segment files to avoid replication errors during concurrent uploads.
- Battle/E2E harness is less flaky due to better cleanup and convergence waiting.

## What's Changed

### Restore Semantics

- Non-follow `restore` now moves any `-wal/-shm` created under the temp base to the final output base.
- If `-wal` exists but `-shm` does not, `restore` creates an empty `-shm` so the output is always a complete set without opening the DB during restore.

### Replica Sidecar Robustness

- WAL apply now treats "offset beyond EOF" as a rewindable invalid position and resets local WAL to avoid getting stuck.

### Storage Robustness

- Snapshot/WAL segment listing now ignores transient `*.tmp` files (created during upload+rename) so replication doesn't error when listing while a write is in-flight.

### Test Harness

- Battle tests now clean up common per-run artifacts.
- Chaos test final validation waits for full convergence before asserting no data loss.

## Benchmark Snapshot (E2E)

Using `tests/benchmarks/run_all_benchmarks.sh`:

- Latency (50 samples): mean `75.89 ms`, median `91.40 ms`, p95 `121.11 ms`, p99 `127.05 ms`
- Throughput (500 txns each payload):
  - `100B`: `137546.85 TPS`, `13.12 MB/s`
  - `1KB`: `90573.56 TPS`, `88.45 MB/s`
  - `10KB`: `18434.43 TPS`, `180.02 MB/s`
  - `100KB`: `4757.36 TPS`, `464.59 MB/s`

## Verification

- `cargo fmt --check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test -q`
- E2E battle suite:
  - `tests/battle/run_battle_tests.sh --full`
- E2E benchmarks:
  - `tests/benchmarks/run_all_benchmarks.sh`
