# replited v0.3.2

Release date: 2026-02-06

## Highlights

- Replica sidecar stability hardening on snapshot/WAL streaming paths.
- Refactored large sidecar flows into clearer state-based handlers.
- Unified stream protocol helpers for LSN token and stream error mapping.
- Reduced async runtime blocking by moving selected SQLite/file operations to blocking boundaries.
- Added sidecar-focused integration scenarios for restore/retry/recover and connected CI coverage.

## What's Changed

### Stability

- Replaced panic-prone paths in critical sidecar streaming flow with explicit error handling.
- Improved WAL refresh and recovery handling paths, including clearer restore escalation behavior.
- Clarified file open/truncate intent on WAL/snapshot write paths.

### Refactoring

- Split `run_single_db` into state handlers (`bootstrap`, `catchup`, `streaming`, `recovery`).
- Split WAL/snapshot streaming logic into smaller units with shared protocol conversion helpers.
- Consolidated duplicated path/error mapping helpers into shared protocol utilities.

### Async / Performance

- Removed blocking sleep usage from key sync path and switched to async wait loops.
- Moved selected SQLite and file I/O sections behind `tokio::task::spawn_blocking`.

### Test & CI

- Added unit tests for:
  - ACK regression rejection
  - stream error mapping roundtrip
  - snapshot metadata validation
  - WAL refresh restore/recovery escalation paths
- Added battle scenarios:
  - sidecar restore
  - sidecar retry
  - sidecar recover
- Added non-blocking CI job for sidecar battle scenarios.
- Added non-blocking clippy step in unit test workflow.

## Verification

- `cargo test --all-targets` passed.
- `cargo clippy --all-targets` passed.
- Sidecar battle scenarios (`restore`, `retry`, `recover`) passed locally.
- PocketBase real-use E2E validation passed:
  - collection create/update/delete
  - record create/read/update/delete
  - replica API reflection under `replica-sidecar --exec`.
