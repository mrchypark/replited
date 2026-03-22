# Manifest-Only Breaking Cut Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Remove legacy backup/restore compatibility and keep only the manifest-first archival path plus direct stream replication.

**Architecture:** The archival lane becomes manifest-only. Restore, follow, purge, and replication publish paths will stop falling back to list-based discovery and legacy snapshot/WAL segment writes. Unsupported archival backends will fail fast at config/runtime boundaries instead of entering compatibility mode.

**Tech Stack:** Rust 2021, Tokio, rusqlite, OpenDAL, Python integration/E2E harnesses, GitHub Actions

---

### Task 1: Remove Legacy Restore Planner

**Files:**
- Modify: `src/sync/restore.rs`
- Modify: `src/sync/restore/manifest_plan.rs`
- Test: `src/sync/restore.rs`

**Steps:**
1. Delete `LegacyRestoreSource`, `ListRestoreSource`, `RestoreDataPlan::Legacy`, and legacy selection loops.
2. Make restore plan discovery manifest-only and fail when manifest metadata is absent or malformed.
3. Remove timestamp-bounded legacy fallback semantics and fail clearly for unsupported timestamp restore.
4. Rewrite restore tests to assert manifest-only behavior.

### Task 2: Remove Legacy Publish Fallbacks

**Files:**
- Modify: `src/sync/replicate.rs`
- Modify: `src/storage/storage_client.rs`
- Test: `src/sync/replicate.rs`
- Test: `src/storage/storage_client.rs`

**Steps:**
1. Delete `Error::INVALID_CONFIG` fallback branches that call `write_snapshot()` and `write_wal_segment()`.
2. Remove or narrow legacy writer APIs no longer used by runtime.
3. Keep manifest publish semantics and restart/resume behavior intact.
4. Rewrite or remove tests that exist only to verify fallback behavior.

### Task 3: Convert Follow Path To Manifest-Only

**Files:**
- Modify: `src/sync/restore/follow.rs`
- Modify: `src/sync/restore.rs`
- Test: `src/sync/restore/follow.rs`

**Steps:**
1. Remove dependence on `decide_restore_info()` and list-based discovery in follow mode.
2. Use `latest.json` and generation manifest metadata to detect new generations and new WAL packs.
3. Keep follow mode aligned with manifest restore request-cost accounting.
4. Add regression tests for manifest-only follow generation transitions.

### Task 4: Fail Fast On Unsupported Archival Backends

**Files:**
- Modify: `src/storage/storage_client.rs`
- Modify: `src/cmd/purge_generation.rs`
- Modify: `src/config/mod.rs`
- Modify: `src/config/arg.rs`
- Test: `src/storage/storage_client.rs`
- Test: `src/cmd/purge_generation.rs`

**Steps:**
1. Turn unsupported archival backends into hard errors for manifest-only commands and runtime setup.
2. Remove `continue on INVALID_CONFIG` compatibility behavior from purge.
3. Keep stream replication config valid; narrow only archival backup/restore support.
4. Update tests to reflect fail-fast behavior.

### Task 5: Rewrite Integration Surface

**Files:**
- Modify: `tests/integration_test.py`
- Modify: `tests/release_e2e.py`
- Modify: `tests/restore_output_set.rs`
- Modify: `src/sync/restore/apply_wal.rs`

**Steps:**
1. Remove test seeding that depends on legacy `write_snapshot()` / `write_wal_segment()` APIs.
2. Make integration tests require manifest path as the only valid archival restore path.
3. Keep release E2E focused on stream path correctness.
4. Replace any remaining legacy-only fixtures with manifest-first artifacts.

### Task 6: Shrink CI and Docs To Supported Surface

**Files:**
- Modify: `.github/workflows/fs_integration_test.yml`
- Modify: `.github/workflows/ftp_integration_test.yml`
- Modify: `.github/workflows/s3_integration_test.yml`
- Modify: `README.md`
- Modify: `docs/sidecar-config.md`
- Modify: `docs/stream-replication.md`

**Steps:**
1. Remove or disable FTP/S3 archival CI if they are no longer supported after the cut.
2. Keep fs manifest integration as the canonical archival gate.
3. Update docs to state the supported archival path explicitly.
4. Remove compatibility language that no longer matches runtime behavior.

### Verification

**Run:**
- `cargo test`
- `cargo build --release`
- `python3 tests/integration_test.py 12000 fs ./target/release/replited`
- `python3 tests/release_e2e.py`

**Expected:**
- All Rust tests pass
- Release build succeeds
- FS integration uses `path=manifest` and `list_calls=0`
- Release E2E remains green for stream bootstrap/resume
