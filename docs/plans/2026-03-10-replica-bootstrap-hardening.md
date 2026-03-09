# Replica Bootstrap Hardening Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make new-replica bootstrap bounded-memory and protocol-safe without changing backup/restore semantics.

**Architecture:** Keep the existing snapshot `Meta -> Chunk...` protocol, but redefine bootstrap around a short critical section that fixes a boundary LSN and produces a stable snapshot source before compression/streaming. Split replica bootstrap into explicit `Bootstrapping -> Restoring -> CatchingUp -> Ready -> Streaming` phases, and keep object-storage restore semantics compatible while stream bootstrap changes land.

**Tech Stack:** Rust, Tokio, tonic gRPC streams, rusqlite, tempfile, zstd, Python integration scripts.

---

### Task 1: Lock The Bootstrap Contract

**Files:**
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/database/database/snapshot.rs`
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/base/compress.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/database/database/snapshot.rs`

**Step 1: Write the failing test**

Add a test that proves the stream snapshot path returns a stable file-backed artifact with size/hash metadata and does not require reopening the compressed output for hashing.

**Step 2: Run test to verify it fails**

Run: `cargo test snapshot_for_stream_returns_file_backed_snapshot`
Expected: FAIL because the helper still uses a second read pass or missing metadata contract.

**Step 3: Write minimal implementation**

- Replace the current two-pass `compress_file_to_path + sha256_file` flow with a one-pass helper, for example `compress_file_to_path_and_hash`.
- In `snapshot_to_temp_path()`, define the critical section explicitly:
  1. ensure WAL exists
  2. checkpoint
  3. acquire read lock
  4. capture boundary LSN
  5. create stable temp snapshot source
  6. release read lock
  7. compress/hash/stream from the stable temp source
- Keep the legacy `snapshot()` path untouched for now.

**Step 4: Run test to verify it passes**

Run: `cargo test snapshot_for_stream_returns_file_backed_snapshot`
Expected: PASS

**Step 5: Commit**

```bash
git add src/database/database/snapshot.rs src/base/compress.rs
git commit -m "refactor: harden stream snapshot bootstrap contract"
```

### Task 2: Add Snapshot Streaming Error Coverage

**Files:**
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/sync/server.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/sync/server.rs`

**Step 1: Write the failing tests**

Add tests for:
- `send_snapshot_stream` sends `Meta` first, then chunks matching file contents.
- Missing compressed file yields a stream error response.

**Step 2: Run tests to verify they fail**

Run: `cargo test validate_snapshot_request_accepts_matching_lsn send_snapshot_stream`
Expected: FAIL because coverage for actual file streaming/error paths is missing.

**Step 3: Write minimal implementation**

- Keep the existing protocol unchanged.
- Ensure `send_snapshot_stream()` only depends on file-backed snapshot metadata.
- Emit deterministic stream errors on open/read failure.

**Step 4: Run tests to verify they pass**

Run: `cargo test sync::server::tests`
Expected: PASS

**Step 5: Commit**

```bash
git add src/sync/server.rs
git commit -m "test: cover snapshot stream file sender"
```

### Task 3: Split Replica Bootstrap States

**Files:**
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/cmd/replica_sidecar.rs`
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/cmd/replica_sidecar/streaming.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/cmd/replica_sidecar.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/cmd/replica_sidecar/streaming.rs`

**Step 1: Write the failing tests**

Add tests that distinguish:
- snapshot received but not yet restored
- restore completed and boundary durable
- EOF / transport failure entering retry backoff instead of false success
- unrecoverable bootstrap failure entering fatal behavior

**Step 2: Run tests to verify they fail**

Run: `cargo test replica_sidecar`
Expected: FAIL because current states collapse bootstrap/restore/retry semantics together.

**Step 3: Write minimal implementation**

- Replace the coarse state machine with:
  - `Bootstrapping`
  - `Restoring`
  - `CatchingUp`
  - `Ready`
  - `Streaming`
  - `RetryBackoff`
  - `Fatal`
- Do not ACK snapshot boundary until restore succeeds and local durable progress is persisted.
- Treat clean EOF during steady-state as reconnect/backoff, not success.

**Step 4: Run tests to verify they pass**

Run: `cargo test replica_sidecar`
Expected: PASS

**Step 5: Commit**

```bash
git add src/cmd/replica_sidecar.rs src/cmd/replica_sidecar/streaming.rs
git commit -m "refactor: split replica bootstrap and recovery states"
```

### Task 4: Make Catch-Up Bounded By Bytes

**Files:**
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/sync/restore/apply_wal.rs`
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/cmd/replica_sidecar/streaming.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/sync/restore/apply_wal.rs`

**Step 1: Write the failing test**

Add a test that proves catch-up no longer aggregates a whole WAL index into one `Vec<u8>` before apply.

**Step 2: Run test to verify it fails**

Run: `cargo test apply_wal_frames`
Expected: FAIL because the current implementation still concatenates decompressed WAL bytes.

**Step 3: Write minimal implementation**

- Replace index-wide aggregation with chunk/segment apply.
- Define explicit limits:
  - max decompressed bytes per apply chunk
  - max compressed bytes in flight per replica
- Keep checkpoint/refresh semantics correct while applying in bounded chunks.

**Step 4: Run test to verify it passes**

Run: `cargo test apply_wal_frames`
Expected: PASS

**Step 5: Commit**

```bash
git add src/sync/restore/apply_wal.rs src/cmd/replica_sidecar/streaming.rs
git commit -m "refactor: bound replica catch-up memory by bytes"
```

### Task 5: Add Boundary And Lineage Guards

**Files:**
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/cmd/replica_sidecar/streaming.rs`
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/sync/server.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/cmd/replica_sidecar/streaming.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/sync/server.rs`

**Step 1: Write the failing tests**

Add tests for:
- snapshot boundary lineage mismatch
- catch-up start LSN mismatch after restore
- rewind to last durable local LSN
- rewind to snapshot boundary

**Step 2: Run tests to verify they fail**

Run: `cargo test stream`
Expected: FAIL because rewind scopes and lineage checks are not fully explicit yet.

**Step 3: Write minimal implementation**

- Validate generation/lineage at every handoff:
  - snapshot boundary
  - catch-up start
  - steady-state resume
- Split recovery actions into:
  - retry
  - rewind to last durable local LSN
  - rewind to snapshot boundary
  - full restore
  - fatal

**Step 4: Run tests to verify they pass**

Run: `cargo test stream`
Expected: PASS

**Step 5: Commit**

```bash
git add src/cmd/replica_sidecar/streaming.rs src/sync/server.rs
git commit -m "fix: enforce bootstrap boundary and lineage rules"
```

### Task 6: Preserve Backup/Restore Semantics

**Files:**
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/sync/restore.rs`
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/storage/storage_client.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/tests/restore_output_set.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/storage/storage_client.rs`

**Step 1: Write the failing test**

Add a compatibility test that proves the new stream bootstrap boundary semantics still match the existing restore model of `snapshot + WAL`.

**Step 2: Run test to verify it fails**

Run: `cargo test restore_outputs_db_wal_shm_set_and_reads_latest_rows`
Expected: FAIL if stream-side boundary semantics diverge from restore expectations.

**Step 3: Write minimal implementation**

- Do not change object layout yet.
- Keep existing restore CLI contract unchanged.
- Add compatibility assertions or helper logic so stream bootstrap and restore use the same logical cutoff.

**Step 4: Run test to verify it passes**

Run: `cargo test restore_outputs_db_wal_shm_set_and_reads_latest_rows`
Expected: PASS

**Step 5: Commit**

```bash
git add src/sync/restore.rs src/storage/storage_client.rs tests/restore_output_set.rs
git commit -m "test: lock restore semantics to stream bootstrap boundary"
```

### Task 7: Add Bootstrap Admission Metrics And Limits

**Files:**
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/config/config.rs`
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/sync/server.rs`
- Modify: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/cmd/replica_sidecar.rs`
- Test: `/Users/cypark/.codex/worktrees/replited-perf-redesign/src/config/config.rs`

**Step 1: Write the failing test**

Add config parsing and validation tests for:
- bootstrap byte budget
- per-replica catch-up byte budget
- zero/invalid budget rejection

**Step 2: Run test to verify it fails**

Run: `cargo test test_config`
Expected: FAIL because byte-budget fields do not exist yet.

**Step 3: Write minimal implementation**

- Add explicit byte-budget config for bootstrap and catch-up.
- Enforce admission control by bytes, not only snapshot count.
- Emit metrics/logging fields for:
  - active bootstrap bytes
  - active bootstrap count
  - catch-up bytes in flight
  - rewind/full-restore/fatal counts

**Step 4: Run test to verify it passes**

Run: `cargo test test_config`
Expected: PASS

**Step 5: Commit**

```bash
git add src/config/config.rs src/sync/server.rs src/cmd/replica_sidecar.rs
git commit -m "feat: add bootstrap byte-budget controls"
```

### Task 8: Verification Batch

**Files:**
- Verify only

**Step 1: Run focused Rust tests**

Run:
- `cargo test snapshot_for_stream_returns_file_backed_snapshot`
- `cargo test sync::server::tests`
- `cargo test replica_sidecar`
- `cargo test apply_wal_frames`

Expected: PASS

**Step 2: Run full Rust tests**

Run: `cargo test`
Expected: PASS

**Step 3: Run selected Python scenarios**

Run:
- `python3 tests/simple_replication.py`
- `python3 tests/battle/stream_v2_divergence_test.py`

Expected: PASS

**Step 4: Capture evidence**

Record:
- lock-hold timing before/after
- snapshot bootstrap RSS
- catch-up RSS
- whether EOF now routes through retry/backoff

**Step 5: Commit**

```bash
git add .
git commit -m "test: verify replica bootstrap hardening"
```
