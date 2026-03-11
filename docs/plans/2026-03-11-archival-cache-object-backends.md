# Archival Cache and Object Backend Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reintroduce manifest-backed object archival with `s3` as the reference backend, then apply the same model to `gcs` and `azblob`, while keeping `fs` both as a first-class archival publish target and as the local spool/cache layer that reduces object-store request cost.

**Architecture:** The stream lane stays direct Primary->Replica and does not depend on manifests in its hot path. The archival lane becomes `local fs spool/cache -> per-target manifest-backed publish`, where `fs` is both one archival target and the local cache. Restore/follow remain manifest-first, but resolve each snapshot object and WAL pack through the local fs cache first and only issue remote GETs on cache miss.

**Tech Stack:** Rust 2024, Tokio, rusqlite, OpenDAL (`fs`, `s3`, `gcs`, `azblob`), tonic/prost, Python integration/E2E harnesses, GitHub Actions

---

## Scope

### In
- `fs` as a first-class archival publish backend
- `fs` as the shared local spool/cache layer
- `s3` as the first object-store reference backend
- `gcs` and `azblob` as follow-on backends once the reference path is proven
- manifest-first publish/restore/follow for all supported archival targets
- restore/follow request-cost reduction through cache-first object resolution
- explicit single-writer archival publish contract for non-`fs` targets

### Out
- `ftp` archival support
- list-based restore fallback
- background manifest compaction
- multi-level archival pack classes
- multi-writer object-store fencing beyond the chosen single-writer contract

## Why This Direction

### What problem this solves
- The current breaking cut made archival runtime `fs`-only, which removes object-store archival from the core product.
- Object stores charge per request, especially for `GET` and `LIST`, so direct remote reads on every restore/follow step are too expensive.
- `fs` is already the simplest and cheapest local primitive we control. Using it as both cache/spool and a real archival target preserves the fast path and keeps the architecture coherent.

### Why this is the smallest viable next step
- We keep the current manifest-first restore truth source.
- We do not reintroduce legacy list-based discovery.
- We avoid speculative multi-writer protocols for object stores in the first step and instead require a single archival writer per target/database.
- We prove the model on one object backend first, then extend the same semantics to the other supported object backends.
- We add one clear optimization layer, `local fs cache`, that helps both `fs` and object backends without changing stream hot-path semantics.

### Why multiple archival targets still matter
- Even without automatic cross-target arbitration, additional archival targets still provide independent durability domains and operator-controlled recovery options.
- The first useful version keeps recovery explicit: operators choose the truth source they trust instead of the runtime guessing across diverged targets.

## Canonical Model

### Lanes
- `stream lane`: direct gRPC snapshot/WAL stream between Primary and Replica
- `archival lane`: local fs spool/cache plus one or more manifest-backed archival targets

### Archival roles
- `local fs spool/cache`
  - durable local copy of snapshot objects and WAL pack objects
  - read-through cache for restore/follow
  - write staging area for archival publish
- `archival publish target`
  - `fs`
  - `s3` in the first object-store phase
  - `gcs` and `azblob` after the reference path is proven

### Manifest truth
- Each archival target has its own `latest.json` and generation manifest namespace.
- Restore/follow choose exactly one target as the truth source for manifest metadata.
- The first useful version uses an explicit operator-selected truth source, not automatic multi-target arbitration.
- If the selected truth source is unreachable, restore/follow fail clearly even if the local cache already contains the referenced data objects.
- Data objects referenced by the chosen manifest are resolved via local cache first.
- The cache root is explicitly configured and defaults to a dedicated subdirectory under the database metadata directory.

## Workflow Logic

### Publish workflow
1. Primary produces a snapshot object or WAL pack object.
2. The object is written to the local fs spool/cache under its immutable object key.
3. For each configured archival target:
   - write the immutable object to the target if it is not already present
   - rewrite the generation manifest for that target
   - publish `latest.json` for that target last
4. Cache retention keeps recent objects locally after remote publish.

### Why this order matters
- Writing the immutable object first prevents manifests from referencing missing data.
- Rewriting the manifest before `latest.json` ensures `latest.json` is the visibility boundary.
- Publishing to local fs first guarantees restore/follow can use the cache even if remote publish is slower.

### Restore workflow
1. Select the archival target to restore from.
2. Read `latest.json` from that target.
3. Read the generation manifest from that target.
4. Build the exact restore plan from `snapshot + contiguous WAL packs`.
5. For each referenced object:
   - check local fs cache for the exact object key
   - if hit, use local bytes
   - if miss, GET from the chosen target and populate the local cache
   - verify fetched bytes against the manifest checksum before admitting them into the cache or using them for restore/follow
   - if cache write fails after a successful remote GET, continue restore with the fetched bytes and surface the cache failure as a warning
6. Restore the snapshot locally.
7. Apply WAL packs in manifest order.

### Follow workflow
1. Poll the selected target’s `latest.json`.
2. If unchanged, do nothing.
3. If changed, read the new manifest.
4. Compare current local progress with the manifest’s contiguous pack chain.
5. Resolve newly required objects through local cache first, remote on miss.
6. Apply same-generation packs or switch generation if the manifest points to a newer snapshot.

### Why this order matters
- Manifest metadata is read before object data so restore/follow know the exact required object set.
- Cache lookup comes before remote GET so object-store request count is minimized.
- Generation switches always restore the new snapshot first so later WAL packs have the correct base snapshot.

## Runtime Contracts

### Single-writer archival contract
- For `s3`, `gcs`, and `azblob`, the runtime assumes exactly one archival writer per `db + target`.
- `fs` can keep stronger local compare-under-lock semantics.
- Non-`fs` targets do not attempt generalized distributed fencing in this phase.
- The runtime mechanism for the first version is operational rather than distributed: one configured archival publisher process owns a given `db + target`, and duplicate publishers are unsupported.
- Publish retries must remain idempotent by immutable object key and by `latest.json` payload content.

### Cache contract
- Cache keys are the same immutable object keys used by manifests.
- Cache lookup must never guess by generation/index alone; exact key match only.
- Cache root and `fs` archival target root must be distinct configured paths in the first useful version.
- Cache may be pruned by generation rollover rules, but never while referenced by the current restore/follow target.
- Cache pruning must conservatively keep the union of objects referenced by the chosen manifest, objects recently published by the local archival writer, and objects still needed by local follow/apply progress.

### Restore correctness contract
- No list-based restore discovery.
- Gaps in the WAL pack chain are hard failures.
- Corrupt or mismatched manifest metadata is a hard failure.
- Cache corruption is treated as a miss if detected before use; otherwise restore fails loudly.
- Remote object bytes fetched on cache miss must match the checksum recorded in the manifest before they are used or cached.

### Capacity and startup contract
- Cache/spool capacity must be explicitly budgeted by byte size in implementation, not left unbounded.
- Startup recovery must treat incomplete temp files as garbage and preserve only complete immutable object files.
- The first useful version may use simple local garbage collection tied to generation rollover and explicit byte-budget thresholds.

## Delivery Phases

### Phase A
- `fs` archival publish remains supported
- `s3` becomes the reference object backend
- local cache/spool is introduced and used in restore/follow

### Phase B
- apply the same reference model to `gcs` and `azblob`
- reuse the same manifest, cache, and publish ordering rules

## Implementation Order

### Task 1: Add canonical archival target model and cache-root contract
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/src/config/config.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/config/storage_params.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/cmd/replicate.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/config/arg.rs`
- Test: `/Users/cypark/Documents/project/replited/src/config/config.rs`

**Steps:**
1. Write a failing config test that `fs` and `s3` archival targets are accepted while `ftp` remains rejected.
2. Write a failing config test that the cache root is explicit or defaults by a documented convention.
3. Run the targeted config tests and verify they fail.
4. Implement the minimal validation and config-surface change.
5. Run the targeted config tests and verify they pass.
6. Commit.

### Task 2: Re-enable manifest publish for the reference object backend
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/storage/operator.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/sync/replicate.rs`
- Test: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Test: `/Users/cypark/Documents/project/replited/src/sync/replicate.rs`

**Steps:**
1. Write failing tests that `s3` no longer fails fast on manifest publish setup.
2. Write failing tests that publish order remains `object -> manifest -> latest`.
3. Run those tests and verify they fail.
4. Implement minimal single-writer publish semantics for the reference object backend.
5. Run those tests and verify they pass.
6. Commit.

### Task 3: Introduce local cache/spool abstraction
**Files:**
- Create: `/Users/cypark/Documents/project/replited/src/storage/cache.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/storage/mod.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Test: `/Users/cypark/Documents/project/replited/src/storage/cache.rs`

**Steps:**
1. Write failing tests for exact-key cache put/get/miss behavior.
2. Run the tests and verify they fail.
3. Implement a minimal local fs cache API keyed by immutable object key.
4. Run the tests and verify they pass.
5. Commit.

### Task 4: Add cache budget and safe local recovery rules
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/src/storage/cache.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Test: `/Users/cypark/Documents/project/replited/src/storage/cache.rs`

**Steps:**
1. Write failing tests for startup cleanup of incomplete temp objects and budget-based pruning that does not remove pinned objects.
2. Run the tests and verify they fail.
3. Implement the minimal recovery and pruning rules.
4. Run the tests and verify they pass.
5. Commit.

### Task 5: Make object reads cache-first with checksum admission
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/sync/restore.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/sync/restore/follow.rs`
- Test: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Test: `/Users/cypark/Documents/project/replited/src/sync/restore.rs`

**Steps:**
1. Write failing tests for `read object -> cache hit avoids remote GET` and `cache miss populates cache`.
2. Write a failing test that remote bytes are rejected when their checksum does not match the manifest.
3. Run the tests and verify they fail.
4. Implement cache-first object resolution with checksum verification.
5. Run the tests and verify they pass.
6. Commit.

### Task 6: Keep `fs` as both cache and archival target
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/sync/replicate.rs`
- Test: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`

**Steps:**
1. Write a failing test that `fs` archival publish still works when the same local fs layer is also used as cache/spool.
2. Run the test and verify it fails.
3. Implement the minimal requirement that cache root and `fs` archival target root are distinct and both continue to work in the same deployment.
4. Run the test and verify it passes.
5. Commit.

### Task 7: Add explicit truth-source restore/follow selection
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/src/sync/restore.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/sync/restore/follow.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/config/arg.rs`
- Test: `/Users/cypark/Documents/project/replited/src/sync/restore.rs`
- Test: `/Users/cypark/Documents/project/replited/src/sync/restore/follow.rs`

**Steps:**
1. Write failing tests for explicit truth-source selection across archival targets.
2. Run the tests and verify they fail.
3. Implement the minimal explicit target-selection logic without automatic arbitration.
4. Run the tests and verify they pass.
5. Commit.

### Task 8: Add request-cost and cache-hit observability
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/sync/restore.rs`
- Modify: `/Users/cypark/Documents/project/replited/tests/integration_test.py`
- Test: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Test: `/Users/cypark/Documents/project/replited/tests/integration_test.py`

**Steps:**
1. Write failing tests for stable restore diagnostics including cache hits/misses and remote GET counts.
2. Run the tests and verify they fail.
3. Implement minimal counters/logging.
4. Run the tests and verify they pass.
5. Commit.

### Task 9: Extend the reference model to `gcs` and `azblob`
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/src/config/config.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/storage/operator.rs`
- Modify: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`
- Test: `/Users/cypark/Documents/project/replited/src/storage/storage_client.rs`

**Steps:**
1. Write failing tests that `gcs` and `azblob` follow the same manifest publish/read rules as the `s3` reference path.
2. Run the tests and verify they fail.
3. Implement the minimal backend-specific wiring needed to reuse the proven reference path.
4. Run the tests and verify they pass.
5. Commit.

### Task 10: Expand supported-surface integration gates
**Files:**
- Modify: `/Users/cypark/Documents/project/replited/tests/integration_test.py`
- Modify: `/Users/cypark/Documents/project/replited/.github/workflows/fs_integration_test.yml`
- Modify: `/Users/cypark/Documents/project/replited/.github/workflows/release.yml`
- Modify: `/Users/cypark/Documents/project/replited/README.md`
- Test: `/Users/cypark/Documents/project/replited/tests/integration_test.py`

**Steps:**
1. Add a failing integration scenario for `fs` archival plus cache-first restore diagnostics.
2. Add a failing integration scenario that can be enabled for object-store-compatible environments later.
3. Run the local integration path and verify the new assertions fail.
4. Implement the minimal harness/doc changes.
5. Re-run the local integration path and verify it passes.
6. Commit.

## Verification

**Run:**
- `cargo test`
- `cargo build --release`
- `python3 tests/integration_test.py 12000 ./target/release/replited`
- `python3 tests/release_e2e.py`

**Expected:**
- All Rust tests pass
- Release build succeeds
- `fs` archival still passes as a publish target
- the `s3` reference path is covered by targeted test or harnessed smoke verification
- `gcs` and `azblob` have parity tests for manifest publish/read behavior
- restore diagnostics show `path=manifest`
- restore diagnostics include cache-hit/cache-miss counters
- normal restore path still shows `list_calls=0`

## Review Checklist
- Does each task explain both what changes and why the order is necessary?
- Does the publish workflow keep `latest.json` as the visibility boundary?
- Does the restore/follow workflow always compute the manifest plan before touching data objects?
- Is `fs` clearly treated as both cache/spool and first-class archival target?
- Are object-store backends restored without reintroducing list-based discovery?
- Is the single-writer contract explicit enough for non-`fs` targets?
- Is the truth-source selection policy explicit enough to avoid hidden cross-target arbitration?
