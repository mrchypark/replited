# Archival Cache and Object Backends PR Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reintroduce object-store archival with a manifest-first, cache-first design while keeping `fs` both as a first-class archival target and as the local spool/cache layer.

**Architecture:** The stream lane remains direct Primary->Replica and stays independent from manifests in its hot path. The archival lane becomes `local fs spool/cache -> per-target manifest-backed publish`, and restore/follow resolve manifest-referenced objects through the local cache first, then the selected archival target on miss.

**Tech Stack:** Rust 2024, Tokio, rusqlite, OpenDAL (`fs`, `s3`, `gcs`, `azblob`), tonic/prost, Python integration/E2E harnesses, GitHub Actions

---

## Ordering Rule

This sequence is ordered for **minimum implementation risk**, not fastest visible value.

The ordering principle is:

1. establish config and code seams first
2. introduce passive local components before wiring them into runtime flows
3. prove one reference object backend before widening backend coverage
4. add observability before declaring the architecture validated

## PR 1: Config Surface and Archival Model

**Why first**
- Lowest-risk structural change
- Creates the explicit seams all later PRs depend on
- Lets later PRs avoid mixed concerns in config parsing and runtime behavior

**Files:**
- Modify: `src/config/config.rs`
- Modify: `src/config/storage_params.rs`
- Modify: `src/config/arg.rs`
- Modify: `src/cmd/replicate.rs`
- Test: `src/config/config.rs`

**Scope**
- Reintroduce canonical archival target vocabulary for `fs`, `s3`, `gcs`, `azblob`
- Keep `ftp` rejected
- Add explicit `cache_root` contract
- Add explicit archival truth-source selection surface without wiring runtime behavior yet
- Keep current runtime behavior unchanged where possible

**Merge criteria**
- Config tests prove `fs` and `s3` are accepted
- Config tests prove `ftp` remains rejected
- `cache_root` is explicit or has one documented default
- No runtime publish/restore behavior changes yet

## PR 2: Local Cache/Spool Component

**Why second**
- The cache is a passive dependency for both publish and restore
- Isolating it before runtime integration reduces rollback risk
- Startup cleanup and pruning are easier to validate before other code depends on them

**Files:**
- Create: `src/storage/cache.rs`
- Modify: `src/storage/mod.rs`
- Test: `src/storage/cache.rs`

**Scope**
- Exact-key immutable local object cache API
- Cache put/get/miss behavior
- Startup cleanup of incomplete temp files
- Byte-budget pruning that preserves pinned/live objects

**Merge criteria**
- Unit tests cover hit, miss, put, startup cleanup, and pruning
- Cache is not wired into restore/follow/publish yet
- Existing archival behavior remains unchanged

## PR 3: Reference Object Backend Publish Path

**Why third**
- Publish must be proven on one reference object backend before reads depend on it
- `object -> manifest -> latest` ordering is a correctness boundary
- Keeping this isolated limits blast radius if manifest publish semantics need adjustment

**Files:**
- Modify: `src/storage/storage_client.rs`
- Modify: `src/storage/operator.rs`
- Modify: `src/sync/replicate.rs`
- Test: `src/storage/storage_client.rs`
- Test: `src/sync/replicate.rs`

**Scope**
- Re-enable manifest-backed archival publish for `s3` as the reference object backend
- Keep `fs` archival publish working
- Enforce immutable object publish before manifest rewrite and `latest.json` publish last
- Keep the operational single-writer contract explicit for non-`fs`

**Merge criteria**
- Publish order is tested as `object -> manifest -> latest`
- `s3` no longer fails fast during manifest-backed publish setup
- Existing stream path remains untouched

## PR 4: Cache-First Restore and Follow

**Why fourth**
- Restore/follow become cheaper only after the cache and one remote publish path both exist
- This is the first high-risk integration PR, so it comes after the passive pieces are already proven
- It is still narrower than bringing in more backends

**Files:**
- Modify: `src/storage/storage_client.rs`
- Modify: `src/sync/restore.rs`
- Modify: `src/sync/restore/follow.rs`
- Test: `src/storage/storage_client.rs`
- Test: `src/sync/restore.rs`
- Test: `src/sync/restore/follow.rs`

**Scope**
- Resolve manifest-referenced objects through local cache first
- On miss, GET from the selected truth-source target and populate cache
- Verify fetched bytes against manifest checksum before admitting them into cache or using them
- Add explicit truth-source selection behavior for restore/follow
- Keep list-based discovery disabled

**Merge criteria**
- Cache hit avoids remote GET
- Cache miss performs remote GET and populates cache
- Checksum mismatch is a hard failure
- Restore/follow use only explicit target selection and manifest metadata

## PR 5: FS Dual Role and Local Path Safety

**Why fifth**
- `fs` as both cache layer and archival target is important, but path aliasing is easier to reason about after cache-first reads exist
- This isolates the sharp edge of “same local technology, different roles” away from the reference backend work

**Files:**
- Modify: `src/storage/storage_client.rs`
- Modify: `src/sync/replicate.rs`
- Modify: `src/config/config.rs`
- Test: `src/storage/storage_client.rs`
- Test: `src/config/config.rs`

**Scope**
- Keep `fs` as first-class archival publish backend
- Enforce that cache root and `fs` archival target root are distinct in this version
- Ensure publish and restore both behave correctly when both roles are configured

**Merge criteria**
- `fs` archival publish still works
- Local cache root and `fs` target root cannot silently alias
- Restore/follow continue to use cache-first resolution safely

## PR 6: Observability and Request-Cost Diagnostics

**Why sixth**
- Observability should land before widening backend scope
- It gives a verification boundary for the claim that cache-first restore reduces request cost
- It also makes later regressions easier to detect

**Files:**
- Modify: `src/storage/storage_client.rs`
- Modify: `src/sync/restore.rs`
- Modify: `src/sync/restore/follow.rs`
- Modify: `tests/integration_test.py`
- Modify: `tests/release_e2e.py`

**Scope**
- Add cache hit/miss diagnostics
- Add request-cost counters for metadata/object requests
- Surface `LIST` usage and require `LIST 0` on the normal manifest restore path
- Extend integration output to show which path was used and how many remote requests were made

**Merge criteria**
- Integration output shows cache/request-cost diagnostics
- Normal manifest restore path shows `LIST 0`
- Diagnostics are stable enough to gate later parity work

## PR 7: S3 Reference Integration and CI Gate

**Why seventh**
- End-to-end validation of the reference object backend belongs after core mechanics and diagnostics exist
- This keeps CI churn out of early refactor PRs

**Files:**
- Modify: `tests/integration_test.py`
- Modify: `.github/workflows/fs_integration_test.yml`
- Add or modify: `.github/workflows/s3_integration_test.yml`
- Modify: `README.md`
- Modify: `docs/stream-replication.md`

**Scope**
- Add canonical `s3` integration validation for manifest-first, cache-first restore
- Keep `fs` as the fastest canonical local gate
- Update docs to describe `fs` dual role and `s3` reference status

**Merge criteria**
- `fs` and `s3` integration paths both pass
- CI shows request-cost diagnostics for the `s3` path
- Docs match the actual supported runtime surface

## PR 8: GCS and Azblob Parity

**Why last**
- These are deliberate parity extensions, not prerequisites for validating the architecture
- They should reuse the proven `s3` model rather than influencing the core shape

**Files:**
- Modify: `src/storage/operator.rs`
- Modify: `src/storage/storage_client.rs`
- Modify: `tests/integration_test.py`
- Add or modify: `.github/workflows/gcs_integration_test.yml`
- Add or modify: `.github/workflows/azblob_integration_test.yml`
- Modify: `README.md`

**Scope**
- Apply the same manifest-backed publish and cache-first restore rules to `gcs` and `azblob`
- Reuse the same single-writer operational contract
- Reuse the same checksum admission and request-cost diagnostics

**Merge criteria**
- `gcs` and `azblob` match `s3` behavior at the API and integration level
- No core restore/publish design changes are needed to support them

## Cross-PR Rules

- Do not reintroduce list-based restore discovery.
- Do not couple stream hot-path behavior to manifests.
- Do not implement multi-writer distributed fencing in this sequence.
- Do not merge backend parity work before `s3` reference path and diagnostics are proven.
- Keep `fs` usable as both cache technology and archival target, but require distinct roots in this version.

## Verification Ladder

After each PR:
- run targeted Rust tests for touched modules
- run `cargo test`

After PR 4 and later:
- run `tests/integration_test.py` against `fs`

After PR 7 and later:
- run `tests/integration_test.py` against `s3`
- confirm normal restore path reports `LIST 0`

After PR 8:
- run parity integration checks for `gcs` and `azblob`

## Review Note

This PR order was chosen after cross-checking the architecture plan against a risk-minimized sequencing pass. The main conclusion was to place config seams first, the cache as an isolated passive component second, one reference object backend before read-path integration, and backend parity last.
