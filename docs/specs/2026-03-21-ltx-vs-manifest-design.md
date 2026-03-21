# LTX vs Manifest Archival Design

## Goal

Decide whether `replited` should stay with its current manifest-first archival format,
adopt LTX as an archival object format, or support LTX only as an experimental or
interop-oriented capability.

## Current State

`replited` currently uses:

- manifest metadata in `latest.json` + generation manifest JSON
- a compressed SQLite snapshot object as the base image
- compressed raw WAL pack objects keyed by generation/index/offset
- restore planning based on exact generation/index/offset continuity
- cache-first reads through `StorageClient`

This design is already aligned with:

- explicit truth-source selection
- backend-neutral object access through `opendal`
- exact resume semantics from local WAL length and manifest offsets
- low-discovery restore request cost

## Important Clarification

`LTX` and `manifest` are not pure opposites.

- `manifest` is primarily a metadata and restore-planning model
- `LTX` is primarily an object encoding format for snapshots/incrementals

The realistic choices are:

1. Keep current manifest metadata and current raw snapshot/WAL objects.
2. Keep current manifest metadata but store snapshot/incremental objects as LTX.
3. Move toward a walrust/litestream-style runtime with TXID/LTX semantics and object listing.

Only option 2 is plausibly low-risk enough to explore inside `replited`.

## Comparison

### Current manifest + raw snapshot/WAL packs

Strengths:

- Matches the existing `generation/index/offset` model directly.
- Restore planning is simple and exact.
- Object semantics are close to SQLite's real snapshot/WAL outputs.
- Backend neutrality is already solved through `StorageClient`.
- Cache-first restore and request-cost instrumentation already exist.
- Easier to reason about resume and partial-progress recovery.

Weaknesses:

- Raw WAL packs are `replited`-specific and not interoperable with Litestream tooling.
- Integrity is object-level plus SQLite integrity-check, not a transaction-chain format.
- Compaction and migration to/from other SQLite archival ecosystems are weaker.
- Restore still materializes native WAL files rather than applying logical page deltas.

### LTX object format

Strengths:

- Mature snapshot/incremental page-delta format with checksum-chain semantics.
- Stronger interop story with Litestream-style tooling.
- Good fit for future migration/import/export tooling.
- Potentially smaller or more compaction-friendly incremental artifacts.

Weaknesses:

- Requires a TXID/checksum-chain model that `replited` does not currently expose.
- Does not naturally match `generation/index/offset` restore progress.
- Incremental LTX creation is harder than persisting raw WAL bytes.
- Runtime adoption would touch replicate, restore, manifest schema, cache semantics, and tests.
- walrust's broader runtime is S3/listing/TXID oriented and does not fit `replited`'s current architecture.

## Recommendation

Do not replace the current manifest archival runtime with LTX now.

Recommended path:

1. Keep the current manifest archival runtime as the product path.
2. Add an experimental LTX comparison slice outside the main archival hot path.
3. Benchmark first at the format level:
   - snapshot encode time
   - snapshot decode/restore time
   - artifact size
   - correctness via digest/integrity
4. Only if snapshot-level results are promising, add a second experiment for incremental LTX.

## Why This Direction

- It preserves the working architecture and avoids cross-cutting churn.
- It gives us data before changing the hot path.
- It keeps the door open for Litestream interop without prematurely rewriting the runtime.
- It isolates the hardest unknown: whether LTX brings enough practical benefit for `replited` workloads.

## Benchmark Criteria

The first benchmark should compare:

- current compressed snapshot artifact size
- current restore time for the base snapshot path
- LTX snapshot encode time
- LTX snapshot decode time
- LTX snapshot artifact size
- restored digest equality
- integrity-check pass/fail

The second benchmark, only if phase 1 is promising, should compare:

- incremental artifact size
- end-to-end archival catch-up time
- restore catch-up time
- CPU cost of encode/apply
- failure/recovery behavior under partial progress

## Decision Threshold

LTX should move beyond experimental scope only if it shows a meaningful win in at least one of:

- restore wall-clock time
- artifact size
- operational interoperability

without introducing unacceptable losses in:

- correctness confidence
- resume simplicity
- backend neutrality
- implementation complexity
