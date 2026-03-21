# LTX Vs Manifest Benchmark Plan

**Goal:** Compare the current manifest archival format against an experimental LTX format before making runtime architecture changes.

**Architecture:** Keep the existing manifest archival path untouched and add an experimental, benchmark-oriented LTX slice beside it. Measure snapshot-level behavior first, then decide whether incremental LTX work is justified.

**Tech Stack:** Rust, `replited`, vendored `litetx`, Python benchmark scripts, SQLite integrity/digest checks.

---

### Task 1: Preserve intent and scope

**Files:**
- Create: `.codex/context/ACTIVE_TASK.md`
- Create: `docs/specs/2026-03-21-ltx-vs-manifest-design.md`
- Create: `docs/plans/2026-03-21-ltx-vs-manifest-benchmark-plan.md`

Verification:
- Design note clearly distinguishes metadata model vs object format.
- Plan keeps current manifest runtime as the stable path.

### Task 2: Add a minimal experimental LTX helper

**Files:**
- Modify: `Cargo.toml`
- Create: `vendor/litetx/**`
- Create: `src/experimental/mod.rs`
- Create: `src/experimental/ltx_compare.rs`
- Create: `src/bin/ltx_probe.rs`
- Test: `tests/ltx_compare_test.rs`

Goal:
- Encode a SQLite DB snapshot to LTX.
- Decode that LTX back into a SQLite DB.
- Report artifact size and elapsed time.

Verification:
- A failing round-trip test is added first.
- Round-trip preserves digest and passes `integrity_check`.

### Task 3: Add a side-by-side benchmark script

**Files:**
- Create: `tests/benchmarks/format_compare_bench.py`
- Modify: `tests/benchmarks/run_all_benchmarks.sh`

Goal:
- Compare current manifest snapshot path and experimental LTX snapshot path on the same dataset.

Metrics:
- encode time
- restore/decode time
- artifact size
- digest equality
- integrity-check result

Verification:
- Script emits JSON results.
- Both paths run from the same generated workload.

### Task 4: Record benchmark results and decision

**Files:**
- Create: `docs/benchmarks/2026-03-21-ltx-vs-manifest-results.md`

Goal:
- Summarize measured differences and recommend one of:
  - keep LTX experimental only
  - extend to incremental LTX experiment
  - drop LTX exploration

Verification:
- Results include command lines, machine assumptions, and exact metrics.
