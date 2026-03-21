# 2026-03-21 LTX Vs Manifest Snapshot Benchmark

## Scope

This is a snapshot-only comparison.

Measured paths:

- `manifest`: current `replited`-style compressed SQLite snapshot
- `ltx`: experimental LTX snapshot round-trip via `vendor/litetx`

Not measured yet:

- incremental LTX generation
- end-to-end archival publish/restore in the main `replited` runtime
- request-cost differences at the object-store level

## Command

```bash
cargo build --release --bin ltx_probe
python3 tests/benchmarks/format_compare_bench.py -n 2000 -p 4096 -o tests/benchmarks/results/format_compare_results.json
```

## Results

### Repeat payload (`b"x" * 4096`)

- `manifest`
  - encode: `0.0121s`
  - decode: `0.0118s`
  - artifact bytes: `15864`
  - artifact ratio vs DB: `0.0017`
- `ltx`
  - encode: `0.0493s`
  - decode: `0.0304s`
  - artifact bytes: `75070`
  - artifact ratio vs DB: `0.0081`

### Random payload (`4096` random bytes per row)

- `manifest`
  - encode: `0.0198s`
  - decode: `0.0132s`
  - artifact bytes: `8227719`
  - artifact ratio vs DB: `0.8920`
- `ltx`
  - encode: `0.0563s`
  - decode: `0.0348s`
  - artifact bytes: `8292316`
  - artifact ratio vs DB: `0.8990`

## Correctness

Both formats passed:

- digest equality against source DB
- `PRAGMA integrity_check`

## Interpretation

Current evidence does not justify replacing the current manifest snapshot format with LTX.

Reasons:

- On highly compressible data, the current manifest snapshot is much smaller and faster.
- On random payloads, LTX does not recover enough in size or speed to justify added complexity.
- The experiment did not expose a clear snapshot-level advantage for LTX in `replited`'s current environment.

## Current Recommendation

Keep LTX experimental.

Good next steps:

1. If interoperability with Litestream matters, keep `vendor/litetx` as a probe/import-tool foundation.
2. Do not wire LTX into `replicate` / `restore` hot paths yet.
3. Only consider an incremental LTX experiment if there is a concrete interoperability or compaction requirement that the current manifest path cannot satisfy.
