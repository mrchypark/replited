# replited v0.3.5

Release date: 2026-02-09

## Highlights

- Docs-only release: make documentation consistent with current stream replication implementation and config parsing.

## What's Changed

### Stream Replication Docs Consistency

- Update stream replication guide to reflect direct snapshot streaming (`stream_snapshot_v2` with `zstd`) and WAL streaming (`stream_wal_v2`).
- Clarify DB identity rules and recommended `remote_db_name` usage on replicas.

### Config Reference + Samples

- Document canonical `params.type` values as lowercase (with aliases where applicable).
- Fix FTP config key reference (`params.username`) and update `etc/sample.toml`.
- Correct documented `max_concurrent_snapshots` default to `5`.

## Verification

- `cargo test`

