# Managed Reader Generation Handoff

## Problem

The current `replica-sidecar --exec` mode supervises a single reader process.
When the replica needs to apply WAL bytes, refresh checkpoints, rewind WAL, or
restore a new snapshot, the sidecar stops that reader, mutates the same SQLite
file set, and starts the reader again.

That model is not stable enough for PocketBase/latest-state on Kubernetes:

- a long-lived reader can keep stale SQLite WAL/SHM state;
- app readiness drops whenever the sidecar restarts the child;
- avoiding restarts lets the app stay available but can expose malformed or
  stale database state when the sidecar mutates files underneath it.

The required operational target is stricter: with three read replicas, each
reader API must stay available and eventually follow writer flushes with fresh
records, without serving a half-applied generation.

## Design

Introduce a managed-reader proxy mode instead of mutating the database files
used by the active child.

### Runtime Ownership

- The sidecar owns the externally visible readiness/listen port.
- The application child binds only to a private loopback port chosen by the
  sidecar.
- The proxy forwards traffic to the active child.
- Kubernetes readiness is tied to the proxy and active-child health, not to a
  staging child or WAL apply section.

### Generation Directories

Each managed child uses an immutable generation directory:

```text
managed-root/
  active -> generations/<generation-id>/
  generations/
    <generation-id>/
      data.db
      data.db-wal
      data.db-shm
      auxiliary.db
```

The sidecar never writes `data.db`, `data.db-wal`, or `data.db-shm` inside the
currently active directory. WAL streaming, snapshot restore, WAL rewind, SHM
refresh, and checkpoint refresh happen only in a staging directory.

### LSN Separation

Track two distinct positions:

- `applied_lsn`: latest WAL position durably applied to staging storage.
- `served_lsn`: latest WAL position proven visible through the active reader
  API after handoff.

Replica ACKs to the primary must not advance beyond the point this replica can
recover from. Serving freshness checks must use `served_lsn`, not
`applied_lsn`.

### Handoff

The sidecar switches active traffic only after all gates pass:

1. staging DB files are complete and sidecar-owned;
2. SQLite `quick_check` passes;
3. WAL/header/offset metadata matches the expected generation boundary;
4. the staging child starts on a private port;
5. the staging child health endpoint returns healthy;
6. a target record written after the last writer flush is observable through
   the staging child API;
7. the proxy atomically switches to the staging child;
8. `served_lsn` is persisted;
9. the old child is gracefully stopped and the old generation is garbage
   collected only after the proxy switch is confirmed.

Failure at any gate keeps the old active child serving traffic and marks the
staging generation failed.

## Compatibility

Keep current `--exec` behavior as the default. Add the generation-handoff mode
behind explicit flags so existing users do not get a proxy or command-template
change unexpectedly.

Candidate CLI shape:

```bash
replited --config replica.toml replica-sidecar \
  --exec-managed-proxy 0.0.0.0:8090 \
  --exec-child-template 'app serve --http 127.0.0.1:{port} --dir {dir}'
```

The template receives at least:

- `{port}`: private child HTTP port;
- `{dir}`: generation directory mounted as the app data directory;
- `{db}`: generation-local SQLite DB path when needed.

## Test Plan

Unit tests:

- proxy does not route traffic until an active child is installed;
- handoff keeps the old active target when staging health fails;
- handoff switches atomically only after the verification gate passes;
- `applied_lsn` and `served_lsn` advance independently;
- raw child command templates are never logged with secret-bearing values.

Integration tests:

- latest-state live test with three managed readers, concurrent writer flushes,
  and repeated handoffs;
- snapshot restore during active traffic;
- WAL index rotation and offset-zero restart;
- checkpoint blocked by an old reader;
- child SIGTERM grace path before SIGKILL.

Kubernetes rollout gates:

- canary one read pod first;
- verify fresh writer smoke is visible through the canary API;
- scale to three only after canary has stable proxy readiness;
- verify a new writer smoke reaches all three reader APIs;
- do not declare recovery until all three reader pods stay ready while serving
  fresh records after at least one post-scale writer flush.
