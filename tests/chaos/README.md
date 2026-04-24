# vind PocketBase Chaos Test

This harness runs a real PocketBase primary and three `replica-sidecar` replicas inside a `vind` cluster.

What it checks:
- writes continue against the PocketBase primary
- reads through the shared replica Service stay healthy during repeated failures
- each replica converges after chaos stops

Entry point:

```bash
bash tests/chaos/run_pocketbase_vind_chaos.sh
```

Useful environment variables:
- `DURATION_SECS=180`
- `CHAOS_INTERVAL_SECS=15`
- `NODE_DOWN_SECS=8`
- `WRITE_INTERVAL_SECS=0.1`
- `READ_INTERVAL_SECS=0.05`
- `FINAL_SYNC_TIMEOUT_SECS=180`
- `KEEP_CLUSTER=1`
- `SUDO_VCLUSTER=1`
- `CHAOS_MODE=pod`
- `CHAOS_MODE=node`

Notes:
- The harness uses `vind` through `vcluster use driver docker`.
- It first tries multi-node `vind`. If that fails on the local runtime, it falls back to single-node `vind` and uses replica pod deletion chaos instead.
- On some local setups, multi-node `vind` creation requires elevated privileges. If you specifically want worker-node restart chaos, retry with `SUDO_VCLUSTER=1`.
- The chaos client runs inside the cluster so replica reads go through normal Kubernetes Service load balancing, not host-side port forwarding.
