#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_DIR="$ROOT_DIR/tests/chaos"
VIND_DIR="$TEST_DIR/vind"
POCKETBASE_DIR="$TEST_DIR/pocketbase"
NAMESPACE="${NAMESPACE:-replited-chaos}"
CLUSTER_NAME="${CLUSTER_NAME:-replited-chaos}"
REPLITED_IMAGE="${REPLITED_IMAGE:-replited-vind-chaos:local}"
POCKETBASE_VERSION="${POCKETBASE_VERSION:-0.36.9}"
ADMIN_EMAIL="${POCKETBASE_ADMIN_EMAIL:-test@example.com}"
ADMIN_PASS="${POCKETBASE_ADMIN_PASS:-password123456}"
DURATION_SECS="${DURATION_SECS:-180}"
CHAOS_INTERVAL_SECS="${CHAOS_INTERVAL_SECS:-15}"
NODE_DOWN_SECS="${NODE_DOWN_SECS:-8}"
WRITE_INTERVAL_SECS="${WRITE_INTERVAL_SECS:-0.1}"
READ_INTERVAL_SECS="${READ_INTERVAL_SECS:-0.05}"
FINAL_SYNC_TIMEOUT_SECS="${FINAL_SYNC_TIMEOUT_SECS:-180}"
PAYLOAD_SIZE="${PAYLOAD_SIZE:-1024}"
KEEP_CLUSTER="${KEEP_CLUSTER:-0}"
SUDO_VCLUSTER="${SUDO_VCLUSTER:-0}"
VCLUSTER_BIN="${VCLUSTER_BIN:-vcluster}"
CHAOS_MODE="${CHAOS_MODE:-auto}"
CHAOS_JOB_NAME="pocketbase-chaos-client"

if [[ "$SUDO_VCLUSTER" == "1" ]]; then
  VCLUSTER_CMD=(sudo -E "$VCLUSTER_BIN")
else
  VCLUSTER_CMD=("$VCLUSTER_BIN")
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

cleanup() {
  set +e
  jobs -pr | xargs -r kill >/dev/null 2>&1 || true
  kubectl -n "$NAMESPACE" delete job "$CHAOS_JOB_NAME" --ignore-not-found >/dev/null 2>&1 || true
  if [[ "$KEEP_CLUSTER" != "1" ]]; then
    "${VCLUSTER_CMD[@]}" delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT INT TERM

render_manifest() {
  local rendered="$1"
  sed "s|__REPLITED_IMAGE__|$REPLITED_IMAGE|g" \
    "$POCKETBASE_DIR/app-template.yaml" > "$rendered"
}

build_replited_image() {
  docker build -t "$REPLITED_IMAGE" -f "$ROOT_DIR/Dockerfile.dev" "$ROOT_DIR"
}

import_image_into_vind_nodes() {
  local node
  while IFS= read -r node; do
    [[ -n "$node" ]] || continue
    docker save "$REPLITED_IMAGE" | docker exec -i "$node" ctr -n=k8s.io images import - >/dev/null
  done < <(docker ps --format '{{.Names}}' | grep "^vcluster\." | grep "\.${CLUSTER_NAME}$" || true)
}

wait_for_rollout() {
  echo "Waiting for primary rollout..."
  kubectl -n "$NAMESPACE" rollout status statefulset/primary-pocketbase --timeout=180s
  echo "Waiting for replica rollout..."
  kubectl -n "$NAMESPACE" rollout status statefulset/replica-pocketbase --timeout=300s
}

bootstrap_primary_admin() {
  kubectl -n "$NAMESPACE" exec pod/primary-pocketbase-0 -c pocketbase -- \
    /usr/local/bin/pocketbase superuser upsert "$ADMIN_EMAIL" "$ADMIN_PASS" --dir /pb/primary >/dev/null
}

apply_chaos_client() {
  kubectl -n "$NAMESPACE" create configmap pocketbase-chaos-client \
    --from-file=pocketbase_vind_client.py="$TEST_DIR/pocketbase_vind_client.py" \
    --dry-run=client -o yaml | kubectl apply -f -

  cat <<EOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: $CHAOS_JOB_NAME
  namespace: $NAMESPACE
spec:
  backoffLimit: 0
  template:
    metadata:
      labels:
        app: $CHAOS_JOB_NAME
    spec:
      restartPolicy: Never
      tolerations:
        - key: node-role.kubernetes.io/control-plane
          operator: Exists
          effect: NoSchedule
        - key: node-role.kubernetes.io/master
          operator: Exists
          effect: NoSchedule
      containers:
        - name: client
          image: python:3.12-slim
          imagePullPolicy: IfNotPresent
          command:
            - python
            - /scripts/pocketbase_vind_client.py
          env:
            - name: PRIMARY_URL
              value: http://primary-api.$NAMESPACE.svc.cluster.local:8090
            - name: REPLICA_SERVICE_URL
              value: http://replica-read.$NAMESPACE.svc.cluster.local:8090
            - name: REPLICA_POD_TEMPLATE
              value: http://replica-pocketbase-{index}.replica-headless.$NAMESPACE.svc.cluster.local:8090
            - name: REPLICA_COUNT
              value: "3"
            - name: POCKETBASE_ADMIN_EMAIL
              value: $ADMIN_EMAIL
            - name: POCKETBASE_ADMIN_PASS
              value: $ADMIN_PASS
            - name: DURATION_SECS
              value: "$DURATION_SECS"
            - name: WRITE_INTERVAL_SECS
              value: "$WRITE_INTERVAL_SECS"
            - name: READ_INTERVAL_SECS
              value: "$READ_INTERVAL_SECS"
            - name: FINAL_SYNC_TIMEOUT_SECS
              value: "$FINAL_SYNC_TIMEOUT_SECS"
            - name: PAYLOAD_SIZE
              value: "$PAYLOAD_SIZE"
          volumeMounts:
            - name: scripts
              mountPath: /scripts
      volumes:
        - name: scripts
          configMap:
            name: pocketbase-chaos-client
EOF
}

job_done() {
  local status
  status="$(kubectl -n "$NAMESPACE" get job "$CHAOS_JOB_NAME" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
  [[ "$status" == "True" ]]
}

job_failed() {
  local status
  status="$(kubectl -n "$NAMESPACE" get job "$CHAOS_JOB_NAME" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}' 2>/dev/null || true)"
  [[ "$status" == "True" ]]
}

wait_for_job_result() {
  local deadline
  deadline=$(( $(date +%s) + DURATION_SECS + FINAL_SYNC_TIMEOUT_SECS + 180 ))
  while (( $(date +%s) < deadline )); do
    if job_done; then
      return 0
    fi
    if job_failed; then
      return 1
    fi
    sleep 2
  done
  return 1
}

chaos_loop() {
  local events=0
  mapfile -t workers < <(docker ps --format '{{.Names}}' | grep "^vcluster\.worker-" | grep "\.${CLUSTER_NAME}$")
  mapfile -t replicas < <(kubectl -n "$NAMESPACE" get pods -l app=replica-pocketbase -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')

  local mode="$CHAOS_MODE"
  if [[ "$mode" == "auto" ]]; then
    if [[ "${#workers[@]}" -ge 3 ]]; then
      mode="node"
    else
      mode="pod"
    fi
  fi

  while true; do
    if job_done || job_failed; then
      break
    fi

    events=$((events + 1))

    if [[ "$mode" == "node" ]]; then
      local idx node
      idx=$(( RANDOM % ${#workers[@]} ))
      node="${workers[$idx]}"
      echo "[CHAOS $events] restarting worker node $node"
      docker stop "$node" >/dev/null
      sleep "$NODE_DOWN_SECS"
      docker start "$node" >/dev/null
    else
      if [[ "${#replicas[@]}" -eq 0 ]]; then
        mapfile -t replicas < <(kubectl -n "$NAMESPACE" get pods -l app=replica-pocketbase -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
      fi
      local idx pod
      idx=$(( RANDOM % ${#replicas[@]} ))
      pod="${replicas[$idx]}"
      echo "[CHAOS $events] deleting replica pod $pod"
      kubectl -n "$NAMESPACE" delete pod "$pod" --wait=false >/dev/null
      sleep "$NODE_DOWN_SECS"
      mapfile -t replicas < <(kubectl -n "$NAMESPACE" get pods -l app=replica-pocketbase -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
    fi

    sleep "$CHAOS_INTERVAL_SECS"
  done
}

main() {
  require_cmd cargo
  require_cmd docker
  require_cmd kubectl
  require_cmd "$VCLUSTER_BIN"

  build_replited_image

  echo "Switching vcluster driver to docker..."
  "${VCLUSTER_CMD[@]}" use driver docker >/dev/null
  echo "Creating vind cluster $CLUSTER_NAME..."
  if [[ "$CHAOS_MODE" == "pod" ]]; then
    "${VCLUSTER_CMD[@]}" create "$CLUSTER_NAME" --upgrade
  elif ! "${VCLUSTER_CMD[@]}" create "$CLUSTER_NAME" --upgrade --values "$VIND_DIR/vcluster.yaml"; then
    echo "vind multi-node cluster creation failed; retrying single-node." >&2
    echo "If you specifically need worker-node chaos on this machine, retry with SUDO_VCLUSTER=1." >&2
    "${VCLUSTER_CMD[@]}" delete "$CLUSTER_NAME" >/dev/null 2>&1 || true
    "${VCLUSTER_CMD[@]}" create "$CLUSTER_NAME" --upgrade
  fi

  echo "Importing local replited image into vind nodes..."
  import_image_into_vind_nodes

  local rendered
  rendered="$(mktemp)"
  render_manifest "$rendered"
  echo "Applying PocketBase chaos manifests..."
  kubectl apply -f "$rendered" >/dev/null
  rm -f "$rendered"

  wait_for_rollout
  echo "Bootstrapping PocketBase primary superuser..."
  bootstrap_primary_admin
  echo "Creating in-cluster chaos client job..."
  apply_chaos_client

  chaos_loop &
  local chaos_pid=$!

  if ! wait_for_job_result; then
    echo "Chaos client job did not complete successfully." >&2
    kubectl -n "$NAMESPACE" logs "job/$CHAOS_JOB_NAME" || true
    return 1
  fi
  wait "$chaos_pid" || true
  kubectl -n "$NAMESPACE" logs "job/$CHAOS_JOB_NAME"
}

main "$@"
