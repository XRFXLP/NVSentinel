#!/usr/bin/env bash
set -euo pipefail

variant=${1:?variant name required}
priority=${2:?priority flag true/false required}
coalesce=${3:?coalescing flag true/false required}
nodes_file=${4:-/tmp/nvsentinel-target-nodes-matrix.txt}
events=${EVENTS:-50000}
duration=${DURATION:-2m}
node_order=${NODE_ORDER:-grouped}
namespace=${NAMESPACE:-nvsentinel}
workload_namespace=${WORKLOAD_NAMESPACE:-test-allow-completion}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

mongo_uri() {
  local ctx cert_dir
  ctx=$(kubectl config current-context)
  cert_dir="$HOME/.nvsentinel/certs/$ctx"
  mkdir -p "$cert_dir"
  kubectl get secret mongo-app-client-cert-secret -n "$namespace" -o jsonpath='{.data.ca\.crt}' | base64 -d > "$cert_dir/ca.crt"
  kubectl get secret mongo-app-client-cert-secret -n "$namespace" -o jsonpath='{.data.tls\.crt}' | base64 -d > "$cert_dir/tls.crt"
  kubectl get secret mongo-app-client-cert-secret -n "$namespace" -o jsonpath='{.data.tls\.key}' | base64 -d > "$cert_dir/tls.key"
  cat "$cert_dir/tls.crt" "$cert_dir/tls.key" > "$cert_dir/creds.pem"
  chmod 600 "$cert_dir"/*.crt "$cert_dir"/*.key "$cert_dir/creds.pem"
  echo "mongodb://localhost:37017/HealthEventsDatabase?directConnection=true&authMechanism=MONGODB-X509&authSource=\$external&tls=true&tlsCAFile=$cert_dir/ca.crt&tlsCertificateKeyFile=$cert_dir/creds.pem&tlsAllowInvalidHostnames=true"
}

with_mongo_pf() {
  local uri pf_pid
  kubectl port-forward -n "$namespace" pod/mongodb-0 37017:27017 >/tmp/nvsentinel-mongo-pf.log 2>&1 &
  pf_pid=$!
  trap '[[ -n "${pf_pid:-}" ]] && kill "$pf_pid" >/dev/null 2>&1 || true' RETURN
  sleep 2
  uri=$(mongo_uri)
  mongosh "$uri" --quiet --eval "$1"
  kill "$pf_pid" >/dev/null 2>&1 || true
  pf_pid=""
}

reset_state() {
  # Stop processors first; otherwise they can immediately recreate resume tokens
  # between DB cleanup and the clean-state assertion.
  kubectl scale deploy/fault-quarantine -n "$namespace" --replicas=0 >/dev/null
  kubectl scale deploy/node-drainer -n "$namespace" --replicas=0 >/dev/null
  kubectl rollout status deploy/fault-quarantine -n "$namespace" --timeout=180s >/dev/null || true
  kubectl rollout status deploy/node-drainer -n "$namespace" --timeout=180s >/dev/null || true

  kubectl delete pod -n "$namespace" \
    event-generator-baseline10k event-generator-priority10k event-generator-coalesce10k event-generator-combined10k \
    --ignore-not-found >/dev/null 2>&1 || true
  kubectl delete pod -n "$workload_namespace" -l app=allow-completion-matrix --ignore-not-found >/dev/null 2>&1 || true

  while read -r node; do
    [[ -z "$node" ]] && continue
    kubectl label node "$node" dgxc.nvidia.com/nvsentinel-state- --overwrite >/dev/null 2>&1 || true
    kubectl annotate node "$node" quarantineHealthEvent- --overwrite >/dev/null 2>&1 || true
    kubectl uncordon "$node" >/dev/null 2>&1 || true
  done < "$nodes_file"

  with_mongo_pf 'db.HealthEvents.deleteMany({}); db.ResumeTokens.deleteMany({}); printjson({healthEvents: db.HealthEvents.countDocuments({}), resumeTokens: db.ResumeTokens.countDocuments({})})'
  kubectl patch configmap -n "$namespace" circuit-breaker --type=merge \
    -p '{"data":{"status":"CLOSED","cursor":"CREATE"}}' >/dev/null
}

assert_clean_state() {
  local bad_nodes db_counts cb_status cb_cursor
  bad_nodes=$(kubectl get nodes $(cat "$nodes_file") -o jsonpath='{range .items[*]}{.metadata.name}{" state="}{.metadata.labels.dgxc\.nvidia\.com/nvsentinel-state}{" annotation="}{.metadata.annotations.quarantineHealthEvent}{" unschedulable="}{.spec.unschedulable}{"\n"}{end}' \
    | awk '$2!="state=" || $3!="annotation=" || $4=="unschedulable=true" {print}')
  if [[ -n "$bad_nodes" ]]; then
    echo "target node state is not clean:" >&2
    echo "$bad_nodes" >&2
    exit 1
  fi

  db_counts=$(with_mongo_pf 'print(db.HealthEvents.countDocuments({}))' | tail -n 1)
  if [[ "$db_counts" != "0" ]]; then
    echo "health-event datastore is not clean: HealthEvents=$db_counts" >&2
    exit 1
  fi

  cb_status=$(kubectl get configmap -n "$namespace" circuit-breaker -o jsonpath='{.data.status}')
  cb_cursor=$(kubectl get configmap -n "$namespace" circuit-breaker -o jsonpath='{.data.cursor}')
  if [[ "$cb_status" != "CLOSED" || "$cb_cursor" != "CREATE" ]]; then
    echo "circuit breaker is not clean: status=$cb_status cursor=$cb_cursor" >&2
    exit 1
  fi
}

create_workload() {
  kubectl create namespace "$workload_namespace" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  while read -r node; do
    [[ -z "$node" ]] && continue
    kubectl run -n "$workload_namespace" "allow-completion-${node}" \
      --image=public.ecr.aws/docker/library/busybox:latest \
      --restart=Never \
      --overrides="{\"spec\":{\"nodeName\":\"${node}\",\"terminationGracePeriodSeconds\":30,\"containers\":[{\"name\":\"sleeper\",\"image\":\"public.ecr.aws/docker/library/busybox:latest\",\"command\":[\"sh\",\"-c\",\"sleep 3600\"],\"resources\":{\"requests\":{\"cpu\":\"5m\",\"memory\":\"16Mi\"},\"limits\":{\"cpu\":\"20m\",\"memory\":\"32Mi\"}}}],\"metadata\":{\"labels\":{\"app\":\"allow-completion-matrix\"}}}}" \
      >/dev/null 2>&1 || true
    kubectl label -n "$workload_namespace" pod "allow-completion-${node}" app=allow-completion-matrix --overwrite >/dev/null 2>&1 || true
  done < "$nodes_file"
  kubectl wait -n "$workload_namespace" --for=condition=Ready pod -l app=allow-completion-matrix --timeout=180s >/dev/null
}

configure_variant() {
  python3 - <<PY | kubectl apply -f - >/dev/null
import json, subprocess, re
cm=json.loads(subprocess.check_output(['kubectl','get','configmap','-n','$namespace','node-drainer','-o','json']))
config=cm['data']['config.toml']
if 'drainCoalescingEnabled = ' in config:
    config=re.sub(r'drainCoalescingEnabled = (true|false)', 'drainCoalescingEnabled = $coalesce', config)
else:
    config += '\\ndrainCoalescingEnabled = $coalesce\\n'
cm['data']['config.toml']=config
print(json.dumps(cm))
PY
  kubectl set env deploy/node-drainer -n "$namespace" NODE_DRAINER_PRIORITY_QUEUE_ENABLED="$priority" >/dev/null
  kubectl scale deploy/fault-quarantine -n "$namespace" --replicas=1 >/dev/null
  kubectl scale deploy/node-drainer -n "$namespace" --replicas=1 >/dev/null
  kubectl rollout status deploy/fault-quarantine -n "$namespace" --timeout=180s >/dev/null
  kubectl rollout status deploy/node-drainer -n "$namespace" --timeout=180s >/dev/null
}

run_generator() {
  kubectl create configmap -n "$namespace" event-generator-targets --from-file=nodes.txt="$nodes_file" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl delete pod -n "$namespace" "event-generator-${variant}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl run -n "$namespace" "event-generator-${variant}" \
    --image=localhost:5001/nvsentinel-event-generator:local \
    --restart=Never \
    --overrides="{\"spec\":{\"nodeName\":\"nvsentinel-worker\",\"containers\":[{\"name\":\"event-generator-${variant}\",\"image\":\"localhost:5001/nvsentinel-event-generator:local\",\"imagePullPolicy\":\"Always\",\"args\":[\"-socket=/var/run/nvsentinel/nvsentinel.sock\",\"-target-nodes-file=/config/nodes.txt\",\"-total-events=${events}\",\"-duration=${duration}\",\"-fatal-only=true\",\"-error-code=163\",\"-fatal-action=RESTART_BM\",\"-node-order=${node_order}\"],\"volumeMounts\":[{\"name\":\"nvsentinel-socket\",\"mountPath\":\"/var/run\"},{\"name\":\"targets\",\"mountPath\":\"/config\"}]}],\"volumes\":[{\"name\":\"nvsentinel-socket\",\"hostPath\":{\"path\":\"/var/run\",\"type\":\"Directory\"}},{\"name\":\"targets\",\"configMap\":{\"name\":\"event-generator-targets\"}}]}}" \
    >/dev/null
  kubectl wait -n "$namespace" --for=jsonpath='{.status.phase}'=Succeeded "pod/event-generator-${variant}" --timeout=900s >/dev/null
}

capture_summary() {
  local start_ts=$1 end_ts=$2 pf_log pf_pid port metrics fq_img dr_img
  pf_log=/tmp/node-drainer-pf-${variant}.log
  kubectl port-forward -n "$namespace" deploy/node-drainer 0:2112 > "$pf_log" 2>&1 &
  pf_pid=$!
  sleep 2
  port=$(grep -oE '127.0.0.1:[0-9]+' "$pf_log" | head -1 | cut -d: -f2)
  metrics=$(curl -s "http://localhost:${port}/metrics" || true)
  kill "$pf_pid" >/dev/null 2>&1 || true
  fq_img=$(kubectl get deploy -n "$namespace" fault-quarantine -o jsonpath='{.spec.template.spec.containers[0].image}')
  dr_img=$(kubectl get deploy -n "$namespace" node-drainer -o jsonpath='{.spec.template.spec.containers[0].image}')

  printf '\n=== %s ===\n' "$variant"
  printf 'priority=%s coalescing=%s events=%s nodes=%s duration=%s node_order=%s wall_seconds=%s\n' \
    "$priority" "$coalesce" "$events" "$(wc -l < "$nodes_file")" "$duration" "$node_order" "$((end_ts-start_ts))"
  printf 'images: fault-quarantine=%s node-drainer=%s\n' "$fq_img" "$dr_img"
  printf 'generator_tail:\n'
  kubectl logs -n "$namespace" "event-generator-${variant}" --tail=5
  printf 'node_states:\n'
  kubectl get nodes $(cat "$nodes_file") -L dgxc.nvidia.com/nvsentinel-state --no-headers | awk '{c[$6]++} END {for (k in c) print k,c[k]}' | sort
  printf 'metrics:\n'
  printf '%s\n' "$metrics" | egrep 'node_drainer_queue_depth|node_drainer_events_received_total|node_drainer_queue_requeues_total|node_drainer_node_time_to_draining_seconds_count|node_drainer_node_time_to_draining_seconds_sum|node_drainer_event_handling_duration_seconds_count|node_drainer_event_handling_duration_seconds_sum|node_drainer_overlap' || true
}

require_cmd kubectl
require_cmd mongosh
[[ -s "$nodes_file" ]] || { echo "nodes file is empty: $nodes_file" >&2; exit 1; }

reset_state
assert_clean_state
create_workload
configure_variant

start_ts=$(date -u +%s)
run_generator
for _ in $(seq 1 60); do
  draining=$(kubectl get nodes $(cat "$nodes_file") -L dgxc.nvidia.com/nvsentinel-state --no-headers | awk '$6=="draining"{c++} END{print c+0}')
  [[ "$draining" == "$(wc -l < "$nodes_file")" ]] && break
  sleep 5
done
end_ts=$(date -u +%s)
capture_summary "$start_ts" "$end_ts"
