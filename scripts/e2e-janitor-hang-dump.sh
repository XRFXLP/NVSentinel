#!/usr/bin/env bash
#
# Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [experiment] Collects the state of janitor and janitor-provider after a failed E2E run, to find
# where a reboot call hangs under kind 0.33. For each pod it records the TCP connections in the pod
# network namespace, then sends SIGQUIT so the Go runtime writes every goroutine stack to the log.
# It also records the netfilter state and kindnetd logs of the node that runs the pod.
# Usage: ./e2e-janitor-hang-dump.sh [output_dir]

set -uo pipefail

OUT="${1:-/tmp/debug-artifacts/hang}"
NS=nvsentinel
mkdir -p "$OUT"

selector() {
  local labels
  labels=$(kubectl -n "$NS" get deploy "$1" \
    -o go-template="{{range \$k, \$v := .spec.selector.matchLabels}}{{\$k}}={{\$v}},{{end}}")
  echo "${labels%,}"
}

dump_node() {
  local node=$1 dir="$OUT/node-$1"
  [ -d "$dir" ] && return
  mkdir -p "$dir"
  docker exec "$node" nft list ruleset > "$dir/nft-ruleset.txt" 2>&1
  docker exec "$node" iptables-save > "$dir/iptables-save.txt" 2>&1
  docker exec "$node" sh -c 'conntrack -L 2>&1 || cat /proc/net/nf_conntrack 2>&1' > "$dir/conntrack.txt"
  docker exec "$node" ip route > "$dir/ip-route.txt" 2>&1
  local kindnet
  kindnet=$(kubectl -n kube-system get pods -l app=kindnet --field-selector "spec.nodeName=$node" -o name | head -n1)
  if [ -n "$kindnet" ]; then
    kubectl -n kube-system logs "$kindnet" --timestamps > "$dir/kindnet.log" 2>&1
    kubectl -n kube-system logs "$kindnet" --timestamps --previous > "$dir/kindnet-previous.log" 2>&1
    kubectl -n kube-system get "$kindnet" -o yaml > "$dir/kindnet-pod.yaml" 2>&1
  fi
}

# Records a pod and its node, and prints "node pid" for the signal step.
inspect_pod() {
  local deploy=$1 container=$2 labels pod="" node cid pid dir
  labels=$(selector "$deploy")
  [ -n "$labels" ] && pod=$(kubectl -n "$NS" get pods -l "$labels" -o name | head -n1)
  if [ -z "$pod" ]; then
    echo "no pod for $deploy" > "$OUT/$deploy-missing.txt"
    return
  fi
  dir="$OUT/$deploy"
  mkdir -p "$dir"

  node=$(kubectl -n "$NS" get "$pod" -o jsonpath='{.spec.nodeName}')
  cid=$(kubectl -n "$NS" get "$pod" -o jsonpath="{.status.containerStatuses[?(@.name==\"$container\")].containerID}")
  cid=${cid#containerd://}
  pid=$(docker exec "$node" crictl inspect --output go-template --template '{{.info.pid}}' "$cid")
  echo "pod=$pod node=$node container=$cid pid=$pid" > "$dir/target.txt"
  kubectl -n "$NS" get "$pod" -o jsonpath="{.status.containerStatuses[?(@.name==\"$container\")].restartCount}" \
    > "$dir/restarts-before.txt"
  kubectl -n "$NS" get "$pod" -o yaml > "$dir/pod.yaml" 2>&1
  kubectl -n "$NS" logs "$pod" -c "$container" --timestamps > "$dir/current.log" 2>&1
  docker exec "$node" nsenter -t "$pid" -n ss -tniop > "$dir/ss.txt" 2>&1
  dump_node "$node"
  echo "$node $pid"
}

# Reads the goroutine dump that SIGQUIT wrote to the log of the container that ended.
collect_goroutines() {
  local deploy=$1 container=$2 labels pod="" restarts before
  labels=$(selector "$deploy")
  [ -n "$labels" ] && pod=$(kubectl -n "$NS" get pods -l "$labels" -o name | head -n1)
  [ -z "$pod" ] && return
  before=$(cat "$OUT/$deploy/restarts-before.txt")
  for _ in $(seq 1 30); do
    restarts=$(kubectl -n "$NS" get "$pod" -o jsonpath="{.status.containerStatuses[?(@.name==\"$container\")].restartCount}")
    [ "${restarts:-0}" -gt "${before:-0}" ] && break
    sleep 2
  done
  kubectl -n "$NS" logs "$pod" -c "$container" --previous > "$OUT/$deploy/goroutines.log" 2>&1
}

kubectl -n "$NS" get rebootnodes -o yaml > "$OUT/rebootnodes.yaml" 2>&1
kubectl -n "$NS" get leases -o yaml > "$OUT/leases.yaml" 2>&1
provider=$(inspect_pod janitor-provider janitor-provider | tail -n1)
janitor=$(inspect_pod janitor janitor | tail -n1)

# Stop both processes before either ends: the end of one closes its connections, which could
# unblock the other before it writes its stacks. SIGQUIT stays pending until SIGCONT.
for target in "$provider" "$janitor"; do
  [ -n "$target" ] && docker exec "${target% *}" kill -STOP "${target#* }"
done
for target in "$provider" "$janitor"; do
  [ -n "$target" ] && docker exec "${target% *}" kill -QUIT "${target#* }"
done
for target in "$provider" "$janitor"; do
  [ -n "$target" ] && docker exec "${target% *}" kill -CONT "${target#* }"
done

collect_goroutines janitor-provider janitor-provider
collect_goroutines janitor janitor
echo "hang dump written to $OUT"
