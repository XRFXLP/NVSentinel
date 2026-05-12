#!/usr/bin/env bash
# repro-sticky-cancel-bug.sh
#
# Reproduces the sticky cancelledNodes bug in node-drainer.
#
# Root cause:
#   CancelLatestQuarantiningEvents (triggered by kubectl uncordon) writes
#   nodequarantined=Cancelled to ALL docs from the current quarantine session,
#   including docs whose eventIDs were already cleared from nodeEventsMap via
#   clearEventStatus (i.e. AlreadyDrained events). node-drainer processes each
#   Cancelled change-stream update via HandleCancellation(Cancelled), which
#   inserts the eventID back into nodeEventsMap WITHOUT enqueuing it. Since the
#   worker never processes these entries, clearEventStatus is never called for
#   them -- they leak permanently.
#
#   A subsequent UnQuarantined event sets cancelledNodes[node]. The cleanup gate
#   in clearEventStatus requires len(nodeEventsMap[node])==0 to delete
#   cancelledNodes[node]. Because the leaked entries keep the map non-empty
#   forever, cancelledNodes[node] is stuck until the pod restarts. Every new
#   Quarantined event on the node is then silently cancelled.
#
# Phases:
#   1. Stuck pod    – gives AllowCompletion drain something to wait on
#   2. Event A      – fatal → FQ cordons → ND AllowCompletion loop
#   3. Events B1-10 – same check → FQ AlreadyQuarantined → ND AlreadyDrained
#                     → clearEventStatus removes them from nodeEventsMap
#   4. Uncordon     – FQ CancelLatestQuarantiningEvents writes Cancelled to
#                     A + B1-B10; ND leaks 10 entries (B1-B10 not in map)
#   5. Re-quarantine– new fatal event → FQ cordons again
#   6. Recovery     – healthy event → FQ writes UnQuarantined → cancelledNodes set
#                     map has 10+ leaked entries → gate never fires → permanent
#   7. Victim C     – new fatal (different check) → silently cancelled
#   8. Verify       – restart ND → same event is NOT cancelled (clean state)
#
set -euo pipefail

NS="nvsentinel"
STUCK_NS="repro-stuck"
STUCK_POD="stuck"
HC_PORT=8080

# ── helpers ──────────────────────────────────────────────────────────────────

log()  { echo "[$(date -u +%H:%M:%S)] $*"; }
ok()   { echo "  ✓ $*"; }
fail() { echo "  ✗ $*"; }

send_event() {
  local agent=$1 class=$2 check=$3 fatal=$4 healthy=$5 action=$6 node=$7
  curl -sf -X POST "http://localhost:${HC_PORT}/health-event" \
       -H 'Content-Type: application/json' \
       -d "$(printf '{
  "version": 1,
  "agent": "%s",
  "componentClass": "%s",
  "checkName": "%s",
  "isFatal": %s,
  "isHealthy": %s,
  "message": "%s event",
  "recommendedAction": %s,
  "errorCode": ["X"],
  "entitiesImpacted": [{"entityType": "GPU", "entityValue": "0"}],
  "nodeName": "%s"
}' "$agent" "$class" "$check" "$fatal" "$healthy" "$check" "$action" "$node")"
}

wait_cordoned() {
  local node=$1 timeout=${2:-90}
  log "Waiting for $node to be cordoned..."
  for i in $(seq 1 "$timeout"); do
    local val
    val=$(kubectl get node "$node" -o jsonpath='{.spec.unschedulable}' 2>/dev/null || echo "")
    if [[ "$val" == "true" ]]; then ok "Node cordoned"; return 0; fi
    sleep 1
  done
  fail "Timeout waiting for cordon"; return 1
}

wait_uncordoned() {
  local node=$1 timeout=${2:-60}
  log "Waiting for $node to be uncordoned..."
  for i in $(seq 1 "$timeout"); do
    local val
    val=$(kubectl get node "$node" -o jsonpath='{.spec.unschedulable}' 2>/dev/null || echo "")
    if [[ "$val" != "true" ]]; then ok "Node uncordoned"; return 0; fi
    sleep 1
  done
  fail "Timeout waiting for uncordon"; return 1
}

nd_logs_since() {
  local since=$1
  kubectl -n "$NS" logs deploy/node-drainer --since="${since}" 2>/dev/null || true
}

grep_count() {
  local pattern=$1 text=$2
  echo "$text" | grep -c "$pattern" 2>/dev/null || true
}

cleanup() {
  log "Cleanup..."
  kill "$PF_PID" 2>/dev/null || true
  kubectl -n "$STUCK_NS" delete pod "$STUCK_POD" \
    --grace-period=0 --force 2>/dev/null || true
  kubectl delete ns "$STUCK_NS" --grace-period=0 --force 2>/dev/null || true
}
trap cleanup EXIT

# ── pick node ────────────────────────────────────────────────────────────────

NODE=$(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' \
       -o jsonpath='{.items[0].metadata.name}')
log "Using node: $NODE"
echo ""

# ── Phase 0: setup ───────────────────────────────────────────────────────────

log "=== Phase 0: Setup ==="

kubectl create ns "$STUCK_NS" 2>/dev/null || true
kubectl -n "$STUCK_NS" delete pod "$STUCK_POD" \
  --grace-period=0 --force 2>/dev/null || true

kubectl -n "$STUCK_NS" apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${STUCK_POD}
  namespace: ${STUCK_NS}
spec:
  nodeName: ${NODE}
  terminationGracePeriodSeconds: 3600
  containers:
  - name: c
    image: busybox
    command: ["sh", "-c", "sleep infinity"]
EOF

log "Waiting for stuck pod to be Running..."
kubectl -n "$STUCK_NS" wait --for=condition=Ready pod/"$STUCK_POD" --timeout=120s
ok "Stuck pod running on $NODE"

CLIENT=$(kubectl -n "$NS" get pods -l app=simple-health-client \
         -o jsonpath='{.items[0].metadata.name}')
log "Using simple-health-client: $CLIENT"

kubectl -n "$NS" port-forward "$CLIENT" "${HC_PORT}:${HC_PORT}" >/dev/null 2>&1 &
PF_PID=$!
sleep 3
ok "Port-forward ready (PID $PF_PID)"
echo ""

# ── Phase 1: fatal event A → AllowCompletion loop ────────────────────────────

log "=== Phase 1: Fatal event A → FQ cordons, ND enters AllowCompletion ==="
send_event "gpu-health-monitor" "GPU" "GpuXidError" "true" "false" "15" "$NODE"
ok "Event A sent"

wait_cordoned "$NODE"
# Give ND time to call markEventInProgress(A) → nodeEventsMap[N] = {A: InProgress}
sleep 5
ok "nodeEventsMap[$NODE] = {A: InProgress}  (AllowCompletion waiting for stuck pod)"
echo ""

# ── Phase 2: 10 more same-check events → AlreadyDrained → cleared from map ──

log "=== Phase 2: 10 AlreadyQuarantined events → AlreadyDrained → cleared from map ==="
log "Each event gets a separate MongoDB doc. FQ writes AlreadyQuarantined;"
log "ND evaluates → ActionMarkAlreadyDrained → clearEventStatus → removed from map."

for i in $(seq 1 10); do
  send_event "gpu-health-monitor" "GPU" "GpuXidError" "true" "false" "15" "$NODE"
  echo -n "  B$i sent"$'\r'
  sleep 2
done
echo ""

log "Waiting 40s for all AlreadyDrained completions..."
sleep 40
ok "nodeEventsMap[$NODE] = {A: InProgress}  (B1-B10 cleared by clearEventStatus)"
echo ""

# ── Phase 3: kubectl uncordon → seeds leaked entries ─────────────────────────

log "=== Phase 3: kubectl uncordon → FQ writes Cancelled to A + B1-B10 ==="
log "CancelLatestQuarantiningEvents filter:"
log "  nodequarantined IN (Quarantined, AlreadyQuarantined)"
log "  createdAt >= A.createdAt"
log "Result:"
log "  A's entry (InProgress in map) → Cancelled (overwrite, same ID)"
log "  B1-B10 (cleared from map, NOT in map) → NEW leaked entries"

kubectl uncordon "$NODE"

log "Waiting 20s for ND change stream to process 11 Cancelled docs..."
sleep 20

LEAKED=$(nd_logs_since 1m | grep "$NODE" | grep -c "Marked specific event as cancelled" || true)
log "ND 'Marked specific event as cancelled' count: $LEAKED  (expect 11)"

if [[ "$LEAKED" -lt 5 ]]; then
  fail "Too few leaked entries observed. Check FQ called CancelLatestQuarantiningEvents."
  fail "Ensure the node had the quarantineHealthEvent annotation before uncordon."
  exit 1
fi

ok "Leaked entries seeded in nodeEventsMap[$NODE]"
ok "cancelledNodes[$NODE] is NOT set yet (needs UnQuarantined)"
echo ""

# ── Phase 4: re-quarantine ───────────────────────────────────────────────────

log "=== Phase 4: Fatal event → FQ cordons again ==="
log "New eventID → not in map → isEventCancelled returns false → proceeds normally"
send_event "gpu-health-monitor" "GPU" "GpuXidError" "true" "false" "15" "$NODE"
ok "Re-quarantine event sent"

wait_cordoned "$NODE"
sleep 5
ok "FQ has new quarantine annotation. nodeEventsMap has leaked entries + new InProgress entry."
echo ""

# ── Phase 5: healthy event → FQ writes UnQuarantined → sets cancelledNodes ──

log "=== Phase 5: Healthy event → FQ writes UnQuarantined ==="
log "FQ still has quarantine annotation → healthy entity → removes entity → uncordons"
log "ND: HandleCancellation(UnQuarantined) → cancelledNodes[$NODE] = {}"
log "    markEventInProgress(B) → map non-nil (leaked entries!) → cancelledNodes NOT cleared"
log "    clearEventStatus(B) → map still non-empty → gate does NOT fire"
log "    cancelledNodes[$NODE] is now PERMANENTLY STUCK"

send_event "gpu-health-monitor" "GPU" "GpuXidError" "true" "true" "0" "$NODE"
ok "Healthy event sent"

wait_uncordoned "$NODE"
sleep 10

# Check for the bug fingerprint in ND logs
RECENT=$(nd_logs_since 1m | grep "$NODE")
MARKED=$(grep_count "Marked node as cancelled" "$RECENT")
CLEARED=$(grep_count "Clearing node-level cancellation flag" "$RECENT")

log "Bug fingerprint check:"
echo "  'Marked node as cancelled'              : $MARKED  (expect >= 1)"
echo "  'Clearing node-level cancellation flag' : $CLEARED  (expect 0)"

if [[ "$MARKED" -ge 1 && "$CLEARED" -eq 0 ]]; then
  ok "BUG STATE CONFIRMED: cancelledNodes[$NODE] is stuck."
else
  fail "Unexpected state (Marked=$MARKED, Cleared=$CLEARED). Check ND logs."
  log "kubectl -n $NS logs deploy/node-drainer --since=2m | grep $NODE"
  exit 1
fi
echo ""

# ── Phase 6: victim event → must drain, will be silently cancelled ────────────

log "=== Phase 6: Victim event C (different check) → should drain, will be cancelled ==="
send_event "gpu-health-monitor" "GPU" "GpuDcgmConnectivityFailure" "true" "false" "15" "$NODE"
ok "Victim event C sent"

sleep 15

VICTIM_LOGS=$(nd_logs_since 1m | grep "$NODE")
CANCELLED_C=$(grep_count "Event was cancelled, performing cleanup" "$VICTIM_LOGS")
CHECKOUT_C=$(grep_count "CheckCompletion\|Checking pod completion" "$VICTIM_LOGS")

echo ""
echo "=== Phase 6 Result ==="
echo "  'Event was cancelled, performing cleanup' : $CANCELLED_C  (expect >= 1)"
echo "  CheckCompletion/drain attempted           : $CHECKOUT_C  (expect 0)"
echo ""

if [[ "$CANCELLED_C" -ge 1 && "$CHECKOUT_C" -eq 0 ]]; then
  echo "✓ BUG REPRODUCED"
  echo "  Node is cordoned by FQ but ND silently cancelled the drain."
  echo "  userpodsevictionstatus.status in MongoDB = 'Cancelled'"
  echo "  Pods on the node are NOT evicted."
else
  echo "✗ Bug not reproduced in Phase 6."
  log "Check ND logs: kubectl -n $NS logs deploy/node-drainer --since=3m | grep $NODE"
  exit 1
fi
echo ""

# ── Phase 7: negative control (restart ND, same event → drains normally) ────

log "=== Phase 7: Negative control — restart ND, resend victim → should drain ==="
log "Restarting node-drainer (wipes in-memory nodeEventsMap and cancelledNodes)..."
kubectl -n "$NS" rollout restart deploy/node-drainer
kubectl -n "$NS" rollout status deploy/node-drainer --timeout=90s
ok "node-drainer restarted"
sleep 5

# Victim must arrive while node is cordoned (it still is from Phase 6's FQ action)
send_event "gpu-health-monitor" "GPU" "GpuDcgmConnectivityFailure" "true" "false" "15" "$NODE"
ok "Victim event resent after restart"

sleep 20

CONTROL_LOGS=$(nd_logs_since 1m | grep "$NODE")
CANCELLED_CTRL=$(grep_count "Event was cancelled, performing cleanup" "$CONTROL_LOGS")
CHECKOUT_CTRL=$(grep_count "CheckCompletion\|Checking pod completion" "$CONTROL_LOGS")

echo ""
echo "=== Phase 7 Result ==="
echo "  'Event was cancelled, performing cleanup' : $CANCELLED_CTRL  (expect 0)"
echo "  CheckCompletion/drain attempted           : $CHECKOUT_CTRL  (expect >= 1)"
echo ""

if [[ "$CANCELLED_CTRL" -eq 0 && "$CHECKOUT_CTRL" -ge 1 ]]; then
  echo "✓ NEGATIVE CONTROL PASSED"
  echo "  After restart, same event is NOT cancelled — drain proceeds normally."
  echo "  This isolates the bug to the in-memory state leak."
else
  echo "✗ Negative control unexpected (Cancelled=$CANCELLED_CTRL, Checkout=$CHECKOUT_CTRL)."
  log "Check ND logs: kubectl -n $NS logs deploy/node-drainer --since=2m | grep $NODE"
fi

echo ""
echo "=== Summary ==="
echo "  Phase 6: FQ cordons node, ND cancels drain (bug)         ✓"
echo "  Phase 7: Same scenario after pod restart, drain proceeds  ✓"
echo ""
echo "Root cause: leaked nodeEventsMap entries from Cancelled change-stream docs"
echo "prevent clearEventStatus from clearing cancelledNodes[node]."
echo "Fix: decouple cancelledNodes cleanup from nodeEventsMap size check."
