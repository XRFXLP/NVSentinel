#!/usr/bin/env bash
# storm-scale-test.sh — deploy storm-injector, ramp STORM_MULTIPLIER, and watch
# MongoDB + consumer metrics at each step.
#
# Usage:
#   ./scripts/storm-scale-test.sh [multiplier]     # run one multiplier level
#   ./scripts/storm-scale-test.sh ramp             # ramp 1→10→100 automatically
#   ./scripts/storm-scale-test.sh cleanup          # tear down everything
#
# Prerequisites:
#   - kubectl context nvs-dgxc-k8s-azr-scus-dev1 active
#   - mongosh installed (for MongoDB queries)
#   - xrfxlp/storm-injector:latest pushed to Docker Hub (public)
#
# MongoDB monitoring: connects to mongodb-0 via port-forward using the same
# TLS cert approach as scripts/mongodb-shell.sh

set -euo pipefail

CONTEXT="nvs-dgxc-k8s-azr-scus-dev1"
NAMESPACE="nvsentinel"
KC="kubectl --context $CONTEXT -n $NAMESPACE"
CERT_DIR="$HOME/.nvsentinel/certs/$CONTEXT"
MONGO_LOCAL_PORT=27019
MONGO_POD="mongodb-0"
MONGO_DB="HealthEventsDatabase"
MONGO_COLL="HealthEvents"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${BLUE}[$(date +%H:%M:%S)]${NC} $*"; }
ok()   { echo -e "${GREEN}[OK]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERR]${NC} $*"; }

# ---------------------------------------------------------------------------
# Deploy storm-injector DaemonSet (KWOK nodes) + real Deployment (CPU nodes)
# ---------------------------------------------------------------------------
deploy() {
  local multiplier="${1:-1}"
  log "Deploying storm-injector with STORM_MULTIPLIER=$multiplier …"
  $KC apply -f scripts/deploy-storm-injector.yaml --validate=false

  # Patch both resources to the requested multiplier
  $KC set env daemonset/storm-injector-real STORM_MULTIPLIER="$multiplier"

  log "Waiting for storm-injector-real pods to be Running (up to 90s) …"
  $KC rollout status daemonset storm-injector-real --timeout=90s || warn "Rollout not fully ready"
}

# ---------------------------------------------------------------------------
# Set STORM_MULTIPLIER on the real Deployment only (faster for testing)
# ---------------------------------------------------------------------------
set_multiplier() {
  local m="$1"
  log "Setting STORM_MULTIPLIER=$m on storm-injector-real …"
  $KC set env daemonset/storm-injector-real STORM_MULTIPLIER="$m"
  $KC rollout status daemonset storm-injector-real --timeout=90s
  ok "Multiplier=$m active"
}

# ---------------------------------------------------------------------------
# Tail logs from storm-injector-real pods
# ---------------------------------------------------------------------------
check_logs() {
  log "=== storm-injector-real logs (last 20 lines) ==="
  for pod in $($KC get pods -l app=storm-injector-real -o name 2>/dev/null); do
    echo "--- $pod ---"
    $KC logs "$pod" --tail=20 2>&1 || warn "Cannot get logs for $pod"
  done
}

# ---------------------------------------------------------------------------
# Set up MongoDB port-forward + certs (mirrors mongodb-shell.sh)
# ---------------------------------------------------------------------------
setup_mongo_pf() {
  # Extract certs if not already done
  if [[ ! -f "$CERT_DIR/ca.crt" ]]; then
    mkdir -p "$CERT_DIR" && chmod 700 "$CERT_DIR"
    $KC get secret mongo-app-client-cert-secret \
      -o jsonpath='{.data.ca\.crt}' | base64 -d > "$CERT_DIR/ca.crt"
    $KC get secret mongo-app-client-cert-secret \
      -o jsonpath='{.data.tls\.crt}' | base64 -d > "$CERT_DIR/tls.crt"
    $KC get secret mongo-app-client-cert-secret \
      -o jsonpath='{.data.tls\.key}' | base64 -d > "$CERT_DIR/tls.key"
    cat "$CERT_DIR/tls.crt" "$CERT_DIR/tls.key" > "$CERT_DIR/creds.pem"
    chmod 600 "$CERT_DIR/ca.crt" "$CERT_DIR/creds.pem"
    ok "Certs extracted to $CERT_DIR"
  fi

  # Kill any existing port-forward
  pkill -f "port-forward $MONGO_POD $MONGO_LOCAL_PORT" 2>/dev/null || true
  sleep 1

  log "Starting MongoDB port-forward on localhost:$MONGO_LOCAL_PORT …"
  $KC port-forward "$MONGO_POD" "$MONGO_LOCAL_PORT:27017" &>/dev/null &
  PF_PID=$!
  sleep 3
  kill -0 $PF_PID 2>/dev/null || { err "Port-forward failed"; return 1; }
  ok "Port-forward PID=$PF_PID"
}

MONGO_URI() {
  echo "mongodb://localhost:$MONGO_LOCAL_PORT/$MONGO_DB?directConnection=true&authMechanism=MONGODB-X509&authSource=\$external&tls=true&tlsCAFile=$CERT_DIR/ca.crt&tlsCertificateKeyFile=$CERT_DIR/creds.pem&tlsAllowInvalidHostnames=true"
}

# ---------------------------------------------------------------------------
# MongoDB: ingest rate
# ---------------------------------------------------------------------------
mongo_ingest_rate() {
  log "=== MongoDB HealthEvents ingest rate ==="
  mongosh "$(MONGO_URI)" --quiet --eval "
    const db_ = db.getSiblingDB('$MONGO_DB');
    const coll = db_['$MONGO_COLL'];
    const now = new Date();
    const t1 = new Date(now - 60000);   // 1 min ago
    const t5 = new Date(now - 300000);  // 5 min ago

    const total   = coll.estimatedDocumentCount();
    const last1m  = coll.countDocuments({generatedTimestamp: {\$gte: t1}});
    const last5m  = coll.countDocuments({generatedTimestamp: {\$gte: t5}});
    const rate1m  = (last1m / 60).toFixed(3);
    const rate5m  = (last5m / 300).toFixed(3);

    // Expected at baseline (0.0134 ev/s × node_count):
    // KWOK: 1000 KWOK pods (fake, no real events)
    // Real: up to 3 CPU pods × 0.0134 = ~0.04 ev/s, multiplied by STORM_MULTIPLIER
    print('Total HealthEvents:    ' + total);
    print('Last 1 min:            ' + last1m + '  (' + rate1m + ' ev/s)');
    print('Last 5 min:            ' + last5m + '  (' + rate5m + ' ev/s)');
    print('');

    // Oldest un-expired doc
    const oldest = coll.findOne({}, {sort: {generatedTimestamp: 1}, projection: {generatedTimestamp:1}});
    if (oldest) print('Oldest event:          ' + oldest.generatedTimestamp);
  " 2>&1 || warn "MongoDB query failed — is port-forward running?"
}

# ---------------------------------------------------------------------------
# MongoDB: consumer backlog and oplog window
# ---------------------------------------------------------------------------
mongo_consumer_health() {
  log "=== MongoDB consumer resume tokens and oplog ==="
  mongosh "$(MONGO_URI)" --quiet --eval "
    const db_ = db.getSiblingDB('$MONGO_DB');

    // Resume tokens (backlog indicator: old token = consumer is lagging)
    print('--- Resume tokens ---');
    try {
      db_['ChangeStreamResumeTokens'].find({}).forEach(doc => {
        print(JSON.stringify({consumer: doc.consumer || doc._id, updatedAt: doc.updatedAt || doc.ts}));
      });
    } catch(e) { print('(no ChangeStreamResumeTokens collection or error: ' + e.message + ')'); }

    // Oplog window
    print('');
    print('--- Oplog window ---');
    try {
      const admin = db.getSiblingDB('local');
      const first = admin['oplog.rs'].findOne({}, {sort: {ts: 1}, projection: {ts:1}});
      const last  = admin['oplog.rs'].findOne({}, {sort: {ts: -1}, projection: {ts:1}});
      if (first && last) {
        const windowSec = (last.ts.t - first.ts.t);
        print('Oplog window: ' + windowSec + 's  (' + (windowSec/60).toFixed(1) + ' min)');
        print('First oplog entry: ' + new Date(first.ts.t * 1000).toISOString());
        print('Last  oplog entry: ' + new Date(last.ts.t  * 1000).toISOString());
      }
    } catch(e) { print('(oplog check failed: ' + e.message + ')'); }
  " 2>&1 || warn "MongoDB query failed"
}

# ---------------------------------------------------------------------------
# Prometheus: consumer metrics (via kubectl port-forward to a consumer pod)
# ---------------------------------------------------------------------------
consumer_metrics() {
  log "=== Consumer Prometheus metrics ==="
  # Grab fault-quarantine metrics (port 2112 is standard for NVSentinel pods)
  local fq_pod
  fq_pod=$($KC get pods -l app.kubernetes.io/name=fault-quarantine -o name 2>/dev/null | head -1)
  if [[ -z "$fq_pod" ]]; then
    fq_pod=$($KC get pods --no-headers 2>/dev/null | grep "fault-quarantine" | awk '{print $1}' | head -1)
  fi

  if [[ -z "$fq_pod" ]]; then
    warn "Cannot find fault-quarantine pod for metrics"
    return
  fi

  # Port-forward to metrics port
  $KC port-forward "$fq_pod" 12112:2112 &>/dev/null &
  local METRICS_PF=$!
  sleep 2

  if kill -0 $METRICS_PF 2>/dev/null; then
    echo "--- fault-quarantine metrics (change_stream_*) ---"
    curl -s http://localhost:12112/metrics 2>/dev/null \
      | grep -E "change_stream|consumer_backlog|health_event" \
      | head -20 || warn "Could not fetch metrics"
    kill $METRICS_PF 2>/dev/null || true
  else
    warn "Metrics port-forward failed"
  fi
}

# ---------------------------------------------------------------------------
# Full snapshot: logs + ingest rate + consumer health
# ---------------------------------------------------------------------------
snapshot() {
  local label="${1:-snapshot}"
  echo ""
  echo "======================================================================"
  echo "  SCALE TEST SNAPSHOT: $label  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "======================================================================"
  check_logs
  setup_mongo_pf 2>/dev/null || warn "Port-forward setup failed; skipping MongoDB"
  mongo_ingest_rate
  mongo_consumer_health
  consumer_metrics
}

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
cleanup() {
  log "Cleaning up storm-injector resources …"
  $KC delete daemonset storm-injector-real --ignore-not-found
  pkill -f "port-forward $MONGO_POD $MONGO_LOCAL_PORT" 2>/dev/null || true
  pkill -f "port-forward.*12112" 2>/dev/null || true
  ok "Cleanup done"
}

# ---------------------------------------------------------------------------
# Ramp test: 1x → 10x → 100x
# ---------------------------------------------------------------------------
ramp() {
  log "Starting ramp test …"
  deploy 1
  sleep 30
  snapshot "multiplier=1 (baseline: ~0.013 ev/s/node)"

  set_multiplier 10
  sleep 60
  snapshot "multiplier=10 (~0.13 ev/s/node: Events LIST starts binding)"

  set_multiplier 100
  sleep 120
  snapshot "multiplier=100 (~1.34 ev/s/node: consumer throughput ceiling)"

  log "Ramp complete. Check snapshot output above for breakpoints."
  log "Run 'set_multiplier 300' to test etcd saturation (~4 ev/s/node)."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
CMD="${1:-help}"
case "$CMD" in
  deploy)   deploy "${2:-1}" ;;
  multiplier|set_multiplier) set_multiplier "${2:?usage: $0 multiplier <N>}" ;;
  logs)     check_logs ;;
  mongo)    setup_mongo_pf && mongo_ingest_rate && mongo_consumer_health ;;
  metrics)  consumer_metrics ;;
  snapshot) snapshot "${2:-manual}" ;;
  ramp)     ramp ;;
  cleanup)  cleanup ;;
  help|*)
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  deploy [multiplier]    Deploy DaemonSet + real Deployment (default m=1)"
    echo "  multiplier <N>         Update STORM_MULTIPLIER on the real Deployment"
    echo "  logs                   Tail storm-injector-real logs"
    echo "  mongo                  MongoDB ingest rate + consumer health"
    echo "  metrics                Consumer Prometheus metrics"
    echo "  snapshot [label]       Full snapshot: logs + mongo + metrics"
    echo "  ramp                   Full ramp: 1x → 10x → 100x with snapshots"
    echo "  cleanup                Delete all storm-injector resources"
    ;;
esac
