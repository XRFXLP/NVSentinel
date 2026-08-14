#!/usr/bin/env bash
set -euo pipefail
NAMESPACE=nvsentinel
DEPLOY=mongo-bench-conn
STEPS=(5 10 15 20 25 30 40 50)
CLIENTS_PER_POD=500

MONGO_CMD='mongosh "mongodb://databaseAdmin:qBulXLpOVvPttYfz0@mongodb-rs0-0.mongodb-rs0.nvsentinel.svc.cluster.local:27017/admin?authSource=admin&directConnection=true&tls=true&tlsInsecure=true" --tlsCertificateKeyFile /tmp/tls-internal.pem --quiet'

measure() {
  local replicas=$1
  kubectl exec -n nvsentinel mongodb-rs0-0 -c mongod -- bash -c     "$MONGO_CMD --eval \"
    var ss = db.serverStatus();
    var conns = ss.connections;
    var mem = ss.mem;
    var wt = ss.wiredTiger.cache;
    print('METRIC|replicas=${replicas}|clients=$(( ${replicas} *  ))' +
      '|conn_current=' + conns.current +
      '|conn_active=' + conns.active +
      '|conn_rejected=' + conns.rejected +
      '|mem_resident_MB=' + mem.resident +
      '|wt_cache_MB=' + Math.round(wt['bytes currently in the cache']/1048576)
    );\" 2>&1" 2>/dev/null
}

wait_stable() {
  local target=$1; local prev=0; local cur; local attempts=0
  echo "  Waiting for connections to stabilize..."
  while true; do
    cur=$(kubectl exec -n nvsentinel mongodb-rs0-0 -c mongod -- bash -c "$MONGO_CMD --eval 'db.serverStatus().connections.current' 2>/dev/null | tail -1" 2>/dev/null || echo 0)
    [ "$cur" -eq "$prev" ] && [ "$attempts" -gt 2 ] && echo "  Stable at conn=$cur" && break
    [ "$attempts" -gt 30 ] && echo "  Timeout" && break
    prev=$cur; attempts=$(($attempts+1)); sleep 10
  done
}

echo "=== Percona Connection Scaling Sweep ==="
echo "Steps: ${STEPS[*]} replicas x ${CLIENTS_PER_POD} clients"
echo "BASELINE:"
measure 0

for replicas in "${STEPS[@]}"; do
  echo ""
  echo "--- ${replicas} replicas | $((${replicas}*${CLIENTS_PER_POD})) clients ---"
  kubectl scale deployment "$DEPLOY" -n "$NAMESPACE" --replicas="$replicas"
  kubectl rollout status deployment/"$DEPLOY" -n "$NAMESPACE" --timeout=300s 2>/dev/null || true
  wait_stable "$((${replicas}*${CLIENTS_PER_POD}*3))"
  measure "$replicas"
done

echo ""
echo "=== Sweep complete. Scaling down. ==="
kubectl scale deployment "$DEPLOY" -n "$NAMESPACE" --replicas=0
