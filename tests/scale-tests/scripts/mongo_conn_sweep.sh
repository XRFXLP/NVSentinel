#!/usr/bin/env bash
# mongo_conn_sweep.sh — scale mongo-bench-conn Deployment and record MongoDB metrics at each step
# Waits for connection count to stabilize before measuring.

set -euo pipefail

NAMESPACE=nvsentinel
DEPLOY=mongo-bench-conn
STEPS=(50 75 100 125 150 167)
CLIENTS_PER_POD=500

MONGO_PW=$(kubectl get secret mongodb -n "$NAMESPACE" -o jsonpath='{.data.mongodb-root-password}' | base64 -d)
MONGO_URI="mongodb://root:${MONGO_PW}@mongodb-0.mongodb-headless.${NAMESPACE}.svc.cluster.local:27017/admin?authSource=admin"
MONGO_TLS="--tls --tlsCertificateKeyFile=/certs/mongodb.pem --tlsCAFile=/certs/mongodb-ca-cert"
EXEC_PREFIX="kubectl exec -n $NAMESPACE mongodb-0 -c mongodb -- bash -c"

mongosh_eval() {
  $EXEC_PREFIX "mongosh \"$MONGO_URI\" $MONGO_TLS --quiet --eval \"$1\"" 2>/dev/null
}

wait_stable() {
  local target_conns=$1
  local prev=0 cur attempts=0
  echo "  Waiting for connections to stabilize (target≈${target_conns})..."
  while true; do
    cur=$(mongosh_eval "db.serverStatus().connections.current" 2>/dev/null | tail -1 || echo 0)
    if [ "$cur" -eq "$prev" ] && [ "$attempts" -gt 2 ]; then
      echo "  Stable at conn_current=${cur}"
      break
    fi
    if [ "$attempts" -gt 30 ]; then
      echo "  Timed out waiting for stability (cur=${cur})"
      break
    fi
    prev=$cur
    attempts=$((attempts+1))
    sleep 10
  done
}

measure() {
  local replicas=$1
  mongosh_eval "
    var ss = db.serverStatus();
    var conns = ss.connections;
    var mem = ss.mem;
    var wt = ss.wiredTiger.cache;
    var ei = ss.extra_info || {};
    print('METRIC' +
      '|replicas=${replicas}' +
      '|clients=$(( replicas * CLIENTS_PER_POD ))' +
      '|conn_current=' + conns.current +
      '|conn_active=' + conns.active +
      '|conn_rejected=' + conns.rejected +
      '|mem_resident_MB=' + mem.resident +
      '|wt_cache_MB=' + Math.round(wt['bytes currently in the cache']/1048576) +
      '|user_time_us=' + (ei.user_time_us || 0) +
      '|sys_time_us=' + (ei.system_time_us || 0)
    );
  "
}

echo "=== MongoDB Connection Memory Sweep ==="
echo "Steps: ${STEPS[*]} replicas × ${CLIENTS_PER_POD} clients/pod"
echo ""
echo "BASELINE:"
measure 0

PREV_USER_US=0; PREV_SYS_US=0; PREV_TS=$(date +%s)

for replicas in "${STEPS[@]}"; do
  total_clients=$(( replicas * CLIENTS_PER_POD ))
  expected_conns=$(( total_clients * 3 ))
  echo ""
  echo "--- ${replicas} replicas | ${total_clients} clients | ~${expected_conns} expected connections ---"

  kubectl scale deployment "$DEPLOY" -n "$NAMESPACE" --replicas="$replicas"
  kubectl rollout status deployment/"$DEPLOY" -n "$NAMESPACE" --timeout=300s 2>/dev/null || true

  wait_stable "$expected_conns"

  RESULT=$(measure "$replicas")
  echo "$RESULT"

  # CPU delta
  NOW_TS=$(date +%s); ELAPSED=$(( NOW_TS - PREV_TS ))
  USER_US=$(echo "$RESULT" | grep -o 'user_time_us=[0-9]*' | cut -d= -f2 || echo 0)
  SYS_US=$(echo  "$RESULT" | grep -o 'sys_time_us=[0-9]*'  | cut -d= -f2 || echo 0)
  if [ "${PREV_USER_US:-0}" -gt 0 ] && [ "$ELAPSED" -gt 0 ]; then
    DELTA_US=$(( USER_US - PREV_USER_US + SYS_US - PREV_SYS_US ))
    CPU_PCT=$(( DELTA_US / (ELAPSED * 10000) ))
    echo "  cpu_delta_pct=${CPU_PCT}%  interval=${ELAPSED}s"
  fi
  PREV_USER_US=${USER_US}; PREV_SYS_US=${SYS_US}; PREV_TS=$NOW_TS
done

echo ""
echo "=== Sweep complete. Scaling down. ==="
kubectl scale deployment "$DEPLOY" -n "$NAMESPACE" --replicas=0
