#!/bin/bash
# NCCL All-Reduce Preflight Check - Shell-based POC
set -euo pipefail

GANG_CONFIG_DIR="${GANG_CONFIG_DIR:-/etc/preflight}"
NPROCS_PER_NODE="${NPROCS_PER_NODE:-8}"
BW_THRESHOLD="${BW_THRESHOLD_GBPS:-100}"

echo "=== NCCL All-Reduce Preflight Check ==="
echo "Pod: $POD_NAME, Node: $NODE_NAME"
echo "Config: $GANG_CONFIG_DIR"

# Debug: show what's in config dir
ls -la "$GANG_CONFIG_DIR/"

# Wait for gang formation (expected_count + peers list complete).
GANG_TIMEOUT="${GANG_TIMEOUT:-600}"
WAITED=0
while true; do
    EXPECTED_COUNT=$(cat "$GANG_CONFIG_DIR/expected_count" 2>/dev/null | tr -d '[:space:]' || echo "")
    MASTER_ADDR=$(cat "$GANG_CONFIG_DIR/master_addr" 2>/dev/null | tr -d '[:space:]' || echo "")
    MASTER_PORT=$(cat "$GANG_CONFIG_DIR/master_port" 2>/dev/null | tr -d '[:space:]' || echo "29500")
    # wc -l counts newlines; if last line has no trailing newline it undercounts.
    PEER_COUNT=$(awk 'END{print NR}' "$GANG_CONFIG_DIR/peers" 2>/dev/null || echo "0")

    if [[ -n "$EXPECTED_COUNT" ]] && [[ -n "$MASTER_ADDR" ]] && [[ "$PEER_COUNT" -ge "$EXPECTED_COUNT" ]]; then
        echo "Gang formation complete: $PEER_COUNT/$EXPECTED_COUNT peers"
        break
    fi

    if [[ "$WAITED" -ge "$GANG_TIMEOUT" ]]; then
        echo "ERROR: Gang formation timeout after ${GANG_TIMEOUT}s (expected=$EXPECTED_COUNT peers=$PEER_COUNT)"
        exit 2
    fi

    echo "Waiting for gang formation... ($WAITED/${GANG_TIMEOUT}s) expected=$EXPECTED_COUNT peers=$PEER_COUNT"
    sleep 5
    WAITED=$((WAITED + 5))
done

echo "Expected: $EXPECTED_COUNT nodes"
echo "Master: $MASTER_ADDR:$MASTER_PORT"
# Determine rank from pod name suffix: {name}-{index}
if [[ "$POD_NAME" =~ -([0-9]+)$ ]]; then
    MY_RANK="${BASH_REMATCH[1]}"
else
    # Fallback: derive rank from peers list if available
    if [[ -f "$GANG_CONFIG_DIR/peers" ]]; then
        MY_RANK=$(cat "$GANG_CONFIG_DIR/peers" | cut -d':' -f1 | sort | nl -v0 | grep "^${POD_NAME}$" | awk '{print $1}' || true)
    fi
fi
if [[ -z "${MY_RANK}" ]]; then
    echo "ERROR: Could not determine rank for pod $POD_NAME"
    exit 2
fi
echo "My rank: $MY_RANK (from pod name)"

# Export for bench.py
export BW_THRESHOLD_GBPS="$BW_THRESHOLD"
export NPROCS_PER_NODE
export MESSAGE_SIZES="${MESSAGE_SIZES:-4G,8G}"

echo "Running torchrun: nnodes=$EXPECTED_COUNT, rank=$MY_RANK, master=$MASTER_ADDR:$MASTER_PORT"

# Use the exact same bench script as the main container
cat > /tmp/bench.py << 'PYEOF'
import os, time, torch
import torch.distributed as dist

def bench(size, iters=20, warmup=5):
    local_rank = int(os.environ.get("LOCAL_RANK", 0))
    t = torch.randn(size // 4, device=f"cuda:{local_rank}")
    for _ in range(warmup):
        dist.all_reduce(t)
    torch.cuda.synchronize()
    start = time.time()
    for _ in range(iters):
        dist.all_reduce(t)
    torch.cuda.synchronize()
    elapsed = (time.time() - start) / iters
    ws = dist.get_world_size()
    algo = size / elapsed / 1e9
    bus = algo * 2 * (ws - 1) / ws
    return elapsed * 1000, algo, bus

dist.init_process_group(backend="nccl")
sizes = [4 * 1024**3, 8 * 1024**3]
if dist.get_rank() == 0:
    print("        Size    Time (ms)  AlgoBW (GB/s)   BusBW (GB/s)")
for sz in sizes:
    t, algo, bus = bench(sz)
    if dist.get_rank() == 0:
        print(f"{sz/1024**3:8.2f} GB {t:12.2f} {algo:14.2f} {bus:14.2f}")
dist.destroy_process_group()
PYEOF

exec torchrun \
    --nnodes="$EXPECTED_COUNT" \
    --nproc_per_node="$NPROCS_PER_NODE" \
    --node_rank="$MY_RANK" \
    --master_addr="$MASTER_ADDR" \
    --master_port="$MASTER_PORT" \
    /tmp/bench.py
