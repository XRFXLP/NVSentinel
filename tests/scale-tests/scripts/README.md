# KOM Benchmark Runner

Reproduces the full KOM microbenchmark suite on any Kubernetes cluster.

## Prerequisites

```bash
# 1. KWOK controller
kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/latest/download/kwok.yaml

# 2. KOM deployed with STORE_ONLY (no side effects during benchmark)
helm upgrade kubernetes-object-monitor <chart> -n nvsentinel \
  --set processingStrategy=STORE_ONLY \
  --set resources.limits.memory=8Gi

# 3. Prometheus scraping KOM metrics (ServiceMonitor in nvsentinel namespace)

# 4. Start kubectl proxy and Prometheus port-forward in separate terminals
kubectl proxy --port=8001 &
kubectl port-forward -n prometheus <prometheus-pod> 9090:9090 &

# 5. Freeze DNS autoscaler BEFORE running (prevents CoreDNS flooding real nodes)
kubectl scale deployment kube-dns-autoscaler -n kube-system --replicas=0
```

## Run

```bash
# Full benchmark (~6-8 hours for 100k nodes)
python3 kom_benchmark.py \
  --proxy http://localhost:8001 \
  --prom http://localhost:9090 \
  --pod-source-namespace gpu-operator \
  --output results/$(date +%Y%m%d)

# Skip specific steps
python3 kom_benchmark.py --skip churn,policy  # memory + restart only

# After run, re-enable DNS autoscaler
kubectl scale deployment kube-dns-autoscaler -n kube-system --replicas=1
```

## What it measures

| Step | What | Time estimate |
|---|---|---|
| MEASURE | Sizes one real node + one real pod from the cluster | 30s |
| VERIFY | Creates one KWOK node, confirms size matches ±15% | 1 min |
| MEMORY | Node sweep 100→100k + pod sweep 0→100k | 3–4h |
| CHURN | Pod churn 10/50/200 pods/s × 5min each | 30 min |
| RESTART | Restart storm at 100 and 10k nodes | 30 min |
| POLICY | CEL complexity, lookup(), scope, MCR sweep | 1h |

## Critical: the node status template

Step 1 (MEASURE) samples a **real node from your cluster** and uses its exact status
(all conditions, all images, all fields) as the template for every KWOK node.
This ensures KWOK node objects are the same size as your production nodes.

**Do not skip the VERIFY step.** It checks that the KWOK node is within 15% of
the real node's size before starting any sweep.

## Output

`results/results.json` — machine-readable results for all steps.

Key fields:
- `object_sizes.node_bytes` — real node size used as template
- `memory.nodes[*].heap_alloc_mb` — what Prometheus shows at each scale point
- `memory.nodes[*].limit_recommendation_mi` — what to set in `resources.limits.memory`
- `restart[*].per_node_startup_ms` — K12 bottleneck: ms per node at startup
