## Test strategy

Graded points establish the scaling curves; the top point produces the SLAs. Roughly 2.5 hours of run time.

### Run shape

| Scale | Duration | Load | Purpose |
|---|---|---|---|
| 1k, 5k, 10k, 25k, 50k | ~15 min each | 2% of fleet/hour continuous, one 250-node burst | A1 sizing curve, A2 API-server load curve |
| **100k** | ~60 min | two halves, see below | A3 SLAs, peak load |
| 10k (side runs) | ~20 min | see "Side runs" | A1 memory model terms |

### Load profile

Small points use a fleet-proportional rate (2%/hour) purely to have activity while we take sizing readings — 1k gives ~3 events in 10 min, which is not enough for percentiles, and that is fine because those points are not where SLAs come from.

At 100k we run two rates back to back on the same fleet:

| Phase | Rate | Utilisation | Reports as |
|---|---|---|---|
| 0-30 min | 0.5 nodes/s | ~15% of measured capacity | expected-case SLA |
| 30-60 min | ~2 nodes/s | ~60% of measured capacity | under-load SLA |

0.5 nodes/s is about two orders of magnitude above a realistic fleet fault rate but only ~15% of what we measured the pipeline can absorb, so on its own it would produce unloaded latencies. The second half is where queueing and a P99 that differs from P50 will show up. Two SLA columns is more useful than one — customers want the normal case and the bad day.

Bursts of 100 / 250 / 500 / 1000 nodes at 15-minute spacing, overlaid on both halves.

### Held constant

10 pods/node, node-drainer `qps=200/400`, fault-quarantine `qps=20/40`, `Immediate` eviction, `RESTART_BM` action. One variable moves at a time so any bend in a curve is attributable.

### Side runs (10k, ~20 min)

The A1 table needs `fixed overhead per cached object` and `inflation factor`, which only come from:

- **Padding placement sweep** — vary retained vs stripped bytes at fixed node count. Yields both terms. A stripped-byte slope of ~0 also confirms each component's transform behaves as its code claims.
- **Pods-per-node 5 / 10 / 20** — validates `node_throughput = eviction_rate / pods_per_node`, which is what lets the SLA table extrapolate to fleets with different pod density.

### Open decision: eviction mode

`Immediate` measures our capability. `AllowCompletion` is the shipped default, and with long-running pods it does not drain at all — we have observed nodes sitting in `draining` indefinitely while node-drainer correctly waits for pods to finish.

Proposal: run the sweep in `Immediate`, plus one `AllowCompletion` run at 10k with finite-lifetime pods. One extra run, and it is the difference between publishing our capability and publishing what a customer sees.

### Not covered, stated in the report

Saturation to failure, component restart and recovery, GPU-requesting workloads (`drainGPUPods = false` by default), PodDisruptionBudgets, multi-hour soak.

### Prerequisite

The labeler pod is currently `Pending` in the benchmark cluster and has never run, so its throughput numbers cannot be produced until that is fixed.
