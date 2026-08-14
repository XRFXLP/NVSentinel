# Kubernetes Object Monitor (KOM) — Scale Benchmark Report (v1.15.0)

This report explains how `kubernetes-object-monitor` v1.15.0 behaved under node and pod scale, churn, restart, resync, and policy-complexity tests. Most measurements were collected on AWS EKS with KWOK-simulated nodes and pods. Results collected on OCI are called out explicitly and should be treated as directional rather than directly comparable.

## Contents

1. Executive summary
   - How to read the evidence
2. Test Environment
   - How KOM processes an object
3. Metrics Used
4. MB-2.1 — Memory and resource sizing
5. MB-2.2 — Pod Churn Rate Sweep
6. MB-2.3 — Watch Restart Storm
7. MB-2.4 — ResyncPeriod Spike
8. MB-C2.1 — CEL Predicate Complexity
9. MB-C2.2 — nodeAssociation with lookup()
10. MB-C2.3 — Namespace-scoped vs Cluster-scoped Watches
11. MB-C2.4 — maxConcurrentReconciles Sweep
12. Bottlenecks
13. Configuration Guidance
14. Pass/Fail Summary
15. Methodology Gaps
16. Relevant Code Locations

## Executive summary

For operators, the results reduce to five decisions:

1. **Size memory from peak container working set during startup, not from the settled Go heap.** A restart forces every enabled informer to LIST and deserialize its full watch scope again.
2. **Namespace-scope Pod and Event policies.** Scope controls the informer cache itself, so it reduces both cached objects and irrelevant reconciles.
3. **Use queue wait as the detection-latency signal.** Reconcile execution stayed near 10 ms while queueing delayed work by seconds or minutes.
4. **Increase concurrency only when the queue is growing.** At low churn, `maxConcurrentReconciles=1` kept up. At very large KWOK scales, higher concurrency was required to drain simulated heartbeats.
5. **Give cache synchronization enough time at large scale.** Initial LIST and deserialization, not steady-state watches, dominate restart recovery.

The largest tested footprint—100,000 nodes plus 100,000 namespace-scoped pods—peaked at approximately **28.8 GiB working set** during cache sync. The benchmark recommendation is a **44 GiB memory limit** after applying a 1.5× safety factor.

### How to read the evidence

- **Measured** values came from Prometheus or the test timeline.
- **Derived** values (sizing rules and safe-object formulas) extrapolate from measured throughput or memory and include their assumptions.
- **Code context** explains why the implementation produces the observed behavior.

---

## Test Environment

| Component | Detail |
| --- | --- |
| Cluster | AWS EKS us-east-1 (c8a.16xlarge worker nodes) |
| KOM image | v1.15.0 baseline; locally rebuilt tag `20260727052921` for post-fix scale runs, including the 10m cache-sync timeout |
| KOM config (scale tests) | `processingStrategy=STORE_ONLY`, `maxConcurrentReconciles=100`, `resyncPeriod=24h`; rebuilt benchmark image with explicit `cache-sync-timeout=10m` default |
| KOM config (churn/latency tests) | `processingStrategy=STORE_ONLY`, `maxConcurrentReconciles=1`; the current harness sets `resyncPeriod=24h` during churn measurement, then restores `5m` |
| KOM resources | `requests: 500m CPU / 512Mi RAM`, `limits: 2 CPU / 32Gi RAM` (bumped from 8Gi for scale tests) |
| Simulated objects | KWOK nodes + pods against real API server + etcd |
| Prometheus scrape | 10 s interval |
| Real worker nodes | 3 schedulable nodes (32 CPU / 128 GiB each) |

**KWOK node objects** were patched post-creation to carry realistic status payloads matching production nodes: 8 conditions (including skyhook, infiniband, GPU checks), full capacity/allocatable, nodeInfo, and 50 image entries (~13.7 KB total per node vs ~3.5 KB unpatched).

**Real cluster pods (always present, not watched by KOM):** 65 pods across system namespaces (nvsentinel: 15, kube-system: 27, prometheus: 8, dgxc-system: 15) running on 6 real EKS worker nodes. KOM's pod policy was namespace-scoped to `benchmark` only — these system pods were never cached. Their memory contribution to KOM is negligible.

The raw environment notes identify six real EKS workers in total and three as schedulable benchmark workers. This report interprets those statements as six workers present, with three eligible for benchmark placement.

> **Configuration terminology:** `maxConcurrentReconciles` is abbreviated **MCR** below. `resyncPeriod` is controller-runtime's informer sync period; it causes cached objects to be reconsidered periodically even when the API object did not change.

`STORE_ONLY` does not bypass KOM's publisher. KOM still builds and sends the health event through Platform Connector with the store-only processing strategy; downstream components are expected not to perform remediation. The benchmark therefore includes KOM-side event publication cost for actual state transitions.

### How KOM processes an object

The following code path provides context for the benchmark:

1. Policies are grouped by Kubernetes Group/Version/Kind (GVK), and KOM creates one controller and one unstructured informer cache per enabled GVK.
2. A namespace on a policy narrows that GVK's cache. If any enabled policy for the same GVK is cluster-scoped, the shared cache becomes cluster-scoped.
3. Each reconcile reads an `unstructured.Unstructured` object from the controller-runtime client, evaluates every enabled policy for that GVK, resolves its node association, and compares the result with in-memory match state.
4. KOM publishes only state transitions: the first `false → true` predicate transition emits an unhealthy event, and `true → false` emits a healthy event.
5. After a transition is published, KOM persists match state in the associated Node annotation `nvsentinel.nvidia.com/k8s-object-monitor-policy-matches`. This prevents a restart from treating every still-unhealthy object as a new transition.

Two implementation details materially affect interpretation:

- CEL evaluation is protected by a single mutex in the shared CEL environment. Higher MCR can overlap object reads, publishing, and annotation updates, but CEL execution itself is serialized.
- CEL `lookup()` calls the manager's cached controller-runtime client. It adds object lookup and conversion work. A GVK already present in the manager cache is cache-backed; an unwatched GVK may require cache initialization or an API read depending on controller-runtime client configuration. It should not be described categorically as either zero API calls or one live API GET per reconcile.

Relevant implementation: `pkg/initializer/initializer.go`, `pkg/controller/reconciler.go`, `pkg/policy/evaluator.go`, `pkg/cel/environment.go`, and `pkg/annotations/manager.go` under `health-monitors/kubernetes-object-monitor/`.

---

## Metrics Used

All metrics are from KOM's `:2112/metrics` endpoint, scraped by Prometheus. The binary default is `:8080`, but the Helm deployment passes the global metrics port, which defaults to `2112`.

| Metric | What it measures |
| --- | --- |
| `go_memstats_heap_alloc_bytes` | Go heap in use — proxy for informer cache size |
| `container_memory_working_set_bytes` | **OS-level RSS minus inactive file cache — what the cgroup OOM limit applies to.** Always size `resources.limits.memory` against this, not heap_alloc. |
| `controller_runtime_reconcile_total` | Total reconcile calls (by controller, result) |
| `controller_runtime_reconcile_time_seconds` | Per-reconcile wall time histogram |
| `workqueue_depth` | Items waiting in the work queue |
| `workqueue_queue_duration_seconds` | **Time an item waits in queue before processing starts** — the best available proxy for detection latency |
| `workqueue_work_duration_seconds` | Time spent actively processing an item — what reconcile P99 measures |
| `workqueue_adds_total` | Cumulative items enqueued (spike = resync) |
| `workqueue_adds_total` rate vs `workqueue_work_duration_seconds_count` rate | When add rate > drain rate, queue is saturating and memory pressure can rise |
| `k8s_object_monitor_cache_sync_duration_seconds` | Initial informer-cache synchronization time, labelled by resource kind |
| `k8s_object_monitor_policy_matches_total` | First predicate-true transitions, after a successful unhealthy-event publication |
| `k8s_object_monitor_policy_evaluation_errors_total` | CEL eval failures |
| `k8s_object_monitor_health_events_publish_errors_total` | Failed gRPC health-event publications |
| `k8s_object_monitor_reconciliation_errors_total` | Resource GET and per-policy reconcile failures observed by KOM |
| `process_cpu_seconds_total` | KOM container CPU |

> **Registry note:** Prior to image `20260727055730`, all `k8s_object_monitor_*` metrics were registered to Go's default Prometheus registry, but the metrics server served from controller-runtime's registry. They were silently absent from `/metrics`. Fixed in `20260727055730`.

> **Latency boundary:** queue duration starts when controller-runtime enqueues a key, not when the Kubernetes condition first changed at the source. Therefore `queue_duration + work_duration` is the best KOM-side detection estimate, but it excludes API-server/watch-delivery delay and downstream publication time.

> **Error-count caveat:** `ResourceReconciler.Reconcile` records per-policy failures and continues, then returns success to controller-runtime. As a result, `controller_runtime_reconcile_total{result="error"}` alone cannot prove that every policy evaluation or health-event publication succeeded. Check the three KOM error counters and logs as well.

> **Collection boundary:** the checked-in benchmark harness records `go_memstats_heap_alloc_bytes`, but it does not query `container_memory_working_set_bytes`. Working-set values in this report were collected separately from container/cgroup Prometheus data and are not present in the harness JSON results. Future runs should collect both metrics in the same snapshot.

---

## MB-2.1 — Memory: How to size `resources.limits.memory`

**Primary rule:** monitor peak `container_memory_working_set_bytes` during KOM startup and size `resources.limits.memory` from that value. This is the closest available metric to what the cgroup limit enforces.

`go_memstats_heap_alloc_bytes` is still useful for understanding live cache growth, but it is not sufficient for limit sizing. In these runs, working set was commonly **2–3× live heap** after including Go arenas, stacks, and runtime overhead. Where only heap data is available, **2.5× heap** is a rough first estimate—not a substitute for measuring startup peak working set.

**Minimum: 2 Gi regardless of cluster size.** Even a 250-node cluster running for months accumulates ~2 Gi of working set from GC overhead and arena retention across millions of reconcile cycles.

---

### How many objects does KOM watch?

A GPU cluster running NVSentinel + GPU operator has approximately **14 DaemonSet pods per node** (5 NVSentinel + 6 GPU operator + 3 system). Always use **namespace-scoped pod policies** — watching only `gpu-operator` namespace (~6 pods/node) instead of all pods cluster-wide keeps the pod count manageable and avoids the 84% throughput drop from system pod noise (see B4).

| Cluster size | Nodes watched | Pods watched (gpu-operator ns only) |
| ------------ | ------------- | ----------------------------------- |
| 500 nodes    | 500           | ~3,000                              |
| 2,000 nodes  | 2,000         | ~12,000                             |
| 5,000 nodes  | 5,000         | ~30,000                             |
| 10,000 nodes | 10,000        | ~60,000                             |

---

### Pod sweep (namespace-scoped, ~9.3 KB pods)

Policy: `PodFailed` — `resource.status.phase == "Failed"` in `benchmark` namespace. Config: `maxConcurrentReconciles=100`, `resyncPeriod=24h`, AWS EKS, ~9.3 KB padded pods.

| Pods watched | `heap_alloc` | `working_set` (settled) | `working_set` (peak, cache sync) | Recommended limit |
| --- | --- | --- | --- | --- |
| 10,000 | ~2,494 MB | ~3.1 GiB | ~4.7 GiB | **8 Gi** |
| 50,000 | ~6,440 MB | ~7.0 GiB | ~7.4 GiB | **12 Gi** |
| 100,000 | ~4,798 MB | ~9.3 GiB | ~12.0 GiB | **18 Gi** |

Rule of thumb: **add ~2 Gi to `resources.limits.memory` per 10,000 pods watched** (~1.5 GiB peak working_set increase × 1.5 safety factor).

---

### Node sweep (cluster-scoped)

Policy: `NodeNotReady` — `has(resource.status.conditions) && resource.status.conditions.exists(c, c.type == "Ready" && c.status == "False")`.  
Config: `maxConcurrentReconciles=100`, `resyncPeriod=24h`, AWS EKS, 13.4 KB KWOK nodes (real c8a.16xlarge status payload).

| Nodes | `heap_alloc` | `working_set` (settled) | `working_set` (peak, cache sync) | Recommended limit |
| --- | --- | --- | --- | --- |
| 100 | 75 MB | ~150 MiB | ~150 MiB | **2 Gi** |
| 1,000 | 168 MB | ~350 MiB | ~350 MiB | **2 Gi** |
| 5,000 | 864 MB | ~1.7 GiB | ~1.7 GiB | **3 Gi** |
| 10,000 | 904 MB | ~1.1 GiB | ~2.2 GiB | **4 Gi** |
| 25,000 | 2,955 MB | ~5.5 GiB | ~5.5 GiB | **9 Gi** |
| 50,000 | 4,461 MB | ~6.5 GiB | ~11.3 GiB | **17 Gi** |
| 100,000 | ~8,800 MB | **~10.7 GiB** | **~19 GiB** | **29 Gi** |

> **heap_alloc vs working_set:** `heap_alloc` is only the live Go heap. `working_set` (what the cgroup limit applies to) includes heap_sys, goroutine stacks, and retained idle arena pages. During the initial cache sync on startup, Go allocates heavily for the LIST response deserialization — `working_set` peaks ~2× the settled value before GC returns idle pages to the OS. **Size the limit against the peak working_set, not settled heap_alloc.**

> **MCR matters at high node counts:** At MCR=1, KWOK heartbeats (approximately 143 events/s at 100k nodes) exceed the measured reconcile drain rate (approximately 7/s), causing persistent queue backlog and severe memory pressure. The 100k-node KWOK run used `maxConcurrentReconciles=100`. See B8.

> **Measurement validity:** Results for 100, 1k, 5k, 10k, and 25k nodes used MCR=1 with the queue fully drained between steps. The 100k result used MCR=100 with the queue drained.

Rule of thumb: **+110 MB heap_alloc per 1,000 nodes at 13.4 KB/node** (Go large-object allocator at ~1.5× expansion for objects >32 KB).

> **Why individual heap samples are not monotonic:** Go garbage collection and arena retention make point-in-time `heap_alloc` noisy, which is why the 50k and 100k rows do not form a perfectly increasing sequence. Peak working set during a controlled cache-sync window is the more stable sizing input.

**Resync saturation — separate from memory, also hits at scale:**

With the chart's 5-minute resync period, KOM re-evaluates all watched nodes every 5 minutes. At ~230 rec/s, this takes `N / 230` seconds. Above ~65k nodes, the queue never fully drains before the next resync fires—fault detection latency grows to 16+ minutes (see B6). The memory-sizing sweeps used a 24-hour period specifically to keep this resync effect out of those measurements.

| Nodes       | Resync drain time | Safe?           | Detection lag        |
| ----------- | ----------------- | --------------- | -------------------- |
| 10,000      | 43 s              | ✅              | <1 s steady-state    |
| 25,000      | 109 s             | ✅              | <1 s steady-state    |
| 50,000      | 217 s             | ✅ (83s margin) | <1 s steady-state    |
| **~65,000** | **~283 s**        | **⚠ limit**     | growing              |
| 75,000      | 326 s             | ❌ saturated    | **~16 min constant** |

---

### Node × Pod combined (both policies active)

Both `NodeNotReady` (cluster-scoped) and `PodFailed` (namespace-scoped, `benchmark`) policies were active simultaneously. Config: `maxConcurrentReconciles=100`, `resyncPeriod=24h`, and the rebuilt image's explicit 10-minute cache-sync timeout on AWS EKS. The timeout came from the binary default, so the Helm deployment and benchmark harness did not need to pass an additional argument.

| Nodes | Pods | `heap_alloc` (settled) | `working_set` (settled) | `working_set` (peak, cache sync) | Recommended limit |
| --- | --- | --- | --- | --- | --- |
| 10,000 | 100,000 | ~3,460 MB | ~5.3 GiB | **~9.1 GiB** | **14 Gi** |
| 50,000 | 100,000 | ~18,581 MB | ~11.6 GiB | **~16.9 GiB** | **26 Gi** |
| 100,000 | 10,000 | 9,055 MB | ~10.8 GiB | **~20.7 GiB** | **32 Gi** |
| 100,000 | 50,000 | ~14,717 MB | ~15.4 GiB | **~25.1 GiB** | **38 Gi** |
| 100,000 | 100,000 | ~11,360 MB | ~22.9 GiB | **~28.8 GiB** | **44 Gi** |

**Pod contribution at 100k nodes:** peak working_set increased by ~1.55 GiB (20.7 − 19.1) for 10k pods, and ~4.4 GiB (25.1 − 20.7) for 40k additional pods — consistent with the additive model (node and pod informer caches are independent).

> **Size the limit against the peak working_set during cache sync**, not the settled value. Every KOM restart triggers a full re-LIST of all watched objects — the peak is hit on every restart.

---

### Large custom resources (50 KB CRs)

> ⚠️ _OCI cluster results — not re-run on AWS. Directionally valid for sizing custom resources._

OOMKill hit at ~27,000 CRs with the 2 Gi default limit. Rule of thumb: **+90 MB per 1,000 CRs** at this size.

| CRs watched | `heap_alloc` in Prometheus | `resources.limits.memory` to set |
| --- | --- | --- |
| 1,000 | 96 MB | 2 Gi |
| 10,000 | ~900 MB | **3 Gi** |
| ~27,000 | ~2,000 MB | ← configured 2 Gi test limit OOMs here |
| 50,000 | ~3,400 MB | **10 Gi** |

---

### Combined sizing formula

Pod and node informer caches are independent — memory is additive. Validated by direct measurement (see Node × Pod combined section above).

**Size against peak `working_set` during cache sync** — this is what the cgroup OOM limit applies to and it's hit on every KOM restart.

From direct AWS measurements (13.4 KB KWOK nodes, ~9.3 KB pods, MCR=100):

| Scale                  | Peak working_set | Recommended limit |
| ---------------------- | ---------------- | ----------------- |
| 100k nodes only        | ~19 GiB          | 29 Gi             |
| 100k pods only         | ~12 GiB          | 18 Gi             |
| 100k nodes + 10k pods  | ~20.7 GiB        | 32 Gi             |
| 100k nodes + 50k pods  | ~25.1 GiB        | 38 Gi             |
| 100k nodes + 100k pods | ~28.8 GiB        | **44 Gi**         |

```
# Sizing formula (based on 13.4 KB nodes, ~9.3 KB pods)
peak_ws_GiB  ≈  (nodes_watched / 100000 × 19)
             +  (pods_watched  / 100000 × 12)

limit_Gi  =  max(ceil(peak_ws_GiB × 1.5), 2)
```

The formula is intentionally simple and conservative. At 100k nodes plus 100k pods it estimates 31 GiB peak and a 47 GiB limit, while the direct combined measurement was 28.8 GiB and produced the 44 GiB recommendation. Prefer a measured startup peak for a known workload; use the formula when no combined measurement is available.

---

## MB-2.2 — Pod Churn Rate Sweep

Policy: `NodeNotReady` (Node watch) + `PodFailed` (namespace-scoped Pod watch). `maxConcurrentReconciles=1`. Churn: steady create/delete of pods on KWOK nodes. _Actual delivery rates were limited by test harness throughput (kubectl proxy), not KOM._

| Scenario | Target | Actual rate | Reconcile/s | P50 (ms) | P90 (ms) | P99 (ms) | Queue depth | CPU% | Heap (MB) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| C1 | 10 pods/s | ~8/s | 12.5 | 3.0 | 6.9 | 9.7 | 0 | 1.0 | 32.5 |
| C2 | 50 pods/s | ~28/s | 48.8 | 3.1 | 7.4 | 9.8 | 1,256 (peak) | 3.1 | 122.7 |
| C3 | 200 pods/s | ~80/s | 126.3 | 2.6 | 4.7 | 8.9 | 19,851 (avg) | 7.7 | 410.5 |

**Key findings:**

- **P99 latency is flat at 9–10 ms across all churn rates** — KOM is not latency-bound at these rates with MCR=1.
- **Queue depth is the leading saturation indicator.** At C2, queue peaked at 2,508 then drained. At C3, it grew to 24,702 and hadn't cleared by run end — KOM is falling behind at ~80+ pods/s.
- **CPU scales linearly** with effective throughput: 1% → 3% → 8%.
- **Throughput ceiling observed at ~290 rec/s** at C3 tail. The knee is above 100 pods/s delivered.
- 0 reconcile errors across all three scenarios.

---

## MB-2.3 — Watch Restart Storm

### At 10,000 pods (100 KWOK nodes)

Pre-restart baseline: 391 MB heap.

| Elapsed   | State                       | Heap (MB) | Queue    | Tput/s  |
| --------- | --------------------------- | --------- | -------- | ------- |
| 0 s       | Restart triggered           | 374       | 0        | 2.3     |
| +16 s     | Pod Ready                   | 377       | 19,204   | 125.5   |
| +30–70 s  | Syncing                     | 451–622   | 13k–1.5k | 283–296 |
| **+84 s** | **Queue = 0, fully synced** | 570       | 0        | 224     |

| Metric                  | Value     |
| ----------------------- | --------- |
| Time to pod Ready       | 16 s      |
| Re-list + sync duration | 68 s      |
| Total recovery          | **84 s**  |
| Peak queue depth        | 19,204    |
| Peak throughput         | 296 rec/s |
| Peak P99 latency        | 9.8 ms    |
| Event loss              | **0**     |

### At 10,000 nodes (K12 bottleneck)

Pre-restart baseline: 557 MB heap.

> **Environment note:** This restart timeline is the slow-API/OCI result, not the direct in-cluster AWS EKS result. It is retained because it exposes the startup algorithm's sensitivity to per-request latency. The later EKS run is summarized under B1.

| Elapsed    | State                       | Heap (MB) | Queue | Tput/s |
| ---------- | --------------------------- | --------- | ----- | ------ |
| 0 s        | Restart triggered           | —         | —     | —      |
| +150 s     | Old pod resync burst        | 421       | 9,336 | 219    |
| +480 s     | Old pod resync burst (2nd)  | 511       | 8,102 | 216    |
| **+600 s** | **New pod Ready (10 min!)** | 416       | 8,754 | 224    |
| +647 s     | **Fully synced**            | 420       | 0     | 0      |

| Metric                     | 100 nodes | 10,000 nodes |
| -------------------------- | --------- | ------------ |
| Time to pod Ready          | 16 s      | **600 s**    |
| Re-list + sync after Ready | 68 s      | 47 s         |
| Total recovery             | 84 s      | **647 s**    |

**Root cause (K12):** KOM first LISTs Nodes, then calls `GetMatches` for every Node to restore policy match state. In the current implementation, `LoadAllMatches` therefore performs one LIST plus O(N) per-node reads. At 10k nodes: `10,000 × ~60 ms/node = 600 s`. The informer re-list itself (once Ready) only takes 47 s—the pre-Ready state-loading phase scales linearly with node count and per-read latency.

State loading runs once for every registered GVK controller. A deployment with both Node and Pod policies therefore repeats the full Node state-loading pass for each controller, even though all controllers restore from the same Node annotations.

**During the 600s startup window** the old pod stays alive and its 5-minute resync fires twice, adding compounding queue bursts before the new pod is even ready.

The readiness endpoint is a simple ping and does not assert that informer caches have synchronized. Treat "Pod Ready" and `k8s_object_monitor_cache_sync_duration_seconds` as separate milestones when analyzing restart recovery.

---

## MB-2.4 — ResyncPeriod Spike

Setup: ~12,000–13,000 objects (10k bench pods + accumulated churn), `resyncPeriod=1m`, MCR=1.

> **Provenance:** these are manually captured measurements. The current `benchmarks/kom.py` advertises a resync scenario but does not implement a `_run_resync()` path; `scripts/kom_benchmark.py` only calculates a theoretical saturation table. This scenario is therefore not reproducible from the checked-in automation without an additional manual step.

Each resync re-queues all watched objects simultaneously. Three consecutive cycles captured:

| Resync cycle | Objects re-queued | Peak queue | Tput/s (drain) | P99 (ms) | Peak CPU% |
| --- | --- | --- | --- | --- | --- |
| Cycle 1 (+26 s) | 12,341 | 22,960 | 234 | 9.2 | 11.5 |
| Cycle 2 (+75 s) | 12,772 | 24,440 | 236 | 9.7 | 11.2 |
| Cycle 3 (+134 s) | 12,404 | 23,635 | 226 | 9.8 | 11.2 |

**At `resyncPeriod=5m` (default):** 12k objects × 60s/230 rec/s = 52s to drain — leaves 4+ min of idle time. Comfortable.

**At `resyncPeriod=1m`:** 12k objects need 52s to drain out of 60s. Queue never fully clears between resyncs at this scale.

**P99 latency is completely flat at 9–10 ms** during resync bursts — the reconciler processes resync events identically to watch events.

**Safety formula:**

```
safe_max_objects = throughput_per_s × resync_period_s × 0.8
                 = 230 × 300 × 0.8 ≈ 55,000 objects  (MCR=1, default 5m resync)
```

---

## MB-C2.1 — CEL Predicate Complexity

> ⚠️ _OCI cluster results — not re-run on AWS. Directionally valid (CEL is CPU-bound; throughput numbers may differ slightly on AWS)._

Setup: ~30 pods/s churn, namespace-scoped `benchmark` Pod watch, MCR=1.

| Predicate | Tput/s | P50 (ms) | P99 (ms) | CPU% | Description |
| --- | --- | --- | --- | --- | --- |
| simple | 291.6 | 2.5 | 7.5 | 11.4 | `phase == "Running"` |
| medium | 211.1 | 2.6 | 7.5 | 8.6 | phase + label key existence |
| complex | 206.9 | 3.1 | 9.8 | 9.0 | phase + conditions filter + label prefix scan |
| **catchall** | **6.5** | **177.7** | **248.2** | **1.3** | `has(resource.metadata.name)` |

**Simple → complex: 30% throughput drop**, P99 rises 7.5 → 9.8 ms. Array traversal (`conditions.filter`) and map iteration (`labels.exists`) are measurably expensive but manageable.

KOM compiles each policy expression to a CEL AST during initialization, but it constructs a CEL program from that AST inside every evaluation. Evaluations are also serialized by the environment mutex. Predicate complexity results include both expression work and that per-evaluation setup.

**Catch-all is a cliff:** 97% throughput drop, P50 jumps 72× (2.5 → 178 ms). **Not** from CEL cost—`has(resource.metadata.name)` is trivial. The cost is **annotation writes**: every pod that first matches triggers `annotationMgr.AddMatch()` (about 200 ms in this run). The manager reads the Node, updates the policy-match annotation, and sends a Kubernetes `Update` with conflict retry. With 26k+ pods all matching, KOM creates 26k state-transition updates. CPU drops to 1.3% because KOM is blocked on I/O, not CPU.

After the initial match storm settles, catchall would return to normal throughput. The 248 ms P99 is the first-time transition cost only.

---

## MB-C2.2 — nodeAssociation with lookup()

**Setup:** AWS EKS, `phase == "Running"` predicate, ~26 pods/s churn, 100 KWOK nodes (13.4KB each).

| Association | Tput/s | P50 (ms) | P99 (ms) | CPU% |
| --- | --- | --- | --- | --- |
| None (default — `obj.GetName()`) | 26.0 | 4.1 | 9.9 | 2.6% |
| Direct CEL: `resource.spec.nodeName` | **26.1** | 4.4 | 10.0 | 2.6% |
| `lookup('v1','Node','',resource.spec.nodeName).metadata.name` | 26.1 | **12.0** | **24.8** | **4.4%** |

**Direct CEL has no measurable overhead in this run**—throughput, latency, and CPU are effectively identical to the no-association case.

**`lookup()` adds ~8ms P50 and ~15ms P99** in this run. The implementation uses the manager's cached controller-runtime client, so this should not be described as one unconditional live API GET per reconcile. The added time includes cached object retrieval, unstructured conversion, CEL work, and contention on the CEL environment's global evaluation mutex. A cache miss or a client configuration that bypasses the cache could still make API latency relevant, but the current code path is cache-backed.

**CPU increases from 2.6% → 4.4%** with `lookup()`. Throughput does not decrease because the queue stays empty at this churn rate.

---

## MB-C2.3 — Namespace-scoped vs Cluster-scoped Watches

> ⚠️ _OCI cluster results — not re-run on AWS. Directionally valid (the 84% throughput drop from system pod noise is structural, not cluster-specific)._

Setup: `phase == "Running"` predicate, ~30 pods/s churn in `benchmark` namespace.

| Scope | Tput/s | P50 (ms) | P99 (ms) | CPU% | Heap (MB) |
| --- | --- | --- | --- | --- | --- |
| Namespace-scoped (`namespace: benchmark`) | 115.0 | 7.4 | 10.0 | 10.9 | 190 |
| Cluster-scoped (no namespace) | **18.3** | 3.1 | 9.8 | 1.3 | 142 |

**Cluster-scoped drops throughput 84%.** With cluster scope, KOM's informer watches all pods across the entire cluster (~500+ system pods from kube-system, prometheus, nvsentinel, etc.). Every system pod event triggers a reconcile that evaluates the CEL predicate — the reconcile budget is consumed by noise unrelated to the workload being monitored.

**Counterintuitive:** cluster-scoped heap is _lower_ (142 vs 190 MB) because the snapshot was taken before all churn pods had accumulated in the cache.

---

## MB-C2.4 — maxConcurrentReconciles Sweep

**Setup:** AWS EKS, `PodFailed` policy (benchmark namespace), ~26 pods/s churn, 30s warmup, 60s measurement, queue drained to 0 before each MCR value.

| MCR | Tput/s | P50 (ms) | P99 (ms) | CPU% | Queue |
| --- | ------ | -------- | -------- | ---- | ----- |
| 1   | 25.9   | 3.0      | 13.9     | 2.5% | 0     |
| 2   | 26.1   | 3.1      | 15.7     | 2.4% | 0     |
| 4   | 26.2   | 3.0      | 15.8     | 2.4% | 0     |
| 8   | 26.0   | 3.1      | 14.3     | 2.3% | 0     |

**All MCR values are identical** — throughput, latency, and CPU are flat across 1→8. Queue stays at 0 throughout. This confirms: **MCR only matters when the queue is persistently non-zero.** At ~26 pods/s effective churn, the single reconcile goroutine (MCR=1) keeps up easily with 0 queue depth.

**When does MCR matter?** In these runs, only above ~150 pods/s sustained churn (where the queue starts accumulating). Below that measured load, MCR=1 was sufficient. Above saturation, MCR=2 is the first useful step. Higher values can show diminishing returns because CEL evaluation is globally serialized and match-state/annotation work also contains shared or I/O-bound sections; the benchmark does not isolate `matchStatesMu` as the sole cause.

---

## Bottlenecks

Eight bottlenecks were identified, broadly ordered by operational severity.

---

### B1 — Startup per-node annotation reads (K12) `CRITICAL on slow API servers`

**Root cause:** For each registered GVK controller, KOM LISTs Nodes and then reads each Node again to restore policy match state. Cost is approximately `controller count × node count × per-node read latency`.

| Environment | Node count | Time to Ready | Per-node GET cost |
| --- | --- | --- | --- |
| OCI (through Teleport proxy) | 10,000 | **600 s** | ~60ms — Teleport overhead |
| AWS EKS (direct in-cluster) | 100,000 | **5 s** | ~0.05ms — masked by 15s probe |

**The bottleneck is the combination of read count and read latency.** On AWS EKS with direct in-cluster access (approximately 0.05 ms/read), the measured 100k-node state-loading phase completed in 5 seconds. On OCI through Teleport (approximately 60 ms/read), 10k nodes took 600 seconds.

The readiness probe is a simple `/readyz` ping, not a cache-sync check. The 15-second probe delay can hide a fast state-loading phase, while `k8s_object_monitor_cache_sync_duration_seconds` should be used to identify when informers actually synchronized.

**K12 is severe on any cluster with high API server latency** (proxy, VPN, cross-region, shared/overloaded API server). It is negligible on fast direct-access clusters like EKS.

**During the startup window**, the old pod continues running and its resync timer fires — creating queue bursts before the new pod is ready.

**Fix (K12):** Parse all policy-match annotations directly from the existing Node LIST instead of issuing a follow-up GET for each Node. The amount of data and local iteration remain O(N), but API round trips fall from O(N) to one LIST.

**Workaround now:** Keep `terminationGracePeriodSeconds` high and `maxUnavailable=0` so the old pod stays alive through the new pod's full startup.

---

### B2 — Memory ceiling with a 2 GiB test limit `CRITICAL at large object counts`

**Root cause:** Go's unstructured informer cache expands raw JSON substantially in heap because objects are represented as nested maps and interfaces. In the tests that used a 2 GiB limit, that configured limit was a hard wall. The approximately 4× expansion is an empirical estimate, not a constant guaranteed by the code.

The Helm chart's actual default is **256Mi**, not 2 GiB. The benchmark raised the limit—first to 2 GiB in the OOM-boundary experiments, and to 32 GiB for large scale sweeps. The 2 GiB value elsewhere in this report is a recommended minimum or a test configuration, never the chart default.

**Why KOM uses more memory than typed-informer controllers (e.g. fault-quarantine):** KOM stores every watched object as `unstructured.Unstructured` — a `map[string]interface{}` where every key string and value is a separate heap allocation per object. A 13.4 KB node JSON expands to ~40–80 KB in heap because field names are not shared across objects. Typed informers (using generated Go structs) pack fields inline with no per-object key allocations, using 2–3× less memory for the same object count. This is an inherent trade-off of KOM's design — CEL predicates require runtime access to arbitrary fields across any Kubernetes resource type, which requires unstructured deserialization. There is no way to avoid this without abandoning the generic policy model.

| Object type | Size | `working_set` hits 2 GiB at (peak, cache sync) |
| --- | --- | --- |
| Pods (~9.3 KB, AWS) | ~9.3 KB | ~15,000–20,000 pods |
| Nodes (~13.4 KB, AWS c8a.16xlarge) | ~13.4 KB | ~10,000–15,000 nodes |
| 50 KB CRs (OCI) | 50 KB | ~10,000 CRs |

**What `resources.limits.memory` actually limits:**

`resources.limits.memory` sets the **cgroup working set limit** (`container_memory_working_set_bytes` — what `kubectl top` shows). This is NOT the same as `go_memstats_heap_alloc_bytes`. The relationship:

```
heap_alloc  ≤  heap_inuse  ≤  heap_sys  ≈  working_set
              (live objects) (arena in use) (arena + stacks + runtime)
```

Go retains freed heap memory in its arena for reuse (`heap_idle`). A burst that allocates 2 GB and then frees most of it leaves the working set near 2 GB even though `heap_alloc` is much lower. **The limit must be sized for the working set, not the heap_alloc.**

**Two-component memory model (validated against a production cluster, 249 GPU nodes + 496 pods, 22.8M lifetime reconciles):**

| Component | Size | Driver |
| --- | --- | --- |
| Informer cache (`heap_alloc` from formula) | ~85 MB | Object count × size |
| GC overhead (live heap beyond cache) | ~777 MB | Reconcile churn accumulation |
| Go arena retained from load peaks (`heap_idle`) | ~1,090 MB | Historical bursts (resync, restart) |
| **Working set (`container_memory_working_set_bytes`)** | **2,224 MiB** | What OOMKill triggers on |

The formula predicts `heap_alloc` (the cache floor). Set the pod limit on the working set:

Use the combined sizing formula from the Node × Pod section above. Size against **peak working_set**, not heap_alloc.

---

### B3 — Catch-all / broad policy annotation write storm `HIGH (first-match burst)`

**Root cause:** First match on any object triggers `annotationMgr.AddMatch()`—implemented as a read/modify/`Update` of the associated Node with conflict retry. A policy that matches everything creates one Node-annotation update attempt per newly matched object at startup/restart; actual concurrency is bounded by MCR.

**Measured:** catchall policy → 6.5 rec/s (vs 291 rec/s for simple), P99 = 248 ms.

**Fix:** Write predicates that match only genuinely unhealthy objects. Avoid patterns like `has(resource.metadata.name)`. Any policy matching >10% of watched objects will trigger a significant annotation write burst on first deployment.

---

### B4 — Cluster-scoped Pod watch consumes reconcile budget on system pods `HIGH`

**Root cause:** Without `namespace` set, the Pod informer is cluster-wide. Every kube-system, prometheus, and nvsentinel pod event triggers a reconcile — drowning out the signal.

**Measured:** cluster-scoped → 18.3 rec/s vs namespace-scoped → 115 rec/s (84% drop) on identical workload.

**Fix (H2):** Always set `namespace` on Pod and Kubernetes Event policies. Split multi-namespace needs into multiple policies.

---

### B5 — Queue saturation above ~80–100 pods/s churn `MEDIUM`

**Root cause:** Single reconcile goroutine (MCR=1) sustains ~230–290 rec/s. Above ~80–100 pods/s of delivered churn, the queue grows faster than it drains.

**Measured:** At ~80 pods/s, queue grew to 24,702 and didn't clear by run end.

**Fix (H5):** Increase `maxConcurrentReconciles` to 2 if queue is consistently non-zero. Benchmark higher values before adopting them: CEL evaluation is globally serialized, while transition handling also includes state locks, gRPC publication, and Node annotation I/O.

> **⚠️ Critical framing:** The reconcile processing P99 (9–10 ms) is **not** the time-to-detection. The true detection latency is `queue_wait + processing_time`. Under any queue backlog, detection latency is dominated by queue wait:
>
> | Scenario | Reconcile P99 | Queue wait P99 | **True detection P99** |
> | --- | --- | --- | --- |
> | Steady state (100 nodes, 5m resync) | 10 ms | ~1 s | **~1 s** |
> | Heavy churn (C3, ~80 pods/s) | 9 ms | ~10 s | **~10 s** |
> | 10k node restart storm | 10 ms | **~96 s (1.6 min)** | **~96 s** |
>
> The `workqueue_queue_duration_seconds` histogram is the correct metric for detection latency SLOs — not `controller_runtime_reconcile_time_seconds`. Monitor `histogram_quantile(0.99, rate(workqueue_queue_duration_seconds_bucket{job="kubernetes-object-monitor"}[1m]))` to understand how long faults wait before KOM acts on them.

---

### B6 — ResyncPeriod burst exhausts throughput at large object counts `MEDIUM`

**Root cause:** Every resync re-queues all N cached objects at once. If `N / throughput_per_s > resync_period_s`, the queue never drains between resyncs.

**Safety formula:**

```
safe_max_objects = throughput_per_s × resync_period_s × 0.8
                 = 230 × 300 × 0.8 ≈ 55,000 objects  (MCR=1, default 5m resync)
```

**Fix (H5):** Never set `resyncPeriod` below `object_count / throughput_per_s`. Lengthen the period or increase MCR as object count grows.

---

### B7 — Cache sync timeout on restart at high node counts `HIGH at 10k+ nodes`

**Root cause:** Controller-runtime waits for informer caches to complete their initial LIST+sync before starting reconciliation. The code on `main` did not set `ctrlcontroller.Options.CacheSyncTimeout`, so controller-runtime's effective **2-minute default** applied. That was insufficient at 100k nodes (13.4 KB each, or roughly a 1.34 GB aggregate LIST payload). KOM exited with `Fatal error: timed out waiting for cache to be synced` and crash-looped.

**Observed:** At 100k nodes, KOM reliably crash-looped with the 2-minute timeout. The Pod informer also appears timed out in logs even though it would sync in seconds — because the timeout is global, not per-informer. The crash log says `failed to wait for pod caches to sync` (last error in the chain) even though the node LIST is the actual bottleneck.

**This bottleneck does NOT appear during a live-running KOM** — informers are maintained via watch (incremental events), no re-LIST needed. It surfaces only on KOM restart (rolling update, OOM kill, pod eviction). During normal operation KOM built up to 100k nodes incrementally with no issues.

**Fix validated by the benchmark:** the local code change added `--cache-sync-timeout` with an explicit **10-minute default**, passed that value to each controller's `ctrlcontroller.Options.CacheSyncTimeout`, and added a cache-sync duration metric. Image `20260727052921` was rebuilt from those changes and used for the subsequent scale run. Because the Helm deployment does not override the flag, the rebuilt binary's 10-minute default applied.

This fix is not on `main` in the repository state used for this report. On `main`, no KOM flag is defined and the controller-runtime default still applies.

| Nodes | Approx LIST size | Required timeout |
| --- | --- | --- |
| 10,000 | ~134 MB | < 2 min (the tested 2-minute default was adequate) |
| 50,000 | ~670 MB | ~5–8 min |
| 100,000 | ~1.34 GB | 10m explicit default used by the rebuilt image |

---

### B8 — KWOK heartbeat-driven queue saturation at high node counts `MEDIUM (benchmark-specific)`

**Root cause:** KWOK sends periodic node status updates (heartbeats) to keep simulated nodes alive. With many KWOK nodes, the heartbeat injection rate exceeds KOM's reconcile drain rate at MCR=1.

**Measured at 100k KWOK nodes:** Add rate = 143 events/s, drain rate (MCR=1) = ~7 events/s — queue grows at 136 events/s and never drains.

**Under sustained queue saturation, memory pressure rose until KOM was OOMKilled.** Go's GC target is approximately 2× live heap—if live heap is 15 GiB, the runtime can permit total heap to approach 30 GiB before collection. With work injected faster than it drains, queued keys, cached objects, and short-lived reconcile allocations keep memory elevated. The observed working_set crossed the configured limit, confirmed through `kube_pod_container_status_last_terminated_reason`.

**How to detect:** `sum(rate(workqueue_adds_total{job="kubernetes-object-monitor",name="node"}[1m]))` > `sum(rate(workqueue_work_duration_seconds_count{job="kubernetes-object-monitor",name="node"}[1m]))` means the queue is growing.

**Fix:** `maxConcurrentReconciles=100` raises drain rate to ~700 events/s, well above KWOK's 143 events/s. Queue clears within minutes.

**Production relevance:** Real nodes do not send KWOK-style periodic heartbeats at this rate. This bottleneck is benchmark-environment specific. In production, `MCR=1` is sufficient unless churn rate exceeds ~150 pod events/s (see B5).

---

## Configuration Guidance

| Setting | Default | Recommendation |
| --- | --- | --- |
| `resources.limits.memory` | 256Mi | Size against **peak `container_memory_working_set_bytes`** during cache sync (not settled heap_alloc). At 100k nodes + 100k pods: peak ~28.8 GiB → **44 Gi**. Use `max(peak_ws_GiB × 1.5, 2) Gi`. Settled working_set is not the right baseline — every restart hits the peak. |
| `--cache-sync-timeout` | No KOM flag on `main` (effective controller-runtime default: 2m); local fix defaults to 10m | The 2m behavior failed at 100k. The rebuilt benchmark image used the local fix's **10m** default successfully. Confirm startup time with `k8s_object_monitor_cache_sync_duration_seconds`. |
| `maxConcurrentReconciles` | 1 | **100** was required for high-node-count KWOK heartbeat load. In production, increase first to 2 only when add rate exceeds drain rate or queue depth grows persistently; benchmark larger values under the real workload. |
| `[policies.resource].namespace` | unset | **Always set** for Pod/Event policies |
| `resyncPeriod` | 5m | Never set below `object_count / 230`; default safe to ~55k objects |
| Policy predicates | — | Match <10% of watched objects; avoid catch-all patterns |

> **Shared-GVK scope warning:** controllers and caches are grouped by GVK. If one enabled Pod policy omits `namespace`, the Pod cache is cluster-scoped even when other Pod policies specify namespaces.

---

## Pass/Fail Summary (v1.15.0)

| Criterion | Result |
| --- | --- |
| No crash or OOM at expected load (50 pods/s, 10k objects) | ✅ passed |
| Queue depth does not grow unboundedly at expected load | ✅ passed (C2: peak 2,508, drained) |
| No permanent event loss | ⚠️ No loss observed in the restart scenario and 0 controller-runtime reconcile errors, but those errors alone do not prove end-to-end delivery; KOM publish/evaluation error metrics must also remain zero |
| P99 reconcile processing time ≤ 1 s | ✅ Work-duration P99 = 7–10 ms; this is not end-to-end detection latency |
| Automatic recovery after restart (no manual intervention) | ✅ 84s at 10k pods / 647s at 10k nodes |
| Limiting resource identified at saturation | ✅ Memory (2 GiB) and startup reads (K12) |
| Degradation visible in metrics before crash | ✅ Queue depth and heap are leading indicators |

---

## Methodology Gaps

Known gaps vs the requirements in issue #1512 and the shared simulation/observability standards.

| Gap | Impact | Status |
| --- | --- | --- |
| **V1 — Queue wait not reported** (fixed above) | Reconcile P99 = 9ms is misleading — true detection latency is 1s–96s under backlog | Added `workqueue_queue_duration_seconds` to B5; use this for SLOs |
| **S1 — KWOK saturation not distinguishable** | Can't tell if 600s startup bottleneck includes KWOK lease-renewal saturation (parallelism=4) | `cadvisor` not scraping kube-system; no KWOK resource limits set. Mitigation: creation rate was consistent (239 nodes/s end-to-end), suggesting KWOK was not saturated. Fix: set resource limits on kwok-controller and enable cadvisor scraping for kube-system |
| **S2 — No GPU labels on KWOK nodes** | Doesn't affect `NodeNotReady` (checks only Ready condition), but policies that key on `nvidia.com/gpu` labels would behave differently | Low impact for current policy set. Fix: add `nvidia.com/gpu=8`, driver/DCGM labels to KWOK node template |
| **V3 — Single-shot measurements** | Results are point estimates; no min/max spread | Our results were very stable (P99 flat across all runs). Low discovery risk. Fix: run each scenario 3× and report median/spread |
| **V4 — KOM not pinned to a node** | Noisy neighbor could inflate latency | Results look clean (no variance spikes). Low impact for this service. Fix: add `nodeSelector` + Guaranteed QoS to KOM deployment |
| **O3 — Process-level vs container-level resource metrics** | `process_cpu_seconds_total` vs `container_cpu_usage_seconds_total` differ by cgroup overhead | Difference is ~5% for a simple Go service. Fix: enable cadvisor scraping of nvsentinel namespace |
| **O4 — Controller-runtime success does not cover per-policy failures** | `Reconcile` records a policy error and continues, then returns success; controller-runtime's error result can remain zero when evaluation or publication failed | Include `k8s_object_monitor_policy_evaluation_errors_total`, `k8s_object_monitor_health_events_publish_errors_total`, `k8s_object_monitor_reconciliation_errors_total`, and logs in future pass/fail criteria |
| **O5 — Working set is not captured by the harness** | Memory recommendations rely heavily on manually collected cgroup data that is absent from result JSON | Add `container_memory_working_set_bytes` and peak-window timestamps to `framework/prometheus.py` snapshots |
| **A1 — Resync spike is not automated** | MB-2.4 cannot be reproduced by selecting a harness scenario even though `resync` appears in the module documentation | Implement `_run_resync()` or label the scenario manual in generated output |
| **A2 — Result metadata does not identify the rebuilt image** | The cache-timeout fix was supplied by a locally rebuilt image, but result JSON records only `v1.15.0`, making pre-fix and post-fix runs hard to distinguish | Record image tag/digest, Git commit, and effective container arguments with each result |

---

## Relevant Code Locations

| Path | Purpose |
| --- | --- |
| `health-monitors/kubernetes-object-monitor/pkg/metrics/metrics.go` | KOM Prometheus metrics |
| `health-monitors/kubernetes-object-monitor/pkg/initializer/initializer.go` | GVK grouping, namespace-scoped caches, resync, startup state loading, concurrency, and cache-sync timeout |
| `health-monitors/kubernetes-object-monitor/pkg/controller/reconciler.go` | Reconcile loop, transition detection, state locking, publication, and annotation calls |
| `health-monitors/kubernetes-object-monitor/pkg/policy/evaluator.go` | Compiled predicate and node-association selection |
| `health-monitors/kubernetes-object-monitor/pkg/cel/environment.go` | CEL execution mutex and cache-backed `lookup()` |
| `health-monitors/kubernetes-object-monitor/pkg/annotations/manager.go` | Node annotation persistence, startup LIST plus per-node reads, and transition updates |
| `distros/kubernetes/nvsentinel/charts/kubernetes-object-monitor/values.yaml` | Defaults for concurrency, resync, policies, and resources |
| `distros/kubernetes/nvsentinel/charts/kubernetes-object-monitor/templates/deployment.yaml` | Flags passed by Helm, metrics port, and probes |
| `tests/scale-tests/benchmarks/kom.py` | Automated scenario orchestration and temporary MCR/resync changes |
| `tests/scale-tests/framework/prometheus.py` | Metrics captured into benchmark snapshots; currently heap but not container working set |
| `tests/scale-tests/scripts/kom_benchmark.py` | Standalone benchmark helper and theoretical resync table |
| `tests/scale-tests/` | Benchmark report, manifests, results, and supporting scripts |
