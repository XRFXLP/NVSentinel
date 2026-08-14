# Fault-Quarantine — Microbenchmark Results (v1.13.1)

Baseline measurements for `fault-quarantine` v1.13.1. See `docs/nvsentinel-at-scale/scale-issues.md` for the full scale analysis and action item list this benchmark validates.

---

## Test Environment

| Component | Detail |
|---|---|
| Cluster | AWS EKS us-east-1 (c8a.16xlarge worker nodes) |
| FQ version | v1.13.1 |
| FQ config | `--dry-run=false`, `--circuit-breaker-enabled=false` |
| FQ resources | `limits: 1 CPU / 32Gi RAM` |
| MongoDB | 3-member replica set, in-cluster |
| KWOK nodes | Simulated via KWOK against real API server |
| Real worker nodes | 6 schedulable nodes (32 CPU / 128 GiB each) |
| Platform-connectors | 1 per real worker node (5 total) |

---

## Metrics Used

All metrics from FQ's `:2112/metrics` endpoint, scraped by Prometheus.

| Metric | What it measures |
|---|---|
| `container_memory_working_set_bytes` | OS-level RSS — what the cgroup OOM limit applies to |
| `go_memstats_heap_alloc_bytes` | Go heap in use |
| `fault_quarantine_*` | FQ-specific business metrics |
| `workqueue_depth` | Items waiting in the work queue |
| `workqueue_queue_duration_seconds` | True detection latency — time from event to processing start |

---

## MB-FQ-1 — Memory: Node Informer

Fault-quarantine holds a **typed** Node informer (uses generated Go structs, not `unstructured.Unstructured`). This makes it significantly more memory-efficient than KOM for the same node count.

### Node informer memory sweep

| Nodes | `working_set` | Recommended limit |
|---|---|---|
| 10,000 | ~0.37 GiB (397,029,376 bytes) | **2 Gi** |
| 50,000 | ~1.45 GiB (1,557,270,528 bytes) | **3 Gi** |
| 100,000 | ~3.70 GiB (3,969,904,640 bytes) | **6 Gi** |

Rule of thumb: **~370 MB per 10,000 nodes** for FQ's typed Node informer.

**Comparison with KOM (unstructured informer):**

| Nodes | FQ working_set | KOM working_set (settled) | Ratio |
|---|---|---|---|
| 10,000 | ~0.37 GiB | ~1.1 GiB | 3× less |
| 50,000 | ~1.45 GiB | ~6.5 GiB | 4.5× less |
| 100,000 | ~3.70 GiB | ~10.7 GiB | 2.9× less |

FQ uses **3–4× less memory** than KOM for the same node count because it uses typed Go structs (field names are in the struct definition, not allocated per-object). KOM's higher cost is inherent to its generic CEL policy model which requires unstructured deserialization — it cannot be avoided without changing the architecture.

---

## MB-FQ-2 — Cordon Throughput (K2) ✅ MEASURED

### How it works

When FQ decides to quarantine a node it calls `UpdateNode()` which does:
1. **GET** the full Node object from the Kubernetes API server
2. Apply cordon (`spec.unschedulable=true`), taints, and quarantine annotations
3. **UPDATE** the full Node object back to the API server

That is **2 API calls per cordon**. client-go's rate limiter defaults to **QPS=5, Burst=10** — a maximum of 5 sustained API calls per second with an initial burst of 10. FQ inherits these defaults without any configuration knob.

```
sustained cordon rate = QPS / calls_per_cordon = 5 / 2 = 2.5 nodes/s
```

The burst of 10 allows ~5 immediate cordons before the rate limiter engages. After that, the rate is flat at 2.5 nodes/s regardless of how many events are queued.

### Measurement

**Injection:** `fq-event-injector` fleet-storm pattern, 1 fatal event per N distinct KWOK nodes, via platform-connector → MongoDB → FQ change stream.

| N nodes | Total time | Sustained rate | Avg quarantine latency |
|---|---|---|---|
| 100 | ~42s | **2.4 nodes/s** | ~17s |
| 1,000 |  ~7 min | **2.4 nodes/s** | ~110s |

The throughput is **dead flat at 2.4 nodes/s** across both batch sizes — no degradation, no burst effect visible at 15s polling intervals. The client-go rate limiter ceiling dominates entirely.

### Impact

| Fleet size | Time to cordon all |
|---|---|
| 100 nodes | ~42 seconds |
| 1,000 nodes | ~7 minutes |
| 10,000 nodes | ~70 minutes |
| 100,000 nodes | ~11 hours |

During a fleet-wide fabric failure, affected nodes are not isolated within the SLA window. The operator has no lever to increase the rate without a code change.

### Circuit breaker interaction

The circuit breaker trips when cordons in the last `duration` window exceed `percentage` of total nodes:

```toml
[circuitBreaker]
percentage = 50    # default
duration   = "5m"
```

At 2.4 nodes/s, the maximum cordons in a 5-minute window is **720**. For large clusters this means the circuit breaker **never trips** even in a total fleet failure:

```
cordons_in_5min = 2.4 × 300 = 720
trip condition  = 720 > total_nodes × 0.50
never trips when total_nodes > 1,440
```

For the circuit breaker to be meaningful at scale, `duration` must be long enough to accumulate the target percentage of cordons at the actual cordon rate:

```
required_duration > (CB_percentage/100 × total_nodes) / cordon_rate_per_s

10k nodes, 50%:  (0.50 × 10,000) / 2.4 = 2,083s → need duration > 35 min
50k nodes, 50%:  (0.50 × 50,000) / 2.4 = 10,417s → need duration > 2.9 hours
```

With the default `duration=5m`, the circuit breaker provides **no fleet protection** for clusters larger than ~1,400 nodes — it can never accumulate enough cordons to trip regardless of how severe the fault is.

### Fix

**K2:** Expose `qps` and `burst` as Helm values for FQ's Kubernetes client. Size them from an explicit fleet write budget (e.g. "cordon 1,000 nodes within 60 seconds" requires QPS=33).

**K3:** Replace GET + full UPDATE with `PATCH` using only FQ-owned fields and a field manager. PATCH removes the GET round-trip, halving API call count per cordon → doubles throughput at the same QPS setting.

**File:** `fault-quarantine/pkg/informer/k8s_client.go:61-68` (client-go QPS/burst), `k8s_client.go:142-170` (UpdateNode GET+UPDATE path)

---

## MB-FQ-3 — Change-Stream Consumer Lag: CappedPositionLost ✅ REPRODUCED (×2)

### Invariant

FQ processes the MongoDB change stream serially. Its cursor advances only when it finishes processing an event and calls `MarkProcessed`. The oplog is a fixed-size capped collection (990 MB). When new writes fill the oplog past FQ's cursor position, MongoDB invalidates the cursor.

The time at which this happens:

```
T_trigger = oplog_size / (net_rate × B)

  net_rate   = R_inject - R_fq        (net oplog advancement rate past FQ's cursor)
  oplog_size = 990 MB = 1,038,090,240 bytes
  B          = oplog entry size per event
               (~1,210 bytes for production events with full OCI metadata;
                ~969 bytes for the synthetic test events used here)

  STORE_ONLY events (processingstrategy=2):
    R_fq = 0  →  net_rate = R_inject     (cursor frozen, full injection rate counts)

  EXECUTE_REMEDIATION events (processingstrategy=0):
    R_fq > 0  →  net_rate = R_inject - R_fq  (FQ partially offsets injection)
```

`net_rate` is the effective rate at which the oplog outpaces FQ's cursor. FQ keeps its cursor current only for the events it actually processes; everything else pushes the oplog window past the cursor position.

---

### Reproduction 1 — EXECUTE_REMEDIATION flappy storm

**Mechanism:** High-rate EXECUTE_REMEDIATION events injected via platform-connector. FQ processes them but is slowed by K8s API calls (`UpdateNode`) on every event — cordon on fatal, uncordon on healthy. FQ's drain rate falls behind the injection rate, causing the cursor to age out.

**Injection tool:** `tests/scale-tests/event-injector` (Go binary, 50 worker goroutines, batch size 200, via platform-connector gRPC).

**Pattern:** Alternating fatal/healthy events for the same 100 nodes (flappy storm).

Observed injection rate (5-second averages):

| Elapsed | Events sent | Rate (5s avg) |
|---|---|---|
| 0–5s | 52,400 | 10,480/s |
| 5–10s | 53,600 | 5,360/s |
| 10–15s | 57,400 | 3,827/s |
| 15–20s | 61,800 | 3,090/s |
| 20–25s | 68,800 | 2,752/s |
| 25–30s | 73,800 | 2,460/s |

FQ processing rate under flappy load: **~173 events/s** (K8s API bottleneck).

`CappedPositionLost` triggered at **t ≈ 244 seconds**.

---

### Reproduction 2 — STORE_ONLY noise (more realistic production scenario)

**Mechanism:** Events with `processingstrategy=2` (STORE_ONLY) injected directly into MongoDB. FQ's change stream filter **excludes** STORE_ONLY events entirely — `R_fq = 0`, FQ's cursor is completely frozen. The formula simplifies to:

```
T_trigger = oplog_size / (R_inject × B)
```

This is the production-realistic scenario: KOM runs with `processingstrategy=STORE_ONLY` and continuously injects events. FQ never reads them from the change stream, so its cursor never advances. The oplog fills past the cursor purely from noise.

**Injection tool:** `fq-injector` pod, direct pymongo insert (50 workers, batch 500), bypassing platform-connector.

Injection rate: **~32,000,000 events in ~5 minutes** (direct MongoDB, no platform-connector bottleneck).

`CappedPositionLost` triggered within **~2 minutes** of starting the flood.

```
2026-07-28T03:36:15Z  ERROR  Failed to watch change stream
  error: (CappedPositionLost) Executor error during getMore ::
  caused by :: CollectionScan died due to position in capped
  collection being deleted.
  Last seen record id: RecordId(7667417492750336179)

2026-07-28T03:36:15Z  ERROR  Application encountered a fatal error
  error: event watcher failed: event watcher terminated:
         event watcher channel closed unexpectedly
```

---

### Production impact

1. FQ crashes (fatal error, Kubernetes restarts it)
2. `recoverFromStaleResumeToken` opens a new stream from **current** oplog position
3. All HealthEvents inserted between last saved resume token and restart are **permanently skipped**
4. Fatal GPU errors in that window receive no quarantine, no drain, no remediation
5. `events_successfully_processed_total` resets to 0 — **no visibility that events were missed**

This is the most severe FQ failure mode: silent data loss with no alert.

On restart, FQ logs the cold-start reconciliation result showing quarantined nodes it recovered from the HealthEvents collection — but it has no awareness of which unprocessed events it skipped.

---

### Mitigation (not yet implemented)

- **K11 (scale-issues.md):** Increase oplog size proportional to peak event rate; explicitly size for worst-case `R_inject` including STORE_ONLY components
- Alert on `fault_quarantine_event_backlog_count` growth rate exceeding the oplog drain rate
- FQ cold-start recovery should replay missed EXECUTE_REMEDIATION events from the HealthEvents collection rather than reopening the stream from "now"
- Bound FQ's per-event K8s API call latency to prevent cursor from ageing out under flappy conditions

---

## MB-FQ-4 — Stale-State MongoDB Query Cost (C4) ✅ MEASURED

### What fires and when

FQ's startup is O(1) for normal restarts — opens a change stream and syncs the K8s node informer in ~2.8s regardless of collection size.

The O(N) collection scan from C4 fires only in the **stale-state recovery path**: when FQ restarts and finds a node has the `quarantineHealthEventIsCordoned` annotation but is NOT unschedulable (cordon was removed while FQ was down). For each such stale node, FQ runs:

```
filter: {healthevent.nodename: X, healtheventstatus.nodequarantined: {$in: ["Quarantined","UnQuarantined"]}}
sort:   {createdAt: -1}
FindOne
```

Without a secondary index on `(nodename, nodequarantined, createdAt)`, this is a full collection scan — O(N) per stale node.

### Measurement

Setup: Q=10 stale nodes (quarantined by FQ, then manually uncordoned while FQ was down), 10 matching `Quarantined` records embedded in N total events.

| N (total events) | Q (stale nodes) | Cold-start time | Notes |
|---|---|---|---|
| 0 | 0 | 2.770s | Baseline — connection setup only |
| 3,000,000 | 10 | 2.838s | Stale queries fire (confirmed by logs), no measurable difference |
| 5,000,010 | 10 | 2.850s | Still no measurable difference |

### Finding

At Q=10 and N=5M, the 10 stale-state queries complete in milliseconds — MongoDB's in-memory scan is fast enough that the O(N) cost is invisible against the ~2.1s MongoDB connection baseline.

The stale-state query cost only becomes operationally significant at:
- Very large N (tens of millions of events, which requires large oplog/PVC)
- Or large Q (many nodes manually uncordoned while FQ was down — an unusual operator action)

At production-realistic N (a few million events with 30-day TTL at modest rates) and typical Q (single-digit stale nodes per restart), the cost is negligible.

**C4 (adding secondary indexes) remains a correctness improvement** for query semantics but is not an urgent performance fix for the stale-state path at current scale.

---

## MB-FQ-5 — CEL Ruleset Cost ✅ MEASURED

### How it works

FQ evaluates all enabled rulesets **in parallel goroutines** for every event. Each ruleset evaluation runs two CEL expressions:
1. **HealthEvent rule** — checks agent, componentClass, isFatal, checkName against the event proto
2. **Node rule** — checks node labels/annotations from the in-memory Node informer cache

Since goroutines run concurrently, the per-event CEL cost = `max(slowest_ruleset)`, not the sum. Adding more rulesets only increases cost if they exceed available goroutines or if individual expressions are expensive.

### Measurement

Injected 300 non-matching events (agent=`event-generator`) per run via platform-connector. FQ evaluates all rulesets but fires no cordons — isolates CEL cost from K8s API overhead. Measured `fault_quarantine_event_handling_duration_seconds`.

| Rulesets | avg_ms | P99_ms |
|---|---|---|
| 1 (GPU) | 0.1 | 5.0 |
| 2 (GPU + NIC) | 0.1 | 5.0 |
| 4 (GPU + NIC + Syslog + CSP) | 0.1 | 5.0 |
| 50 (synthetic, all non-matching) | **0.8** | 5.0 |

### Finding

CEL evaluation is negligible — flat at 0.1ms for 1–4 rulesets, rising to only 0.8ms at 50 rulesets. P99 is dominated by the MongoDB status write (`updateNodeQuarantineStatus`), not CEL.

Parallel goroutine execution means ruleset count has no meaningful impact on detection latency at realistic scales (4–20 rulesets). CEL cost is not a bottleneck for FQ.

---

## Bottlenecks

### BFQ-1 — Cordon throughput ceiling `CRITICAL at scale`

**Root cause:** GET + full Node UPDATE per cordon, default 5 QPS / 10 burst client-go limits. Sustained cordon rate ~2.5 nodes/s.

**Impact:** 1,000 faulted nodes → ~7 min to cordon. 10,000 faulted nodes → >1 hour. Fleet-wide fabric failure is not mitigated in time.

**Fix (K2, K3):** Expose QPS/burst as Helm values. Replace GET+UPDATE with informer-backed PATCH (only FQ-owned fields). Use SSA field manager or patch precondition for conflict handling.

**File:** `fault-quarantine/pkg/informer/k8s_client.go:61-68`

---

### BFQ-2 — Serial change-stream processing → CappedPositionLost `HIGH` ✅ REPRODUCED

**Root cause:** FQ processes MongoDB change stream serially — one event at a time. When per-event processing is slow (K8s API calls for cordon/uncordon), FQ falls behind the injection rate. Once the resume token is older than the oplog window, MongoDB invalidates the cursor.

**Reproduced:** ~1,413 events/s sustained for ~244s with flappy (fatal→healthy) events triggered `CappedPositionLost`. FQ crashed and silently skipped all events in the gap on restart. See MB-FQ-3 for full reproduction details.

**Impact:** Silent data loss — fatal GPU errors in the gap window receive no cordon, no drain, no remediation. No metric indicates missed events.

**Fix (architecture A3):** Partitioned consumer groups keyed by node. Each partition processes its nodes independently. Short-term: increase oplog size (K11) and alert on backlog growth.

**File:** `store-client/pkg/datastore/providers/mongodb/watcher/watch_store.go:340-386`

---

### BFQ-3 — Stale-state collection scan `LOW (narrow fault scenario)`

**Root cause:** `cancelLatestQuarantiningEvents` does a full collection scan — filter `{nodename: X, nodequarantined: IN ["Quarantined","UnQuarantined"]}` with `sort: createdAt DESC` — without a secondary index on `(nodename, nodequarantined, createdAt)`.

**When it fires:** Only during the stale-state recovery path at startup — when a node has the `quarantineHealthEventIsCordoned` annotation but is NOT unschedulable (cordon was manually removed while FQ was down). Does NOT fire on normal restarts or during steady-state event processing.

**Measured impact:** At N=5M events and Q=10 stale nodes, 10 collection scans completed in milliseconds — not measurable against the ~2.8s connection baseline (see MB-FQ-4). Only becomes significant at very large N (tens of millions) with many stale nodes simultaneously.

**Fix (C4):** Add compound index on `(healthevent.nodename, healtheventstatus.nodequarantined, createdAt)` — reduces each scan from O(N) to O(log N).

**File:** `fault-quarantine/pkg/eventwatcher/event_watcher.go:440-547`

---

## Configuration Guidance

| Setting | Default | Recommendation |
|---|---|---|
| `resources.limits.memory` | 1Gi | **6 Gi** at 100k nodes (node informer ~3.7 GiB + headroom). Scale: ~370 MB per 10k nodes. |
| client-go QPS/burst | 5/10 | Expose via Helm (K2); size from explicit fleet write budget |
| MongoDB `maxPoolSize` | 100 | Set to 2–5 to reduce per-pod connection floor (K1) |
| MongoDB `maxConnIdleTime` | 0 (unlimited) | Set to 60s to stop connection count drift (K1) |

---

## Relevant Code Locations

| Path | Purpose |
|---|---|
| `fault-quarantine/pkg/informer/k8s_client.go:61-68` | client-go QPS/burst defaults — BFQ-1 |
| `fault-quarantine/pkg/eventwatcher/event_watcher.go:440-547` | MongoDB query patterns — BFQ-3 |
| `store-client/pkg/datastore/providers/mongodb/watcher/watch_store.go` | Change-stream consumer loop — BFQ-2 |
| `fault-quarantine/pkg/metrics/metrics.go` | FQ Prometheus metrics |
| `docs/nvsentinel-at-scale/scale-issues.md` | Full scale analysis with all action items |
