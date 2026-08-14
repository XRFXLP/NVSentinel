# Event Generation Strategies — NVSentinel Scale Benchmarks

Cross-component summary of injection mechanisms, patterns, and rates used across Epic #1511 microbenchmarks.

---

## Contents

1. Injection Mechanisms
2. Event Patterns
3. Rates by Component
4. Coverage Gaps

---

## Injection Mechanisms

Three distinct attack surfaces were used across components:

| Mechanism | Used by | Rate range | Notes |
|---|---|---|---|
| **KWOK object churn** — kubectl via real API server + etcd | KOM | ≈8–143 events/s | Most realistic path; tests informer, reconcile, annotation write |
| **gRPC sidecar** — `event-generator` → platform-connector over emptyDir UDS | PC | 1–200/s per connector | Fleet-scale: 1,000 connectors × 1/s = 1,000 fleet events/s |
| **Direct MongoDB insert** — pymongo, 50 workers, batch 500 | FQ, ND, FR, MongoDB | 107k–1,382/s | Bypasses platform-connector; tests storage, oplog, change-stream |

---

## Event Patterns

### Fleet-storm
- **Used by:** FQ, KOM
- **Pattern:** 1 event per N distinct nodes, all fired simultaneously
- **Purpose:** Tests concurrent cordon/reconcile throughput; exposes client-go QPS ceiling
- **Result:** Cordon/drain rate flat at 2.4 nodes/s (FQ), 1.1 nodes/s (ND/FR) regardless of N — rate limiter dominates

### Flappy storm
- **Used by:** FQ, PC
- **Pattern:** Alternating fatal↔healthy events for the same N nodes, at maximum injection rate
- **Purpose:** Every event is a real state change — no deduplication possible; maximally stresses FQ K8s API calls and PC condition-update path
- **Rates:** FQ injected at 10,480/s peak (decaying); PC at 1/s per connector (FLAPPY_MODE cycles 8 check names)
- **Result:** FQ processes only 173 events/s (K8s API bottleneck); PC APF collapse at burst=40×1,000

### STORE_ONLY flood
- **Used by:** FQ, ND
- **Pattern:** Events with `processingstrategy=STORE_ONLY` (processingstrategy=2) injected directly into MongoDB
- **Purpose:** FQ's change-stream filter excludes STORE_ONLY entirely → cursor frozen at R_fq=0 → oplog advances purely from noise → tests `CappedPositionLost` trigger speed
- **Rate:** ≈32M events in 5 minutes (≈107k/s) via 50 pymongo workers, batch 500
- **Result:** `CappedPositionLost` within 2 minutes; cursor never advanced; entire change-stream replay lost on restart

### EXECUTE_REMEDIATION flood
- **Used by:** FQ, ND
- **Pattern:** High-rate events the component DOES process (processingstrategy=0); component must act on every event
- **Purpose:** Net cursor lag = injection rate − processing rate; tests how fast the oplog outpaces the consumer when processing is slow
- **Rates:** FQ injected at 2,460–10,480/s; FQ processed ≈173/s → net oplog advancement at 2,287–10,307/s → `CappedPositionLost` at t≈244s
- **ND:** 33,000/s flood → 990MB oplog cycled in ≈38s; cursor frozen at ≈2.5/s advance rate (one quarantine event per FQ cordon)

### Cold-start replay
- **Used by:** ND, FR
- **Pattern:** N events pre-seeded into MongoDB (mix of remediation-ready + STORE_ONLY noise as haystack), then component cold-started; measures scan cost before change stream resumes
- **Rate:** Not injection-rate-limited — measures scan throughput (0.5–0.6 µs/event in-memory; degrades above 10M docs when disk reads begin)
- **Scale:** ND: 1M, 3M, 5M, 10M, 20M events; FR: 500k, 1M, 3M, 5M events
- **Result without C4 index:** 14.5s at 20M events (O(N) scan); **with C4 partial index:** 0.01s (O(1), 1,456× speedup)

### XID 95 single-node burst
- **Used by:** PC
- **Pattern:** Single node receiving health events at 197/s (17M events/24h observed production rate) — simulated via EVENT_RATE=200 on one connector pod
- **Purpose:** Tests unbounded work-queue OOM when processing rate (5/s) is <<<< injection rate (200/s)
- **Result:** Queue grows at 190 items/s, 1,266 bytes/item → 240 KB/s memory growth → OOM at 128Mi limit in ≈5 minutes → permanent crash loop; all 77k+ queued events lost on restart

### Startup burst storm
- **Used by:** PC
- **Pattern:** `burst × N_pods` simultaneous API calls fired at StatefulSet startup; tests APF saturation as a function of `burst` parameter
- **Configs tested:**
  - `burst=10, N=1,000` (10,000 simultaneous calls): APF seats ≈1,280 (75% threshold) → **safe**
  - `burst=15, N=1,000` (15,000 calls): latency 10,225ms, throughput 30× slower → **collapses**
  - `burst=40, N=1,000` (40,000 calls): latency 21,809ms, throughput 55× slower → **collapses**
- **Result:** Cliff between 10K and 15K simultaneous startup calls; EVENT_RATE had no effect on collapse severity

### Connection sweep
- **Used by:** MongoDB
- **Pattern:** Deployment scaled incrementally (5→200 replicas × 500 clients/pod); each step waits for connections to stabilize before measuring memory
- **Purpose:** Derives memory/connection model; validates 65K cap status; finds CPU ceiling
- **Rate:** Connections established at ≈500/pod in <4s; measurement at stable state
- **Scale:** 42 → 187,920 confirmed connections; 64Gi model validated; crash at ≈250K (CPU storm during TLS handshake burst)

---

## Rates by Component

| Component | Normal operating rate | Stress rate | Hard ceiling (bottleneck) |
|---|---|---|---|
| **KOM** | ≈8/s pod churn, ≈143/s KWOK heartbeat at 100k | ResyncPeriod burst: 13k objects/cycle | ≈290 rec/s (MCR=1, QPS=5); 65k-node queue never drains |
| **FQ** | 173 events/s processed | 10,480/s injected (flappy) | 2.4 nodes/s cordon (K8s API, QPS=5/2 calls/cordon) |
| **ND** | ≈1.1 nodes/s drain | 33,000/s oplog flood | 1.1 nodes/s (K8s API); oplog cycled in 38s at 33k/s |
| **FR** | ≈1.1 nodes/s | 4.5 nodes/s (QPS=20, c=4) | Recheck saturation at ≈130 concurrent CRs |
| **PC** | 1/s per connector × 1,000 | 200/s single node (XID 95) | APF 1,700 seats; K8s Event LIST O(N²); queue OOM at 128Mi |
| **MongoDB** | ≈500 stored events/s (production) | 1,382/s (single client pod) | Server: no bottleneck found; client tool ceiling at ≈1,382/s |

---

## Common Bottleneck: client-go QPS=5 / burst=10

Every component that writes to the Kubernetes API server is bounded by the same default:

| Component | API calls per operation | Effective throughput | Fix |
|---|---|---|---|
| FQ (cordon) | 2 (GET + PATCH Node) | **2.4 nodes/s** | K2: expose QPS/burst as Helm values |
| ND (drain) | ≈4 (evict + check + events) | **1.1 nodes/s** | K2: same |
| FR (create CR) | ≈7 (GET + CREATE + status reads) | **1.1 nodes/s** | K2: same |
| PC (condition update) | 2–4 (GET + PUT + LIST Events) | **1.25 events/s** at QPS=5 | EventRecorder removes LIST; raises to 5/s at QPS=10 |

---

## Coverage Gaps

Stress patterns not yet measured:

1. **Cross-component correlated burst** — all 1,000 PCs fire simultaneously (simulated power event or XID 95 fleet-wide); tests combined APF + MongoDB write + FQ cordon pipeline under correlated load
2. **FQ + ND concurrent consumers** — both components consuming the same oplog simultaneously at different advance rates; tests whether inter-consumer oplog window contention accelerates `CappedPositionLost`
3. **XID 95 at fleet scale** — 197/s from 100 nodes simultaneously (not just 1); requires 100 connector pods each at EVENT_RATE=197, stressing both the K8s Event namespace and the MongoDB write path together
4. **Rolling upgrade under load** — platform-connector DaemonSet rolling update while events are flowing; tests whether the burst storm from restarting N pods simultaneously collapses the API server mid-upgrade
