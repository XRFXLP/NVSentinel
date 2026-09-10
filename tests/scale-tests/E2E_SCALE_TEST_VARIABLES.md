# End-to-End Scale Test — Scale Variables

The dials that change how big the test is. Resource limits (memory, CPU) are excluded — those
are sized in response to the scale, not swept. Plumbing (sockets, ports, image tags, namespaces,
cert paths, kubeconfig context) is excluded too.

| # | Variable | Values used |
|---|---|---|
| 1 | KWOK node count | 1k, 5k, 10k, 25k, 50k, 80k — 80k is the ceiling at production node size, see PRODUCTION_BASELINE_OBSERVED.md section 10 |
| 2 | KWOK node object size | ≈3.5 KB unpatched → **≈13.4 KB** patched (conditions + ≈50-entry image list) |
| 3 | Pod count | 0, 1k, 10k, 50k, 100k, 200k (697k cluster-wide for Preflight) |
| 4 | Pod object size | **9335 B** default; Preflight profiles 5.4 / 7.1 / 17.6 / 38.8 KB |
| 5 | Pods per node | 100 nodes default |
| 6 | Node / pod creation parallelism | 80 (lower on API 429s) |
| 7 | Event injection rate | 10, 30, 50, 150, 200, 500 ev/s; floods 1.4k–107k/s |
| 8 | Per-node event rate | 0.02 / 0.067 / 0.333 / 1.0 / 2.0 / 2.8 (= 30→4200 ev/s at 1500 nodes) |
| 9 | Total events injected | 300 → 32M |
| 10 | Seeded MongoDB doc count | 0, 100k, 1M, 3M, 5M, 10M, 20M |
| 11 | Injection workers / batch size | 20–50 workers; batch 200 / 500 |
| 12 | Pod churn rate | 10, 50, 200 pods/s |
| 13 | Churn node count | 100 |
| 14 | Concurrency level (in-flight CRs) | 1, 10, 50, 100 |
| 15 | Retained / completed CR count | 0, 100, 500, 1k, 5k, 10k |
| 16 | Drain / cordon node count | 10, 100, 500, 750, 1000 |
| 17 | Gang size | 2, 16, 128, 1000 |
| 18 | Background pod population | 0, 1k, 5k, 10k |
| 19 | Connector fleet size | 1, 100, 1000 (manifest default 100; ≈5000 max) |
| 20 | MongoDB connection count | 42 → 187,920 (replicas × 500 clients/pod; crash ≈250k) |
| 21 | MongoDB write rate | 30, 150, 500, 1500, 2800, 4200, 10000 ev/s |
| 22 | Rule / ruleset count | HEA 1, 5, 22; FQ 1, 2, 4, 50 |
| 23 | Preflight check count | 0, 1, 2, 3 |
| 24 | Query time window | 300, 1000, 3600, 86400, 604800, 2592000 s |
| 25 | Data retention / TTL | 30 d (2592000 s); Janitor 336h; recommend 7 d |
| 26 | maxConcurrentReconciles | KOM 1, 2, 4, 8, 100; FR 1, 4 |
| 27 | client-go QPS / Burst | 5/10, 10/15, 20/40 |
| 28 | Resync period | 1m, 5m, 24h |
| 29 | Failure fraction | 10% (150), 25% (375), 50% (750) of 1500 nodes |
| 30 | Workload pod replicas | 3000 (2/node), 22500 (15/node), 1500 |
| 31 | Oplog size | 990 MB (Bitnami); ≈400 MB auto (Percona); recommend ≥15 GB |
| 32 | Measurement window / repetitions | 60 s warmup / 300 s duration / 3 reps; HEA 10 s / 60 s |
