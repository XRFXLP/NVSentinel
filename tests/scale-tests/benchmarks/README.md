# NVSentinel Component Benchmarks

Each file in this directory corresponds to one GitHub issue.

| File | Issue | Component | Status |
|---|---|---|---|
| `kom.py` | #1512 | Kubernetes Object Monitor | ✅ Complete |
| `fault_quarantine.py` | #1518 | Fault Quarantine Module | 🚧 Throughput done, ruleset/cordon/CB pending |
| `node_drainer.py` | #1517 | Node Drainer | 📋 Stub |
| `fault_remediation.py` | #1519 | Fault Remediation | 📋 Stub |
| `health_analyzer.py` | #1523 | Health Events Analyzer | 📋 Stub |
| `event_exporter.py` | #1514 | Event Exporter | 📋 Stub |
| `janitor.py` | #1524 | Janitor | 📋 Stub |
| `platform_connector.py` | #1525 | Platform Connector | 📋 Stub |
| `mongodb_bitnami.py` | #1515 | Bitnami MongoDB | 📋 Stub |
| `mongodb_percona.py` | #1516 | Percona MongoDB | 📋 Stub |
| `preflight.py` | #1522 | Preflight | 📋 Stub |

## Adding a new benchmark

1. Copy the structure from `kom.py`
2. Import from `framework/` — don't copy helpers
3. Use `ClusterSetup.sample_node()` before creating any KWOK nodes
4. Use `ClusterSetup.verify_node_size()` before any memory sweep
5. Use `BenchmarkRun` with `repetitions >= 3` (V3 requirement)
6. Record warmup_s and measurement_s on every `BenchmarkRun` (V2 requirement)
7. Use `LatencyTracker` for end-to-end event latency (V1 requirement)
8. Call `results.save(output)` after each scenario so progress isn't lost

## Running

```bash
# From tests/scale-tests/
python3 bench.py kom --help
python3 bench.py fq  --help
python3 bench.py all --help
```
