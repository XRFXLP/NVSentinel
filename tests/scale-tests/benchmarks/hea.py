"""
benchmarks/hea.py — Health Events Analyzer microbenchmarks.

Implements GitHub issue #1523: Microbenchmark Health Events Analyzer.

Scale target: 100k nodes.

Scenarios implemented
---------------------
  throughput    Scenario 1: Event throughput sweep. Injects at increasing
                rates; finds the rate at which HEA processing latency degrades.

  rule-sweep    Scenario 2: Rule-count sweep. Enables 1/3/7 rules via
                configmap patch, measures per-rule MongoDB query time.

  historical    Scenario 4: Historical data size. Pre-seeds the HealthEvents
                collection with 100k / 1M documents, measures how collection
                size affects per-rule aggregation pipeline time.

  index         Scenario 7: Index comparison. Measures query performance with
                the full compound index, then with only _id (index dropped),
                then restores the index.

Injection method
----------------
  Direct MongoDB insert bypasses platform-connector. Requires:
  - kubectl port-forward to MongoDB primary (--mongo-port)
  - Port-forward to HEA metrics endpoint (--metrics-url)
  - mongo-app-client-cert-secret accessible in nvsentinel namespace

  Event format matches what platform-connector would write:
  processingstrategy=1 (EXECUTE_REMEDIATION) so HEA's change stream picks it up.
"""
from __future__ import annotations

import json
import re
import subprocess
import time
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator

from framework.cluster import ClusterClient
from framework.log import log
from framework.mongo import DB_NAME, COLL_NAME, NAMESPACE, SECRET, MongoInjector
from framework.prometheus import PrometheusClient, Snapshot
from framework.results import BenchmarkResults, BenchmarkRun

_HEA_DEPLOY   = "health-events-analyzer"
_HEA_CFGMAP   = "health-events-analyzer-config"
_COMPOUND_IDX = (
    "healthevent.nodename_1_healthevent.entitiesimpacted.entitytype_1_"
    "healthevent.entitiesimpacted.entityvalue_1_"
    "healthevent.generatedtimestamp.seconds_1"
)


# ── HEA event factory ─────────────────────────────────────────────────────────

def _make_hea_event(node: str, seq: int = 0) -> dict:
    """
    Build a HealthEvent document that HEA's change stream filter will match.

    - agent != "health-events-analyzer"
    - ishealthy = false
    - processingstrategy = 1 (EXECUTE_REMEDIATION)
    - generatedtimestamp populated (required by rules that reference it)
    """
    now = datetime.now(timezone.utc)
    error_codes = ["79", "80", "81", "74", "92", "48", "31", "63"]
    return {
        "createdAt": now,
        "healthevent": {
            "version":    1,
            "agent":      "syslog-health-monitor",
            "isfatal":    True,
            "ishealthy":  False,
            "checkname":  "GpuXidError",
            "message":    f"XID {error_codes[seq % 8]} - HEA benchmark",
            "errorcode":  [error_codes[seq % 8]],
            "entitiesimpacted": [{"entitytype": "gpu", "entityvalue": str(seq % 8)}],
            "metadata":   {},
            "generatedtimestamp": {
                "seconds": int(now.timestamp()),
                "nanos":   now.microsecond * 1000,
            },
            "nodename":         node,
            "processingstrategy": 1,   # EXECUTE_REMEDIATION
        },
        "healtheventstatus": {
            "nodequarantined": "",
            "spanids":         {},
        },
    }


# ── Metrics scraper ───────────────────────────────────────────────────────────

def _scrape(url: str) -> dict[str, float]:
    """Parse Prometheus text exposition into {metric_key: value}."""
    try:
        raw = urllib.request.urlopen(url, timeout=5).read().decode()
    except Exception:
        return {}
    result: dict[str, float] = {}
    for line in raw.splitlines():
        if line.startswith("#"):
            continue
        parts = line.rsplit(" ", 1)
        if len(parts) != 2:
            continue
        try:
            result[parts[0]] = float(parts[1])
        except ValueError:
            pass
    return result


def _counter(m: dict, name: str) -> float:
    return m.get(name, 0.0)


def _histogram_quantile(m: dict, metric: str, quantile: float,
                        extra_label: str = "",
                        baseline: dict | None = None) -> float | None:
    """
    Compute `quantile` from Prometheus histogram bucket entries.

    Pass `baseline` (a prior scrape) to compute quantile from the DELTA
    since that snapshot — avoids contamination from prior measurement windows.

    `extra_label` is an optional substring that must appear in the key
    (used to filter by rule_name etc.).
    """
    buckets: list[tuple[float, float]] = []
    inf_count: float = 0.0

    for k, v in m.items():
        if metric not in k:
            continue
        if extra_label and extra_label not in k:
            continue
        le_match = re.search(r'le="([^"]+)"', k)
        if not le_match:
            continue
        delta = v - (baseline.get(k, 0.0) if baseline else 0.0)
        if delta < 0:
            delta = 0.0
        le_str = le_match.group(1)
        if le_str == "+Inf":
            inf_count = delta
        else:
            try:
                buckets.append((float(le_str), delta))
            except ValueError:
                pass

    if not buckets or inf_count == 0:
        return None

    buckets.sort()
    target = quantile * inf_count
    prev_le, prev_count = 0.0, 0.0
    for le, count in buckets:
        if count >= target:
            if count == prev_count:
                return prev_le * 1000
            frac = (target - prev_count) / (count - prev_count)
            return (prev_le + frac * (le - prev_le)) * 1000
        prev_le, prev_count = le, count
    return buckets[-1][0] * 1000 if buckets else None


def _events_processed(m: dict) -> float:
    return m.get("health_event_analyzer_events_successfully_processed_total", 0.0)


# ── kubectl helpers ───────────────────────────────────────────────────────────

def _kubectl(*args: str) -> str:
    return subprocess.check_output(["kubectl", *args],
                                   stderr=subprocess.DEVNULL).decode().strip()


def _hea_pod_name() -> str:
    return _kubectl("get", "pod", "-n", NAMESPACE,
                    "-l", f"app.kubernetes.io/name={_HEA_DEPLOY}",
                    "-o", "jsonpath={.items[0].metadata.name}")


def _restart_hea_and_wait(metrics_url: str = "http://localhost:2113/metrics",
                          local_port: int = 2113, timeout: int = 120) -> None:
    """
    Rollout-restart HEA, wait for it to be Ready, then refresh the
    metrics port-forward so subsequent metric reads go to the new pod.
    """
    log("    Restarting HEA...")
    subprocess.check_call(
        ["kubectl", "rollout", "restart", "-n", NAMESPACE,
         f"deployment/{_HEA_DEPLOY}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    subprocess.check_call(
        ["kubectl", "rollout", "status", "-n", NAMESPACE,
         f"deployment/{_HEA_DEPLOY}", f"--timeout={timeout}s"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(3)

    # Kill any existing port-forward on local_port and open a fresh one
    subprocess.run(["pkill", "-f", f"port-forward.*{local_port}"],
                   capture_output=True)
    time.sleep(1)
    subprocess.Popen(
        ["kubectl", "port-forward", "-n", NAMESPACE,
         f"deployment/{_HEA_DEPLOY}", f"{local_port}:2112"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(4)   # wait for port-forward to establish and change stream to open

    # Wait until metrics endpoint responds
    deadline = time.time() + 30
    while time.time() < deadline:
        m = _scrape(metrics_url)
        if m:
            break
        time.sleep(2)


# ── Main benchmark class ──────────────────────────────────────────────────────

@dataclass
class HEABenchmark:
    client:      ClusterClient
    prom:        PrometheusClient
    metrics_url: str  = "http://localhost:2113/metrics"
    mongo_port:  int  = 27018
    version:     str  = "unknown"
    output:      str  = "results"

    def __post_init__(self) -> None:
        self.results = BenchmarkResults(component="hea", version=self.version)
        self._node_counter = 0

    def _node(self, i: int) -> str:
        return f"hea-bench-node-{i:06d}"

    def _inject_bulk(self, inj: MongoInjector, n_events: int,
                     n_nodes: int = 100, batch: int = 500) -> int:
        """
        Bulk-insert `n_events` using insert_many.
        Avoids per-document port-forward RTT — inserts arrive in the oplog
        as individual entries and HEA processes them one at a time from the
        change stream, giving us HEA's true serial throughput.
        """
        docs = [_make_hea_event(self._node(i % n_nodes), seq=i)
                for i in range(n_events)]
        for start in range(0, n_events, batch):
            inj._coll.insert_many(docs[start:start + batch], ordered=False)
        return n_events

    def _drain_wait(self, base: float, expected: int,
                    timeout_s: int = 300) -> tuple[float, float]:
        """
        Poll until HEA processes `expected` events (above `base`) or timeout.
        Returns (actual_processed, elapsed_s).
        """
        t0       = time.time()
        deadline = t0 + timeout_s
        while time.time() < deadline:
            m         = _scrape(self.metrics_url)
            processed = _events_processed(m) - base
            if processed >= expected:
                return processed, time.time() - t0
            time.sleep(2)
        m = _scrape(self.metrics_url)
        return _events_processed(m) - base, time.time() - t0

    def _record(self, run: BenchmarkRun, snap: Snapshot) -> None:
        run.add(snap)
        self.results.add_run(run)

    def _reset_hea(self) -> None:
        """Restart HEA so its Prometheus histograms reset to zero."""
        _restart_hea_and_wait(self.metrics_url)

    def _get_mongo_injector(self) -> MongoInjector:
        return MongoInjector.from_cluster(local_port=self.mongo_port)

    # ── Scenario 1: Throughput ────────────────────────────────────────────────

    def run_throughput(self, rates: list[int] | None = None,
                       warmup_s: int = 10, measure_s: int = 60) -> None:
        """
        Pre-inject a batch of events (simulating target_rate × measure_s arrivals)
        then measure how long HEA takes to drain the queue. This avoids the
        port-forward insertion bottleneck and captures HEA's true serial throughput.

        HEA processes events serially: one event × N_rules aggregation pipelines.
        The knee point is when queue latency grows unboundedly (backlog never drains).
        """
        if rates is None:
            rates = [30, 150, 500]

        log("MB-HEA-1 — Event Throughput Sweep")
        log(f"  Rates: {rates} events/s  warmup={warmup_s}s  measure_window={measure_s}s")

        # Fresh HEA restart so histograms start from 0
        self._reset_hea()

        with self._get_mongo_injector() as inj:
            # Warmup: small batch so HEA's change stream is warm
            warmup_n = warmup_s * min(rates[0], 30)
            log(f"  Warmup: inserting {warmup_n} events...")
            self._inject_bulk(inj, warmup_n)
            m0_warm = _scrape(self.metrics_url)
            base_warm = _events_processed(m0_warm)
            self._drain_wait(base_warm, warmup_n, timeout_s=600)
            log("  Warmup complete.")

            for target_rate in rates:
                n_events = target_rate * measure_s
                log(f"  [{target_rate}/s] Injecting {n_events} events (bulk)...")

                m0      = _scrape(self.metrics_url)
                base    = _events_processed(m0)
                t_inject_start = time.time()
                self._inject_bulk(inj, n_events)
                t_inject_end = time.time()
                inject_elapsed = t_inject_end - t_inject_start

                log(f"  [{target_rate}/s] Bulk insert done in {inject_elapsed:.1f}s. Waiting for drain...")
                processed, drain_s = self._drain_wait(base, n_events, timeout_s=600)
                actual_rate = processed / drain_s if drain_s > 0 else 0

                m1  = _scrape(self.metrics_url)
                p50 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket", 0.50,
                    baseline=m0)
                p90 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket", 0.90,
                    baseline=m0)
                p99 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket", 0.99,
                    baseline=m0)

                log(f"  [{target_rate}/s] target={n_events}  processed={processed:.0f}"
                    f"  actual={actual_rate:.1f}/s  drain={drain_s:.0f}s"
                    + (f"  P50={p50:.0f}ms  P90={p90:.0f}ms  P99={p99:.0f}ms" if p50 else ""))

                run = BenchmarkRun(
                    scenario=f"throughput_{target_rate}rps",
                    component="hea",
                    warmup_s=warmup_s,
                    measurement_s=measure_s,
                    metadata={"target_rate": target_rate},
                )
                snap = Snapshot(label=f"rate={target_rate}/s")
                snap.events_rps        = round(actual_rate, 2)
                snap.processing_p50_ms = round(p50, 1) if p50 else None
                snap.processing_p99_ms = round(p99, 1) if p99 else None
                snap.extras = {
                    "target_rate":   target_rate,
                    "injected":      n_events,
                    "processed":     processed,
                    "drain_s":       round(drain_s, 1),
                    "inject_s":      round(inject_elapsed, 1),
                    "p90_ms":        round(p90, 1) if p90 else None,
                }
                self._record(run, snap)

    # ── Scenario 2: Rule-count sweep ──────────────────────────────────────────

    def _patch_rule_count(self, enabled: int) -> None:
        """
        Patch HEA configmap to enable only `enabled` rules (disable rest).
        Works by toggling evaluate_rule = true/false in config.toml.
        """
        raw = _kubectl("get", "configmap", _HEA_CFGMAP, "-n", NAMESPACE,
                       "-o", "jsonpath={.data.config\\.toml}")
        lines   = raw.splitlines()
        out     = []
        rule_no = 0
        for line in lines:
            if line.strip() == "[[rules]]":
                rule_no += 1
            if line.strip().startswith("evaluate_rule"):
                line = f"evaluate_rule = {'true' if rule_no <= enabled else 'false'}"
            out.append(line)
        new_toml = "\n".join(out)

        # Patch via kubectl patch
        patch = json.dumps({"data": {"config.toml": new_toml}})
        subprocess.check_call(
            ["kubectl", "patch", "configmap", _HEA_CFGMAP, "-n", NAMESPACE,
             "--type=merge", f"--patch={patch}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def run_rule_sweep(self, rule_counts: list[int] | None = None,
                       n_events: int = 100, measure_s: int = 60) -> None:
        if rule_counts is None:
            rule_counts = [1, 3, 7]

        log("MB-HEA-2 — Rule-Count Sweep")
        log(f"  Rule counts: {rule_counts}  n_events={n_events} per point")

        try:
            with self._get_mongo_injector() as inj:
                for n_rules in rule_counts:
                    log(f"  Enabling {n_rules} rule(s)...")
                    self._patch_rule_count(n_rules)
                    _restart_hea_and_wait(self.metrics_url)

                    log(f"  Injecting {n_events} events (bulk)...")
                    m0   = _scrape(self.metrics_url)
                    base = _events_processed(m0)
                    self._inject_bulk(inj, n_events)
                    processed, drain_s = self._drain_wait(base, n_events, timeout_s=600)
                    m1 = _scrape(self.metrics_url)

                    rule_names = _get_rule_names()
                    rule_times: dict[str, float | None] = {}
                    for rule in rule_names:
                        rule_times[rule] = _histogram_quantile(
                            m1,
                            "mongo_query_execution_duration_seconds_bucket",
                            0.50,
                            extra_label=f'rule_name="{rule}"',
                            baseline=m0,
                        )

                    p50 = _histogram_quantile(
                        m1, "health_event_analyzer_event_handling_duration_seconds_bucket", 0.50,
                        baseline=m0)
                    actual_rate = processed / drain_s if drain_s > 0 else 0
                    log(f"  [{n_rules} rules] processed={processed:.0f}"
                        f"  actual={actual_rate:.1f}/s"
                        + (f"  P50={p50:.0f}ms" if p50 else "")
                        + f"  rule_times(P50ms)={rule_times}")

                    run = BenchmarkRun(
                        scenario=f"rule_sweep_{n_rules}rules",
                        component="hea",
                        warmup_s=0,
                        measurement_s=measure_s,
                        metadata={"n_rules": n_rules},
                    )
                    snap = Snapshot(label=f"{n_rules} rules")
                    snap.events_rps        = round(actual_rate, 2)
                    snap.processing_p50_ms = round(p50, 1) if p50 else None
                    snap.extras = {
                        "n_rules":    n_rules,
                        "n_events":   n_events,
                        "processed":  processed,
                        "drain_s":    round(drain_s, 1),
                        "rule_times": rule_times,
                    }
                    self._record(run, snap)
        finally:
            log("  Restoring all 22 rules...")
            self._patch_rule_count(99)   # 99 = enable all
            _restart_hea_and_wait(self.metrics_url)

    # ── Scenario 4: Historical data size ──────────────────────────────────────

    def _preseed(self, inj: MongoInjector, n: int, n_nodes: int = 100,
                 batch: int = 500) -> None:
        """
        Bulk-insert N historical events spread across the SAME node names used
        by test events (_node(i % n_nodes)), so Stage 2 nodename filters
        actually have a realistic population to scan through.

        Timestamps are spread across the past 29 days so Stage 0 time-window
        queries cover the pre-seeded data.
        """
        log(f"    Pre-seeding {n:,} docs in batches of {batch}...")
        old_ts = int(datetime.now(timezone.utc).timestamp()) - 30 * 86400   # 30 days ago
        inserted = 0
        while inserted < n:
            size = min(batch, n - inserted)
            docs = []
            for i in range(size):
                seq = inserted + i
                # Use same node names as test events for realistic Stage 2 cardinality
                doc = _make_hea_event(self._node(seq % n_nodes), seq)
                doc["healthevent"]["generatedtimestamp"]["seconds"] = (
                    old_ts + seq % (86400 * 29)
                )
                doc["createdAt"] = datetime.fromtimestamp(
                    old_ts + seq % (86400 * 29), tz=timezone.utc
                )
                docs.append(doc)
            inj._coll.insert_many(docs, ordered=False)
            inserted += size
            if inserted % 100_000 == 0:
                log(f"    ... {inserted:,}/{n:,}")
        log(f"    Done: {n:,} docs pre-seeded")

    def run_historical(self, sizes: list[int] | None = None,
                       inject_rate: float = 30, measure_s: int = 60) -> None:
        if sizes is None:
            sizes = [100_000, 1_000_000]

        log("MB-HEA-4 — Historical Data Size Sweep")
        log(f"  Sizes: {[f'{s:,}' for s in sizes]}  inject_rate={inject_rate}/s")

        with self._get_mongo_injector() as inj:
            for target_size in sizes:
                log(f"  Clearing collection and restarting HEA (fresh histogram)...")
                inj._coll.delete_many({})
                self._reset_hea()

                self._preseed(inj, target_size)
                count = inj._coll.count_documents({})
                log(f"  Collection has {count:,} docs")

                n_events = 100
                log(f"  Injecting {n_events} events (bulk)...")
                m0   = _scrape(self.metrics_url)
                base = _events_processed(m0)
                self._inject_bulk(inj, n_events)
                processed, drain_s = self._drain_wait(base, n_events, timeout_s=600)
                m1 = _scrape(self.metrics_url)

                p50 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket", 0.50,
                    baseline=m0)
                p99 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket", 0.99,
                    baseline=m0)

                rule_names = _get_rule_names()
                rule_p50: dict[str, float | None] = {}
                for rule in rule_names:
                    rule_p50[rule] = _histogram_quantile(
                        m1, "mongo_query_execution_duration_seconds_bucket", 0.50,
                        extra_label=f'rule_name="{rule}"', baseline=m0)

                actual_rate = processed / drain_s if drain_s > 0 else 0
                log(f"  [{target_size:,} docs] processed={processed:.0f}"
                    f"  actual={actual_rate:.1f}/s"
                    + (f"  P50={p50:.0f}ms  P99={p99:.0f}ms" if p50 else ""))

                run = BenchmarkRun(
                    scenario=f"historical_{target_size}docs",
                    component="hea",
                    warmup_s=0,
                    measurement_s=int(drain_s),
                    metadata={"seeded_docs": target_size},
                )
                snap = Snapshot(label=f"{target_size:,} docs")
                snap.events_rps        = round(actual_rate, 2)
                snap.processing_p50_ms = round(p50, 1) if p50 else None
                snap.processing_p99_ms = round(p99, 1) if p99 else None
                snap.extras = {
                    "seeded_docs": target_size,
                    "n_events":    n_events,
                    "processed":   processed,
                    "drain_s":     round(drain_s, 1),
                    "rule_p50":    rule_p50,
                }
                self._record(run, snap)

    # ── Scenario 7: Index comparison ──────────────────────────────────────────

    def run_index_comparison(self, n_events: int = 100,
                             measure_s: int = 60) -> None:
        log("MB-HEA-7 — Index Comparison")
        # Fresh HEA restart for clean histograms
        self._reset_hea()

        rule_names = _get_rule_names()

        def _measure_pass(inj: MongoInjector, label: str) -> tuple[float, float, float | None, float | None, dict]:
            m0   = _scrape(self.metrics_url)
            base = _events_processed(m0)
            self._inject_bulk(inj, n_events)
            processed, drain_s = self._drain_wait(base, n_events, timeout_s=600)
            m1 = _scrape(self.metrics_url)
            # Use baseline=m0 so we measure only THIS pass's events, not cumulative
            p50 = _histogram_quantile(
                m1, "health_event_analyzer_event_handling_duration_seconds_bucket", 0.50,
                baseline=m0)
            p99 = _histogram_quantile(
                m1, "health_event_analyzer_event_handling_duration_seconds_bucket", 0.99,
                baseline=m0)
            rp50 = {r: _histogram_quantile(
                        m1, "mongo_query_execution_duration_seconds_bucket", 0.50,
                        extra_label=f'rule_name="{r}"', baseline=m0)
                    for r in rule_names}
            log(f"  {label}: processed={processed:.0f}"
                + (f"  P50={p50:.0f}ms  P99={p99:.0f}ms" if p50 else ""))
            return processed, drain_s, p50, p99, rp50

        with self._get_mongo_injector() as inj:
            log(f"  Pass 1: compound index present (baseline) — {n_events} events...")
            pw, dw, p50w, p99w, rp50w = _measure_pass(inj, "with index")

            log(f"  Dropping compound index '{_COMPOUND_IDX}'...")
            try:
                inj._coll.drop_index(_COMPOUND_IDX)
                log("    Dropped.")
            except Exception as e:
                log(f"    Warning: could not drop index: {e}")

            log(f"  Pass 2: compound index absent — {n_events} events...")
            pwo, dwo, p50wo, p99wo, rp50wo = _measure_pass(inj, "without index")

            log("  Recreating compound index...")
            try:
                from pymongo import ASCENDING
                inj._coll.create_index([
                    ("healthevent.nodename",                         ASCENDING),
                    ("healthevent.entitiesimpacted.entitytype",      ASCENDING),
                    ("healthevent.entitiesimpacted.entityvalue",     ASCENDING),
                    ("healthevent.generatedtimestamp.seconds",       ASCENDING),
                ])
                log("    Recreated.")
            except Exception as e:
                log(f"    Warning: could not recreate index: {e}")

            run = BenchmarkRun(
                scenario="index_comparison",
                component="hea",
                warmup_s=0,
                measurement_s=measure_s,
            )
            snap = Snapshot(label="with_index")
            snap.processing_p50_ms = round(p50w, 1) if p50w else None
            snap.processing_p99_ms = round(p99w, 1) if p99w else None
            snap.extras = {
                "with_index":    {"processed": pw, "drain_s": round(dw, 1), "rule_p50": rp50w},
                "without_index": {"processed": pwo, "drain_s": round(dwo, 1), "rule_p50": rp50wo},
            }
            self._record(run, snap)


    # ── MongoDB co-load test (in-cluster injector) ───────────────────────────

    def run_coload(self, inject_rate: float = 100.0, n_events: int = 6_000,
                   seed_docs: int = 100_000) -> None:
        """
        Measure the impact of HEA's background COLLSCAN queries on MongoDB's
        ability to serve concurrent health-event inserts.

        Pass 1 — HEA OFF (0 replicas): inject at `inject_rate` events/s and
        measure insert latency.  MongoDB only serves inserts; no COLLSCAN load.

        Pass 2 — HEA ON (1 replica): same injection rate.  HEA continuously
        runs 22 aggregation COLLSCANs per processed event in the background.
        Measures whether that COLLSCAN pressure degrades insert latency and
        what processing rate HEA achieves under concurrent write load.
        """
        log("MB-HEA-COLOAD — MongoDB Co-Load Test (in-cluster injector)")
        log(f"  inject_rate={inject_rate}/s  n_events={n_events:,}  "
            f"seed={seed_docs:,}")

        # The injector runs as a Kubernetes Job inside the cluster so inserts
        # go directly to MongoDB (~0.5ms each) instead of through the
        # port-forward (~250ms each).  This removes the port-forward as a
        # confounding variable and produces clean, comparable timing for
        # HEA OFF vs HEA ON.
        MONGO_URI = (
            "mongodb://mongodb-rs0.nvsentinel.svc.cluster.local:27017"
            f"/{DB_NAME}?replicaSet=rs0&tls=true"
            "&tlsCertificateKeyFile=/tmp/combined.pem"
            "&tlsCAFile=/etc/ssl/client-certs/ca.crt"
            "&authMechanism=MONGODB-X509"
        )

        # mongosh script: inserts n_events at inject_rate and prints a
        # single RESULT line with achieved rate and latency percentiles.
        interval_ms = int(1000 / inject_rate)
        _SCRIPT = f"""
cat /etc/ssl/client-certs/tls.crt /etc/ssl/client-certs/tls.key > /tmp/combined.pem
mongosh "{MONGO_URI}" --eval '
const N = {n_events};
const intervalMs = {interval_ms};
const latencies = [];
const start = Date.now();

for (let i = 0; i < N; i++) {{
    const target = start + i * intervalMs;
    const now = Date.now();
    const wait = target - now;
    if (wait > 0) sleep(wait);

    const t0 = Date.now();
    db.HealthEvents.insertOne({{
        createdAt: new Date(),
        healthevent: {{
            agent: "syslog-health-monitor",
            ishealthy: false,
            processingstrategy: NumberInt(1),
            nodename: "coload-node-" + (i % 100),
            message: "coload benchmark",
            generatedtimestamp: {{
                seconds: NumberLong(Math.floor(Date.now()/1000)),
                nanos: NumberInt(0)
            }}
        }},
        healtheventstatus: {{ spanIds: {{}} }}
    }});
    latencies.push(Date.now() - t0);
}}

const elapsed = (Date.now() - start) / 1000;
latencies.sort((a, b) => a - b);
const p50  = latencies[Math.floor(N * 0.500)];
const p99  = latencies[Math.floor(N * 0.990)];
const p999 = latencies[Math.min(Math.floor(N * 0.999), N - 1)];
print("RESULT injected=" + N +
      " elapsed=" + elapsed.toFixed(2) +
      " achieved=" + (N / elapsed).toFixed(1) +
      " p50=" + p50 +
      " p99=" + p99 +
      " p999=" + p999);
'
"""

        _JOB_TMPL = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "hea-coload-{label}", "namespace": NAMESPACE},
            "spec": {
                "ttlSecondsAfterFinished": 300,
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "tolerations": [
                            {"key": "dedicated", "operator": "Equal",
                             "value": "system-workload", "effect": "NoSchedule"},
                            {"key": "dedicated", "operator": "Equal",
                             "value": "system-workload", "effect": "NoExecute"},
                        ],
                        "containers": [{
                            "name": "injector",
                            "image": "docker.io/percona/percona-server-mongodb:7.0.39-21",
                            "command": ["/bin/bash", "-c"],
                            "args": [_SCRIPT],
                            "volumeMounts": [{"name": "client-certs",
                                              "mountPath": "/etc/ssl/client-certs"}],
                        }],
                        "volumes": [{"name": "client-certs", "secret": {
                            "secretName": "mongo-app-client-cert-secret",
                            "items": [
                                {"key": "tls.crt", "path": "tls.crt"},
                                {"key": "tls.key", "path": "tls.key"},
                                {"key": "ca.crt",  "path": "ca.crt"},
                            ],
                        }}],
                    }
                },
            },
        }

        def _run_injector_job(label: str) -> dict | None:
            """Create Job, wait for completion, parse RESULT line."""
            job_name = f"hea-coload-{label.lower().replace(' ', '-')}"
            # Delete old job if present
            subprocess.run(
                ["kubectl", "delete", "job", job_name, "-n", NAMESPACE,
                 "--ignore-not-found"],
                capture_output=True)

            spec = json.loads(json.dumps(_JOB_TMPL))
            spec["metadata"]["name"] = job_name
            manifest = json.dumps(spec)

            subprocess.run(
                ["kubectl", "apply", "-f", "-", "-n", NAMESPACE],
                input=manifest.encode(), capture_output=True, check=True)
            log(f"    Job {job_name} created — waiting for completion...")

            # Poll until Succeeded or Failed (timeout 10 min)
            deadline = time.time() + 600
            while time.time() < deadline:
                out = subprocess.check_output(
                    ["kubectl", "get", "job", job_name, "-n", NAMESPACE,
                     "-o", "jsonpath={.status.conditions[*].type}"],
                    stderr=subprocess.DEVNULL).decode()
                if "Complete" in out:
                    break
                if "Failed" in out:
                    log(f"    Job {job_name} FAILED")
                    return None
                time.sleep(10)
            else:
                log(f"    Job {job_name} timed out")
                return None

            logs = subprocess.check_output(
                ["kubectl", "logs", "-n", NAMESPACE,
                 f"job/{job_name}"], stderr=subprocess.DEVNULL).decode()

            for line in logs.splitlines():
                if line.startswith("RESULT "):
                    parts = dict(kv.split("=") for kv in line[7:].split())
                    result = {
                        "label":         label,
                        "injected":      int(parts["injected"]),
                        "elapsed_s":     float(parts["elapsed"]),
                        "achieved_rate": float(parts["achieved"]),
                        "insert_p50_ms": float(parts["p50"]),
                        "insert_p99_ms": float(parts["p99"]),
                        "insert_p999_ms": float(parts["p999"]),
                    }
                    log(f"    [{label}] achieved={result['achieved_rate']}/s  "
                        f"P50={result['insert_p50_ms']}ms  "
                        f"P99={result['insert_p99_ms']}ms  "
                        f"P99.9={result['insert_p999_ms']}ms")
                    return result
            log(f"    No RESULT line found in {job_name} logs")
            return None

        # ── Seed collection ───────────────────────────────────────────────
        with self._get_mongo_injector() as inj:
            log(f"  Seeding {seed_docs:,} docs...")
            inj._coll.delete_many({})
            self._preseed(inj, seed_docs)
            log(f"  Collection has {inj._coll.count_documents({}):,} docs")

        # ── Pass 1: HEA OFF ───────────────────────────────────────────────
        log("  Pass 1 — HEA OFF: scaling to 0 replicas...")
        subprocess.check_call(
            ["kubectl", "scale", "deployment", _HEA_DEPLOY,
             "-n", NAMESPACE, "--replicas=0"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(5)
        m0_off = _scrape(self.metrics_url)
        result_off = _run_injector_job("hea-off")

        # ── Pass 2: HEA ON ────────────────────────────────────────────────
        log("  Pass 2 — HEA ON: restoring to 1 replica...")
        subprocess.check_call(
            ["kubectl", "scale", "deployment", _HEA_DEPLOY,
             "-n", NAMESPACE, "--replicas=1"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        _restart_hea_and_wait(self.metrics_url)
        m0_on = _scrape(self.metrics_url)
        base_on = _events_processed(m0_on)
        result_on = _run_injector_job("hea-on")

        # HEA processing rate during the ON pass
        if result_on:
            time.sleep(5)
            m1_on = _scrape(self.metrics_url)
            hea_processed = _events_processed(m1_on) - base_on
            hea_rate = hea_processed / result_on["elapsed_s"]
            result_on["hea_processed"] = hea_processed
            result_on["hea_rate"] = round(hea_rate, 2)
            log(f"    HEA processed {hea_processed:.0f} events "
                f"({hea_rate:.1f}/s) during injection")

        # ── Summary ───────────────────────────────────────────────────────
        if result_off and result_on:
            overhead_p50  = (result_on["insert_p50_ms"]  /
                             result_off["insert_p50_ms"]  - 1) * 100
            overhead_p99  = (result_on["insert_p99_ms"]  /
                             result_off["insert_p99_ms"]  - 1) * 100
            log(f"  HEA overhead on insert latency: "
                f"P50 {overhead_p50:+.0f}%  P99 {overhead_p99:+.0f}%")

        run = BenchmarkRun(
            scenario="coload",
            component="hea",
            warmup_s=0,
            measurement_s=int(n_events / inject_rate),
            metadata={"inject_rate": inject_rate, "seed_docs": seed_docs,
                      "n_events": n_events},
        )
        snap = Snapshot(label="coload")
        snap.extras = {"hea_off": result_off, "hea_on": result_on}
        self._record(run, snap)

    # ── Scenario 5: Window-size sweep ────────────────────────────────────────

    def run_window_sweep(self, windows_s: list[int] | None = None,
                         seed_docs: int = 1_000_000,
                         n_events: int = 100) -> None:
        """
        Measure how Stage 0 query cost (and end-to-end per-event latency) scales
        with the time window used by the aggregation pipeline.

        For each window:
          1. Run explain() on the Stage 0 query to get docs examined/returned and
             execution time — this is a direct MongoDB measurement, no HEA involved.
          2. Inject n_events and measure HEA per-event P50 via histogram delta.

        The collection is seeded once and reused for all window sizes.
        Events are injected with generatedtimestamp = now so they fall inside
        every window regardless of size.
        """
        if windows_s is None:
            # 5min, 1h, 16min (actual burst), 24h (actual), 7d (actual), 30d
            windows_s = [300, 1_000, 3_600, 86_400, 604_800, 2_592_000]

        log("MB-HEA-5 — Window-Size Sweep")
        log(f"  Windows: {[f'{w}s' for w in windows_s]}  seed={seed_docs:,}  n_events={n_events}")

        self._reset_hea()

        with self._get_mongo_injector() as inj:
            # Seed with timestamps spanning NOW-30d to NOW so every window size
            # gets a realistic fraction of docs.  _preseed caps at now-24h by
            # default, so we override here with a full 30-day range.
            log(f"  Clearing and seeding {seed_docs:,} docs (timestamps: now-30d → now)...")
            inj._coll.delete_many({})
            now_seed = int(datetime.now(timezone.utc).timestamp())
            span_s   = 30 * 86400
            docs_pending: list[dict] = []
            inserted = 0
            while inserted < seed_docs:
                size = min(500, seed_docs - inserted)
                for i in range(size):
                    seq = inserted + i
                    doc = _make_hea_event(self._node(seq % 100), seq)
                    ts  = now_seed - span_s + seq % span_s   # uniform over 30 days
                    doc["healthevent"]["generatedtimestamp"]["seconds"] = ts
                    doc["createdAt"] = datetime.fromtimestamp(ts, tz=timezone.utc)
                    docs_pending.append(doc)
                inj._coll.insert_many(docs_pending, ordered=False)
                docs_pending = []
                inserted += size
                if inserted % 100_000 == 0:
                    log(f"    ... {inserted:,}/{seed_docs:,}")
            log(f"  Collection has {inj._coll.count_documents({}):,} docs")
            # Use the seed-end time as reference so window fractions align with
            # the seeded timestamp range (now_seed-30d … now_seed).
            ref_ts = int(time.time())

            for window_s in windows_s:
                label = _window_label(window_s)
                log(f"  Window {label} ({window_s}s):")

                now_s = int(time.time())

                # ── Stage 0 explain (no HEA involved) ──────────────────────
                try:
                    plan = inj._coll.database.command(
                        "explain",
                        {"aggregate": COLL_NAME,
                         "pipeline": [{"$match": {"$expr": {"$and": [
                             {"$gte": ["$healthevent.generatedtimestamp.seconds",
                                       ref_ts - window_s]},
                             {"$lte": ["$healthevent.generatedtimestamp.seconds",
                                       ref_ts]},
                         ]}}}],
                         "cursor": {}},
                        verbosity="executionStats",
                    )
                    es          = plan.get("executionStats", {})
                    s0_examined = es.get("totalDocsExamined")
                    s0_returned = es.get("nReturned")
                    s0_ms       = es.get("executionTimeMillis")
                    log(f"    Stage 0 explain: examined={s0_examined}  "
                        f"returned={s0_returned}  time={s0_ms}ms")
                except Exception as e:
                    log(f"    Stage 0 explain failed: {e}")
                    s0_examined = s0_returned = s0_ms = None

                # ── HEA end-to-end (inject + drain) ────────────────────────
                m0   = _scrape(self.metrics_url)
                base = _events_processed(m0)
                self._inject_bulk(inj, n_events)
                processed, drain_s = self._drain_wait(base, n_events, timeout_s=600)
                m1 = _scrape(self.metrics_url)

                p50 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket",
                    0.50, baseline=m0)
                p99 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket",
                    0.99, baseline=m0)

                rule_names = _get_rule_names()
                rule_p50 = {r: _histogram_quantile(
                    m1, "mongo_query_execution_duration_seconds_bucket",
                    0.50, extra_label=f'rule_name="{r}"', baseline=m0)
                    for r in rule_names}

                log(f"    HEA: processed={processed:.0f}"
                    + (f"  P50={p50:.0f}ms  P99={p99:.0f}ms" if p50 else ""))

                run = BenchmarkRun(
                    scenario=f"window_{label}",
                    component="hea",
                    warmup_s=0,
                    measurement_s=int(drain_s),
                    metadata={"window_s": window_s, "window_label": label,
                              "seed_docs": seed_docs},
                )
                snap = Snapshot(label=label)
                snap.processing_p50_ms = round(p50, 1) if p50 else None
                snap.processing_p99_ms = round(p99, 1) if p99 else None
                snap.extras = {
                    "window_s":    window_s,
                    "stage0_docs_examined": s0_examined,
                    "stage0_docs_returned": s0_returned,
                    "stage0_exec_ms":       s0_ms,
                    "processed":   processed,
                    "rule_p50":    rule_p50,
                }
                self._record(run, snap)

    # ── Scenario 6: Event distribution ───────────────────────────────────────

    def run_event_distribution(self, seed_docs: int = 100_000,
                               n_events: int = 100) -> None:
        """
        Measure how event distribution patterns affect HEA per-event latency.

        Sub-scenarios:
          uniform     Events and history spread across 100 nodes (baseline).
          hot_node    History concentrated on 1 node; test events on same node.
                      Stage 2 sees 100× more historical events per query.
          hot_entity  History on 1 node / 1 GPU entity; test events same node+entity.
                      Rules that filter by entity see a smaller but denser result.
          duplicates  Same event signature repeated (same node + errorcode + entity).
                      Tests whether duplicate-pattern rules short-circuit or do more work.
          fatal_mix   Mix of isfatal=true and isfatal=false events.
                      Some rules gate on isfatal; tests selectivity of that filter.
        """
        log("MB-HEA-6 — Event Distribution")
        log(f"  seed={seed_docs:,}  n_events={n_events} per sub-scenario")

        self._reset_hea()

        with self._get_mongo_injector() as inj:

            def _measure(label: str, seed_fn, inject_fn) -> None:
                """Seed, restart HEA (clean histogram), inject, measure."""
                log(f"\n  [{label}] Seeding...")
                inj._coll.delete_many({})
                seed_fn()
                count = inj._coll.count_documents({})
                log(f"  [{label}] Collection: {count:,} docs. Restarting HEA...")
                self._reset_hea()

                m0   = _scrape(self.metrics_url)
                base = _events_processed(m0)
                inject_fn()
                processed, drain_s = self._drain_wait(base, n_events, timeout_s=600)
                m1 = _scrape(self.metrics_url)

                p50 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket",
                    0.50, baseline=m0)
                p99 = _histogram_quantile(
                    m1, "health_event_analyzer_event_handling_duration_seconds_bucket",
                    0.99, baseline=m0)
                rule_names = _get_rule_names()
                rule_p50 = {r: _histogram_quantile(
                    m1, "mongo_query_execution_duration_seconds_bucket",
                    0.50, extra_label=f'rule_name="{r}"', baseline=m0)
                    for r in rule_names}

                log(f"  [{label}] processed={processed:.0f}"
                    + (f"  P50={p50:.0f}ms  P99={p99:.0f}ms" if p50 else ""))

                run = BenchmarkRun(
                    scenario=f"distribution_{label}",
                    component="hea",
                    warmup_s=0,
                    measurement_s=int(drain_s),
                    metadata={"distribution": label, "seed_docs": seed_docs},
                )
                snap = Snapshot(label=label)
                snap.processing_p50_ms = round(p50, 1) if p50 else None
                snap.processing_p99_ms = round(p99, 1) if p99 else None
                snap.extras = {
                    "distribution": label,
                    "seed_docs":    seed_docs,
                    "collection":   count,
                    "processed":    processed,
                    "rule_p50":     rule_p50,
                }
                self._record(run, snap)

            old_ts = int(datetime.now(timezone.utc).timestamp()) - 30 * 86400

            def _seed_uniform():
                """100 nodes, even spread (baseline)."""
                self._preseed(inj, seed_docs, n_nodes=100)

            def _seed_hot_node():
                """All history on node-000000."""
                log("    Seeding all docs on node-000000...")
                docs, inserted = [], 0
                batch = 500
                while inserted < seed_docs:
                    size = min(batch, seed_docs - inserted)
                    for i in range(size):
                        seq = inserted + i
                        doc = _make_hea_event(self._node(0), seq)  # always node 0
                        doc["healthevent"]["generatedtimestamp"]["seconds"] = (
                            old_ts + seq % (86400 * 29))
                        doc["createdAt"] = datetime.fromtimestamp(
                            old_ts + seq % (86400 * 29), tz=timezone.utc)
                        docs.append(doc)
                    inj._coll.insert_many(docs, ordered=False)
                    docs = []
                    inserted += size

            def _seed_hot_entity():
                """All history on node-000000, entity gpu-0."""
                log("    Seeding all docs on node-000000 / entity gpu-0...")
                docs, inserted = [], 0
                batch = 500
                while inserted < seed_docs:
                    size = min(batch, seed_docs - inserted)
                    for i in range(size):
                        seq = inserted + i
                        doc = _make_hea_event(self._node(0), seq)
                        doc["healthevent"]["entitiesimpacted"] = [
                            {"entitytype": "gpu", "entityvalue": "0"}]
                        doc["healthevent"]["generatedtimestamp"]["seconds"] = (
                            old_ts + seq % (86400 * 29))
                        doc["createdAt"] = datetime.fromtimestamp(
                            old_ts + seq % (86400 * 29), tz=timezone.utc)
                        docs.append(doc)
                    inj._coll.insert_many(docs, ordered=False)
                    docs = []
                    inserted += size

            # 1. Uniform baseline
            _measure(
                "uniform",
                seed_fn=_seed_uniform,
                inject_fn=lambda: self._inject_bulk(inj, n_events, n_nodes=100),
            )

            # 2. Hot node — all history and test events on one node
            _measure(
                "hot_node",
                seed_fn=_seed_hot_node,
                inject_fn=lambda: self._inject_bulk(inj, n_events, n_nodes=1),
            )

            # 3. Hot entity — all history and test events on one node/entity
            _measure(
                "hot_entity",
                seed_fn=_seed_hot_entity,
                inject_fn=lambda: [
                    inj._coll.insert_one({
                        **_make_hea_event(self._node(0), i),
                        "healthevent": {
                            **_make_hea_event(self._node(0), i)["healthevent"],
                            "entitiesimpacted": [{"entitytype": "gpu", "entityvalue": "0"}],
                        },
                    }) for i in range(n_events)
                ],
            )

            # 4. Duplicate pattern — same node + same errorcode repeated
            _measure(
                "duplicates",
                seed_fn=_seed_uniform,
                inject_fn=lambda: [
                    inj._coll.insert_one({
                        **_make_hea_event(self._node(0), 0),  # identical signature
                    }) for _ in range(n_events)
                ],
            )

            # 5. Fatal mix — half isfatal=True, half isfatal=False
            def _inject_fatal_mix():
                docs = []
                for i in range(n_events):
                    doc = _make_hea_event(self._node(i % 100), i)
                    doc["healthevent"]["isfatal"] = (i % 2 == 0)
                    docs.append(doc)
                inj._coll.insert_many(docs, ordered=False)

            _measure(
                "fatal_mix",
                seed_fn=_seed_uniform,
                inject_fn=_inject_fatal_mix,
            )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _window_label(seconds: int) -> str:
    if seconds < 3600:
        return f"{seconds // 60}min"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"



def _get_rule_names() -> list[str]:
    """Read active rule names from the HEA configmap."""
    try:
        raw = _kubectl("get", "configmap", _HEA_CFGMAP, "-n", NAMESPACE,
                       "-o", "jsonpath={.data.config\\.toml}")
        names: list[str] = []
        in_rules = False
        enabled  = False
        name     = ""
        for line in raw.splitlines():
            stripped = line.strip()
            if stripped == "[[rules]]":
                if name and enabled:
                    names.append(name)
                in_rules = True
                name     = ""
                enabled  = False
            elif in_rules and stripped.startswith("name ="):
                name = stripped.split('"')[1]
            elif in_rules and stripped.startswith("evaluate_rule = true"):
                enabled = True
        if name and enabled:
            names.append(name)
        return names
    except Exception:
        return []
