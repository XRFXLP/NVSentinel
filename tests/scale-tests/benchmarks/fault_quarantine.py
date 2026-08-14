"""
benchmarks/fault_quarantine.py — Fault Quarantine Module microbenchmarks.

Implements GitHub issue #1518: Microbenchmark Fault Quarantine Module.

Scenarios
---------
  throughput        Event throughput sweep (nominal → peak → knee)
  fleet_storm       1 fatal event per N nodes simultaneously — cordon throughput
  noisy_node        Sequential burst per node — backlog-driven P99 latency
  sustained_rate    Continuous distinct events — find unbounded-backlog threshold
  post_cordon_flood Flood already-cordoned nodes — handleAlreadyQuarantined cost
  ruleset           Ruleset count × CEL complexity sweep
  circuit_breaker   Circuit breaker threshold and recovery

Injection method
----------------
  Direct MongoDB insert (bypasses platform-connector) to isolate the
  DB → FQ change stream → API server cordon path. Requires:
  - kubectl access to extract mongo-app-client-cert-secret
  - Port-forward to mongodb-0:27017 (or pass --mongo-port)
  - FQ deployed and watching the datastore change stream

  For end-to-end platform-connector path, use --via-grpc with a
  platform-connector socket (falls back to GrpcInjector).
"""
from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field

from framework.cluster import ClusterClient, ClusterSetup
from framework.events import (
    EventRateController,
    GrpcInjector,
    HealthEventSpec,
    make_fatal_gpu_event_factory,
    make_healthy_event_factory,
)
from framework.log import log
from framework.mongo import MongoInjector
from framework.prometheus import NVSentinelMetrics, PrometheusClient, Snapshot
from framework.results import BenchmarkResults, BenchmarkRun, LatencyTracker


FQ_JOB = "fault-quarantine"


@dataclass
class FQBenchmark:
    client: ClusterClient
    prom: PrometheusClient
    socket_path: str = "/var/run/nvsentinel/nvsentinel.sock"
    namespace: str = "benchmark"
    pod_sample_namespace: str = "gpu-operator"
    size_tolerance_pct: float = 15.0
    warmup_s: int = 60
    duration_s: int = 300
    repetitions: int = 3
    snapshot_interval_s: int = 15
    skip: set = field(default_factory=set)
    event_rates: list[int] = field(default_factory=lambda: [10, 50, 200, 500])
    ruleset_counts: list[int] = field(default_factory=lambda: [6, 25, 100])
    cordon_node_counts: list[int] = field(default_factory=lambda: [10, 100, 500])
    processing_strategy: str = "STORE_ONLY"
    skip_verify: bool = False
    no_cleanup: bool = False
    dry_run: bool = False
    version: str = "unknown"
    output: str = "results"

    def __post_init__(self):
        self.setup   = ClusterSetup(self.client, self.pod_sample_namespace, self.size_tolerance_pct)
        self.metrics = NVSentinelMetrics(self.prom, job=FQ_JOB)
        self.results = BenchmarkResults(component="fq", version=self.version)
        self.injector = GrpcInjector(socket_path=self.socket_path)

    def _fq_snapshot(self, label: str) -> Snapshot:
        snap = Snapshot(label=label)
        snap.heap_alloc_mb      = self.metrics.heap_alloc_mb()
        snap.cpu_pct            = self.metrics.cpu_pct()
        snap.events_rps         = self.metrics.fq_events_rps()
        snap.processing_p50_ms  = self.metrics.fq_handling_p50_ms()
        snap.processing_p99_ms  = self.metrics.fq_handling_p99_ms()
        snap.queue_depth        = int(self.metrics.fq_backlog() or 0)
        snap.extras = {
            "cordon_p50_ms":    self.metrics.fq_cordon_p50_ms(),
            "cordon_p99_ms":    self.metrics.fq_cordon_p99_ms(),
            "cordons_rps":      self.metrics.fq_cordons_rps(),
            "ruleset_eval_rps": self.metrics.fq_ruleset_eval_rps(),
            "breaker_util":     self.metrics.fq_breaker_utilization(),
        }
        return snap

    def _node_names(self, count: int) -> list[str]:
        """Return `count` actual KWOK node names from the cluster."""
        names, cont = [], None
        while len(names) < count:
            path = "/api/v1/nodes?labelSelector=type%3Dkwok&limit=500"
            if cont:
                path += f"&continue={cont}"
            resp = self.client.get(path)
            names += [n["metadata"]["name"] for n in resp.get("items", [])]
            cont = resp.get("metadata", {}).get("continue")
            if not cont:
                break
        return sorted(names)[:count]

    def _wait_backlog_drain(self, timeout_s: int = 300, poll_s: int = 5) -> float:
        """Wait for FQ event backlog to reach 0. Returns drain time in seconds."""
        t0 = time.time()
        while time.time() - t0 < timeout_s:
            backlog = self.metrics.fq_backlog() or 0
            if backlog == 0:
                return time.time() - t0
            time.sleep(poll_s)
        return time.time() - t0

    def _uncordon_nodes(self, node_names: list[str]) -> None:
        """Remove cordon + FQ annotations from nodes via K8s proxy."""
        for name in node_names:
            self.client.patch_status(
                f"/api/v1/nodes/{name}",
                {"spec": {"unschedulable": False}},
            )

    # ── Scenario: fleet storm ─────────────────────────────────────────────────

    def _run_fleet_storm(self, node_counts: list[int] | None = None) -> None:
        """
        MB-FQ-2: Fleet storm — 1 fatal event per N nodes simultaneously.

        Injects N distinct node events at once and measures:
        - Time for FQ to cordon all N nodes (cordon throughput)
        - P50/P99 cordon latency from fault_quarantine_node_quarantine_duration_seconds
        - Backlog growth and drain rate

        The bottleneck: serial change stream + 5 QPS client-go limit → ~2.5 nodes/s.
        """
        counts = node_counts or self.cordon_node_counts
        log("\n── FQ Fleet Storm (cordon throughput) ───────────────────────")

        with MongoInjector.from_cluster() as inj:
            for n in counts:
                node_names = self._node_names(n)
                log(f"  Storm: {n} nodes")

                run = BenchmarkRun(
                    scenario=f"fleet_storm_{n}_nodes",
                    component="fq",
                    metadata={"node_count": n},
                )

                for rep in range(self.repetitions):
                    # Ensure nodes are uncordoned before injection
                    self._uncordon_nodes(node_names)
                    time.sleep(2)

                    # Snapshot before
                    cordons_before = self.metrics.fq_cordons_rps(window="10s") or 0

                    t_inject = time.time()
                    inj.inject_fleet_storm(node_names)
                    log(f"    rep{rep+1}: {n} events injected, waiting for drain...")

                    drain_s = self._wait_backlog_drain(timeout_s=600)
                    elapsed = time.time() - t_inject

                    snap = self._fq_snapshot(f"fleet_storm_{n}_rep{rep+1}")
                    snap.extras["storm_nodes"]       = n
                    snap.extras["total_elapsed_s"]   = round(elapsed, 1)
                    snap.extras["drain_s"]           = round(drain_s, 1)
                    snap.extras["cordon_rate_nodes_s"] = round(n / elapsed, 2)

                    run.add(snap)
                    log(f"    rep{rep+1}: elapsed={elapsed:.1f}s "
                        f"cordon_rate={n/elapsed:.2f} nodes/s "
                        f"P99_cordon={snap.extras.get('cordon_p99_ms')}ms")

                    # Cleanup
                    self._uncordon_nodes(node_names)
                    inj.clear_test_events(node_names)
                    time.sleep(5)

                self.results.add_run(run)
                self.results.save(self.output)

    # ── Scenario: noisy node backlog ──────────────────────────────────────────

    def _run_noisy_node_backlog(
        self,
        node_count:        int = 3,
        events_per_node:   int = 500,
    ) -> None:
        """
        MB-FQ-3: Noisy node backlog — sequential burst per node.

        Injects `events_per_node` events for node[0], then node[1], then node[2].
        Since FQ processes the change stream serially:
        - node[0] gets cordoned quickly (first in queue)
        - node[1]'s first fatal event waits behind node[0]'s remaining events
        - node[2]'s first fatal event waits behind ALL of node[0]+node[1]'s events

        P99 cordon latency for node[2] = processing time for 2×events_per_node events.

        Measures:
        - fault_quarantine_node_quarantine_duration_seconds P99 (the real detection lag)
        - fault_quarantine_event_backlog_count growth
        - Per-event cost on the handleAlreadyQuarantinedNode path
        """
        log(f"\n── FQ Noisy Node Backlog ({node_count} nodes × {events_per_node} events) ──")

        node_names = self._node_names(node_count)
        node_groups = [(n, events_per_node) for n in node_names]

        with MongoInjector.from_cluster() as inj:
            for rep in range(self.repetitions):
                self._uncordon_nodes(node_names)
                time.sleep(2)

                run = BenchmarkRun(
                    scenario=f"noisy_node_{node_count}n_{events_per_node}ev",
                    component="fq",
                    metadata={"node_count": node_count, "events_per_node": events_per_node},
                )

                t0 = time.time()
                inj.inject_noisy_node_backlog(node_groups)
                log(f"  rep{rep+1}: {node_count * events_per_node} events injected, draining...")

                # Poll backlog + snapshot every poll_s
                samples = []
                while True:
                    backlog = self.metrics.fq_backlog() or 0
                    snap = self._fq_snapshot(f"noisy_rep{rep+1}")
                    snap.extras["backlog"] = backlog
                    samples.append(snap)
                    if backlog == 0 and time.time() - t0 > 5:
                        break
                    time.sleep(self.snapshot_interval_s)

                elapsed = time.time() - t0
                peak_backlog = max(s.extras.get("backlog", 0) for s in samples)
                p99 = samples[-1].extras.get("cordon_p99_ms")

                run.metadata["total_elapsed_s"] = round(elapsed, 1)
                run.metadata["peak_backlog"]     = peak_backlog
                for s in samples:
                    run.add(s)

                log(f"  rep{rep+1}: elapsed={elapsed:.1f}s "
                    f"peak_backlog={peak_backlog} "
                    f"P99_cordon={p99}ms")

                self._uncordon_nodes(node_names)
                inj.clear_test_events(node_names)
                time.sleep(5)

            self.results.add_run(run)
            self.results.save(self.output)

    # ── Scenario: sustained rate sweep ───────────────────────────────────────

    def _run_sustained_rate(self) -> None:
        """
        MB-FQ-4: Sustained rate sweep — find the event rate at which
        FQ's backlog grows unboundedly.

        Injects distinct events (varying node/entity/errorcode) at controlled
        rates and measures whether fault_quarantine_event_backlog_count
        stabilizes or grows monotonically.

        The knee point is the rate at which FQ can no longer drain the
        change stream before new events arrive.
        """
        log("\n── FQ Sustained Rate Sweep ───────────────────────────────────")

        node_names = self._node_names(100)

        with MongoInjector.from_cluster() as inj:
            for rate in self.event_rates:
                log(f"  Rate: {rate} events/s")

                run = BenchmarkRun(
                    scenario=f"sustained_{rate}ev_s",
                    component="fq",
                    metadata={"target_rate_ev_s": rate},
                )

                for rep in range(self.repetitions):
                    # Warmup
                    inj.inject_sustained_rate(node_names, rate, self.warmup_s,
                                              is_fatal=False)
                    time.sleep(2)

                    samples = []
                    t0 = time.time()

                    # Inject at rate while sampling
                    import threading
                    stop = threading.Event()

                    def inject_loop():
                        inj.inject_sustained_rate(node_names, rate, self.duration_s)
                        stop.set()

                    t = threading.Thread(target=inject_loop, daemon=True)
                    t.start()

                    while not stop.is_set():
                        snap = self._fq_snapshot(f"sustained_{rate}_rep{rep+1}")
                        snap.extras["elapsed_s"] = round(time.time() - t0, 1)
                        samples.append(snap)
                        time.sleep(self.snapshot_interval_s)

                    t.join()

                    # Determine if backlog was stable or growing
                    backlogs = [self.metrics.fq_backlog() or 0 for _ in range(3)]
                    time.sleep(2)
                    backlog_growing = backlogs[-1] > backlogs[0] * 1.1

                    snap = Snapshot(label=f"sustained_{rate}_rep{rep+1}")
                    snap.events_rps        = (sum(s.events_rps or 0 for s in samples)
                                               / max(len(samples), 1))
                    snap.processing_p99_ms = samples[-1].processing_p99_ms
                    snap.queue_depth       = samples[-1].queue_depth
                    snap.extras["backlog_growing"]   = backlog_growing
                    snap.extras["final_backlog"]     = backlogs[-1]

                    run.add(snap)
                    log(f"    rep{rep+1}: events={snap.events_rps:.1f}/s "
                        f"backlog={backlogs[-1]} "
                        f"{'GROWING ⚠' if backlog_growing else 'stable ✅'}")

                    inj.clear_test_events(node_names)
                    self._wait_backlog_drain(timeout_s=60)

                self.results.add_run(run)
                self.results.save(self.output)

    # ── Scenario: post-cordon flood ───────────────────────────────────────────

    def _run_post_cordon_flood(
        self,
        node_count:      int = 10,
        events_per_node: int = 200,
    ) -> None:
        """
        MB-FQ-5: Post-cordon flood — cost of handleAlreadyQuarantinedNode path.

        1. Cordon N nodes via fleet storm
        2. Flood those already-cordoned nodes with more events
        3. Measure per-event processing cost (no K8s writes, just DB reads +
           updateNodeQuarantineStatus writes)

        Reveals: 2 MongoDB writes per event even on the already-handled path
        (updateNodeQuarantineStatus + MarkProcessed). At high volume, this
        becomes the MongoDB write throughput bottleneck.
        """
        log(f"\n── FQ Post-Cordon Flood ({node_count} nodes × {events_per_node} events) ──")

        node_names = self._node_names(node_count)

        with MongoInjector.from_cluster() as inj:
            # Step 1: Cordon all nodes
            log("  Step 1: Fleet storm to cordon all nodes...")
            self._uncordon_nodes(node_names)
            time.sleep(2)
            inj.inject_fleet_storm(node_names)
            self._wait_backlog_drain(timeout_s=120)
            log(f"  {node_count} nodes cordoned. Now flooding...")

            run = BenchmarkRun(
                scenario=f"post_cordon_{node_count}n_{events_per_node}ev",
                component="fq",
                metadata={"node_count": node_count, "events_per_node": events_per_node},
            )

            # Step 2: Flood already-cordoned nodes
            t0 = time.time()
            total = inj.inject_post_cordon_flood(node_names, events_per_node)
            log(f"  {total} events injected, draining...")

            samples = []
            while True:
                backlog = self.metrics.fq_backlog() or 0
                snap = self._fq_snapshot("post_cordon")
                snap.extras["backlog"] = backlog
                samples.append(snap)
                if backlog == 0 and time.time() - t0 > 5:
                    break
                time.sleep(self.snapshot_interval_s)

            elapsed = time.time() - t0
            log(f"  {total} events processed in {elapsed:.1f}s "
                f"({total/elapsed:.1f} events/s) "
                f"P99={samples[-1].processing_p99_ms}ms")

            for s in samples:
                run.add(s)
            self.results.add_run(run)
            self.results.save(self.output)

            # Cleanup
            self._uncordon_nodes(node_names)
            inj.clear_test_events(node_names)

    # ── Scenario: event throughput ────────────────────────────────────────────

    def _run_throughput(self, node_tmpl) -> None:
        log("\n── FQ Throughput sweep ───────────────────────────────────────")

        # Need some KWOK nodes as event targets
        self.setup.create_kwok_nodes(0, 100, node_tmpl)

        for rate in self.event_rates:
            log(f"  Rate: {rate} events/s")
            node_names = self._node_names(100)
            factory = make_fatal_gpu_event_factory(
                node_names,
                processing_strategy=self.processing_strategy,
            )

            run = BenchmarkRun(
                scenario=f"throughput_{rate}ev_s",
                component="fq",
                warmup_s=self.warmup_s,
                measurement_s=self.duration_s,
                metadata={"target_rate_ev_s": rate, "strategy": self.processing_strategy},
            )

            for rep in range(self.repetitions):
                tracker = LatencyTracker()
                controller = EventRateController(
                    inject_fn=lambda spec: self.injector.inject_one(spec),
                    rate_per_second=rate,
                    spec_factory=factory,
                    parallelism=max(1, rate // 10),
                )
                controller.start()
                time.sleep(self.warmup_s)

                samples = []
                t0 = time.time()
                while time.time() - t0 < self.duration_s:
                    snap = self._fq_snapshot(f"rep{rep+1}")
                    samples.append(snap)
                    # Drain injected IDs for latency tracking (V1)
                    tracker.record_inject_batch(list(controller.drain_injected().keys()))
                    time.sleep(self.snapshot_interval_s)

                controller.stop()

                def avg(key: str):
                    vals = [getattr(s, key) for s in samples if getattr(s, key) is not None]
                    return round(sum(vals)/len(vals), 1) if vals else None

                snap = Snapshot(label=f"throughput_{rate}_rep{rep+1}")
                snap.events_rps        = avg("events_rps")
                snap.processing_p50_ms = avg("processing_p50_ms")
                snap.processing_p99_ms = avg("processing_p99_ms")
                snap.queue_depth       = avg("queue_depth")
                snap.heap_alloc_mb     = avg("heap_alloc_mb")
                snap.cpu_pct           = avg("cpu_pct")
                snap.extras["e2e_latency"] = tracker.summary()

                run.add(snap)
                log(f"    rep{rep+1}: events={snap.events_rps}/s "
                    f"P99={snap.processing_p99_ms}ms "
                    f"backlog={snap.queue_depth}")

            self.results.add_run(run)
            self.results.save(self.output)

        if not self.no_cleanup:
            self.setup.delete_kwok_nodes()

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self) -> None:
        log(f"FQ Benchmark  reps={self.repetitions}  warmup={self.warmup_s}s  "
            f"strategy={self.processing_strategy}")

        log("\n── Step 1: Sampling real objects ─────────────────────────────")
        node_tmpl = self.setup.sample_node()
        self.results.object_sizes = {"node_bytes": node_tmpl.raw_bytes}

        log("\n── Step 2: Verifying KWOK node size ──────────────────────────")
        if not self.skip_verify:
            self.setup.freeze_dns_autoscaler()
            self.setup.verify_node_size(node_tmpl)
        else:
            self.setup.freeze_dns_autoscaler()

        try:
            if "fleet_storm" not in self.skip:
                self._run_fleet_storm()
            if "noisy_node" not in self.skip:
                self._run_noisy_node_backlog()
            if "sustained_rate" not in self.skip:
                self._run_sustained_rate()
            if "post_cordon_flood" not in self.skip:
                self._run_post_cordon_flood()
            if "throughput" not in self.skip:
                self._run_throughput(node_tmpl)
            if "ruleset" not in self.skip:
                log("  ruleset sweep — not yet implemented")
            if "circuit_breaker" not in self.skip:
                log("  circuit_breaker — not yet implemented")
        finally:
            self.setup.restore_dns_autoscaler()

        self.results.finish()
        path = self.results.save(self.output)
        self.results.print_summary()
        log(f"Results: {path}")
