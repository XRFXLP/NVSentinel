"""
benchmarks/janitor.py — Janitor microbenchmarks.

Implements GitHub issue #1524: Microbenchmark Janitor.

Scenarios implemented
---------------------
  admission   Scenario 2: Admission webhook latency under retained CR load.
              Pre-populates N completed RebootNode CRs and measures P50/P90/P99
              admission latency as N increases. The webhook makes an uncached,
              unfiltered cluster-wide LIST on every admission request — this
              exposes O(N) scaling in the webhook's duplicate-detection path.

Scenarios planned (require fake janitor-provider + KWOK lifecycle stages)
---------------------
  lock        Scenario 3: Node-lock contention
  concurrent  Scenario 1: Concurrent maintenance actions
  ttl         Scenario 7: TTL cleanup throughput
  recovery    Scenario 8: Restart / cold recovery
"""
from __future__ import annotations

import concurrent.futures
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from framework.cluster import ClusterClient
from framework.log import log
from framework.prometheus import PrometheusClient, Snapshot
from framework.results import BenchmarkResults, BenchmarkRun


_GROUP   = "janitor.dgxc.nvidia.com"
_VERSION = "v1alpha1"
_REBOOT  = f"/apis/{_GROUP}/{_VERSION}/rebootnodes"
_LABEL_KEY   = "benchmark.nvsentinel/janitor"
_LABEL_VAL   = "admission-webhook"


@dataclass
class JanitorBenchmark:
    client: ClusterClient
    prom: PrometheusClient
    retained_counts: list[int] = field(
        default_factory=lambda: [0, 100, 500, 1_000, 5_000, 10_000]
    )
    probes_per_point: int = 15
    version: str = "unknown"
    output: str = "results"

    def __post_init__(self) -> None:
        self.results = BenchmarkResults(component="janitor", version=self.version)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _kwok_nodes(self, count: int, cache_file: str = "/tmp/janitor-bench-kwok-nodes.txt") -> list[str]:
        import os
        # Use cached node list to avoid slow 99k-node pagination on every run
        if os.path.exists(cache_file):
            with open(cache_file) as f:
                cached = [l.strip() for l in f if l.strip()]
            if len(cached) >= count:
                log(f"  using {count} cached node names from {cache_file}")
                return sorted(cached)[:count]

        log(f"  fetching KWOK node names (this may take several minutes on large clusters)...")
        names: list[str] = []
        cont = None
        while len(names) < count:
            path = "/api/v1/nodes?labelSelector=type%3Dkwok&limit=500"
            if cont:
                path += f"&continue={cont}"
            resp = self.client.get(path)
            names += [n["metadata"]["name"] for n in resp.get("items", [])]
            cont = resp.get("metadata", {}).get("continue")
            if not cont:
                break
        if len(names) < count:
            raise RuntimeError(
                f"Need {count} KWOK nodes for benchmark, found only {len(names)}"
            )
        names = sorted(names)
        # Cache for future runs
        with open(cache_file, "w") as f:
            f.write("\n".join(names))
        log(f"  cached {len(names)} node names to {cache_file}")
        return names[:count]

    def _now(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _cr_name(self, prefix: str, idx: int) -> str:
        return f"bench-jan-{prefix}-{idx:06d}"

    def _create_reboot_cr(self, name: str, node: str, timeout: int = 30) -> float:
        """
        POST a RebootNode CR. Returns admission round-trip latency in seconds,
        or -1.0 on failure.
        """
        body = {
            "apiVersion": f"{_GROUP}/{_VERSION}",
            "kind": "RebootNode",
            "metadata": {
                "name": name,
                "labels": {_LABEL_KEY: _LABEL_VAL},
            },
            "spec": {"nodeName": node},
        }
        t0 = time.perf_counter()
        ok = self.client.post(_REBOOT, body, timeout=timeout)
        elapsed = time.perf_counter() - t0
        return elapsed if ok else -1.0

    def _mark_completed(self, name: str) -> None:
        """
        Patch the status subresource to set completionTime.
        This makes the webhook treat the CR as inactive (not a duplicate blocker)
        and stops the Janitor controller from trying to process it.
        """
        ts = self._now()
        self.client.patch_status(
            f"{_REBOOT}/{name}/status",
            {
                "status": {
                    "completionTime": ts,
                    "conditions": [
                        {
                            "type": "SignalSent",
                            "status": "True",
                            "reason": "BenchmarkPreload",
                            "lastTransitionTime": ts,
                            "message": "pre-populated by janitor admission benchmark",
                        },
                        {
                            "type": "NodeReady",
                            "status": "True",
                            "reason": "BenchmarkPreload",
                            "lastTransitionTime": ts,
                            "message": "pre-populated by janitor admission benchmark",
                        },
                    ],
                }
            },
        )

    def _delete_bench_crs(self) -> int:
        items = self.client.list_all(_REBOOT, label_selector=f"{_LABEL_KEY}={_LABEL_VAL}")
        for item in items:
            self.client.delete(f"{_REBOOT}/{item['metadata']['name']}")
        return len(items)

    def _webhook_p99_ms(self) -> float | None:
        """
        Query the K8s API server admission webhook duration metric.
        Returns P99 in ms, or None if the metric is unavailable.
        """
        try:
            q = (
                'histogram_quantile(0.99, rate('
                'apiserver_admission_webhook_admission_duration_seconds_bucket'
                '{name=~".*rebootnode.*"}[2m])) * 1000'
            )
            return self.prom.query(q)
        except Exception:
            return None

    # ── Scenario 2: Admission webhook ─────────────────────────────────────────

    def run_admission_webhook(self) -> None:
        """
        MB-JAN-2: Admission webhook latency under retained CR load.

        Root cause being measured: the webhook uses an uncached client and calls
        client.List(all RebootNodes) with NO field/label selectors on every
        admission request. The entire cluster-wide CR list is fetched and filtered
        in-memory. Latency therefore scales O(N) with the total retained CR count.

        Method
        ------
        For each point in retained_counts:
          1. Incrementally add completed RebootNode CRs for distinct KWOK nodes.
             "Completed" (completionTime set) means the webhook counts them in
             the LIST but does not treat them as duplicate-blockers.
          2. Fire probes_per_point admission requests for nodes NOT in the
             retained set. Record wall-clock time per POST (includes webhook
             round-trip to API server).
          3. Compute P50/P90/P99 from the probe latencies.
          4. Delete probe CRs; retained CRs accumulate across iterations.
        Final cleanup deletes all benchmark CRs.
        """
        log("\n── MB-JAN-2: Admission Webhook Latency ──────────────────────")
        log(f"  cr_kind:         RebootNode")
        log(f"  retained_counts: {self.retained_counts}")
        log(f"  probes/point:    {self.probes_per_point}")

        max_nodes = max(self.retained_counts) + self.probes_per_point
        log(f"  fetching {max_nodes} KWOK node names...")
        all_nodes = self._kwok_nodes(max_nodes)

        run = BenchmarkRun(
            scenario="admission_webhook",
            component="janitor",
            warmup_s=0,
            measurement_s=0,
            metadata={
                "cr_kind": "RebootNode",
                "retained_counts": self.retained_counts,
                "probes_per_point": self.probes_per_point,
                "webhook_list_selector": "none (unfiltered cluster-wide LIST)",
            },
        )

        prev_count = 0

        for retained_count in sorted(self.retained_counts):
            # ── 1. Incrementally populate retained CRs ────────────────────
            if retained_count > prev_count:
                to_add = retained_count - prev_count
                log(f"\n  pre-populating {to_add} completed CRs "
                    f"(total will be {retained_count})...")

                def _create_and_complete(i: int) -> None:
                    name = self._cr_name("ret", i)
                    if self._create_reboot_cr(name, all_nodes[i]) >= 0:
                        self._mark_completed(name)

                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                    list(ex.map(_create_and_complete, range(prev_count, retained_count)))
                time.sleep(3)  # let API server index settle
            prev_count = retained_count

            # ── 2. Probe admission latency ────────────────────────────────
            probe_nodes = all_nodes[retained_count : retained_count + self.probes_per_point]
            probe_names: list[str] = []
            latencies_ms: list[float] = []

            log(f"  retained={retained_count:6d}  probing...")
            for j, node in enumerate(probe_nodes):
                name = self._cr_name("probe", retained_count * 1000 + j)
                latency_s = self._create_reboot_cr(name, node)
                if latency_s >= 0:
                    latencies_ms.append(latency_s * 1_000)
                    probe_names.append(name)
                else:
                    log(f"    WARNING: admission failed for probe {j} (node={node})")

            # Delete probe CRs immediately
            for name in probe_names:
                self.client.delete(f"{_REBOOT}/{name}")

            if not latencies_ms:
                log("    ERROR: all probes failed — is Janitor deployed?")
                continue

            # ── 3. Compute statistics ─────────────────────────────────────
            latencies_ms.sort()
            n = len(latencies_ms)
            p50 = statistics.median(latencies_ms)
            p90 = latencies_ms[int(n * 0.90)]
            p99 = latencies_ms[min(int(n * 0.99), n - 1)]
            p_webhook = self._webhook_p99_ms()

            snap = Snapshot(label=f"retained_{retained_count}")
            snap.extras = {
                "retained_count": retained_count,
                "admission_p50_ms":    round(p50, 1),
                "admission_p90_ms":    round(p90, 1),
                "admission_p99_ms":    round(p99, 1),
                "admission_min_ms":    round(min(latencies_ms), 1),
                "admission_max_ms":    round(max(latencies_ms), 1),
                "apiserver_webhook_p99_ms": round(p_webhook, 1) if p_webhook else None,
                "samples": n,
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            log(f"  retained={retained_count:6d}  "
                f"P50={p50:6.1f}ms  P90={p90:6.1f}ms  P99={p99:6.1f}ms  "
                f"max={max(latencies_ms):6.1f}ms"
                + (f"  webhook_p99={p_webhook:.1f}ms" if p_webhook else ""))

        # ── Cleanup ───────────────────────────────────────────────────────────
        log("\n  cleaning up benchmark CRs...")
        deleted = self._delete_bench_crs()
        log(f"  deleted {deleted} CRs")

    # ── Scenario 7: TTL cleanup throughput ───────────────────────────────────────

    def _count_bench_crs(self) -> int:
        resp = self.client.get(f"{_REBOOT}?labelSelector={_LABEL_KEY}%3D{_LABEL_VAL}&limit=1")
        remaining = resp.get("metadata", {}).get("remainingItemCount", 0)
        return remaining + len(resp.get("items", []))

    def _create_expired_cr(self, name: str, node: str, ttl: str = "1s", past_s: int = 60) -> bool:
        """Create a RebootNode CR that is already past its TTL expiry."""
        from datetime import datetime, timezone, timedelta
        body = {
            "apiVersion": f"{_GROUP}/{_VERSION}",
            "kind": "RebootNode",
            "metadata": {
                "name": name,
                "labels": {_LABEL_KEY: _LABEL_VAL},
                "annotations": {"nvsentinel.nvidia.com/ttl": ttl},
            },
            "spec": {"nodeName": node},
        }
        if not self.client.post(_REBOOT, body):
            return False
        # Set completionTime in the past so TTL is already elapsed
        past = (datetime.now(timezone.utc) - timedelta(seconds=past_s)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.client.patch_status(
            f"{_REBOOT}/{name}/status",
            {"status": {
                "completionTime": past,
                "conditions": [
                    {"type": "SignalSent", "status": "True", "reason": "BenchmarkTTL",
                     "lastTransitionTime": past, "message": "ttl benchmark"},
                    {"type": "NodeReady", "status": "True", "reason": "BenchmarkTTL",
                     "lastTransitionTime": past, "message": "ttl benchmark"},
                ],
            }},
        )
        return True

    def run_ttl_cleanup(
        self,
        cr_counts: list[int] | None = None,
        poll_interval_s: float = 2.0,
        drain_timeout_s: int = 300,
        metrics_url: str = "http://localhost:2112/metrics",
    ) -> None:
        """
        Scenario 7: TTL cleanup throughput.

        Creates N completed, already-expired RebootNode CRs and measures how fast
        the TTL reconciler deletes them. The TTL reconciler is a single-worker
        controller doing GET → expiry check → DELETE per item.

        Sweeps: pre-expired CRs (delete immediately), preserve annotation (no deletion),
        and high CR counts to find the deletion throughput ceiling.
        """
        counts = cr_counts or [100, 500, 1000, 5000, 10000]
        log("\n── Scenario 7: TTL Cleanup Throughput ──────────────────────────")
        log(f"  cr_counts: {counts}")

        nodes = self._kwok_nodes(max(counts) + 10)

        run = BenchmarkRun(
            scenario="ttl_cleanup",
            component="janitor",
            warmup_s=0,
            measurement_s=0,
            metadata={"cr_kind": "RebootNode", "cr_counts": counts, "ttl": "1s"},
        )

        for n in counts:
            log(f"\n  N={n} pre-expired CRs (ttl=1s, completionTime=60s ago)")

            # Create N expired CRs in parallel
            log(f"    creating {n} expired CRs...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
                def _make(i):
                    return self._create_expired_cr(
                        self._cr_name("ttl", i), nodes[i], ttl="1s", past_s=60
                    )
                results = list(ex.map(_make, range(n)))
            created = sum(results)
            log(f"    created {created}, waiting for TTL controller to drain...")

            # Poll until all deleted
            t0 = time.time()
            prev = created
            while True:
                remaining = self._count_bench_crs()
                elapsed = time.time() - t0
                if remaining == 0:
                    break
                if elapsed > drain_timeout_s:
                    log(f"    TIMEOUT after {elapsed:.0f}s, {remaining} CRs remaining")
                    self._delete_bench_crs()
                    break
                if remaining != prev:
                    log(f"    {remaining:5} remaining  ({elapsed:.0f}s)")
                    prev = remaining
                time.sleep(poll_interval_s)

            elapsed = time.time() - t0
            rate = created / elapsed if elapsed > 0 else 0

            # Snapshot Janitor CPU from metrics
            m = self._scrape_metrics(metrics_url)
            cpu_s = m.get('process_cpu_seconds_total', None)
            mem_mb = m.get('process_resident_memory_bytes', 0) / 1e6

            snap = Snapshot(label=f"ttl_n{n}")
            snap.extras = {
                "cr_count":       n,
                "created":        created,
                "drain_s":        round(elapsed, 1),
                "deletion_rate":  round(rate, 1),
                "janitor_mem_mb": round(mem_mb, 1),
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            log(f"    N={n:6}  drain={elapsed:.1f}s  rate={rate:.1f} CR/s  mem={mem_mb:.0f}MB")
            time.sleep(5)

        # Preserve annotation — verify no deletion
        log(f"\n  preserve annotation test (N=50, ttl=1s, preserve=true)")
        preserve_nodes = nodes[max(counts):max(counts) + 50]
        preserve_names = []
        for i, node in enumerate(preserve_nodes):
            name = self._cr_name("preserve", i)
            body = {
                "apiVersion": f"{_GROUP}/{_VERSION}", "kind": "RebootNode",
                "metadata": {
                    "name": name, "labels": {_LABEL_KEY: _LABEL_VAL},
                    "annotations": {
                        "nvsentinel.nvidia.com/ttl": "1s",
                        "nvsentinel.nvidia.com/preserve": "true",
                    },
                },
                "spec": {"nodeName": node},
            }
            if self.client.post(_REBOOT, body):
                past = self._now()
                self.client.patch_status(f"{_REBOOT}/{name}/status", {
                    "status": {"completionTime": past}
                })
                preserve_names.append(name)

        time.sleep(15)
        surviving = sum(1 for name in preserve_names
                        if self.client.get(f"{_REBOOT}/{name}").get("metadata"))
        log(f"    preserve=true: {surviving}/{len(preserve_names)} CRs survived after 15s "
            f"({'✅ correct' if surviving == len(preserve_names) else '❌ unexpected deletion'})")

        snap = Snapshot(label="preserve_annotation")
        snap.extras = {"preserve_crs": len(preserve_names), "survived": surviving,
                       "correct": surviving == len(preserve_names)}
        run.add(snap)
        self.results.add_run(run)
        self.results.save(self.output)

        # Cleanup preserve CRs
        for name in preserve_names:
            self.client.patch_status(f"{_REBOOT}/{name}", {
                "metadata": {"annotations": {"nvsentinel.nvidia.com/preserve": None}}
            })
            self.client.delete(f"{_REBOOT}/{name}")

        log("\n  done")

    # ── Scenario P0-a: Concurrent reconcile / gRPC connection churn ──────────────

    def _scrape_metrics(self, metrics_url: str) -> dict:
        import urllib.request as _ur
        try:
            raw = _ur.urlopen(metrics_url, timeout=5).read().decode()
        except Exception:
            return {}
        result = {}
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

    def _reconcile_total(self, m: dict) -> float:
        return sum(v for k, v in m.items()
                   if "controller_runtime_reconcile_total" in k and 'rebootnode"' in k)

    def _reconcile_p50_ms(self, m: dict) -> float | None:
        buckets: list[tuple[float, float]] = []
        inf_count = 0.0
        for k, v in m.items():
            if "controller_runtime_reconcile_time_seconds_bucket" not in k:
                continue
            if 'rebootnode"' not in k:
                continue
            le_str = next((p.split("=")[1].strip('"')
                           for p in k.split("{")[1].rstrip("}").split(",") if "le=" in p), None)
            if le_str is None:
                continue
            if le_str == "+Inf":
                inf_count = v
            else:
                try:
                    buckets.append((float(le_str), v))
                except ValueError:
                    pass
        if not buckets or inf_count == 0:
            return None
        buckets.sort()
        half = inf_count / 2
        for le, cum in buckets:
            if cum >= half:
                return le * 1000
        return None

    def run_concurrent_reconcile(
        self,
        concurrency_levels: list[int] | None = None,
        measurement_s: int = 60,
        metrics_url: str = "http://localhost:2112/metrics",
    ) -> None:
        """
        P0-a: gRPC connection-per-reconcile churn under N concurrent active CRs.

        Root cause (rebootnode.go:528): controller opens a NEW TCP+TLS gRPC
        connection on every reconcile of an active CR. Each connection also
        triggers a SA TokenReview. Net: O(active_CRs / requeue_interval_s)
        new connections per second at steady state.
        """
        levels = concurrency_levels or [1, 10, 50, 100]
        log("\n── P0-a: Concurrent Reconcile / gRPC Connection Churn ──────────")
        log(f"  concurrency_levels: {levels}")
        log(f"  measurement: {measurement_s}s per level")

        nodes = self._kwok_nodes(max(levels) + 10)

        run = BenchmarkRun(
            scenario="concurrent_reconcile",
            component="janitor",
            warmup_s=15,
            measurement_s=measurement_s,
            metadata={
                "cr_kind": "RebootNode",
                "concurrency_levels": levels,
                "root_cause": "rebootnode.go:528 — new gRPC conn every reconcile",
            },
        )

        for n in levels:
            log(f"\n  concurrency={n}")
            log(f"    creating {n} active CRs...")
            cr_names: list[str] = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                def _create(i: int) -> str | None:
                    name = self._cr_name("active", i)
                    return name if self._create_reboot_cr(name, nodes[i], timeout=15) >= 0 else None
                cr_names = [r for r in ex.map(_create, range(n)) if r]
            log(f"    {len(cr_names)} CRs active, warming up 15s...")
            time.sleep(15)

            m0 = self._scrape_metrics(metrics_url)
            r0 = self._reconcile_total(m0)
            t0 = time.time()
            time.sleep(measurement_s)
            m1 = self._scrape_metrics(metrics_url)
            elapsed = time.time() - t0

            reconcile_rate = (self._reconcile_total(m1) - r0) / elapsed
            p50_ms = self._reconcile_p50_ms(m1)

            snap = Snapshot(label=f"concurrency_{n}")
            snap.extras = {
                "active_crs":             n,
                "reconcile_rate_per_s":   round(reconcile_rate, 2),
                "grpc_connections_per_s": round(reconcile_rate, 2),
                "reconcile_p50_ms":       round(p50_ms, 1) if p50_ms else None,
                "elapsed_s":              round(elapsed, 1),
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            p50_str = f"  p50={p50_ms:.0f}ms" if p50_ms else ""
            log(f"    reconcile/s={reconcile_rate:.2f}  grpc_conns/s≈{reconcile_rate:.2f}{p50_str}")

            log(f"    deleting {len(cr_names)} CRs...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                list(ex.map(lambda name: self.client.delete(f"{_REBOOT}/{name}"), cr_names))
            time.sleep(10)

        log("\n  done")

    # ── Scenario 1: Concurrent maintenance actions ───────────────────────────

    def _keep_nodes_ready(
        self,
        node_names: list[str],
        stop_event,
        interval_s: float = 10.0,
    ) -> None:
        """Background thread: keep a set of nodes at Ready=True every interval_s."""
        import urllib.request as _ur
        import json as _json
        patch = _json.dumps({"status": {"conditions": [
            {"type": "Ready", "status": "True", "reason": "KubeletReady",
             "message": "bench-keeper", "lastHeartbeatTime": "2026-08-06T12:00:00Z",
             "lastTransitionTime": "2026-08-06T12:00:00Z"},
            {"type": "MemoryPressure", "status": "False", "reason": "KubeletHasSufficientMemory",
             "message": "", "lastHeartbeatTime": "2026-08-06T12:00:00Z",
             "lastTransitionTime": "2026-08-06T12:00:00Z"},
            {"type": "DiskPressure", "status": "False", "reason": "KubeletHasNoDiskPressure",
             "message": "", "lastHeartbeatTime": "2026-08-06T12:00:00Z",
             "lastTransitionTime": "2026-08-06T12:00:00Z"},
            {"type": "PIDPressure", "status": "False", "reason": "KubeletHasSufficientPID",
             "message": "", "lastHeartbeatTime": "2026-08-06T12:00:00Z",
             "lastTransitionTime": "2026-08-06T12:00:00Z"},
        ]}}).encode()
        while not stop_event.is_set():
            for name in node_names:
                req = _ur.Request(
                    f"{self.client.proxy}/api/v1/nodes/{name}/status",
                    data=patch, method="PATCH",
                    headers={"Content-Type": "application/merge-patch+json"})
                try: _ur.urlopen(req, timeout=5)
                except: pass
            stop_event.wait(interval_s)

    def _wait_all_complete(
        self,
        cr_names: list[str],
        timeout_s: int = 300,
        poll_s: float = 2.0,
        keep_ready_nodes: list[str] | None = None,
    ) -> tuple[float, dict[str, float]]:
        """
        Poll until all CRs have completionTime set.
        Returns (total_elapsed_s, {name: completion_latency_s}).
        """
        import threading
        t0 = time.time()
        completion_times: dict[str, float] = {}
        pending = set(cr_names)

        # Start node-keeper thread if requested
        stop_event = threading.Event()
        keeper_thread = None
        if keep_ready_nodes:
            keeper_thread = threading.Thread(
                target=self._keep_nodes_ready,
                args=(keep_ready_nodes, stop_event),
                daemon=True)
            keeper_thread.start()

        while pending and (time.time() - t0) < timeout_s:
            still_pending = set()
            for name in list(pending):
                resp = self.client.get(f"{_REBOOT}/{name}")
                ct = resp.get("status", {}).get("completionTime")
                if ct:
                    completion_times[name] = time.time() - t0
                else:
                    still_pending.add(name)
            pending = still_pending
            if pending:
                time.sleep(poll_s)

        stop_event.set()
        if keeper_thread:
            keeper_thread.join(timeout=2)
        return time.time() - t0, completion_times

    def run_concurrent_maintenance(
        self,
        concurrency_levels: list[int] | None = None,
        metrics_url: str = "http://localhost:2112/metrics",
    ) -> None:
        """
        Scenario 1: Concurrent maintenance actions throughput.

        Creates N RebootNode CRs simultaneously for N distinct Ready KWOK nodes.
        The kwok CSP provider waits 30s then returns IsNodeReady=true.

        Measures:
          - Total wall-clock from first CR created to last CR completed
          - Per-CR completion latency (P50/P90/P99)
          - Effective throughput (CRs/s)
          - Janitor reconcile rate and CPU from metrics

        Single-worker model prediction:
          - Signal phase: N × ~5ms (sequential)
          - Wait phase: ~30s (concurrent — all CRs waiting simultaneously)
          - Completion phase: N × ~5ms (sequential)
          - Expected total: ~30s + N×10ms regardless of N

        The knee occurs if the signal or completion phase takes longer than
        the 30s wait (i.e., at N > 3000 with 5ms/reconcile).
        """
        levels = concurrency_levels or [1, 10, 50, 100]
        log("\n── Scenario 1: Concurrent Maintenance Actions ──────────────────")
        log(f"  concurrency_levels: {levels}")
        log(f"  kwok reboot duration: 30s")

        nodes = self._kwok_nodes(max(levels) + 10)

        run = BenchmarkRun(
            scenario="concurrent_maintenance",
            component="janitor",
            warmup_s=0,
            measurement_s=0,
            metadata={
                "cr_kind": "RebootNode",
                "csp": "kwok",
                "kwok_reboot_duration_s": 30,
                "concurrency_levels": levels,
                "worker_count": 1,
            },
        )

        for n in levels:
            log(f"\n  N={n} concurrent RebootNode CRs")

            # Snapshot metrics before
            m0 = self._scrape_metrics(metrics_url)
            r0 = self._reconcile_total(m0)

            # Create all N CRs in parallel
            log(f"    creating {n} CRs...")
            t_create = time.time()
            cr_names: list[str] = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
                def _create(i: int) -> str | None:
                    name = self._cr_name("concurrent", i)
                    return name if self._create_reboot_cr(name, nodes[i], timeout=15) >= 0 else None
                cr_names = [r for r in ex.map(_create, range(n)) if r]
            create_elapsed = time.time() - t_create
            log(f"    {len(cr_names)} CRs created in {create_elapsed:.1f}s, waiting for completion...")

            # Wait for all to complete, keeping benchmark nodes Ready=True
            bench_nodes = [nodes[i] for i in range(n)]
            total_s, completion_times = self._wait_all_complete(
                cr_names, timeout_s=300, keep_ready_nodes=bench_nodes)
            completed = len(completion_times)

            # Snapshot metrics after
            m1 = self._scrape_metrics(metrics_url)
            reconcile_rate = (self._reconcile_total(m1) - r0) / total_s if total_s > 0 else 0
            p50_ms = self._reconcile_p50_ms(m1)

            latencies = sorted(completion_times.values())
            p50_lat = latencies[int(len(latencies) * 0.50)] if latencies else None
            p90_lat = latencies[int(len(latencies) * 0.90)] if latencies else None
            p99_lat = latencies[min(int(len(latencies) * 0.99), len(latencies)-1)] if latencies else None
            throughput = completed / total_s if total_s > 0 else 0

            snap = Snapshot(label=f"concurrent_{n}")
            snap.extras = {
                "n":                    n,
                "completed":            completed,
                "total_s":              round(total_s, 1),
                "throughput_crs_s":     round(throughput, 2),
                "create_elapsed_s":     round(create_elapsed, 1),
                "completion_p50_s":     round(p50_lat, 1) if p50_lat else None,
                "completion_p90_s":     round(p90_lat, 1) if p90_lat else None,
                "completion_p99_s":     round(p99_lat, 1) if p99_lat else None,
                "reconcile_rate_per_s": round(reconcile_rate, 2),
                "reconcile_p50_ms":     round(p50_ms, 1) if p50_ms else None,
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            log(f"    N={n:4d}  total={total_s:.1f}s  "
                f"throughput={throughput:.2f} CR/s  "
                f"p50={p50_lat:.1f}s  p99={p99_lat:.1f}s" if p50_lat else
                f"    N={n:4d}  total={total_s:.1f}s  completed={completed}")

            # Cleanup — delete CRs (TTL will also catch them but we clean up promptly)
            log(f"    cleaning up {len(cr_names)} CRs...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                list(ex.map(lambda name: self.client.delete(f"{_REBOOT}/{name}"), cr_names))
            time.sleep(5)

        log("\n  done")

    # ── Scenario 3: Node-lock contention ─────────────────────────────────────

    _TERMINATE = f"/apis/{_GROUP}/{_VERSION}/terminatenodes"

    def _create_terminate_cr(self, name: str, node: str, timeout: int = 15) -> bool:
        body = {
            "apiVersion": f"{_GROUP}/{_VERSION}",
            "kind": "TerminateNode",
            "metadata": {"name": name, "labels": {_LABEL_KEY: _LABEL_VAL}},
            "spec": {"nodeName": node},
        }
        return self.client.post(self._TERMINATE, body, timeout=timeout)

    def _cr_signal_sent(self, path: str, name: str) -> bool:
        """Return True if the CR has SignalSent=True (lock acquired, action started)."""
        try:
            resp = self.client.get(f"{path}/{name}")
        except Exception:
            return False
        for cond in resp.get("status", {}).get("conditions", []):
            if cond.get("type") in ("SignalSent",) and cond.get("status") == "True":
                return True
        return False

    def run_lock_contention(
        self,
        sweep: list[int] | None = None,
        metrics_url: str = "http://localhost:2112/metrics",
    ) -> None:
        """
        Scenario 3: Node-lock contention at scale.

        The distributed lock uses one Kubernetes Lease per node (O(1) per
        operation — GET + CREATE on acquire, GET + DELETE on release). The key
        scale question: does lock-wait time degrade as N concurrent maintenance
        actions increase? And what is the Lease API write rate at peak load?

        Also verifies the webhook same-type rejection guarantee at scale.

        Sweep: N = nodes under simultaneous cross-type contention.
        For each N:
          1. Create N RebootNode CRs (one per node) — acquires lock per node
          2. Create N TerminateNode CRs for the same N nodes — admitted by
             webhook (cross-type) but blocked by distributed lock
          3. Wait for all RebootNodes to complete (lock released)
          4. Measure: lock-wait P50/P99 (time from RebootNode completion to
             TerminateNode SignalSent=True)
          5. Capture Lease API write rate from Janitor metrics
        """
        sweep = sweep or [5, 20, 50, 100]
        log("\n── Scenario 3: Node-Lock Contention at Scale ───────────────────")
        log(f"  sweep: N={sweep} concurrent cross-type actions per sweep point")

        max_n = max(sweep)
        nodes = self._kwok_nodes(max_n + 10)

        run = BenchmarkRun(
            scenario="lock_contention",
            component="janitor",
            warmup_s=0,
            measurement_s=0,
            metadata={"sweep": sweep},
        )

        import threading

        for n in sweep:
            bench_nodes = nodes[:n]
            log(f"\n  N={n}")

            # ── Webhook rejection (same-type) ─────────────────────────────────
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
                def try_pair(i):
                    nd = bench_nodes[i]
                    ok1 = self._create_reboot_cr(self._cr_name("lk-a1", i), nd) >= 0
                    ok2 = self._create_reboot_cr(self._cr_name("lk-a2", i), nd) >= 0
                    return ok1, ok2
                pairs = list(ex.map(try_pair, range(n)))
            correct = sum(1 for a, b in pairs if (a and not b) or (not a and b))
            # delete admitted pair CRs
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                for i in range(n):
                    ex.submit(lambda nm: self.client.delete(f"{_REBOOT}/{nm}"),
                              self._cr_name("lk-a1", i))
                    ex.submit(lambda nm: self.client.delete(f"{_REBOOT}/{nm}"),
                              self._cr_name("lk-a2", i))
            time.sleep(3)

            # ── Cross-type lock sweep ─────────────────────────────────────────
            rb_names = [self._cr_name("lk-rb", i) for i in range(n)]
            tn_names = [self._cr_name("lk-tn", i) for i in range(n)]

            # Snapshot Lease metrics before
            m0 = self._scrape_metrics(metrics_url)
            lease_writes_0 = sum(v for k, v in m0.items()
                                 if "rest_client_requests_total" in k
                                 and "leases" in k.lower()
                                 and any(m in k for m in ('"create"', '"delete"', '"update"')))

            t0 = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
                rb_ok = [f.result() >= 0 for f in
                         [ex.submit(self._create_reboot_cr, rb_names[i], bench_nodes[i]) for i in range(n)]]
                tn_ok = [f.result() for f in
                         [ex.submit(self._create_terminate_cr, tn_names[i], bench_nodes[i]) for i in range(n)]]

            # Node keeper
            stop = threading.Event()
            threading.Thread(target=self._keep_nodes_ready,
                             args=(bench_nodes, stop), daemon=True).start()

            # Poll until all RebootNodes done + TerminateNodes acquired lock
            rb_done: dict[str, float] = {}
            tn_locked: dict[str, float] = {}
            deadline = time.time() + 250
            while time.time() < deadline:
                for i in range(n):
                    if rb_names[i] not in rb_done:
                        try:
                            if self.client.get(f"{_REBOOT}/{rb_names[i]}").get("status", {}).get("completionTime"):
                                rb_done[rb_names[i]] = time.time() - t0
                        except Exception:
                            rb_done[rb_names[i]] = time.time() - t0  # 404 = deleted = done
                    if tn_names[i] not in tn_locked:
                        try:
                            if self._cr_signal_sent(self._TERMINATE, tn_names[i]):
                                tn_locked[tn_names[i]] = time.time() - t0
                                # Delete immediately to prevent TerminateNode from deleting the node
                                self.client.delete(f"{self._TERMINATE}/{tn_names[i]}")
                        except Exception:
                            pass
                if len(rb_done) >= sum(rb_ok) and len(tn_locked) >= sum(tn_ok) * 0.8:
                    break
                time.sleep(3)
            stop.set()

            # Lease write rate
            m1 = self._scrape_metrics(metrics_url)
            elapsed = time.time() - t0
            lease_writes_1 = sum(v for k, v in m1.items()
                                 if "rest_client_requests_total" in k
                                 and "leases" in k.lower()
                                 and any(m in k for m in ('"create"', '"delete"', '"update"')))
            lease_rate = (lease_writes_1 - lease_writes_0) / elapsed if elapsed > 0 else 0

            # Lock-wait times
            waits = sorted(
                tn_locked[tn_names[i]] - rb_done[rb_names[i]]
                for i in range(n)
                if rb_names[i] in rb_done and tn_names[i] in tn_locked
            )
            p50 = waits[len(waits)//2] if waits else None
            p99 = waits[min(int(len(waits)*0.99), len(waits)-1)] if waits else None

            log(f"    webhook_correct={correct}/{n}  "
                f"lock-wait P50={p50:.1f}s P99={p99:.1f}s  "
                f"lease_writes/s={lease_rate:.2f}" if p50 is not None else
                f"    webhook_correct={correct}/{n}  insufficient data")

            snap = Snapshot(label=f"lock_n{n}")
            snap.extras = {
                "n": n,
                "webhook_correct_pairs": correct,
                "rb_admitted": sum(rb_ok), "tn_admitted": sum(tn_ok),
                "rb_completed": len(rb_done), "tn_lock_acquired": len(tn_locked),
                "lock_wait_p50_s": round(p50, 2) if p50 is not None else None,
                "lock_wait_p99_s": round(p99, 2) if p99 is not None else None,
                "lease_writes_per_s": round(lease_rate, 2),
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            # Cleanup
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                for nm in rb_names: ex.submit(lambda x: self.client.delete(f"{_REBOOT}/{x}"), nm)
                for nm in tn_names: ex.submit(lambda x: self.client.delete(f"{self._TERMINATE}/{x}"), nm)
            time.sleep(5)

        log("\n  done")

    # ── Scenario 5: Reboot/terminate workflow per-phase timing ───────────────

    def run_reboot_workflow(
        self,
        repetitions: int = 5,
        metrics_url: str = "http://localhost:2112/metrics",
    ) -> None:
        """
        Scenario 5: Per-phase timing of the full RebootNode lifecycle.

        Phases measured by polling CR status every 1s:
          T0  — CR created (admission)
          T1  — startTime set (lock acquired, controller started)
          T2  — SignalSent=True (gRPC call to provider succeeded)
          T3  — NodeReady=True (provider returned IsNodeReady=true)
          T4  — completionTime set (CR marked complete, lock released)

        Derived metrics:
          queue_s     = T1 - T0  (controller workqueue latency)
          signal_s    = T2 - T1  (lock acquire + gRPC signal latency)
          provider_s  = T3 - T2  (provider wait / simulated reboot duration)
          complete_s  = T4 - T3  (completion update latency)
          total_s     = T4 - T0  (end-to-end maintenance latency)
        """
        import threading

        log("\n── Scenario 5: Reboot Workflow Per-Phase Timing ────────────────")
        log(f"  repetitions: {repetitions}")
        nodes = self._kwok_nodes(repetitions + 2)

        run = BenchmarkRun(
            scenario="reboot_workflow",
            component="janitor",
            warmup_s=0,
            measurement_s=0,
            metadata={"repetitions": repetitions, "csp": "kwok", "kwok_wait_s": 30},
        )

        phase_results = []

        for rep in range(repetitions):
            node = nodes[rep]
            name = self._cr_name("s5", rep)
            log(f"\n  rep {rep+1}/{repetitions} → node={node}")

            # Node keeper for this rep
            stop = threading.Event()
            threading.Thread(target=self._keep_nodes_ready,
                             args=([node], stop), daemon=True).start()

            # Create CR and record t0
            t0 = time.perf_counter()
            if self._create_reboot_cr(name, node) < 0:
                log(f"    admission failed, skipping")
                stop.set()
                continue

            phases = {"t0": 0.0, "t1": None, "t2": None, "t3": None, "t4": None}
            deadline = time.perf_counter() + 250

            while time.perf_counter() < deadline:
                try:
                    resp = self.client.get(f"{_REBOOT}/{name}")
                except Exception:
                    time.sleep(1)
                    continue

                status = resp.get("status", {})
                now = time.perf_counter() - t0
                conds = {c["type"]: c["status"]
                         for c in status.get("conditions", [])}

                if phases["t1"] is None and status.get("startTime"):
                    phases["t1"] = now

                if phases["t2"] is None and conds.get("SignalSent") == "True":
                    phases["t2"] = now

                if phases["t3"] is None and conds.get("NodeReady") == "True":
                    phases["t3"] = now

                if phases["t4"] is None and status.get("completionTime"):
                    phases["t4"] = now
                    break

                time.sleep(1)

            stop.set()
            self.client.delete(f"{_REBOOT}/{name}")

            if phases["t4"] is None:
                log(f"    timeout — phases={phases}")
                continue

            queue_s   = phases["t1"]
            signal_s  = phases["t2"] - phases["t1"] if phases["t2"] and phases["t1"] else None
            provider_s= phases["t3"] - phases["t2"] if phases["t3"] and phases["t2"] else None
            complete_s= phases["t4"] - phases["t3"] if phases["t4"] and phases["t3"] else None
            total_s   = phases["t4"]

            log(f"    queue={queue_s:.1f}s  signal={signal_s:.1f}s  "
                f"provider={provider_s:.1f}s  complete={complete_s:.1f}s  "
                f"total={total_s:.1f}s")

            phase_results.append({
                "queue_s": queue_s, "signal_s": signal_s,
                "provider_s": provider_s, "complete_s": complete_s,
                "total_s": total_s,
            })
            time.sleep(3)

        if not phase_results:
            log("  no successful repetitions")
            return

        def median(vals):
            s = sorted(v for v in vals if v is not None)
            return s[len(s)//2] if s else None

        snap = Snapshot(label="reboot_workflow")
        snap.extras = {
            "repetitions":      len(phase_results),
            "queue_p50_s":      round(median([r["queue_s"] for r in phase_results]), 2),
            "signal_p50_s":     round(median([r["signal_s"] for r in phase_results]), 2),
            "provider_p50_s":   round(median([r["provider_s"] for r in phase_results]), 2),
            "complete_p50_s":   round(median([r["complete_s"] for r in phase_results]), 2),
            "total_p50_s":      round(median([r["total_s"] for r in phase_results]), 2),
        }
        run.add(snap)
        self.results.add_run(run)
        self.results.save(self.output)

        log(f"\n  P50 summary:")
        log(f"    queue={snap.extras['queue_p50_s']}s  "
            f"signal={snap.extras['signal_p50_s']}s  "
            f"provider={snap.extras['provider_p50_s']}s  "
            f"complete={snap.extras['complete_p50_s']}s  "
            f"total={snap.extras['total_p50_s']}s")
        log("\n  done")

    # ── Scenario 4: GPUReset workflow at scale ────────────────────────────────

    _GPURESET = f"/apis/{_GROUP}/{_VERSION}/gpuresets"
    _JOBS_NS  = f"/apis/batch/v1/namespaces/nvsentinel/jobs"

    def _create_gpureset_cr(self, name: str, node: str, timeout: int = 15) -> bool:
        body = {
            "apiVersion": f"{_GROUP}/{_VERSION}", "kind": "GPUReset",
            "metadata": {"name": name, "labels": {_LABEL_KEY: _LABEL_VAL}},
            "spec": {"nodeName": node, "selector": {"uuids": []}},
        }
        return self.client.post(self._GPURESET, body, timeout=timeout)

    def _patch_job_succeeded(self, job_name: str) -> bool:
        """Patch a Job's status to Succeeded to mock GPU reset completion."""
        return self.client.patch_status(
            f"{self._JOBS_NS}/{job_name}/status",
            {"status": {"active": 0, "succeeded": 1,
                        "completionTime": self._now(),
                        "conditions": [{"type": "Complete", "status": "True",
                                        "lastProbeTime": self._now(),
                                        "lastTransitionTime": self._now()}]}},
        )

    def _gpureset_condition(self, name: str, cond_type: str) -> bool:
        try:
            resp = self.client.get(f"{self._GPURESET}/{name}")
            for c in resp.get("status", {}).get("conditions", []):
                if c.get("type") == cond_type and c.get("status") == "True":
                    return True
        except Exception:
            pass
        return False

    def run_gpureset_workflow(
        self,
        concurrency_levels: list[int] | None = None,
        metrics_url: str = "http://localhost:2112/metrics",
    ) -> None:
        """
        Scenario 4: GPUReset workflow at scale.

        GPUReset differs from RebootNode by creating a Kubernetes Job and
        tearing down/restoring GPU operator pods per node. Scale concern:
        at N concurrent GPUResets, does Job creation throughput or pod
        lifecycle API call rate become a bottleneck?

        With KWOK nodes (no GPU operator pods), teardown/restore are
        instantaneous (nothing to do). Jobs are mocked to Succeeded
        immediately upon creation, isolating the controller throughput.

        Phases:
          T0 — GPUReset CR created
          T1 — ServicesTornDown=True (services cleared, ready to reset)
          T2 — ResetJobCreated=True (Job created by controller)
          T3 — ResetJobCompleted=True (mocked by benchmark)
          T4 — ServicesRestored=True (services restarted)
          T5 — Complete=True (CR done, lock released)
        """
        levels = concurrency_levels or [1, 10, 50]
        log("\n── Scenario 4: GPUReset Workflow at Scale ──────────────────────")
        log(f"  concurrency_levels: {levels} (Jobs mocked to Succeeded)")
        nodes = self._kwok_nodes(max(levels) + 5)

        run = BenchmarkRun(
            scenario="gpureset_workflow",
            component="janitor",
            warmup_s=0, measurement_s=0,
            metadata={"concurrency_levels": levels, "job_mode": "mocked"},
        )

        import threading

        for n in levels:
            log(f"\n  N={n}")
            gr_names = [self._cr_name("gr", i) for i in range(n)]

            # Node keeper
            stop = threading.Event()
            threading.Thread(target=self._keep_nodes_ready,
                             args=(nodes[:n], stop), daemon=True).start()

            t0 = time.time()
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                admitted = [f.result() for f in
                            [ex.submit(self._create_gpureset_cr, gr_names[i], nodes[i])
                             for i in range(n)]]
            log(f"    admitted {sum(admitted)}/{n} GPUReset CRs")

            # Pre-compute expected job names
            job_names = {gr_names[i]: f"{gr_names[i]}-reset-job" for i in range(n)}

            # Background thread: patch Jobs to Succeeded within 0.5s of creation
            job_patched: set[str] = set()
            phase_lock = threading.Lock()

            def _job_mocker(stop_evt):
                while not stop_evt.is_set():
                    for gr_name, job_name in job_names.items():
                        if job_name not in job_patched:
                            try:
                                resp = self.client.get(f"{self._JOBS_NS}/{job_name}")
                                if resp.get("metadata", {}).get("name"):
                                    self._patch_job_succeeded(job_name)
                                    with phase_lock:
                                        job_patched.add(job_name)
                            except Exception:
                                pass
                    stop_evt.wait(0.5)

            mock_stop = threading.Event()
            threading.Thread(target=_job_mocker, args=(mock_stop,), daemon=True).start()

            # Track phase times
            phases = {name: {} for name in gr_names}
            deadline = time.time() + 300

            while time.time() < deadline:
                all_done = True
                for i, gr_name in enumerate(gr_names):
                    if not admitted[i]:
                        continue
                    now = time.time() - t0
                    try:
                        resp = self.client.get(f"{self._GPURESET}/{gr_name}")
                    except Exception:
                        continue

                    conds = {c["type"]: c["status"]
                             for c in resp.get("status", {}).get("conditions", [])}

                    for phase, cond in [("t1", "ServicesTornDown"),
                                        ("t2", "ResetJobCreated"),
                                        ("t4", "ServicesRestored"),
                                        ("t5", "Complete")]:
                        if phase not in phases[gr_name] and conds.get(cond) == "True":
                            phases[gr_name][phase] = now

                    # Mark job mock time
                    if "t2" in phases[gr_name] and "t3" not in phases[gr_name]:
                        jn = job_names[gr_name]
                        if jn in job_patched:
                            phases[gr_name]["t3"] = phases[gr_name]["t2"] + 0.5

                    if "t5" not in phases[gr_name]:
                        all_done = False

                if all_done:
                    break
                time.sleep(2)

            mock_stop.set()

            stop.set()

            # Compute phase durations
            def med(vals):
                s = sorted(v for v in vals if v is not None)
                return round(s[len(s)//2], 1) if s else None

            completed = [gr_names[i] for i in range(n) if "t5" in phases[gr_names[i]]]
            elapsed = time.time() - t0
            throughput = len(completed) / elapsed if elapsed > 0 else 0

            p_teardown = med([phases[nm].get("t1") for nm in completed])
            p_job_create = med([phases[nm].get("t2", 0) - phases[nm].get("t1", 0)
                                for nm in completed if "t1" in phases[nm] and "t2" in phases[nm]])
            p_restore = med([phases[nm].get("t4", 0) - phases[nm].get("t3", 0)
                             for nm in completed if "t3" in phases[nm] and "t4" in phases[nm]])
            p_total = med([phases[nm].get("t5") for nm in completed])

            log(f"    completed={len(completed)}/{sum(admitted)}  "
                f"throughput={throughput:.2f} CR/s  "
                f"teardown={p_teardown}s  job_create={p_job_create}s  "
                f"restore={p_restore}s  total={p_total}s")

            snap = Snapshot(label=f"gpureset_n{n}")
            snap.extras = {
                "n": n, "completed": len(completed), "throughput_crs_s": round(throughput, 2),
                "teardown_p50_s": p_teardown, "job_create_p50_s": p_job_create,
                "restore_p50_s": p_restore, "total_p50_s": p_total,
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            # Cleanup
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                list(ex.map(lambda nm: self.client.delete(f"{self._GPURESET}/{nm}"), gr_names))
            time.sleep(5)

        log("\n  done")

    # ── Scenario 8: Restart / cold recovery ──────────────────────────────────

    def run_cold_recovery(
        self,
        completed_sweep: list[int] | None = None,
        n_completed: int = 1000,
        n_active: int = 10,
        metrics_url: str = "http://localhost:2112/metrics",
    ) -> None:
        """
        Scenario 8: Restart/cold recovery sweep.

        Sweeps N accumulated completed CRs to characterize the startup
        enqueue spike. For each N, pre-creates N completed + n_active
        in-flight CRs, restarts the Janitor pod, and measures:

          startup_s       — pod kill to Ready
          enqueue_drain_s — time for TTL controller to first-reconcile all N CRs
                            (detected by nvsentinel.nvidia.com/expiry annotation)
          resume_s        — active CRs back to SignalSent=True

        Scale concern: startup enqueue spike is O(N/worker_throughput).
        At 349 CR/s, 100k CRs → ~290s before new CRs are processed.
        """
        sweep = completed_sweep or [n_completed]
        log("\n── Scenario 8: Restart / Cold Recovery Sweep ───────────────────")
        log(f"  sweep: N={sweep} accumulated completed CRs")
        log(f"  n_active: {n_active} in-flight CRs per point")

        max_n = max(sweep)
        nodes = self._kwok_nodes(max_n + n_active + 5)
        import threading

        run = BenchmarkRun(
            scenario="cold_recovery_sweep", component="janitor",
            warmup_s=0, measurement_s=0,
            metadata={"sweep": sweep, "n_active": n_active},
        )

        from datetime import datetime, timezone, timedelta

        for n_completed in sweep:
            log(f"\n  ── N={n_completed} accumulated CRs ──")

            # Use last N nodes in cache for active CRs (beyond all completed node indices)
            # This avoids overlap with completed CRs using nodes[0:n_completed]
            active_nodes = nodes[-(n_active + 5):][:n_active]

            # Node keeper for active CRs
            stop = threading.Event()
            threading.Thread(target=self._keep_nodes_ready,
                             args=(active_nodes, stop), daemon=True).start()

            # Pre-create completed CRs with long TTL (10m)
            log(f"  pre-creating {n_completed} completed CRs (TTL=10m, completionTime=5m ago)...")
            past = (datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")

            def _create_persistent_cr(i, _past=past):
                name = self._cr_name("s8c", i)
                body = {
                    "apiVersion": f"{_GROUP}/{_VERSION}", "kind": "RebootNode",
                    "metadata": {"name": name, "labels": {_LABEL_KEY: _LABEL_VAL},
                                 "annotations": {"nvsentinel.nvidia.com/ttl": "10m"}},
                    "spec": {"nodeName": nodes[i]},
                }
                if not self.client.post(_REBOOT, body):
                    return
                self.client.patch_status(f"{_REBOOT}/{name}/status", {
                    "status": {"completionTime": _past, "conditions": [
                        {"type": "SignalSent", "status": "True", "reason": "s8bench",
                         "lastTransitionTime": _past, "message": ""},
                        {"type": "NodeReady", "status": "True", "reason": "s8bench",
                         "lastTransitionTime": _past, "message": ""},
                    ]}})

            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
                list(ex.map(_create_persistent_cr, range(n_completed)))
            time.sleep(5)

            # Pre-create active CRs
            log(f"  pre-creating {n_active} active (in-flight) CRs...")
            active_names = [self._cr_name("s8a", i) for i in range(n_active)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                list(ex.map(lambda i: self._create_reboot_cr(active_names[i], active_nodes[i]),
                            range(n_active)))

            # Wait for signals to be sent
            log(f"  waiting for active CRs to reach SignalSent state...")
            deadline = time.time() + 150
            signal_sent = 0
            while time.time() < deadline:
                signal_sent = sum(1 for nm in active_names if self._cr_signal_sent(_REBOOT, nm))
                if signal_sent >= n_active * 0.8:
                    break
                time.sleep(5)
            log(f"  {signal_sent}/{n_active} active CRs have SignalSent=True")

            # ── Restart Janitor ───────────────────────────────────────────────
            log(f"  restarting Janitor...")
            pods_resp = self.client.get(
                "/api/v1/namespaces/nvsentinel/pods"
                "?labelSelector=app.kubernetes.io%2Fname%3Djanitor")
            old_pods = {p["metadata"]["name"] for p in pods_resp.get("items", [])}
            for pod_name in old_pods:
                self.client.delete(f"/api/v1/namespaces/nvsentinel/pods/{pod_name}")
            t_restart = time.time()

            pod_ready_t = None
            deadline = time.time() + 180
            while time.time() < deadline:
                resp = self.client.get(
                    "/api/v1/namespaces/nvsentinel/pods"
                    "?labelSelector=app.kubernetes.io%2Fname%3Djanitor")
                new_ready = [p for p in resp.get("items", [])
                             if p["metadata"]["name"] not in old_pods
                             and all(c.get("ready") for c in
                                     p.get("status", {}).get("containerStatuses", []))]
                if new_ready:
                    pod_ready_t = time.time() - t_restart
                    log(f"  Janitor ready in {pod_ready_t:.1f}s")
                    break
                time.sleep(3)

            if pod_ready_t is None:
                log("  ERROR: pod not ready in 180s"); stop.set(); continue

            # Measure startup enqueue drain
            log(f"  measuring startup enqueue drain ({n_completed} CRs)...")
            ttl_start = time.time()
            deadline = time.time() + 300
            while time.time() < deadline:
                items = self.client.list_all(_REBOOT, label_selector=f"{_LABEL_KEY}={_LABEL_VAL}")
                s8c = [i for i in items if "s8c" in i["metadata"]["name"]]
                annotated = sum(1 for i in s8c if i.get("metadata", {}).get("annotations", {}).get(
                    "nvsentinel.nvidia.com/expiry"))
                if annotated >= len(s8c) * 0.95 or not s8c:
                    break
                time.sleep(2)
            ttl_drain_t = time.time() - ttl_start

            # Delete completed CRs (10m TTL won't self-delete)
            s8c_names = [self._cr_name("s8c", i) for i in range(n_completed)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
                list(ex.map(lambda nm: self.client.delete(f"{_REBOOT}/{nm}"), s8c_names))

            # Measure active CR resume
            log(f"  measuring active CR resume...")
            resume_start = time.time()
            deadline = time.time() + 300
            while time.time() < deadline:
                resumed = sum(1 for nm in active_names if self._cr_signal_sent(_REBOOT, nm))
                if resumed >= n_active * 0.8:
                    break
                time.sleep(3)
            resume_t = time.time() - resume_start
            stop.set()

            rate = round(n_completed / ttl_drain_t, 1) if ttl_drain_t > 0 else None
            log(f"  N={n_completed:6d}  startup={pod_ready_t:.1f}s  "
                f"drain={ttl_drain_t:.1f}s ({rate} CR/s)  resume={resume_t:.1f}s")

            snap = Snapshot(label=f"n{n_completed}")
            snap.extras = {
                "n_completed": n_completed, "n_active": n_active,
                "startup_s": round(pod_ready_t, 1),
                "ttl_drain_s": round(ttl_drain_t, 1),
                "ttl_drain_rate_crs": rate,
                "active_resume_s": round(resume_t, 1),
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            # Clean up active CRs before next iteration
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                list(ex.map(lambda nm: self.client.delete(f"{_REBOOT}/{nm}"), active_names))
            time.sleep(5)

        # Final cleanup
        log(f"\n  cleaning up...")
        deleted = self._delete_bench_crs()
        log(f"  deleted {deleted} CRs")
        log("\n  done")

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self) -> None:
        self.run_admission_webhook()
