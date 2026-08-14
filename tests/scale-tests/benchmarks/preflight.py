"""
benchmarks/preflight.py — Preflight microbenchmarks.

Implements GitHub issue #1522: Microbenchmark Preflight.

Scale target: 100k nodes (99k KWOK nodes present in cluster).

Scenarios implemented
---------------------
  admission   Scenario 1: Admission webhook throughput and latency.
              Creates GPU-requesting Pods at increasing rates in a
              preflight-enabled namespace and measures P50/P90/P99
              admission latency and failure rate.
"""
from __future__ import annotations

import concurrent.futures
import statistics
import time
import urllib.request
import json
from dataclasses import dataclass, field

from framework.cluster import ClusterClient
from framework.log import log
from framework.prometheus import PrometheusClient, Snapshot
from framework.results import BenchmarkResults, BenchmarkRun

_BENCH_NS = "preflight-bench"
_LABEL_KEY = "benchmark.nvsentinel/preflight"
_LABEL_VAL  = "admission"


@dataclass
class PreflightBenchmark:
    client: ClusterClient
    prom: PrometheusClient
    namespace: str = _BENCH_NS
    # Admission throughput sweep
    rates_per_s: list[int] = field(default_factory=lambda: [10, 50, 200, 500])
    duration_s: int = 30       # measurement window per rate
    warmup_pods: int = 5       # pre-warm webhook before measuring
    version: str = "unknown"
    output: str = "results"

    def __post_init__(self) -> None:
        self.results = BenchmarkResults(component="preflight", version=self.version)
        self._pod_counter = 0

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _pod_name(self) -> str:
        self._pod_counter += 1
        return f"bench-pf-{self._pod_counter:06d}"

    def _create_pod(self, name: str, timeout: int = 15) -> float:
        """
        POST a minimal GPU-requesting Pod (triggers preflight injection).
        Returns wall-clock admission latency in seconds, or -1 on failure.

        Pods use KWOK nodeSelector so they never actually run — we only
        measure the admission webhook response time.
        """
        body = json.dumps({
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": name, "namespace": self.namespace,
                "labels": {_LABEL_KEY: _LABEL_VAL},
            },
            "spec": {
                "containers": [{
                    "name": "bench",
                    "image": "busybox:1.36",
                    "command": ["sleep", "3600"],
                    "resources": {
                        "requests": {"nvidia.com/gpu": "1"},
                        "limits":   {"nvidia.com/gpu": "1"},
                    },
                }],
                "restartPolicy": "Never",
                "nodeSelector": {"type": "kwok"},
                "tolerations": [{"key": "kwok.x-k8s.io/node", "operator": "Exists"}],
            },
        }).encode()

        api = f"/api/v1/namespaces/{self.namespace}/pods"
        t0 = time.perf_counter()
        req = urllib.request.Request(
            f"{self.client.proxy}{api}", data=body, method="POST",
            headers={"Content-Type": "application/json"})
        try:
            urllib.request.urlopen(req, timeout=timeout)
            return time.perf_counter() - t0
        except Exception:
            return -1.0

    def _delete_bench_pods(self) -> int:
        items = self.client.list_all(
            f"/api/v1/namespaces/{self.namespace}/pods",
            label_selector=f"{_LABEL_KEY}={_LABEL_VAL}")
        deleted = 0
        def _del(name):
            self.client.delete(f"/api/v1/namespaces/{self.namespace}/pods/{name}")
            return True
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
            deleted = sum(ex.map(_del, [i["metadata"]["name"] for i in items]))
        return deleted

    def _preflight_cpu_pct(self) -> float | None:
        try:
            resp = self.client.get(
                "/api/v1/namespaces/nvsentinel/pods"
                "?labelSelector=app.kubernetes.io%2Fname%3Dpreflight&limit=1")
            pods = resp.get("items", [])
            if not pods:
                return None
            pod_name = pods[0]["metadata"]["name"]
            metrics = self.client.get(
                f"/apis/metrics.k8s.io/v1beta1/namespaces/nvsentinel/pods/{pod_name}")
            for c in metrics.get("containers", []):
                if c["name"] == "preflight":
                    cpu_nano = int(c["usage"]["cpu"].rstrip("n"))
                    return cpu_nano / 1e7  # nanocores → percent (of 1 core)
        except Exception:
            return None

    # ── Scenario 1: Admission throughput ─────────────────────────────────────

    def run_admission_throughput(self) -> None:
        """
        MB-PF-1: Admission webhook throughput and latency at 100k nodes.

        Pods request nvidia.com/gpu=1 to trigger full preflight injection
        (init container injection into pod spec). No gang coordination.
        KWOK nodeSelector ensures pods never actually run.

        For each target rate R pods/s:
          1. Warm up: create warmup_pods sequentially
          2. Rate-limited burst: create pods at R/s for duration_s seconds
          3. Record per-admission wall-clock latency (P50/P90/P99)
          4. Record success rate (201 Created vs errors)
          5. Clean up all pods
        """
        log("\n── MB-PF-1: Admission Throughput (100k-node cluster) ────────────")
        log(f"  namespace:  {self.namespace} (label: nvsentinel.nvidia.com/preflight=enabled)")
        log(f"  rates:      {self.rates_per_s} pods/s")
        log(f"  window:     {self.duration_s}s per rate")
        log(f"  cluster:    99k KWOK nodes (scale baseline)")

        run = BenchmarkRun(
            scenario="admission_throughput",
            component="preflight",
            warmup_s=0,
            measurement_s=self.duration_s,
            metadata={
                "rates_per_s": self.rates_per_s,
                "duration_s": self.duration_s,
                "gang_coordination": False,
                "init_containers": 3,
                "cluster_nodes": "99k KWOK",
            },
        )

        # Warm up the webhook
        log(f"  warming up ({self.warmup_pods} pods)...")
        for _ in range(self.warmup_pods):
            self._create_pod(self._pod_name())
        time.sleep(2)

        for rate in self.rates_per_s:
            log(f"\n  rate={rate} pods/s")
            interval_s = 1.0 / rate
            target = rate * self.duration_s
            latencies: list[float] = []
            errors_list: list[int] = [0]  # mutable for thread access

            t_start = time.time()
            # Concurrent workers with token-bucket rate limiter.
            # Workers needed ≈ rate × baseline_latency_s (≈ 0.4s → rate×0.5 + buffer).
            import threading, queue as _queue
            n_workers = max(4, int(rate * 0.6) + 4)
            work_q: _queue.Queue = _queue.Queue(maxsize=n_workers * 2)
            lat_lock = threading.Lock()
            stop_evt = threading.Event()

            def _producer():
                t0 = time.time()
                issued = 0
                while not stop_evt.is_set():
                    target_issued = int((time.time() - t0) * rate)
                    while issued < target_issued and not stop_evt.is_set():
                        try:
                            work_q.put(self._pod_name(), timeout=0.1)
                            issued += 1
                        except _queue.Full:
                            pass
                    time.sleep(0.001)

            def _worker():
                while not stop_evt.is_set():
                    try:
                        name = work_q.get(timeout=0.1)
                    except _queue.Empty:
                        continue
                    lat = self._create_pod(name)
                    with lat_lock:
                        if lat >= 0:
                            latencies.append(lat * 1000)
                        else:
                            errors_list[0] += 1
                    work_q.task_done()

            t_start = time.time()
            prod = threading.Thread(target=_producer, daemon=True)
            wkrs = [threading.Thread(target=_worker, daemon=True) for _ in range(n_workers)]
            prod.start()
            for w in wkrs: w.start()
            time.sleep(self.duration_s)
            stop_evt.set()
            for w in wkrs: w.join(timeout=5)

            elapsed = time.time() - t_start
            actual_rate = len(latencies) / elapsed if elapsed > 0 else 0

            n_errors = errors_list[0]
            if latencies:
                latencies.sort()
                n = len(latencies)
                p50 = statistics.median(latencies)
                p90 = latencies[int(n * 0.90)]
                p99 = latencies[min(int(n * 0.99), n - 1)]
                error_pct = n_errors / (n_errors + n) * 100
            else:
                p50 = p90 = p99 = None
                error_pct = 100.0

            cpu = self._preflight_cpu_pct()

            snap = Snapshot(label=f"rate_{rate}")
            snap.extras = {
                "target_rate_per_s":  rate,
                "actual_rate_per_s":  round(actual_rate, 1),
                "admission_p50_ms":   round(p50, 1) if p50 else None,
                "admission_p90_ms":   round(p90, 1) if p90 else None,
                "admission_p99_ms":   round(p99, 1) if p99 else None,
                "error_pct":          round(error_pct, 1),
                "total_pods":         len(latencies) + n_errors,
                "preflight_cpu_pct":  round(cpu, 1) if cpu else None,
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            log(f"    {rate:3d}/s → actual={actual_rate:.1f}/s  "
                f"P50={p50:.0f}ms  P90={p90:.0f}ms  P99={p99:.0f}ms  "
                f"errors={error_pct:.1f}%" if p50 else
                f"    {rate:3d}/s → ALL FAILED")

            # Brief pause between rates
            time.sleep(3)

        log(f"\n  cleaning up pods...")
        deleted = self._delete_bench_pods()
        log(f"  deleted {deleted} pods")

    # ── Scenario 4: Gang coordination at scale ────────────────────────────────

    def run_gang_coordination(
        self,
        gang_sizes: list[int] | None = None,
        repetitions: int = 5,
    ) -> None:
        """
        MB-PF-4: Gang coordination O(N²) scaling characterization.

        The GangController calls `client.List(all pods in namespace)` on every
        pod IP assignment event. For an N-member gang, this fires N times, each
        returning N pod objects — O(N²) total data transfer from the API server.

        Additionally, N concurrent ConfigMap PATCH calls (one per pod registering
        its IP) all target the same ConfigMap → O(N) conflict/retry storm.

        This benchmark measures both bottlenecks directly:

        Part A — List cost:
          Populate the gang namespace with N GPU pods. Measure:
          - `GET /api/v1/namespaces/{ns}/pods` latency (ms)
          - Response bytes (N pods × pod_size)
          - Total O(N²) data = N calls × N pods × pod_size

        Part B — ConfigMap conflict storm:
          Fire N concurrent PATCH requests to the same ConfigMap and measure
          the P99 latency and retry rate under conflict.
        """
        sizes = gang_sizes or [2, 16, 128, 1000]
        log("\n── MB-PF-4: Gang Coordination O(N²) Scaling ────────────────────")
        log(f"  gang sizes: {sizes}")
        log(f"  repetitions per size: {repetitions}")

        run = BenchmarkRun(
            scenario="gang_coordination",
            component="preflight",
            warmup_s=0,
            measurement_s=0,
            metadata={"gang_sizes": sizes, "repetitions": repetitions},
        )

        for n in sizes:
            log(f"\n  N={n} pods")

            # ── Part A: Populate namespace and measure List cost ──────────────
            log(f"    creating {n} GPU pods...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
                list(ex.map(lambda _: self._create_pod(self._pod_name()), range(n)))

            # Measure List latency + bytes (repetitions times)
            list_latencies_ms = []
            list_bytes = []
            pods_api = f"/api/v1/namespaces/{self.namespace}/pods"

            for _ in range(repetitions):
                t0 = time.perf_counter()
                req = urllib.request.Request(
                    f"{self.client.proxy}{pods_api}",
                    headers={"Accept": "application/json"})
                try:
                    resp = urllib.request.urlopen(req, timeout=30)
                    body = resp.read()
                    elapsed_ms = (time.perf_counter() - t0) * 1000
                    list_latencies_ms.append(elapsed_ms)
                    list_bytes.append(len(body))
                except Exception:
                    pass
                time.sleep(0.2)

            if not list_latencies_ms:
                log(f"    List failed for N={n}")
                self._delete_bench_pods()
                continue

            list_latencies_ms.sort()
            p50_list = statistics.median(list_latencies_ms)
            p99_list = list_latencies_ms[min(int(len(list_latencies_ms)*0.99),
                                             len(list_latencies_ms)-1)]
            avg_bytes = statistics.mean(list_bytes)
            bytes_per_pod = avg_bytes / n if n > 0 else 0

            # Total O(N²) cost projection: N reconciles × list_latency
            total_discovery_ms = n * p50_list
            total_bytes_mb = (n * avg_bytes) / 1e6

            log(f"    List: P50={p50_list:.0f}ms  P99={p99_list:.0f}ms  "
                f"bytes={avg_bytes/1024:.0f}KB ({bytes_per_pod:.0f}B/pod)")
            log(f"    O(N²) projection: N×list = {total_discovery_ms/1000:.1f}s  "
                f"N×bytes = {total_bytes_mb:.1f}MB")

            # ── Part B: ConfigMap conflict storm ──────────────────────────────
            cm_name = f"bench-pf-gang-n{n}"
            # Create the ConfigMap
            cm_body = json.dumps({
                "apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": cm_name, "namespace": self.namespace},
                "data": {"peers": "", "expected_count": str(n)},
            }).encode()
            req = urllib.request.Request(
                f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/configmaps",
                data=cm_body, method="POST",
                headers={"Content-Type": "application/json"})
            try:
                urllib.request.urlopen(req, timeout=10)
            except Exception:
                pass

            # Get current resourceVersion
            cm_resp = json.loads(urllib.request.urlopen(
                f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/configmaps/{cm_name}",
                timeout=10).read())
            rv = cm_resp.get("metadata", {}).get("resourceVersion", "")

            # Fire N concurrent PATCH requests (each simulates one pod registering its IP)
            patch_latencies = []
            conflicts = [0]

            def _patch_cm(pod_idx):
                patch = json.dumps({
                    "data": {f"peer-{pod_idx}": f"10.0.{pod_idx//256}.{pod_idx%256}"}
                }).encode()
                req = urllib.request.Request(
                    f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/configmaps/{cm_name}",
                    data=patch, method="PATCH",
                    headers={"Content-Type": "application/strategic-merge-patch+json"})
                t0 = time.perf_counter()
                try:
                    urllib.request.urlopen(req, timeout=15)
                    return (time.perf_counter() - t0) * 1000
                except urllib.error.HTTPError as e:
                    if e.code == 409:
                        conflicts[0] += 1
                    return -1.0

            with concurrent.futures.ThreadPoolExecutor(max_workers=min(n, 50)) as ex:
                results = list(ex.map(_patch_cm, range(n)))
            patch_latencies = [r for r in results if r >= 0]

            if patch_latencies:
                patch_latencies.sort()
                p50_patch = statistics.median(patch_latencies)
                p99_patch = patch_latencies[min(int(len(patch_latencies)*0.99),
                                                len(patch_latencies)-1)]
            else:
                p50_patch = p99_patch = None

            conflict_rate = conflicts[0] / n * 100 if n > 0 else 0
            log(f"    ConfigMap PATCH: P50={p50_patch:.0f}ms  P99={p99_patch:.0f}ms  "
                f"conflicts={conflict_rate:.0f}%" if p50_patch else
                f"    ConfigMap PATCH: all failed")

            # Cleanup
            self._delete_bench_pods()
            self.client.delete(
                f"/api/v1/namespaces/{self.namespace}/configmaps/{cm_name}")
            time.sleep(3)

            snap = Snapshot(label=f"gang_n{n}")
            snap.extras = {
                "gang_size": n,
                # Part A: List cost
                "list_p50_ms": round(p50_list, 1),
                "list_p99_ms": round(p99_list, 1),
                "list_bytes": round(avg_bytes),
                "bytes_per_pod": round(bytes_per_pod),
                "o_n2_total_s": round(total_discovery_ms / 1000, 1),
                "o_n2_total_mb": round(total_bytes_mb, 1),
                # Part B: Conflict storm
                "patch_p50_ms": round(p50_patch, 1) if p50_patch else None,
                "patch_p99_ms": round(p99_patch, 1) if p99_patch else None,
                "conflict_rate_pct": round(conflict_rate, 1),
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

        log("\n  done")

    # ── Scenario 2: Pod-spec size ─────────────────────────────────────────────

    def run_pod_spec_size(self, repetitions: int = 10) -> None:
        """
        MB-PF-2: Admission mutation latency vs pod spec size.

        Tests four pod sizes from minimal to maximally realistic production spec.
        Measures wall-clock admission latency and response body size (patch bytes).
        Larger pod specs require more JSON processing in the webhook.
        """
        def _env(n): return [{"name": f"ENV_{i}", "value": f"value-{i}"} for i in range(n)]
        def _vol(n): return [{"name": f"vol-{i}", "emptyDir": {}} for i in range(n)]
        def _vm(n): return [{"name": f"vol-{i}", "mountPath": f"/mnt/vol-{i}"} for i in range(n)]

        sizes = {
            "minimal": {
                "containers": [{"name": "test", "image": "busybox:1.36",
                    "command": ["sleep", "1"],
                    "resources": {"requests": {"nvidia.com/gpu": "1"},
                                  "limits":   {"nvidia.com/gpu": "1"}}}],
            },
            "typical_gpu": {
                "containers": [{"name": "train", "image": "busybox:1.36",
                    "command": ["sleep", "1"],
                    "env": _env(10),
                    "resources": {"requests": {"nvidia.com/gpu": "8", "memory": "64Gi", "cpu": "16"},
                                  "limits":   {"nvidia.com/gpu": "8", "memory": "64Gi", "cpu": "16"}},
                    "volumeMounts": _vm(3)}],
                "volumes": _vol(3),
            },
            "large": {
                "containers": [
                    {"name": f"worker-{i}", "image": "busybox:1.36",
                     "command": ["sleep", "1"],
                     "env": _env(30),
                     "resources": {"requests": {"nvidia.com/gpu": "1"},
                                   "limits":   {"nvidia.com/gpu": "1"}},
                     "volumeMounts": _vm(5)}
                    for i in range(3)
                ],
                "volumes": _vol(5),
            },
            "max_realistic": {
                "containers": [
                    {"name": f"worker-{i}", "image": "busybox:1.36",
                     "command": ["sleep", "1"],
                     "env": _env(50),
                     "resources": {"requests": {"nvidia.com/gpu": "1"},
                                   "limits":   {"nvidia.com/gpu": "1"}},
                     "volumeMounts": _vm(8)}
                    for i in range(5)
                ],
                "volumes": _vol(8),
                "initContainers": [{"name": f"init-{i}", "image": "busybox:1.36",
                                    "command": ["true"]} for i in range(3)],
            },
        }

        log("\n── MB-PF-2: Pod-Spec Size vs Mutation Latency ──────────────────")
        log(f"  sizes: {list(sizes.keys())}  repetitions: {repetitions}")

        run = BenchmarkRun(
            scenario="pod_spec_size", component="preflight",
            warmup_s=0, measurement_s=0,
            metadata={"sizes": list(sizes.keys()), "repetitions": repetitions},
        )

        for size_name, spec_extra in sizes.items():
            latencies_ms, patch_sizes = [], []
            for rep in range(repetitions):
                name = self._pod_name()
                spec = {
                    "nodeSelector": {"type": "kwok"},
                    "tolerations": [{"key": "kwok.x-k8s.io/node", "operator": "Exists"}],
                    "restartPolicy": "Never",
                    **spec_extra,
                }
                body = json.dumps({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": name, "namespace": self.namespace,
                                 "labels": {_LABEL_KEY: _LABEL_VAL}},
                    "spec": spec,
                }).encode()

                t0 = time.perf_counter()
                req = urllib.request.Request(
                    f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/pods",
                    data=body, method="POST", headers={"Content-Type": "application/json"})
                try:
                    resp = urllib.request.urlopen(req, timeout=30)
                    elapsed_ms = (time.perf_counter() - t0) * 1000
                    resp_body = resp.read()
                    latencies_ms.append(elapsed_ms)
                    patch_sizes.append(len(resp_body))
                except Exception:
                    pass
                time.sleep(0.1)

            if not latencies_ms:
                continue
            latencies_ms.sort()
            n = len(latencies_ms)
            p50 = statistics.median(latencies_ms)
            p90 = latencies_ms[int(n * 0.90)]
            p99 = latencies_ms[min(int(n * 0.99), n - 1)]
            avg_patch = statistics.mean(patch_sizes) if patch_sizes else 0

            log(f"  {size_name:15s}  P50={p50:.0f}ms  P90={p90:.0f}ms  "
                f"P99={p99:.0f}ms  patch={avg_patch/1024:.1f}KB")

            snap = Snapshot(label=size_name)
            snap.extras = {
                "size_name": size_name,
                "admission_p50_ms": round(p50, 1),
                "admission_p90_ms": round(p90, 1),
                "admission_p99_ms": round(p99, 1),
                "avg_patch_bytes": round(avg_patch),
            }
            run.add(snap)

        self.results.add_run(run)
        self.results.save(self.output)
        log(f"\n  cleaning up...")
        self._delete_bench_pods()
        log("\n  done")

    # ── Scenario 3: Check-count sweep ────────────────────────────────────────

    def run_check_count(self, repetitions: int = 10) -> None:
        """
        MB-PF-3: Injection latency vs number of checks (init containers).

        Uses nvsentinel.nvidia.com/preflight-checks annotation to control
        which checks are injected per pod. Measures webhook latency and
        response patch size as check count increases.
        """
        # Available check names from default config
        all_checks = ["preflight-dcgm-diag", "preflight-nccl-loopback",
                      "preflight-nccl-allreduce"]

        configs = {
            0: None,        # no annotation → all checks (default)
            1: all_checks[:1],
            2: all_checks[:2],
            3: all_checks,  # same as default but explicit
        }

        log("\n── MB-PF-3: Check-Count vs Injection Latency ───────────────────")

        run = BenchmarkRun(
            scenario="check_count", component="preflight",
            warmup_s=0, measurement_s=0,
            metadata={"check_configs": {k: v for k, v in configs.items()}},
        )

        for n_checks, check_list in configs.items():
            latencies_ms, patch_sizes = [], []
            for _ in range(repetitions):
                name = self._pod_name()
                annotations = {}
                if check_list is not None:
                    annotations["nvsentinel.nvidia.com/preflight-checks"] = \
                        ",".join(check_list)
                body = json.dumps({
                    "apiVersion": "v1", "kind": "Pod",
                    "metadata": {"name": name, "namespace": self.namespace,
                                 "labels": {_LABEL_KEY: _LABEL_VAL},
                                 "annotations": annotations},
                    "spec": {
                        "containers": [{"name": "test", "image": "busybox:1.36",
                            "command": ["sleep", "1"],
                            "resources": {"requests": {"nvidia.com/gpu": "1"},
                                          "limits":   {"nvidia.com/gpu": "1"}}}],
                        "restartPolicy": "Never",
                        "nodeSelector": {"type": "kwok"},
                        "tolerations": [{"key": "kwok.x-k8s.io/node",
                                         "operator": "Exists"}],
                    },
                }).encode()

                t0 = time.perf_counter()
                req = urllib.request.Request(
                    f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/pods",
                    data=body, method="POST", headers={"Content-Type": "application/json"})
                try:
                    resp = urllib.request.urlopen(req, timeout=30)
                    elapsed_ms = (time.perf_counter() - t0) * 1000
                    latencies_ms.append(elapsed_ms)
                    patch_sizes.append(len(resp.read()))
                except Exception:
                    pass
                time.sleep(0.1)

            if not latencies_ms:
                continue
            latencies_ms.sort()
            n = len(latencies_ms)
            p50 = statistics.median(latencies_ms)
            p99 = latencies_ms[min(int(n * 0.99), n - 1)]
            avg_patch = statistics.mean(patch_sizes) if patch_sizes else 0
            label = "default" if check_list is None else f"{len(check_list)}_checks"
            log(f"  {n_checks} checks ({label:10s})  "
                f"P50={p50:.0f}ms  P99={p99:.0f}ms  patch={avg_patch/1024:.1f}KB")

            snap = Snapshot(label=label)
            snap.extras = {
                "check_count": n_checks if check_list is not None else 3,
                "explicit_selection": check_list is not None,
                "admission_p50_ms": round(p50, 1),
                "admission_p99_ms": round(p99, 1),
                "avg_patch_bytes": round(avg_patch),
            }
            run.add(snap)

        self.results.add_run(run)
        self.results.save(self.output)
        log(f"\n  cleaning up...")
        self._delete_bench_pods()
        log("\n  done")

    # ── Scenario 8: Restart under load ───────────────────────────────────────

    def run_restart(self, repetitions: int = 3) -> None:
        """
        MB-PF-8: Restart/cold recovery at 99k nodes.

        Restarts the preflight pod and measures:
          startup_s       — pod kill to Ready (cache sync dominates)
          first_admit_s   — first successful Pod admission after restart
          gang_resume_s   — first peer registered in an in-flight gang
                            after restart (GangController resumes from cache)
        """
        import threading

        log("\n── MB-PF-8: Restart / Cold Recovery (99k nodes) ─────────────────")
        log(f"  repetitions: {repetitions}")

        run = BenchmarkRun(
            scenario="restart", component="preflight",
            warmup_s=0, measurement_s=0,
            metadata={"repetitions": repetitions, "cluster_nodes": "99k KWOK"},
        )

        for rep in range(repetitions):
            log(f"\n  rep {rep+1}/{repetitions}")

            # Kill preflight pod
            pods_resp = self.client.get(
                "/api/v1/namespaces/nvsentinel/pods"
                "?labelSelector=app.kubernetes.io%2Fname%3Dpreflight")
            old_pods = {p["metadata"]["name"] for p in pods_resp.get("items", [])}
            for pod in old_pods:
                self.client.delete(f"/api/v1/namespaces/nvsentinel/pods/{pod}")
            t_kill = time.time()
            log(f"    killed {old_pods}")

            # Wait for new pod Ready
            pod_ready_s = None
            deadline = t_kill + 120
            while time.time() < deadline:
                resp = self.client.get(
                    "/api/v1/namespaces/nvsentinel/pods"
                    "?labelSelector=app.kubernetes.io%2Fname%3Dpreflight")
                new_ready = [p for p in resp.get("items", [])
                             if p["metadata"]["name"] not in old_pods
                             and all(c.get("ready") for c in
                                     p.get("status", {}).get("containerStatuses", []))]
                if new_ready:
                    pod_ready_s = time.time() - t_kill
                    log(f"    pod ready in {pod_ready_s:.1f}s")
                    break
                time.sleep(3)

            if pod_ready_s is None:
                log("    ERROR: pod not ready in 120s"); continue

            # Measure first successful admission after restart
            first_admit_s = None
            deadline = time.time() + 60
            while time.time() < deadline:
                name = self._pod_name()
                lat = self._create_pod(name)
                if lat >= 0:
                    first_admit_s = time.time() - t_kill
                    log(f"    first admission in {first_admit_s:.1f}s ({lat*1000:.0f}ms)")
                    self.client.delete(
                        f"/api/v1/namespaces/{self.namespace}/pods/{name}")
                    break
                time.sleep(1)

            log(f"    startup={pod_ready_s:.1f}s  first_admit={first_admit_s:.1f}s"
                if first_admit_s else f"    startup={pod_ready_s:.1f}s  first_admit=TIMEOUT")

            snap = Snapshot(label=f"rep{rep+1}")
            snap.extras = {
                "startup_s": round(pod_ready_s, 1),
                "first_admit_s": round(first_admit_s, 1) if first_admit_s else None,
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)
            time.sleep(10)

        log("\n  done")

    # ── Scenario 5: Cluster Pod population ───────────────────────────────────

    def _preflight_rss_mb(self, metrics_url: str = "http://localhost:8080/metrics") -> float | None:
        try:
            raw = urllib.request.urlopen(metrics_url, timeout=5).read().decode()
            for line in raw.splitlines():
                if line.startswith("process_resident_memory_bytes "):
                    return float(line.split()[1]) / 1e6
        except Exception:
            return None

    def run_cluster_pod_population(
        self,
        background_counts: list[int] | None = None,
        gang_size: int = 16,
        gang_reps: int = 3,
        metrics_url: str = "http://localhost:8080/metrics",
    ) -> None:
        """
        MB-PF-5: Cluster Pod population — informer memory and gang discovery
        latency vs namespace pod count.

        Creates M background (non-GPU) pods in the bench namespace — these are
        NOT injected by preflight (hasGPUResources=false) but DO populate the
        pod informer cache. Then forms a small gang and measures:

          rss_mb          — preflight RSS after M pods cached
          gang_peers_s    — time for gang_size peers to register in ConfigMap
                            (DiscoverPeers scans all M+gang_size namespace pods)

        Isolates how namespace pod density affects the in-memory findPeers scan.
        """
        counts = background_counts or [0, 1000, 5000, 10000]
        log("\n── MB-PF-5: Cluster Pod Population ─────────────────────────────")
        log(f"  background pod sweep: {counts}")
        log(f"  gang size per point:  {gang_size}")
        log(f"  baseline RSS: {self._preflight_rss_mb(metrics_url):.0f} MB")

        run = BenchmarkRun(
            scenario="cluster_pod_population", component="preflight",
            warmup_s=0, measurement_s=0,
            metadata={"background_counts": counts, "gang_size": gang_size},
        )

        existing_bg = 0  # track cumulatively created background pods

        for M in counts:
            log(f"\n  M={M} background pods")

            # Create additional background pods (non-GPU, no preflight injection)
            to_add = M - existing_bg
            if to_add > 0:
                log(f"    adding {to_add} background pods...")
                def _create_bg(i, offset=existing_bg):
                    name = f"bg-pod-{offset + i:06d}"
                    body = json.dumps({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {"name": name, "namespace": self.namespace,
                                     "labels": {"bench-bg": "true"}},
                        "spec": {
                            "containers": [{"name": "test", "image": "busybox:1.36",
                                "command": ["sleep", "3600"]}],
                            "restartPolicy": "Never",
                            "nodeSelector": {"type": "kwok"},
                            "tolerations": [{"key": "kwok.x-k8s.io/node",
                                             "operator": "Exists"}],
                        },
                    }).encode()
                    req = urllib.request.Request(
                        f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/pods",
                        data=body, method="POST",
                        headers={"Content-Type": "application/json"})
                    try:
                        urllib.request.urlopen(req, timeout=10)
                        return True
                    except Exception:
                        return False
                with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
                    ok = sum(ex.map(_create_bg, range(to_add)))
                log(f"    {ok}/{to_add} created")
                existing_bg = M
                time.sleep(5)  # let informer cache update

            # Measure preflight RSS
            rss = self._preflight_rss_mb(metrics_url)
            log(f"    preflight RSS: {rss:.0f} MB" if rss else "    RSS: unavailable")

            # Measure gang peer-discovery latency with M background pods present
            gang_peer_times = []
            for rep in range(gang_reps):
                gang_name = f"s5-gang-m{M}-r{rep}"
                cm_name = f"preflight-volcano-{self.namespace}-{gang_name}"

                # Create PodGroup
                pg = json.dumps({
                    "apiVersion": "scheduling.volcano.sh/v1beta1", "kind": "PodGroup",
                    "metadata": {"name": gang_name, "namespace": self.namespace},
                    "spec": {"minMember": gang_size}
                }).encode()
                urllib.request.urlopen(urllib.request.Request(
                    f"{self.client.proxy}/apis/scheduling.volcano.sh/v1beta1"
                    f"/namespaces/{self.namespace}/podgroups",
                    data=pg, method="POST",
                    headers={"Content-Type": "application/json"}), timeout=10)

                # Create gang pods
                def _create_gang(i):
                    body = json.dumps({
                        "apiVersion": "v1", "kind": "Pod",
                        "metadata": {
                            "name": f"{gang_name}-{i:04d}",
                            "namespace": self.namespace,
                            "annotations": {"scheduling.k8s.io/group-name": gang_name},
                            "labels": {_LABEL_KEY: _LABEL_VAL},
                        },
                        "spec": {
                            "containers": [{"name": "test", "image": "busybox:1.36",
                                "command": ["sleep", "300"],
                                "resources": {"requests": {"nvidia.com/gpu": "1"},
                                              "limits":   {"nvidia.com/gpu": "1"}}}],
                            "restartPolicy": "Never",
                            "nodeSelector": {"type": "kwok"},
                            "tolerations": [{"key": "kwok.x-k8s.io/node",
                                             "operator": "Exists"}],
                        }
                    }).encode()
                    try:
                        urllib.request.urlopen(urllib.request.Request(
                            f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/pods",
                            data=body, method="POST",
                            headers={"Content-Type": "application/json"}), timeout=15)
                        return True
                    except Exception:
                        return False

                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                    admitted = sum(ex.map(_create_gang, range(gang_size)))

                # Assign IPs to trigger GangController
                def _assign_ip(i):
                    ip = f"10.{(M//256+i)%256}.{i%256}.{rep+1}"
                    patch = json.dumps({"status": {"phase": "Running", "podIP": ip,
                        "podIPs": [{"ip": ip}],
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "hostIP": "192.168.1.1"}}).encode()
                    try:
                        urllib.request.urlopen(urllib.request.Request(
                            f"{self.client.proxy}/api/v1/namespaces/{self.namespace}"
                            f"/pods/{gang_name}-{i:04d}/status",
                            data=patch, method="PATCH",
                            headers={"Content-Type": "application/merge-patch+json"}),
                            timeout=5)
                        return True
                    except Exception:
                        return False

                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                    list(ex.map(_assign_ip, range(gang_size)))

                # Poll until all peers registered
                t0 = time.time()
                deadline = t0 + 120
                peer_count = 0
                while time.time() < deadline:
                    try:
                        cm = json.loads(urllib.request.urlopen(
                            f"{self.client.proxy}/api/v1/namespaces/{self.namespace}"
                            f"/configmaps/{cm_name}", timeout=10).read())
                        peers = cm.get("data", {}).get("peers", "")
                        peer_count = len([p for p in peers.strip().split("\n") if p])
                        if peer_count >= admitted:
                            break
                    except Exception:
                        pass
                    time.sleep(1)

                elapsed = time.time() - t0
                if peer_count >= admitted:
                    gang_peer_times.append(elapsed)
                    log(f"    rep{rep+1}: {admitted} peers in {elapsed:.1f}s")

                # Cleanup gang
                with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
                    list(ex.map(lambda i: urllib.request.urlopen(
                        urllib.request.Request(
                            f"{self.client.proxy}/api/v1/namespaces/{self.namespace}"
                            f"/pods/{gang_name}-{i:04d}",
                            data=b'{}', method="DELETE",
                            headers={"Content-Type": "application/json"}), timeout=5),
                        range(gang_size)))
                try:
                    urllib.request.urlopen(urllib.request.Request(
                        f"{self.client.proxy}/apis/scheduling.volcano.sh/v1beta1"
                        f"/namespaces/{self.namespace}/podgroups/{gang_name}",
                        data=b'{}', method="DELETE",
                        headers={"Content-Type": "application/json"}), timeout=5)
                except Exception:
                    pass
                time.sleep(3)

            p50_gang = statistics.median(gang_peer_times) if gang_peer_times else None
            log(f"    gang({gang_size}) P50={p50_gang:.1f}s" if p50_gang else
                f"    gang: no completions")

            snap = Snapshot(label=f"bg{M}")
            snap.extras = {
                "background_pods": M,
                "gang_size": gang_size,
                "preflight_rss_mb": round(rss, 0) if rss else None,
                "gang_peer_p50_s": round(p50_gang, 1) if p50_gang else None,
            }
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

        # Cleanup background pods
        log("\n  cleaning up background pods...")
        items = self.client.list_all(
            f"/api/v1/namespaces/{self.namespace}/pods",
            label_selector="bench-bg=true")
        with concurrent.futures.ThreadPoolExecutor(max_workers=30) as ex:
            list(ex.map(lambda i: self.client.delete(
                f"/api/v1/namespaces/{self.namespace}/pods/{i['metadata']['name']}"),
                items))
        log(f"  deleted {len(items)} background pods")
        log("\n  done")

    def run(self) -> None:
        self.run_admission_throughput()
