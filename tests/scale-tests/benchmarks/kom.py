"""
benchmarks/kom.py — Kubernetes Object Monitor microbenchmarks.

Implements GitHub issue #1512: Microbench kubernetes object monitor.

Scenarios
---------
  memory   MB-2.1  Heap vs object count (nodes + pods)
  churn    MB-2.2  Pod churn rate sweep (pods/s)
  restart  MB-2.3  Watch restart storm (K12 bottleneck)
  resync   MB-2.4  ResyncPeriod spike at scale
  policy   MB-C2   CEL complexity, lookup(), scope, MCR sweep
"""
from __future__ import annotations

import json
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from framework.cluster import ClusterClient, ClusterSetup, NodeTemplate
from framework.ingestion import MetricsIngester
from framework.log import log
from framework.prometheus import NVSentinelMetrics, PrometheusClient, take_snapshot, wait_queue_drain
from framework.results import BenchmarkResults, BenchmarkRun, Snapshot


KOM_JOB      = "kubernetes-object-monitor"
KOM_NS       = "nvsentinel"
KOM_DEPLOY   = "kubernetes-object-monitor"
KOM_CONFIGMAP = "kubernetes-object-monitor"


@dataclass
class KOMBenchmark:
    client: ClusterClient
    prom: PrometheusClient
    namespace: str = "benchmark"
    pod_sample_namespace: str = "gpu-operator"
    size_tolerance_pct: float = 15.0
    warmup_s: int = 60
    duration_s: int = 300
    repetitions: int = 3
    snapshot_interval_s: int = 15
    skip: set = field(default_factory=set)
    # Memory sweep — exact counts to snapshot at
    node_counts: list[int] = field(
        default_factory=lambda: [100, 1_000, 5_000, 10_000, 25_000, 50_000, 75_000, 100_000]
    )
    pod_counts: list[int] = field(
        default_factory=lambda: [0, 1_000, 10_000, 50_000, 100_000]
    )
    node_parallelism: int = 80
    pod_parallelism: int = 80
    # Churn sweep
    churn_rates: list[int] = field(default_factory=lambda: [10, 50, 200])
    churn_node_count: int = 100
    # Restart storm
    restart_node_counts: list[int] = field(default_factory=lambda: [100, 10_000])
    skip_verify: bool = False
    no_cleanup: bool = False
    dry_run: bool = False
    version: str = "unknown"
    output: str = "results"
    ingester: MetricsIngester = field(default_factory=MetricsIngester)

    def __post_init__(self):
        self.setup  = ClusterSetup(self.client, self.pod_sample_namespace, self.size_tolerance_pct)
        self.metrics = NVSentinelMetrics(self.prom, job=KOM_JOB)
        self.results = BenchmarkResults(component="kom", version=self.version)

    def _delete_namespace_pods(self, namespace: str) -> None:
        """Delete all pods in namespace via proxy (parallel, never hangs) then clear finalizer."""
        import urllib.request as _ur3
        ns_url = f"{self.client.proxy}/api/v1/namespaces/{namespace}/pods"
        names, cont = [], None
        while True:
            url = ns_url + ("?limit=500" if not cont else f"?limit=500&continue={cont}")
            try:
                resp = json.loads(_ur3.urlopen(url, timeout=30).read())
                names += [p["metadata"]["name"] for p in resp.get("items", [])]
                cont = resp.get("metadata", {}).get("continue")
                if not cont: break
            except: break
        def _del(n):
            req = _ur3.Request(f"{ns_url}/{n}", method="DELETE",
                data=b'{"gracePeriodSeconds":0}', headers={"Content-Type":"application/json"})
            try: _ur3.urlopen(req, timeout=5)
            except: pass
        for chunk in [names[i:i+300] for i in range(0, len(names), 300)]:
            ts = [threading.Thread(target=_del, args=(n,)) for n in chunk]
            for t in ts: t.start()
            for t in ts: t.join()
        if names:
            log(f"  Deleted {len(names)} pods from {namespace}")
        # Clear namespace finalizer so it terminates cleanly
        try:
            ns = json.loads(_ur3.urlopen(
                f"{self.client.proxy}/api/v1/namespaces/{namespace}", timeout=5).read())
            ns["spec"]["finalizers"] = []
            req = _ur3.Request(
                f"{self.client.proxy}/api/v1/namespaces/{namespace}/finalize",
                data=json.dumps(ns).encode(), method="PUT",
                headers={"Content-Type": "application/json"})
            _ur3.urlopen(req, timeout=10)
        except: pass

    # ── Utilities ─────────────────────────────────────────────────────────────

    def _kubectl(self, args: list[str], **kwargs) -> subprocess.CompletedProcess:
        return subprocess.run(["kubectl"] + args, capture_output=True, text=True, **kwargs)

    def _restart_kom(self) -> None:
        """Trigger KOM restart by patching deployment annotation (no kubectl)."""
        import urllib.request as _ur
        import datetime
        patch = json.dumps({"spec": {"template": {"metadata": {"annotations": {
            "kubectl.kubernetes.io/restartedAt": datetime.datetime.utcnow().isoformat() + "Z"
        }}}}}).encode()
        req = _ur.Request(
            f"{self.client.proxy}/apis/apps/v1/namespaces/{KOM_NS}/deployments/{KOM_DEPLOY}",
            data=patch, method="PATCH",
            headers={"Content-Type": "application/strategic-merge-patch+json"})
        try: _ur.urlopen(req, timeout=10)
        except Exception: pass

    def _wait_kom_ready(self, timeout_s: int = 900) -> float:
        """Wait for KOM pod to be 1/1 Ready via proxy (no kubectl). Returns elapsed seconds."""
        import urllib.request as _ur
        t0 = time.time()
        while time.time() - t0 < timeout_s:
            try:
                resp = json.loads(_ur.urlopen(
                    f"{self.client.proxy}/api/v1/namespaces/{KOM_NS}/pods"
                    f"?labelSelector=app.kubernetes.io%2Fname%3D{KOM_DEPLOY}",
                    timeout=5).read())
                pods = resp.get("items", [])
                running = [
                    p for p in pods
                    if p.get("status", {}).get("phase") == "Running"
                    and all(
                        c.get("ready") for c in
                        p.get("status", {}).get("containerStatuses", [{"ready": False}])
                    )
                ]
                if len(running) == 1 and len(pods) == 1:
                    return time.time() - t0
            except Exception:
                pass
            time.sleep(3)
        return time.time() - t0

    def _patch_kom_config(self, toml: str) -> None:
        """Patch KOM configmap and restart via proxy (no kubectl)."""
        import urllib.request as _ur
        patch = json.dumps({"data": {"config.toml": toml}}).encode()
        req = _ur.Request(
            f"{self.client.proxy}/api/v1/namespaces/{KOM_NS}/configmaps/{KOM_CONFIGMAP}",
            data=patch, method="PATCH",
            headers={"Content-Type": "application/strategic-merge-patch+json"})
        try: _ur.urlopen(req, timeout=10)
        except Exception: pass
        self._restart_kom()
        self._wait_kom_ready()

    def _patch_kom_args(self, mcr: int | None = None, resync: str | None = None) -> None:
        patches = []
        if mcr is not None:
            patches.append({
                "op": "replace",
                "path": "/spec/template/spec/containers/0/args/4",
                "value": f"--max-concurrent-reconciles={mcr}",
            })
        if resync is not None:
            patches.append({
                "op": "replace",
                "path": "/spec/template/spec/containers/0/args/5",
                "value": f"--resync-period={resync}",
            })
        if patches:
            self._kubectl([
                "patch", "deployment", KOM_DEPLOY, "-n", KOM_NS,
                "--type=json", f"-p={json.dumps(patches)}",
            ])
            self._restart_kom()
            self._wait_kom_ready()

    def _snapshot(
        self,
        label: str,
        controller: str = "node",
        wait: bool = True,
    ) -> Snapshot:
        return take_snapshot(
            self.metrics,
            label=label,
            controller=controller,
            queue_name=controller,
            wait_drain=wait,
            drain_timeout_s=120,
        )

    def _churn_pods(
        self,
        rate_per_s: int,
        duration_s: int,
        node_count: int = 100,
    ) -> threading.Event:
        """Start pod churn. Returns stop event."""
        stop = threading.Event()
        n = [0]

        tols = [
            {"key": "kwok.x-k8s.io/node", "operator": "Exists", "effect": "NoSchedule"},
            {"key": "node.kubernetes.io/not-ready", "operator": "Exists", "effect": "NoExecute"},
            {"key": "node.kubernetes.io/unreachable", "operator": "Exists", "effect": "NoExecute"},
        ]

        # Pre-build padded pod template matching real pod size (~9.1KB)
        # so the informer cache holds representative objects.
        # Padding goes into an annotation — simple and reliable.
        base_pod = {
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {
                "name": "churn-placeholder",
                "namespace": self.namespace,
                "labels": {"churn": "true", "bench": "true", "app": "benchmark",
                           "tier": "churn", "component": "load-generator"},
                "annotations": {
                    "benchmark.nvsentinel.io/run": "true",
                    "benchmark.nvsentinel.io/padding": "",
                },
            },
            "spec": {
                "nodeName": "kwok-node-000000",
                "tolerations": tols,
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.9",
                    "resources": {"requests": {"cpu": "1m", "memory": "4Mi"}},
                }],
            },
        }
        target_bytes = self.results.object_sizes.get("pod_bytes", 9335)
        pad = max(0, target_bytes - len(json.dumps(base_pod)))
        base_pod["metadata"]["annotations"]["benchmark.nvsentinel.io/padding"] = "X" * pad
        import urllib.request as _urllib_req

        # Pre-serialize the body template ONCE.
        # Replace pod name and nodeName as plain string substitution — no JSON
        # parse/dump per pod. This is safe because names are fixed-length
        # zero-padded integers that never contain JSON special characters.
        body_template = json.dumps(base_pod)
        url = f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/pods"
        counter_lock = threading.Lock()

        def worker(interval: float) -> None:
            t_next = time.monotonic()
            while not stop.is_set():
                with counter_lock:
                    i = n[0]; n[0] += 1
                node_idx = i % node_count
                body = (
                    body_template
                    .replace('"churn-placeholder"', f'"churn-{i:010d}"', 1)
                    .replace('"kwok-node-000000"', f'"kwok-node-{node_idx:06d}"', 1)
                ).encode()
                req = _urllib_req.Request(
                    url, data=body, method="POST",
                    headers={"Content-Type": "application/json"},
                )
                try: _urllib_req.urlopen(req, timeout=10)
                except Exception: pass
                t_next += interval
                gap = t_next - time.monotonic()
                if gap > 0: time.sleep(gap)

        n_threads = max(1, rate_per_s // 5)
        per_thread = rate_per_s / n_threads
        for _ in range(n_threads):
            threading.Thread(target=worker, args=(1.0/per_thread,), daemon=True).start()

        return stop

    # ── Scenario: memory sweep ────────────────────────────────────────────────

    def _run_memory(self, node_tmpl: NodeTemplate) -> None:
        log("\n── MB-2.1: Memory sweep ──────────────────────────────────────")
        run = BenchmarkRun(
            scenario="memory",
            component="kom",
            warmup_s=0,
            measurement_s=0,
        )

        # Node sweep — use the exact counts specified by the caller
        log("  Node sweep:")
        log(f"    counts: {self.node_counts}")
        current = 0

        for target in sorted(set(self.node_counts)):
            if current < target:
                t0 = time.time()
                self.setup.create_kwok_nodes(
                    current, target, node_tmpl,
                    parallelism=self.node_parallelism,
                )
                elapsed = time.time() - t0
                log(f"    Built to {target:,} nodes in {elapsed:.0f}s")
                current = target
            snap = self._snapshot(f"{target:,} nodes", controller="node")
            snap.extras["node_count"] = target
            run.add(snap)
            log(f"    {target:,} nodes → heap={snap.heap_alloc_mb}MB "
                f"queue_wait_P99={snap.queue_wait_p99_ms}ms "
                f"→ limit={snap.limit_recommendation_mi}Mi")
            self.results.runs.append(run)
            self.results.save(self.output)
            self.ingester.push(self.results)

        if not self.no_cleanup:
            self.setup.delete_kwok_nodes()
            current = 0

        # Pod sweep — patch KOM to watch pods in the benchmark namespace
        # before creating any pods, otherwise KOM has no pod policy and the
        # informer cache stays empty regardless of how many pods are created.
        log("\n  Pod sweep:")
        self.setup.create_kwok_nodes(0, 100, node_tmpl,
                                     parallelism=self.node_parallelism)
        self.setup.create_namespace(self.namespace)

        pod_policy_toml = (
            "[[policies]]\n"
            '  name = "PodNotReady"\n'
            "  enabled = true\n"
            "  [policies.resource]\n"
            '    group = ""\n'
            '    version = "v1"\n'
            '    kind = "Pod"\n'
            f'    namespace = "{self.namespace}"\n'
            "  [policies.predicate]\n"
            '    expression = "resource.status.phase == \\"Failed\\""\n'
            "  [policies.nodeAssociation]\n"
            '    expression = "resource.spec.nodeName"\n'
            "  [policies.healthEvent]\n"
            '    componentClass = "Node"\n'
            "    isFatal = false\n"
            '    message = "Pod failed"\n'
            '    recommendedAction = "CONTACT_SUPPORT"\n'
            '    errorCode = ["POD_FAILED"]\n'
        )
        self._patch_kom_config(pod_policy_toml)
        # Also ensure RBAC allows pod listing
        import subprocess
        subprocess.run(
            ["kubectl", "patch", "clusterrole", "kubernetes-object-monitor",
             "--type=json",
             '-p=[{"op":"add","path":"/rules/-","value":'
             '{"apiGroups":[""],"resources":["pods"],"verbs":["get","list","watch"]}}]'],
            capture_output=True,
        )
        log("  Pod policy patched into KOM — informer will now cache benchmark pods")

        pod_current = 0
        pod_targets = sorted(set(self.pod_counts))
        log(f"  Pod sweep counts: {pod_targets}")

        pod_run = BenchmarkRun(scenario="memory_pods", component="kom", warmup_s=0, measurement_s=0)
        for target in pod_targets:
            if target > pod_current:
                self.setup.create_pods(
                    self.namespace, pod_current, target - pod_current,
                    node_count=100,
                    parallelism=self.pod_parallelism,
                )
                pod_current = target
            snap = self._snapshot(f"{target:,} pods", controller="pod")
            snap.extras["pod_count"] = target
            pod_run.add(snap)
            log(f"    {target:,} pods → heap={snap.heap_alloc_mb}MB "
                f"→ limit={snap.limit_recommendation_mi}Mi")
        self.results.add_run(pod_run)

        # Restore NodeNotReady policy after pod sweep
        node_policy_toml = (
            "[[policies]]\n"
            '  name = "NodeNotReady"\n'
            "  enabled = true\n"
            "  [policies.resource]\n"
            '    group = ""\n'
            '    version = "v1"\n'
            '    kind = "Node"\n'
            "  [policies.predicate]\n"
            '    expression = "has(resource.status.conditions) && resource.status.conditions.exists(c, c.type == \\"Ready\\" && c.status == \\"False\\")"\n'
            "  [policies.healthEvent]\n"
            '    componentClass = "Node"\n'
            "    isFatal = true\n"
            '    message = "Node is not ready"\n'
            '    recommendedAction = "CONTACT_SUPPORT"\n'
            '    errorCode = ["NODE_NOT_READY"]\n'
        )
        self._patch_kom_config(node_policy_toml)
        log("  NodeNotReady policy restored after pod sweep")

        if not self.no_cleanup:
            self._delete_namespace_pods(self.namespace)
            self.setup.delete_kwok_nodes()

    # ── Scenario: churn ───────────────────────────────────────────────────────

    def _run_churn(self, node_tmpl: NodeTemplate) -> None:
        log("\n── MB-2.2: Pod churn rate sweep ──────────────────────────────")
        self.setup.create_kwok_nodes(0, self.churn_node_count, node_tmpl,
                                     parallelism=self.node_parallelism)
        self.setup.create_namespace(self.namespace)
        log(f"  Churn node count: {self.churn_node_count}, rates: {self.churn_rates}")

        # ONLY load pod policy — no node policy.
        # Node policy + resync would flood the pod queue with node events,
        # making throughput identical across all churn rates (node resync dominates).
        # Also set a very long resync period to eliminate resync interference.
        self._patch_kom_config(
            "[[policies]]\n"
            '  name = "PodFailed"\n'
            "  enabled = true\n"
            "  [policies.resource]\n"
            '    group = ""\n'
            '    version = "v1"\n'
            '    kind = "Pod"\n'
            f'    namespace = "{self.namespace}"\n'
            "  [policies.predicate]\n"
            '    expression = "resource.status.phase == \\"Failed\\""\n'
            "  [policies.nodeAssociation]\n"
            '    expression = "resource.spec.nodeName"\n'
            "  [policies.healthEvent]\n"
            '    componentClass = "Node"\n'
            "    isFatal = false\n"
            '    message = "Pod failed"\n'
            '    recommendedAction = "CONTACT_SUPPORT"\n'
            '    errorCode = ["POD_FAILED"]\n'
        )
        # Extend resync period so it never fires during a 2-min measurement window
        self._patch_kom_args(resync="24h")
        log("  KOM: pod-only policy + resync=24h (no node resync interference)")

        for rate in self.churn_rates:
            log(f"\n  Rate: {rate} pods/s")
            run = BenchmarkRun(
                scenario=f"churn_{rate}pods_s",
                component="kom",
                warmup_s=self.warmup_s,
                measurement_s=self.duration_s,
                metadata={"target_rate_pods_s": rate},
            )

            for rep in range(self.repetitions):
                # Delete leftover pods via parallel proxy calls — NOT kubectl
                # (kubectl delete --all hangs on thousands of accumulated pods)
                import urllib.request as _ur2
                ns_url = f"{self.client.proxy}/api/v1/namespaces/{self.namespace}/pods"
                pod_names, cont = [], None
                while True:
                    url = ns_url + ("?limit=500" if not cont else f"?limit=500&continue={cont}")
                    try:
                        resp = json.loads(_ur2.urlopen(url, timeout=30).read())
                        pod_names += [p["metadata"]["name"] for p in resp.get("items", [])]
                        cont = resp.get("metadata", {}).get("continue")
                        if not cont: break
                    except: break
                def _del_pod(n):
                    req = _ur2.Request(f"{ns_url}/{n}", method="DELETE",
                        data=b'{"gracePeriodSeconds":0}',
                        headers={"Content-Type":"application/json"})
                    try: _ur2.urlopen(req, timeout=5)
                    except: pass
                for chunk in [pod_names[i:i+300] for i in range(0, len(pod_names), 300)]:
                    ts = [threading.Thread(target=_del_pod, args=(n,)) for n in chunk]
                    for t in ts: t.start()
                    for t in ts: t.join()
                if pod_names:
                    log(f"    Deleted {len(pod_names)} pods before rep{rep+1}")
                # Wait for KOM queue to drain
                for _ in range(30):
                    d = self.metrics.queue_depth("pod")
                    if d is not None and d < 5:
                        break
                    time.sleep(3)

                # Start churn: create pods at rate/s, delete them after 5s TTL
                stop = self._churn_pods(rate, self.warmup_s + self.duration_s,
                                        node_count=self.churn_node_count)
                time.sleep(self.warmup_s)  # discard warmup

                samples = []
                t0 = time.time()
                while time.time() - t0 < self.duration_s:
                    snap = Snapshot(label=f"rep{rep+1}")
                    snap.heap_alloc_mb     = self.metrics.heap_alloc_mb()
                    snap.cpu_pct           = self.metrics.cpu_pct("30s")
                    snap.reconcile_rps     = self.metrics.reconcile_rps("pod", "30s")
                    snap.processing_p99_ms = self.metrics.processing_p99_ms("pod", "30s")
                    snap.queue_depth       = int(self.metrics.queue_depth("pod") or 0)
                    snap.queue_wait_p99_ms = self.metrics.queue_wait_p99_ms("pod", "30s")
                    samples.append(snap)
                    time.sleep(self.snapshot_interval_s)

                stop.set()

                def avg(key: str) -> float | None:
                    vals = [getattr(s, key) for s in samples if getattr(s, key) is not None]
                    return round(sum(vals) / len(vals), 1) if vals else None

                snap = Snapshot(label=f"churn_{rate}_rep{rep+1}")
                snap.heap_alloc_mb     = avg("heap_alloc_mb")
                snap.cpu_pct           = avg("cpu_pct")
                snap.reconcile_rps     = avg("reconcile_rps")
                snap.processing_p99_ms = avg("processing_p99_ms")
                snap.queue_depth       = avg("queue_depth")
                snap.queue_wait_p99_ms = avg("queue_wait_p99_ms")
                run.add(snap)

                log(f"    rep{rep+1}: tput={snap.reconcile_rps}/s "
                    f"P99={snap.processing_p99_ms}ms "
                    f"queue={snap.queue_depth} queue_wait={snap.queue_wait_p99_ms}ms")

            self.results.add_run(run)
            self.results.save(self.output)

        # Restore defaults after churn
        self._patch_kom_args(resync="5m")

        if not self.no_cleanup:
            self._delete_namespace_pods(self.namespace)
            self.setup.delete_kwok_nodes()

    # ── Scenario: restart storm (K12) ─────────────────────────────────────────

    def _run_restart(self, node_tmpl: NodeTemplate) -> None:
        log("\n── MB-2.3: Watch restart storm (K12 bottleneck) ──────────────")

        # Restore NodeNotReady policy so KOM reconciles nodes and writes
        # annotations — K12 measures startup cost of reading those annotations.
        # Without this, annotation state is empty and startup is artificially fast.
        node_policy_toml = (
            "[[policies]]\n"
            '  name = "NodeNotReady"\n'
            "  enabled = true\n"
            "  [policies.resource]\n"
            '    group = ""\n'
            '    version = "v1"\n'
            '    kind = "Node"\n'
            "  [policies.predicate]\n"
            '    expression = "has(resource.status.conditions) && resource.status.conditions.exists(c, c.type == \\"Ready\\" && c.status == \\"False\\")"\n'
            "  [policies.healthEvent]\n"
            '    componentClass = "Node"\n'
            "    isFatal = true\n"
            '    message = "Node is not ready"\n'
            '    recommendedAction = "CONTACT_SUPPORT"\n'
            '    errorCode = ["NODE_NOT_READY"]\n'
        )
        self._patch_kom_config(node_policy_toml)

        for node_count in sorted(set(self.restart_node_counts)):
            log(f"  Restart at {node_count:,} nodes:")
            self.setup.create_kwok_nodes(0, node_count, node_tmpl,
                                         parallelism=self.node_parallelism)

            # Flip ~10% of nodes to NotReady so KOM writes annotation state.
            # K12's per-node GET cost is the same whether annotations exist or not,
            # but this makes the test more realistic.
            import urllib.request as _ur
            not_ready_status = json.dumps({"status": {"conditions": [
                {"type": "Ready", "status": "False", "reason": "KubeletNotReady",
                 "message": "benchmark: forced not ready", "lastHeartbeatTime": "2026-01-01T00:00:00Z",
                 "lastTransitionTime": "2026-01-01T00:00:00Z"},
            ]}}).encode()
            flip_count = max(1, node_count // 10)
            for i in range(flip_count):
                req = _ur.Request(
                    f"{self.client.proxy}/api/v1/nodes/kwok-node-{i:06d}/status",
                    data=not_ready_status, method="PATCH",
                    headers={"Content-Type": "application/merge-patch+json"})
                try: _ur.urlopen(req, timeout=10)
                except: pass

            # Wait for KOM to reconcile and write annotations to nodes
            log(f"    Waiting 60s for KOM to write annotation state to {flip_count} nodes...")
            time.sleep(60)

            heap_pre = self.metrics.heap_alloc_mb()
            log(f"    Pre-restart heap: {heap_pre} MB")

            t_restart = time.time()
            self._restart_kom()

            t_ready = None
            t_synced = None
            peak_queue = 0
            peak_tput = 0

            for _ in range(300):
                time.sleep(5)
                elapsed = time.time() - t_restart
                r = self._kubectl([
                    "get", "pods", "-n", KOM_NS,
                    "-l", f"app.kubernetes.io/name={KOM_DEPLOY}", "--no-headers",
                ])
                lines = [l for l in r.stdout.strip().split("\n") if l]
                ready = lines and "1/1" in lines[0] and "Running" in lines[0] and len(lines) == 1

                depth = self.metrics.queue_depth("node") or 0
                tput  = self.metrics.reconcile_rps("node", "15s") or 0
                peak_queue = max(peak_queue, depth)
                peak_tput  = max(peak_tput, tput)

                if ready and t_ready is None:
                    t_ready = elapsed
                    log(f"    Pod Ready at +{elapsed:.0f}s")

                if t_ready and depth < 20 and t_synced is None:
                    t_synced = elapsed
                    log(f"    Cache synced at +{elapsed:.0f}s")
                    break

            per_node_ms = (t_ready / node_count * 1000) if t_ready else None
            snap = Snapshot(label=f"restart_{node_count}_nodes")
            snap.extras = {
                "node_count": node_count,
                "t_ready_s": round(t_ready, 0) if t_ready else None,
                "t_synced_s": round(t_synced, 0) if t_synced else None,
                "relist_s": round(t_synced - t_ready, 0) if (t_ready and t_synced) else None,
                "peak_queue": peak_queue,
                "peak_tput_rps": round(peak_tput, 1),
                "per_node_startup_ms": round(per_node_ms, 1) if per_node_ms else None,
                "heap_pre_mb": heap_pre,
            }
            log(f"    Ready={t_ready:.0f}s  per-node={per_node_ms:.1f}ms  "
                f"peak_queue={peak_queue:.0f}  peak_tput={peak_tput:.0f}/s")

            run = BenchmarkRun(
                scenario=f"restart_{node_count}_nodes",
                component="kom",
                warmup_s=0,
                measurement_s=int(t_synced or 0),
                metadata=snap.extras,
            )
            run.add(snap)
            self.results.add_run(run)
            self.results.save(self.output)

            if not self.no_cleanup:
                self.setup.delete_kwok_nodes()

    # ── Scenario: policy sweep (MB-C2) ────────────────────────────────────────

    def _run_policy(self, node_tmpl: NodeTemplate) -> None:
        log("\n── MB-C2: Policy configuration sweep ─────────────────────────")
        self.setup.create_kwok_nodes(0, 100, node_tmpl)
        self.setup.create_namespace(self.namespace)

        def make_toml(expr: str, ns: str = "", node_assoc: str | None = None) -> str:
            lines = [
                "[[policies]]", '  name = "Bench"', "  enabled = true",
                "  [policies.resource]", '    group = ""',
                '    version = "v1"', '    kind = "Pod"',
            ]
            if ns:
                lines.append(f'    namespace = "{ns}"')
            lines += ["  [policies.predicate]", f"    expression = {json.dumps(expr)}"]
            if node_assoc:
                lines += ["  [policies.nodeAssociation]",
                          f"    expression = {json.dumps(node_assoc)}"]
            lines += [
                "  [policies.healthEvent]",
                '    componentClass = "Node"', "    isFatal = false",
                '    message = "bench"', '    recommendedAction = "CONTACT_SUPPORT"',
                '    errorCode = ["BENCH"]',
            ]
            return "\n".join(lines) + "\n"

        stop = self._churn_pods(30, 99999)

        def measure_policy(warmup: int = 30, window: int = 60) -> Snapshot:
            time.sleep(warmup)
            samples = []
            t0 = time.time()
            while time.time() - t0 < window:
                s = Snapshot(label="")
                s.reconcile_rps     = self.metrics.reconcile_rps("pod", "30s")
                s.processing_p50_ms = self.metrics.processing_p50_ms("pod", "30s")
                s.processing_p99_ms = self.metrics.processing_p99_ms("pod", "30s")
                s.cpu_pct           = self.metrics.cpu_pct("30s")
                s.queue_wait_p99_ms = self.metrics.queue_wait_p99_ms("pod", "30s")
                if s.reconcile_rps is not None:
                    samples.append(s)
                time.sleep(10)
            avg = lambda k: (
                round(sum(getattr(x, k) for x in samples if getattr(x, k) is not None)
                      / max(len([x for x in samples if getattr(x, k) is not None]), 1), 1)
            )
            snap = Snapshot(label="")
            snap.reconcile_rps     = avg("reconcile_rps")
            snap.processing_p50_ms = avg("processing_p50_ms")
            snap.processing_p99_ms = avg("processing_p99_ms")
            snap.cpu_pct           = avg("cpu_pct")
            snap.queue_wait_p99_ms = avg("queue_wait_p99_ms")
            return snap

        # C2.1: CEL complexity
        log("  MB-C2.1: CEL predicate complexity")
        cel_run = BenchmarkRun(scenario="cel_complexity", component="kom",
                               warmup_s=30, measurement_s=60)
        for name, expr in [
            ("simple",   'resource.status.phase == "Running"'),
            ("medium",   'resource.status.phase == "Running" && has(resource.metadata.labels) && resource.metadata.labels.exists(k, k == "churn")'),
            ("complex",  'resource.status.phase == "Running" && has(resource.status.conditions) && resource.status.conditions.filter(c, c.type in ["Ready","ContainersReady"] && c.status == "True").size() >= 1 && has(resource.metadata.labels) && resource.metadata.labels.exists(k, k.startsWith("churn"))'),
            ("catchall", 'has(resource.metadata.name)'),
        ]:
            self._patch_kom_config(make_toml(expr, ns=self.namespace))
            snap = measure_policy()
            snap.label = name
            snap.extras = {"predicate": name}
            cel_run.add(snap)
            log(f"    {name}: tput={snap.reconcile_rps}/s "
                f"P99={snap.processing_p99_ms}ms "
                f"queue_wait={snap.queue_wait_p99_ms}ms")
        self.results.add_run(cel_run)

        # C2.2: nodeAssociation
        log("  MB-C2.2: nodeAssociation lookup()")
        assoc_run = BenchmarkRun(scenario="nodeassociation", component="kom",
                                 warmup_s=30, measurement_s=60)
        for name, assoc in [
            ("none",          None),
            ("direct_cel",    "resource.spec.nodeName"),
            ("single_lookup", "lookup('v1', 'Node', '', resource.spec.nodeName).metadata.name"),
        ]:
            self._patch_kom_config(
                make_toml('resource.status.phase == "Running"',
                          ns=self.namespace, node_assoc=assoc)
            )
            snap = measure_policy()
            snap.label = name
            snap.extras = {"association": name}
            assoc_run.add(snap)
            log(f"    {name}: tput={snap.reconcile_rps}/s P50={snap.processing_p50_ms}ms")
        self.results.add_run(assoc_run)

        # C2.3: namespace vs cluster scope
        log("  MB-C2.3: Namespace vs cluster scope")
        scope_run = BenchmarkRun(scenario="scope", component="kom",
                                 warmup_s=30, measurement_s=60)
        for name, ns in [("namespace_scoped", self.namespace), ("cluster_scoped", "")]:
            self._patch_kom_config(
                make_toml('resource.status.phase == "Running"', ns=ns)
            )
            time.sleep(15)
            snap = measure_policy()
            snap.heap_alloc_mb = self.metrics.heap_alloc_mb()
            snap.label = name
            scope_run.add(snap)
            log(f"    {name}: tput={snap.reconcile_rps}/s heap={snap.heap_alloc_mb}MB")
        self.results.add_run(scope_run)

        # C2.4: maxConcurrentReconciles
        log("  MB-C2.4: maxConcurrentReconciles sweep")
        mcr_run = BenchmarkRun(scenario="mcr_sweep", component="kom",
                               warmup_s=30, measurement_s=60)
        for mcr in [1, 2, 4, 8]:
            self._patch_kom_args(mcr=mcr)
            snap = measure_policy()
            snap.label = f"mcr_{mcr}"
            snap.extras = {"mcr": mcr}
            mcr_run.add(snap)
            log(f"    MCR={mcr}: tput={snap.reconcile_rps}/s "
                f"P99={snap.processing_p99_ms}ms CPU={snap.cpu_pct}%")

        # Restore MCR=1
        self._patch_kom_args(mcr=1)
        self.results.add_run(mcr_run)
        stop.set()

        if not self.no_cleanup:
            self._delete_namespace_pods(self.namespace)
            self.setup.delete_kwok_nodes()

    # ── Entry point ───────────────────────────────────────────────────────────

    def run(self) -> None:
        log(f"KOM Benchmark  reps={self.repetitions}  warmup={self.warmup_s}s  "
            f"measure={self.duration_s}s")

        # Step 1: Sample real objects from cluster
        log("\n── Step 1: Sampling real objects ─────────────────────────────")
        node_tmpl = self.setup.sample_node()
        pod_tmpl  = self.setup.sample_pod()
        self.results.object_sizes = {
            "node_bytes": node_tmpl.raw_bytes,
            "node_conditions": node_tmpl.conditions,
            "node_images": node_tmpl.images,
            "pod_bytes": pod_tmpl.raw_bytes,
            "pod_namespace": pod_tmpl.source_namespace,
        }

        # Step 2: Verify KWOK node size
        log("\n── Step 2: Verifying KWOK node size ──────────────────────────")
        if not self.skip_verify:
            self.setup.freeze_dns_autoscaler()
            self.setup.verify_node_size(node_tmpl)
        else:
            log("  Skipping size verification (--skip-verify)")
            self.setup.freeze_dns_autoscaler()

        # Run scenarios
        try:
            if "memory"  not in self.skip: self._run_memory(node_tmpl)
            if "churn"   not in self.skip: self._run_churn(node_tmpl)
            if "restart" not in self.skip: self._run_restart(node_tmpl)
            if "policy"  not in self.skip: self._run_policy(node_tmpl)
        finally:
            self.setup.restore_dns_autoscaler()

        self.results.finish()
        path = self.results.save(self.output)
        self.results.print_summary()
        log(f"Results: {path}")
