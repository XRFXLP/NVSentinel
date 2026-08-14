"""
cluster.py — Kubernetes cluster setup helpers.

Handles KWOK node/pod lifecycle, DNS autoscaler management,
and representative object sizing from real cluster samples.
"""
from __future__ import annotations

import json
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any

from .log import log


@dataclass
class NodeTemplate:
    """A real node's status captured for use as KWOK template."""
    source_node: str
    raw_bytes: int
    status: dict = field(repr=False)
    conditions: int = 0
    images: int = 0

    def status_bytes(self) -> bytes:
        return json.dumps({"status": self.status}).encode()


@dataclass
class PodTemplate:
    """A real pod's spec/status captured for sizing reference."""
    source_pod: str
    source_namespace: str
    raw_bytes: int
    phase: str = "Running"


class ClusterClient:
    """Wraps kubectl proxy for Kubernetes API calls."""

    def __init__(self, proxy: str = "http://localhost:8001"):
        self.proxy = proxy

    def get(self, path: str, timeout: int = 30) -> dict:
        return json.loads(
            urllib.request.urlopen(f"{self.proxy}{path}", timeout=timeout).read()
        )

    def post(self, path: str, body: dict, timeout: int = 10) -> bool:
        req = urllib.request.Request(
            f"{self.proxy}{path}",
            data=json.dumps(body).encode(),
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req, timeout=timeout)
            return True
        except urllib.error.HTTPError as e:
            return e.code == 409  # AlreadyExists = ok
        except Exception:
            return False

    def patch_status(self, path: str, body: dict, timeout: int = 10) -> bool:
        req = urllib.request.Request(
            f"{self.proxy}{path}",
            data=json.dumps(body).encode(),
            method="PATCH",
            headers={"Content-Type": "application/merge-patch+json"},
        )
        try:
            urllib.request.urlopen(req, timeout=timeout)
            return True
        except Exception:
            return False

    def delete(self, path: str, timeout: int = 10) -> None:
        req = urllib.request.Request(
            f"{self.proxy}{path}",
            method="DELETE",
            data=b'{"gracePeriodSeconds":0}',
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req, timeout=timeout)
        except Exception:
            pass

    def list_all(self, path: str, label_selector: str = "") -> list[dict]:
        """Paginate through all items matching a label selector."""
        items: list[dict] = []
        cont = None
        url = f"{path}?limit=500"
        if label_selector:
            url += f"&labelSelector={urllib.request.quote(label_selector)}"
        while True:
            full = f"{url}&continue={cont}" if cont else url
            resp = self.get(full)
            items.extend(resp.get("items", []))
            cont = resp.get("metadata", {}).get("continue")
            if not cont:
                break
        return items


class ClusterSetup:
    """
    Manages the benchmark environment on any Kubernetes cluster.

    Handles:
    - Sampling real node and pod objects for representative sizing
    - KWOK controller validation
    - DNS autoscaler freeze/restore (prevents CoreDNS flooding real nodes)
    - KWOK node creation with verified representative status
    - Namespace and pod lifecycle
    """

    KWOK_LABEL = "type=kwok"
    KWOK_ANNOTATION = "kwok.x-k8s.io/node"

    def __init__(
        self,
        client: ClusterClient,
        pod_sample_namespace: str = "gpu-operator",
        size_tolerance_pct: float = 15.0,
    ):
        self.client = client
        self.pod_sample_namespace = pod_sample_namespace
        self.size_tolerance_pct = size_tolerance_pct
        self._node_template: NodeTemplate | None = None
        self._pod_template: PodTemplate | None = None
        self._dns_autoscaler_frozen = False

    # ── Sampling ──────────────────────────────────────────────────────────────

    def sample_node(self) -> NodeTemplate:
        """
        Sample a real Ready node from the cluster and capture its full status.
        Returns a NodeTemplate whose status will be applied to every KWOK node.
        """
        # Only fetch 20 nodes — avoid paginating through 50k+ nodes on large clusters
        resp = self.client.get("/api/v1/nodes?limit=20")
        nodes = resp.get("items", [])
        ready = [
            n for n in nodes
            if any(
                c.get("type") == "Ready" and c.get("status") == "True"
                for c in n.get("status", {}).get("conditions", [])
            )
        ]
        if not ready:
            raise RuntimeError("No Ready nodes found in cluster")

        # Pick the node with the most conditions (richest status = most representative)
        node = max(ready, key=lambda n: len(n.get("status", {}).get("conditions", [])))
        status = node.get("status", {})
        raw = json.dumps(node)

        tmpl = NodeTemplate(
            source_node=node["metadata"]["name"],
            raw_bytes=len(raw),
            status={
                k: v for k, v in status.items()
                if k in (
                    "conditions", "allocatable", "capacity", "nodeInfo",
                    "addresses", "daemonEndpoints", "images",
                    "volumesAttached", "volumesInUse", "runtimeHandlers",
                )
            },
            conditions=len(status.get("conditions", [])),
            images=len(status.get("images", [])),
        )

        log(f"Sampled node '{tmpl.source_node}': "
            f"{tmpl.raw_bytes:,}B ({tmpl.raw_bytes/1024:.1f}KB), "
            f"{tmpl.conditions} conditions, {tmpl.images} images")

        self._node_template = tmpl
        return tmpl

    def sample_pod(self) -> PodTemplate:
        """
        Sample a running pod from pod_sample_namespace for sizing reference.
        Falls back to kube-system if the namespace has no running pods.
        """
        for ns in (self.pod_sample_namespace, "kube-system"):
            pods = self.client.get(f"/api/v1/namespaces/{ns}/pods?limit=20").get("items", [])
            running = [p for p in pods if p.get("status", {}).get("phase") == "Running"]
            if running:
                pod = running[0]
                raw = json.dumps(pod)
                tmpl = PodTemplate(
                    source_pod=pod["metadata"]["name"],
                    source_namespace=ns,
                    raw_bytes=len(raw),
                )
                log(f"Sampled pod '{tmpl.source_pod}' from '{ns}': "
                    f"{tmpl.raw_bytes:,}B ({tmpl.raw_bytes/1024:.1f}KB)")
                self._pod_template = tmpl
                return tmpl
        raise RuntimeError("No running pods found in cluster")

    # ── KWOK verification ─────────────────────────────────────────────────────

    def verify_kwok(self) -> None:
        """Check KWOK controller is running."""
        pods = self.client.get(
            "/api/v1/namespaces/kube-system/pods"
            "?labelSelector=app%3Dkwok-controller"
            "&fieldSelector=status.phase%3DRunning&limit=5"
        ).get("items", [])
        running = [p for p in pods if p.get("status", {}).get("phase") == "Running"]
        if not running:
            raise RuntimeError(
                "KWOK controller not running in kube-system.\n"
                "Install: kubectl apply -f "
                "https://github.com/kubernetes-sigs/kwok/releases/latest/download/kwok.yaml"
            )
        log(f"KWOK controller running: {running[0]['metadata']['name']}")

    def verify_node_size(self, template: NodeTemplate) -> None:
        """
        Create one KWOK node and verify its STATUS size is within tolerance of
        the real node's STATUS size.

        We compare status-only (not total JSON) because real nodes accumulate
        metadata.managedFields history over their lifetime that cannot be
        replicated on fresh KWOK nodes. The status is what NVSentinel actually
        reads and what drives informer cache memory.
        """
        name = "kwok-verify-probe-000"
        self.client.delete(f"/api/v1/nodes/{name}")
        time.sleep(1)

        self._create_kwok_node_raw(name, template)

        kwok = self.client.get(f"/api/v1/nodes/{name}")
        kwok_status_bytes = len(json.dumps(kwok.get("status", {})))
        real_status_bytes = len(json.dumps(template.status))
        diff = abs(kwok_status_bytes - real_status_bytes) / max(real_status_bytes, 1) * 100

        self.client.delete(f"/api/v1/nodes/{name}")

        log(f"Size check (status only) — "
            f"real status: {real_status_bytes:,}B  "
            f"kwok status: {kwok_status_bytes:,}B  "
            f"diff: {diff:.1f}%")
        log(f"  (Full node: real={template.raw_bytes:,}B vs kwok={len(json.dumps(kwok)):,}B — "
            f"gap is metadata.managedFields history, not status)")

        if diff > self.size_tolerance_pct:
            raise RuntimeError(
                f"KWOK node STATUS size differs from real node by {diff:.1f}% "
                f"(tolerance {self.size_tolerance_pct}%). "
                "Check that the status template was captured correctly."
            )
        log(f"✅ Node status size verified (within {self.size_tolerance_pct}% tolerance)")

    # ── DNS autoscaler ────────────────────────────────────────────────────────

    def freeze_dns_autoscaler(self) -> None:
        """
        Scale the kube-dns-autoscaler to 0 replicas.

        CRITICAL: Without this, adding KWOK nodes causes the cluster-proportional
        autoscaler to scale CoreDNS to match node count. On a cluster with 3
        real nodes (330 pod slots), 1000 new CoreDNS replicas fill all slots and
        block NVSentinel from scheduling.
        """
        subprocess.run(
            ["kubectl", "scale", "deployment", "kube-dns-autoscaler",
             "-n", "kube-system", "--replicas=0"],
            capture_output=True,
        )
        self._dns_autoscaler_frozen = True
        log("DNS autoscaler frozen at 0 replicas")

    def restore_dns_autoscaler(self) -> None:
        if self._dns_autoscaler_frozen:
            subprocess.run(
                ["kubectl", "scale", "deployment", "kube-dns-autoscaler",
                 "-n", "kube-system", "--replicas=1"],
                capture_output=True,
            )
            self._dns_autoscaler_frozen = False
            log("DNS autoscaler restored to 1 replica")

    # ── KWOK node lifecycle ───────────────────────────────────────────────────

    def _create_kwok_node_raw(self, name: str, template: NodeTemplate) -> None:
        self.client.post("/api/v1/nodes", {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": name,
                "labels": {
                    "type": "kwok",
                    "benchmark": "true",
                    self.KWOK_ANNOTATION: "fake",
                },
                "annotations": {self.KWOK_ANNOTATION: "fake"},
            },
            "spec": {
                "taints": [{
                    "key": self.KWOK_ANNOTATION,
                    "value": "fake",
                    "effect": "NoSchedule",
                }]
            },
        })
        self.client.patch_status(
            f"/api/v1/nodes/{name}/status",
            {"status": template.status},
        )

    def create_kwok_nodes(
        self,
        start: int,
        end: int,
        template: NodeTemplate,
        parallelism: int = 80,
    ) -> int:
        """Create KWOK nodes [start, end) with the given status template."""
        status_bytes = template.status_bytes()

        def worker(i: int) -> None:
            name = f"kwok-node-{i:06d}"
            body = json.dumps({
                "apiVersion": "v1",
                "kind": "Node",
                "metadata": {
                    "name": name,
                    "labels": {
                        "type": "kwok",
                        "benchmark": "true",
                        self.KWOK_ANNOTATION: "fake",
                    },
                    "annotations": {self.KWOK_ANNOTATION: "fake"},
                },
                "spec": {
                    "taints": [{
                        "key": self.KWOK_ANNOTATION,
                        "value": "fake",
                        "effect": "NoSchedule",
                    }]
                },
            }).encode()

            req = urllib.request.Request(
                f"{self.client.proxy}/api/v1/nodes",
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            try:
                urllib.request.urlopen(req, timeout=10)
            except urllib.error.HTTPError as e:
                if e.code != 409:
                    return

            req2 = urllib.request.Request(
                f"{self.client.proxy}/api/v1/nodes/{name}/status",
                data=status_bytes,
                method="PATCH",
                headers={"Content-Type": "application/merge-patch+json"},
            )
            try:
                urllib.request.urlopen(req2, timeout=10)
            except Exception:
                pass

        count = end - start
        chunk = max(1, count // parallelism)
        ok = 0
        for batch_start in range(start, end, chunk):
            batch_end = min(batch_start + chunk, end)
            threads = [
                threading.Thread(target=worker, args=(i,), daemon=True)
                for i in range(batch_start, batch_end)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            ok += batch_end - batch_start
        return ok

    def delete_kwok_nodes(self) -> int:
        """Delete all KWOK benchmark nodes by name prefix (catches unlabelled strays too)."""
        all_nodes = self.client.list_all("/api/v1/nodes")
        nodes = [n for n in all_nodes if n["metadata"]["name"].startswith("kwok-node-")]
        names = [n["metadata"]["name"] for n in nodes]
        if not names:
            return 0

        def delete_one(name: str) -> None:
            self.client.delete(f"/api/v1/nodes/{name}")

        chunks = [names[i:i+200] for i in range(0, len(names), 200)]
        for chunk in chunks:
            threads = [threading.Thread(target=delete_one, args=(n,)) for n in chunk]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        log(f"Deleted {len(names)} KWOK nodes")
        return len(names)

    # ── Pod lifecycle ─────────────────────────────────────────────────────────

    def create_namespace(self, namespace: str) -> None:
        # If namespace is stuck Terminating, clear its finalizer first
        try:
            ns = self.client.get(f"/api/v1/namespaces/{namespace}")
            if ns.get("status", {}).get("phase") == "Terminating":
                ns["spec"]["finalizers"] = []
                req = urllib.request.Request(
                    f"{self.client.proxy}/api/v1/namespaces/{namespace}/finalize",
                    data=json.dumps(ns).encode(), method="PUT",
                    headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=10)
                import time; time.sleep(3)
        except Exception:
            pass
        subprocess.run(
            ["kubectl", "apply", "-f", "-"],
            input=f"apiVersion: v1\nkind: Namespace\nmetadata:\n  name: {namespace}\n",
            text=True,
            capture_output=True,
        )

    def delete_namespace(self, namespace: str) -> None:
        subprocess.run(
            ["kubectl", "delete", "namespace", namespace, "--grace-period=0"],
            capture_output=True,
        )

    def create_pods(
        self,
        namespace: str,
        start: int,
        count: int,
        node_count: int = 100,
        label: str = "bench",
        parallelism: int = 80,
        tolerations: list[dict] | None = None,
        target_size_bytes: int = 9335,
    ) -> int:
        """Create benchmark pods spread across the first node_count KWOK nodes.

        target_size_bytes: pad labels so the pod JSON matches this size.
        Default 9335B matches a real gpu-operator pod on AWS c8a.16xlarge.
        This ensures the informer cache memory measurements are representative.
        """
        if tolerations is None:
            tolerations = [
                {"key": "kwok.x-k8s.io/node", "operator": "Exists", "effect": "NoSchedule"},
                {"key": "node.kubernetes.io/not-ready", "operator": "Exists", "effect": "NoExecute"},
                {"key": "node.kubernetes.io/unreachable", "operator": "Exists", "effect": "NoExecute"},
            ]

        # Build a base pod to measure its size, then pad to target
        base_labels = {
            "bench": label,
            "app": "benchmark",
            "version": "v1",
            "tier": "benchmark",
            "component": "load-generator",
        }
        base_pod = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "bench-pod-000000000",
                "namespace": namespace,
                "labels": base_labels,
                "annotations": {
                    "benchmark.nvsentinel.io/run": "true",
                    "benchmark.nvsentinel.io/generator": "kom-benchmark",
                },
            },
            "spec": {
                "nodeName": "kwok-node-000000",
                "tolerations": tolerations,
                "containers": [{
                    "name": "pause",
                    "image": "registry.k8s.io/pause:3.9",
                    "resources": {"requests": {"cpu": "1m", "memory": "4Mi"}},
                }],
            },
        }
        base_size = len(json.dumps(base_pod))
        # Pad with label entries until we reach target size
        pad_needed = max(0, target_size_bytes - base_size)
        if pad_needed > 0:
            # Each label "pad_XXX": "YYYY..." adds ~20+pad_value_len bytes
            # Use one large padding annotation to reach target
            base_pod["metadata"]["annotations"]["benchmark.nvsentinel.io/padding"] = "X" * pad_needed

        # Pre-serialize once, substitute name/node via string replace (no per-pod JSON parse)
        body_template = json.dumps(base_pod)
        url = f"{self.client.proxy}/api/v1/namespaces/{namespace}/pods"

        def create_one(i: int) -> None:
            body = (
                body_template
                .replace('"bench-pod-000000000"', f'"bench-pod-{i:09d}"', 1)
                .replace('"kwok-node-000000"', f'"kwok-node-{i % node_count:06d}"', 1)
            ).encode()
            req = urllib.request.Request(
                url, data=body, method="POST",
                headers={"Content-Type": "application/json"},
            )
            try: urllib.request.urlopen(req, timeout=10)
            except urllib.error.HTTPError as e:
                if e.code != 409: pass
            except: pass

        end = start + count
        chunk = max(1, count // parallelism)
        for batch_start in range(start, end, chunk):
            batch_end = min(batch_start + chunk, end)
            threads = [
                threading.Thread(target=create_one, args=(i,), daemon=True)
                for i in range(batch_start, batch_end)
            ]
            for t in threads: t.start()
            for t in threads: t.join()
        return count

    def _create_pods_legacy(
        self,
        namespace: str,
        start: int,
        count: int,
        node_count: int = 100,
        label: str = "bench",
        parallelism: int = 80,
        tolerations: list[dict] | None = None,
    ) -> int:
        """Legacy minimal pod creator — kept for reference."""
        if tolerations is None:
            tolerations = [
                {"key": "kwok.x-k8s.io/node", "operator": "Exists", "effect": "NoSchedule"},
                {"key": "node.kubernetes.io/not-ready", "operator": "Exists", "effect": "NoExecute"},
                {"key": "node.kubernetes.io/unreachable", "operator": "Exists", "effect": "NoExecute"},
            ]

        def create_one(i: int) -> None:
            body = json.dumps({
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": f"bench-pod-{i:09d}",
                    "namespace": namespace,
                    "labels": {"bench": label},
                },
                "spec": {
                    "nodeName": f"kwok-node-{i % node_count:06d}",
                    "tolerations": tolerations,
                    "containers": [{
                        "name": "pause",
                        "image": "registry.k8s.io/pause:3.9",
                        "resources": {"requests": {"cpu": "1m", "memory": "4Mi"}},
                    }],
                },
            }).encode()
            req = urllib.request.Request(
                f"{self.client.proxy}/api/v1/namespaces/{namespace}/pods",
                data=body,
                method="POST",
                headers={"Content-Type": "application/json"},
            )
            try:
                urllib.request.urlopen(req, timeout=10)
            except Exception:
                pass

        end = start + count
        chunk = max(1, count // parallelism)
        for batch_start in range(start, end, chunk):
            batch_end = min(batch_start + chunk, end)
            threads = [
                threading.Thread(target=create_one, args=(i,), daemon=True)
                for i in range(batch_start, batch_end)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        return count
