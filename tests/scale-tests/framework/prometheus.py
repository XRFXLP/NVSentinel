"""
prometheus.py — Prometheus query helpers.

Provides typed metric queries for every NVSentinel component,
plus snapshot() and wait_drain() utilities used by all benchmarks.
"""
from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Snapshot:
    """Point-in-time metric sample for a single benchmark measurement."""
    label: str
    timestamp: float = field(default_factory=time.time)

    # Universal
    heap_alloc_mb: float | None = None
    heap_objects: int | None = None
    cpu_pct: float | None = None
    goroutines: int | None = None

    # Queue
    queue_depth: int | None = None
    queue_wait_p50_ms: float | None = None
    queue_wait_p99_ms: float | None = None
    processing_p50_ms: float | None = None
    processing_p99_ms: float | None = None

    # Throughput
    reconcile_rps: float | None = None
    events_rps: float | None = None

    # Component-specific extras (populated by component benchmarks)
    extras: dict[str, Any] = field(default_factory=dict)

    @property
    def limit_recommendation_mi(self) -> int | None:
        """Recommended resources.limits.memory in MiB based on current heap."""
        if self.heap_alloc_mb is None:
            return None
        return max(int(self.heap_alloc_mb * 2.5), 2048)

    def as_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v is not None}


class PrometheusClient:
    """Thin Prometheus HTTP API client."""

    def __init__(self, url: str = "http://localhost:9090"):
        self.url = url.rstrip("/")

    def query(self, q: str) -> float | None:
        """Instant query. Returns the first result's value as float, or None."""
        try:
            encoded = urllib.parse.quote(q)
            resp = urllib.request.urlopen(
                f"{self.url}/api/v1/query?query={encoded}", timeout=5
            )
            data = json.loads(resp.read())["data"]["result"]
            return float(data[0]["value"][1]) if data else None
        except Exception:
            return None

    def query_range(
        self, q: str, start: float, end: float, step: int = 15
    ) -> list[tuple[float, float]]:
        """Range query. Returns list of (timestamp, value) pairs."""
        try:
            encoded = urllib.parse.quote(q)
            url = (
                f"{self.url}/api/v1/query_range"
                f"?query={encoded}&start={start:.0f}&end={end:.0f}&step={step}"
            )
            resp = urllib.request.urlopen(url, timeout=10)
            data = json.loads(resp.read())["data"]["result"]
            if not data:
                return []
            return [(float(ts), float(v)) for ts, v in data[0]["values"]]
        except Exception:
            return []

    def healthy(self) -> bool:
        try:
            urllib.request.urlopen(f"{self.url}/-/healthy", timeout=3)
            return True
        except Exception:
            return False


class NVSentinelMetrics:
    """
    Typed metric accessors for all NVSentinel components.

    Each method queries Prometheus and returns a typed value.
    All methods accept a `window` parameter (default "1m") for rate/histogram queries.
    """

    def __init__(self, prom: PrometheusClient, job: str = "kubernetes-object-monitor"):
        self.prom = prom
        self._job = job

    def _q(self, query: str) -> float | None:
        return self.prom.query(query)

    def _j(self, suffix: str) -> str:
        """Build a label selector for this job."""
        return f'job="{self._job}"'

    # ── Universal Go runtime ──────────────────────────────────────────────────

    def heap_alloc_mb(self) -> float | None:
        v = self._q(f'go_memstats_heap_alloc_bytes{{{self._j("")}}}')
        return round(v / 1024 / 1024, 1) if v is not None else None

    def heap_objects(self) -> int | None:
        v = self._q(f'go_memstats_heap_objects{{{self._j("")}}}')
        return int(v) if v is not None else None

    def heap_sys_mb(self) -> float | None:
        """Total Go arena reserved from OS — closer to container working set."""
        v = self._q(f'go_memstats_sys_bytes{{{self._j("")}}}')
        return round(v / 1024 / 1024, 1) if v is not None else None

    def goroutines(self) -> int | None:
        v = self._q(f'go_goroutines{{{self._j("")}}}')
        return int(v) if v is not None else None

    def cpu_pct(self, window: str = "1m") -> float | None:
        v = self._q(
            f'rate(process_cpu_seconds_total{{{self._j("")}}}[{window}])'
        )
        return round(v * 100, 1) if v is not None else None

    # ── controller-runtime workqueue ──────────────────────────────────────────

    def queue_depth(self, name: str = "") -> float | None:
        sel = f'{self._j("")},name="{name}"' if name else self._j("")
        return self._q(f'sum(workqueue_depth{{{sel}}})')

    def queue_wait_p50_ms(self, name: str = "", window: str = "1m") -> float | None:
        sel = f'{self._j("")},name="{name}"' if name else self._j("")
        v = self._q(
            f'histogram_quantile(0.50,'
            f'sum(rate(workqueue_queue_duration_seconds_bucket{{{sel}}}[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def queue_wait_p99_ms(self, name: str = "", window: str = "1m") -> float | None:
        sel = f'{self._j("")},name="{name}"' if name else self._j("")
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(workqueue_queue_duration_seconds_bucket{{{sel}}}[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def processing_p50_ms(self, controller: str = "", window: str = "1m") -> float | None:
        sel = (
            f'{self._j("")},controller="{controller}"'
            if controller else self._j("")
        )
        v = self._q(
            f'histogram_quantile(0.50,'
            f'sum(rate(controller_runtime_reconcile_time_seconds_bucket{{{sel}}}[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def processing_p99_ms(self, controller: str = "", window: str = "1m") -> float | None:
        sel = (
            f'{self._j("")},controller="{controller}"'
            if controller else self._j("")
        )
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(controller_runtime_reconcile_time_seconds_bucket{{{sel}}}[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def reconcile_rps(self, controller: str = "", window: str = "1m") -> float | None:
        sel = (
            f'{self._j("")},controller="{controller}"'
            if controller else self._j("")
        )
        v = self._q(
            f'sum(rate(controller_runtime_reconcile_total{{{sel}}}[{window}]))'
        )
        return round(v, 1) if v is not None else None

    # ── KOM-specific ──────────────────────────────────────────────────────────

    def kom_policy_matches_rps(self, window: str = "1m") -> float | None:
        v = self._q(
            f'sum(rate(k8s_object_monitor_policy_matches_total{{{self._j("")}}}[{window}]))'
        )
        return round(v, 2) if v is not None else None

    def kom_eval_errors_rps(self, window: str = "1m") -> float | None:
        v = self._q(
            f'sum(rate(k8s_object_monitor_policy_evaluation_errors_total'
            f'{{{self._j("")}}}[{window}]))'
        )
        return round(v, 2) if v is not None else None

    # ── Fault-quarantine ──────────────────────────────────────────────────────

    def fq_events_rps(self, window: str = "1m") -> float | None:
        v = self._q(
            f'sum(rate(fault_quarantine_events_received_total[{window}]))'
        )
        return round(v, 1) if v is not None else None

    def fq_handling_p50_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.50,'
            f'sum(rate(fault_quarantine_event_handling_duration_seconds_bucket[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def fq_handling_p99_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(fault_quarantine_event_handling_duration_seconds_bucket[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def fq_cordon_p50_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.50,'
            f'sum(rate(fault_quarantine_node_quarantine_duration_seconds_bucket[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def fq_cordon_p99_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(fault_quarantine_node_quarantine_duration_seconds_bucket[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def fq_backlog(self) -> float | None:
        return self._q('fault_quarantine_event_backlog_count')

    def fq_cordons_rps(self, window: str = "1m") -> float | None:
        v = self._q(
            f'sum(rate(fault_quarantine_cordons_applied_total[{window}]))'
        )
        return round(v, 2) if v is not None else None

    def fq_breaker_utilization(self) -> float | None:
        return self._q('fault_quarantine_breaker_utilization')

    def fq_ruleset_eval_rps(self, window: str = "1m") -> float | None:
        v = self._q(
            f'sum(rate(fault_quarantine_ruleset_evaluations_total[{window}]))'
        )
        return round(v, 1) if v is not None else None

    # ── Platform-connector ────────────────────────────────────────────────────

    def pc_events_received_rps(self, window: str = "1m") -> float | None:
        v = self._q(
            f'sum(rate(platform_connector_health_events_received_total[{window}]))'
        )
        return round(v, 1) if v is not None else None

    def pc_k8s_update_p99_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(k8s_platform_connector_node_condition_update_duration_milliseconds_bucket'
            f'[{window}]))by(le))'
        )
        return round(v, 1) if v is not None else None

    def pc_queue_depth(self, queue: str = "databaseStore") -> float | None:
        return self._q(f'platform_connector_workqueue_depth_{queue}')

    # ── Node-drainer ──────────────────────────────────────────────────────────

    def nd_handling_p99_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(node_drainer_event_handling_duration_seconds_bucket[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def nd_eviction_p99_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(node_drainer_pod_eviction_duration_seconds_bucket[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def nd_queue_depth(self) -> float | None:
        return self._q('node_drainer_queue_depth')

    # ── Health-events-analyzer ────────────────────────────────────────────────

    def ha_handling_p99_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(health_event_analyzer_event_handling_duration_seconds_bucket'
            f'[{window}]))by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def ha_events_rps(self, window: str = "1m") -> float | None:
        v = self._q(
            f'sum(rate(health_event_analyzer_events_received_total[{window}]))'
        )
        return round(v, 1) if v is not None else None

    # ── Event-exporter ────────────────────────────────────────────────────────

    def ee_publish_p99_ms(self, window: str = "1m") -> float | None:
        v = self._q(
            f'histogram_quantile(0.99,'
            f'sum(rate(health_events_exporter_publish_duration_seconds_bucket[{window}]))'
            f'by(le))'
        )
        return round(v * 1000, 1) if v is not None else None

    def ee_backlog(self) -> float | None:
        return self._q('health_events_exporter_event_backlog_size')


def wait_queue_drain(
    prom: PrometheusClient,
    job: str,
    controller: str = "",
    timeout_s: int = 300,
    threshold: float = 10.0,
) -> float:
    """
    Block until workqueue_depth drops below threshold.
    Returns the final depth. Does not raise on timeout — caller decides.
    """
    sel = f'job="{job}",name="{controller}"' if controller else f'job="{job}"'
    query = f'sum(workqueue_depth{{{sel}}})'
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        d = prom.query(query)
        if d is not None and d < threshold:
            return d
        time.sleep(5)
    return prom.query(query) or 0.0


def take_snapshot(
    metrics: NVSentinelMetrics,
    label: str,
    controller: str = "",
    queue_name: str = "",
    window: str = "1m",
    wait_drain: bool = True,
    drain_timeout_s: int = 300,
) -> Snapshot:
    """
    Optionally wait for queue to drain, then capture a full metric snapshot.
    Returns a Snapshot dataclass ready for serialization.
    """
    if wait_drain:
        wait_queue_drain(
            metrics.prom,
            job=metrics._job,
            controller=controller or queue_name,
            timeout_s=drain_timeout_s,
        )

    snap = Snapshot(label=label)
    snap.heap_alloc_mb       = metrics.heap_alloc_mb()
    snap.heap_objects        = metrics.heap_objects()
    snap.cpu_pct             = metrics.cpu_pct(window)
    snap.goroutines          = metrics.goroutines()
    snap.queue_depth         = int(metrics.queue_depth(queue_name) or 0)
    snap.queue_wait_p50_ms   = metrics.queue_wait_p50_ms(queue_name, window)
    snap.queue_wait_p99_ms   = metrics.queue_wait_p99_ms(queue_name, window)
    snap.processing_p50_ms   = metrics.processing_p50_ms(controller, window)
    snap.processing_p99_ms   = metrics.processing_p99_ms(controller, window)
    snap.reconcile_rps       = metrics.reconcile_rps(controller, window)
    return snap
