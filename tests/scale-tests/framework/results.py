"""
results.py — Result serialization, reporting, and validity checks.

Implements the validity requirements from issues #1511–#1525:
  V1: Per-event end-to-end latency (harness-side)
  V2: Documented warmup per benchmark
  V3: N >= 3 repetitions, median + min/max spread
  V4: Component pinned to node (documented, not enforced here)
"""
from __future__ import annotations

import json
import os
import statistics
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .log import log
from .prometheus import Snapshot


@dataclass
class BenchmarkRun:
    """
    Container for a single benchmark scenario's results across repetitions.

    Stores the raw snapshots from each repetition plus computed
    median/spread statistics (V3 requirement).
    """
    scenario: str
    component: str
    warmup_s: int
    measurement_s: int
    repetitions: list[Snapshot] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def add(self, snap: Snapshot) -> None:
        self.repetitions.append(snap)

    def _values(self, key: str) -> list[float]:
        return [
            getattr(r, key)
            for r in self.repetitions
            if getattr(r, key) is not None
        ]

    def summary(self, key: str) -> dict[str, float] | None:
        vals = self._values(key)
        if not vals:
            return None
        return {
            "median": round(statistics.median(vals), 2),
            "min": round(min(vals), 2),
            "max": round(max(vals), 2),
            "n": len(vals),
        }

    def report(self) -> dict:
        """
        Build a structured report from repetitions.
        Automatically computes median/min/max for all numeric Snapshot fields.
        """
        numeric_fields = [
            "heap_alloc_mb", "heap_objects", "cpu_pct", "goroutines",
            "queue_depth", "queue_wait_p50_ms", "queue_wait_p99_ms",
            "processing_p50_ms", "processing_p99_ms", "reconcile_rps", "events_rps",
        ]
        stats = {}
        for f in numeric_fields:
            s = self.summary(f)
            if s:
                stats[f] = s

        # limit recommendation from median heap
        heap_med = stats.get("heap_alloc_mb", {}).get("median")
        limit_mi = max(int(heap_med * 2.5), 2048) if heap_med else None

        return {
            "scenario": self.scenario,
            "component": self.component,
            "warmup_s": self.warmup_s,
            "measurement_s": self.measurement_s,
            "n_repetitions": len(self.repetitions),
            "stats": stats,
            "limit_recommendation_mi": limit_mi,
            "metadata": self.metadata,
        }


@dataclass
class BenchmarkResults:
    """
    Top-level results container for a full benchmark session.
    Serializes to JSON for CI ingestion and diff between releases.
    """
    component: str
    version: str
    cluster_info: dict[str, Any] = field(default_factory=dict)
    object_sizes: dict[str, Any] = field(default_factory=dict)
    runs: list[BenchmarkRun] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None

    def add_run(self, run: BenchmarkRun) -> None:
        self.runs.append(run)

    def finish(self) -> None:
        self.finished_at = time.time()

    def as_dict(self) -> dict:
        return {
            "component": self.component,
            "version": self.version,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_s": (
                round(self.finished_at - self.started_at, 1)
                if self.finished_at else None
            ),
            "cluster_info": self.cluster_info,
            "object_sizes": self.object_sizes,
            "runs": [r.report() for r in self.runs],
        }

    def save(self, output_dir: str) -> str:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        ts = time.strftime("%Y%m%d_%H%M%S")
        path = os.path.join(output_dir, f"{self.component}_{ts}.json")
        with open(path, "w") as f:
            json.dump(self.as_dict(), f, indent=2)
        log(f"Results saved → {path}")
        return path

    def print_summary(self) -> None:
        print(f"\n{'='*65}")
        print(f"  {self.component} Benchmark Results")
        print(f"{'='*65}")
        for run in self.runs:
            r = run.report()
            print(f"\n  [{r['scenario']}]  n={r['n_repetitions']}  "
                  f"warmup={r['warmup_s']}s  measure={r['measurement_s']}s")
            stats = r.get("stats", {})
            rows = [
                ("Heap alloc",        stats.get("heap_alloc_mb"),        "MB"),
                ("CPU",               stats.get("cpu_pct"),               "%"),
                ("Reconcile tput",    stats.get("reconcile_rps"),         "rec/s"),
                ("Processing P50",    stats.get("processing_p50_ms"),     "ms"),
                ("Processing P99",    stats.get("processing_p99_ms"),     "ms"),
                ("Queue wait P99",    stats.get("queue_wait_p99_ms"),     "ms"),
                ("Queue depth",       stats.get("queue_depth"),           "items"),
            ]
            for label, s, unit in rows:
                if s:
                    spread = f"  [{s['min']}–{s['max']}]" if s['min'] != s['max'] else ""
                    print(f"    {label:<22} {s['median']:.1f} {unit}{spread}")
            if r.get("limit_recommendation_mi"):
                print(f"    {'→ Set memory limit':<22} {r['limit_recommendation_mi']} Mi")
        print()


class LatencyTracker:
    """
    Tracks per-event end-to-end latency (V1 requirement).

    Records injection timestamp per event ID, then computes latency
    when an observable effect is detected (cordon, annotation, DB record).
    """

    def __init__(self) -> None:
        self._injected: dict[str, float] = {}   # event_id → inject_time
        self._completed: dict[str, float] = {}  # event_id → complete_time

    def record_inject(self, event_id: str) -> None:
        self._injected[event_id] = time.time()

    def record_inject_batch(self, event_ids: list[str]) -> None:
        t = time.time()
        for eid in event_ids:
            self._injected[eid] = t

    def record_complete(self, event_id: str) -> None:
        if event_id in self._injected:
            self._completed[event_id] = time.time()

    def latencies_ms(self) -> list[float]:
        return [
            (self._completed[eid] - self._injected[eid]) * 1000
            for eid in self._completed
            if eid in self._injected
        ]

    def summary(self) -> dict[str, float]:
        lats = sorted(self.latencies_ms())
        if not lats:
            return {}
        n = len(lats)
        return {
            "p50_ms": round(lats[int(n * 0.50)], 1),
            "p90_ms": round(lats[int(n * 0.90)], 1),
            "p99_ms": round(lats[int(n * 0.99)], 1),
            "min_ms": round(lats[0], 1),
            "max_ms": round(lats[-1], 1),
            "n": n,
        }
