"""
ingestion.py — Benchmark result ingestion into metrics backends.

After a benchmark run, summary statistics should be persisted somewhere
so results are:
  - Comparable across releases (diff two JSON files or query Prometheus)
  - Visible in existing dashboards (Grafana)
  - Accessible to CI systems (fail if P99 regresses beyond a threshold)

Three backends are supported:

  Prometheus Pushgateway (recommended)
    Pushes summary stats as labeled Prometheus metrics.
    Grafana can then plot them alongside the live component metrics.
    Retention is controlled by the Prometheus/Thanos setup.

  OpenMetrics file
    Writes a text file in OpenMetrics format that any Prometheus-compatible
    scraper can read, including remote_write pipelines.

  Stdout (always on)
    Human-readable table printed at the end of every run.

Usage
-----
  from framework.ingestion import MetricsIngester

  ingester = MetricsIngester(
      pushgateway="http://pushgateway:9091",   # or None to skip
      openmetrics_path="results/metrics.prom", # or None to skip
  )
  ingester.push(results)   # call after results.finish()
"""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .log import log
from .results import BenchmarkResults


# Metric names pushed to Pushgateway / written to OpenMetrics file.
# All have the prefix `nvsentinel_benchmark_` to avoid collisions.
_METRIC_MAP = {
    # From Snapshot fields
    "heap_alloc_mb":      ("nvsentinel_benchmark_heap_alloc_megabytes",    "gauge",
                           "KOM informer cache heap_alloc at measurement point"),
    "cpu_pct":            ("nvsentinel_benchmark_cpu_percent",              "gauge",
                           "Component CPU utilisation during measurement window"),
    "reconcile_rps":      ("nvsentinel_benchmark_reconcile_rps",           "gauge",
                           "Reconcile throughput (reconciles/s)"),
    "processing_p50_ms":  ("nvsentinel_benchmark_processing_p50_ms",       "gauge",
                           "Reconcile / event handling P50 latency in milliseconds"),
    "processing_p99_ms":  ("nvsentinel_benchmark_processing_p99_ms",       "gauge",
                           "Reconcile / event handling P99 latency in milliseconds"),
    "queue_wait_p99_ms":  ("nvsentinel_benchmark_queue_wait_p99_ms",       "gauge",
                           "Workqueue wait P99 in milliseconds (true detection latency)"),
    "queue_depth":        ("nvsentinel_benchmark_queue_depth",             "gauge",
                           "Workqueue depth at measurement point"),
    "events_rps":         ("nvsentinel_benchmark_events_rps",              "gauge",
                           "Event ingestion throughput (events/s)"),
    "limit_recommendation_mi": ("nvsentinel_benchmark_memory_limit_mi",   "gauge",
                                "Recommended resources.limits.memory in MiB"),
}


def _labels(component: str, scenario: str, version: str) -> dict[str, str]:
    return {
        "component": component,
        "scenario":  scenario,
        "version":   version,
    }


def _label_str(labels: dict[str, str]) -> str:
    return ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))


def _openmetrics_block(
    metric_name: str,
    metric_type: str,
    help_text: str,
    samples: list[tuple[dict[str, str], float]],
) -> str:
    lines = [
        f"# HELP {metric_name} {help_text}",
        f"# TYPE {metric_name} {metric_type}",
    ]
    for labels, value in samples:
        lines.append(f"{metric_name}{{{_label_str(labels)}}} {value}")
    return "\n".join(lines)


@dataclass
class MetricsIngester:
    """
    Ingests benchmark results into Prometheus Pushgateway and/or an
    OpenMetrics text file after a benchmark run completes.

    Parameters
    ----------
    pushgateway : str | None
        Pushgateway URL (e.g. "http://pushgateway:9091").
        Set to None to skip Pushgateway push.
    openmetrics_path : str | None
        File path for OpenMetrics output (e.g. "results/metrics.prom").
        This file can be scraped by a Prometheus node_exporter textfile
        collector or pushed via remote_write.
        Set to None to skip file output.
    job_label : str
        Prometheus job label for Pushgateway (default: "nvsentinel_benchmark").
    extra_labels : dict
        Additional labels added to every metric (e.g. {"cluster": "aws-prod"}).
    """

    pushgateway: str | None = None
    openmetrics_path: str | None = None
    job_label: str = "nvsentinel_benchmark"
    extra_labels: dict[str, str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.extra_labels is None:
            self.extra_labels = {}

    def push(self, results: BenchmarkResults) -> None:
        """
        Ingest all runs from a completed BenchmarkResults object.
        Pushes to Pushgateway and/or writes OpenMetrics file.
        """
        samples: dict[str, list[tuple[dict, float]]] = {}

        for run in results.runs:
            report = run.report()
            base_labels = {
                **_labels(results.component, report["scenario"], results.version),
                **self.extra_labels,
            }

            for field_name, (metric_name, metric_type, help_text) in _METRIC_MAP.items():
                stat = report.get("stats", {}).get(field_name)
                if stat:
                    # Push median as the canonical value
                    if metric_name not in samples:
                        samples[metric_name] = []
                    samples[metric_name].append(
                        ({**base_labels, "stat": "median"}, stat["median"])
                    )
                    samples[metric_name].append(
                        ({**base_labels, "stat": "min"}, stat["min"])
                    )
                    samples[metric_name].append(
                        ({**base_labels, "stat": "max"}, stat["max"])
                    )

                # Also push limit recommendation directly
                if report.get("limit_recommendation_mi"):
                    metric_name = "nvsentinel_benchmark_memory_limit_mi"
                    if metric_name not in samples:
                        samples[metric_name] = []
                    samples[metric_name].append(
                        (base_labels, report["limit_recommendation_mi"])
                    )

        # Also push object size metadata (node/pod sizes from this cluster)
        if results.object_sizes:
            for key in ("node_bytes", "pod_bytes"):
                val = results.object_sizes.get(key)
                if val:
                    mname = f"nvsentinel_benchmark_{key}"
                    samples.setdefault(mname, []).append(
                        ({**self.extra_labels, "component": results.component,
                          "version": results.version}, val)
                    )

        # Push to Pushgateway
        if self.pushgateway:
            self._push_to_gateway(samples, results.component)

        # Write OpenMetrics file
        if self.openmetrics_path:
            self._write_openmetrics(samples)

        log(f"Metrics ingestion complete "
            f"({len(samples)} metric families, "
            f"{sum(len(v) for v in samples.values())} samples)")

    def _push_to_gateway(
        self,
        samples: dict[str, list[tuple[dict, float]]],
        component: str,
    ) -> None:
        """
        Push metrics to Prometheus Pushgateway using the text format.
        URL: POST {pushgateway}/metrics/job/{job}/component/{component}
        """
        lines = []
        for metric_name, metric_samples in samples.items():
            info = _METRIC_MAP.get(
                next((k for k, v in _METRIC_MAP.items() if v[0] == metric_name), ""),
                (metric_name, "gauge", ""),
            )
            lines.append(f"# TYPE {metric_name} {info[1]}")
            for labels, value in metric_samples:
                lines.append(f"{metric_name}{{{_label_str(labels)}}} {value}")
        lines.append("")  # trailing newline

        body = "\n".join(lines).encode()
        url = (
            f"{self.pushgateway.rstrip('/')}/metrics"
            f"/job/{self.job_label}"
            f"/component/{component}"
        )
        req = urllib.request.Request(url, data=body, method="POST",
                                     headers={"Content-Type": "text/plain"})
        try:
            urllib.request.urlopen(req, timeout=10)
            log(f"  Pushed {len(samples)} metric families to Pushgateway: {url}")
        except Exception as e:
            log(f"  WARNING: Pushgateway push failed: {e}")
            log(f"    (is Pushgateway running at {self.pushgateway}?)")

    def _write_openmetrics(self, samples: dict[str, list[tuple[dict, float]]]) -> None:
        """
        Write an OpenMetrics-compatible text file.
        Can be scraped by Prometheus node_exporter textfile collector:
          --collector.textfile.directory=/path/to/dir/
        """
        Path(self.openmetrics_path).parent.mkdir(parents=True, exist_ok=True)
        lines = [f"# Generated by NVSentinel benchmark framework at {time.time():.0f}"]

        for metric_name, metric_samples in samples.items():
            info = _METRIC_MAP.get(
                next((k for k, v in _METRIC_MAP.items() if v[0] == metric_name), ""),
                (metric_name, "gauge", ""),
            )
            lines.append(f"# HELP {metric_name} {info[2]}")
            lines.append(f"# TYPE {metric_name} {info[1]}")
            for labels, value in metric_samples:
                lines.append(f"{metric_name}{{{_label_str(labels)}}} {value}")

        lines.append("# EOF")
        with open(self.openmetrics_path, "w") as f:
            f.write("\n".join(lines) + "\n")
        log(f"  OpenMetrics written to {self.openmetrics_path}")
