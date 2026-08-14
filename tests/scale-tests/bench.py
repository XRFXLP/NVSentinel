#!/usr/bin/env python3
"""
NVSentinel Microbenchmark Runner
=================================

Single entry point for all NVSentinel component microbenchmarks.
Reproducible on any Kubernetes cluster by any contributor.

Quick start
-----------
  # 1. Prerequisites
  kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/latest/download/kwok.yaml
  kubectl proxy --port=8001 &
  kubectl port-forward -n prometheus <prometheus-pod> 9090:9090 &

  # 2. Run KOM benchmark
  python3 bench.py kom --output results/

  # 3. Run fault-quarantine benchmark
  python3 bench.py fq --output results/ --socket /var/run/nvsentinel/nvsentinel.sock

  # 4. Run all benchmarks (Epic #1511)
  python3 bench.py all --output results/

Validity requirements implemented
-----------------------------------
  V1  Per-event end-to-end latency tracked by event ID (--track-latency)
  V2  Warmup duration documented per scenario (--warmup)
  V3  N >= 3 repetitions with median/min/max (--repetitions)
  V4  Component pinning documented (--pin-node)

See --help for all options.
"""

from __future__ import annotations

import argparse
import sys
import textwrap
import time
from pathlib import Path

# Add framework to path
sys.path.insert(0, str(Path(__file__).parent))

from framework.cluster import ClusterClient, ClusterSetup
from framework.ingestion import MetricsIngester
from framework.log import log
from framework.prometheus import NVSentinelMetrics, PrometheusClient, take_snapshot
from framework.results import BenchmarkResults, BenchmarkRun


# ── Argument parsing ──────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="bench.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            NVSentinel Microbenchmark Runner
            Reproduces benchmarks from GitHub issues #1512–#1525.
        """),
    )

    # ── Connection ────────────────────────────────────────────────────────────
    conn = p.add_argument_group("connection")
    conn.add_argument(
        "--proxy", default="http://localhost:8001", metavar="URL",
        help="kubectl proxy URL (default: http://localhost:8001). "
             "Start with: kubectl proxy --port=8001",
    )
    conn.add_argument(
        "--prometheus", default="http://localhost:9090", metavar="URL",
        help="Prometheus URL (default: http://localhost:9090). "
             "Start with: kubectl port-forward -n prometheus <pod> 9090:9090",
    )
    conn.add_argument(
        "--socket", default="/var/run/nvsentinel/nvsentinel.sock", metavar="PATH",
        help="Platform-connector gRPC Unix socket (default: /var/run/nvsentinel/nvsentinel.sock). "
             "Required for end-to-end event injection benchmarks (FQ, ND, FR).",
    )
    conn.add_argument(
        "--namespace", default="benchmark", metavar="NS",
        help="Kubernetes namespace for benchmark pods (default: benchmark)",
    )

    # ── Cluster sampling ──────────────────────────────────────────────────────
    sampling = p.add_argument_group("cluster sampling")
    sampling.add_argument(
        "--pod-namespace", default="gpu-operator", metavar="NS",
        help="Namespace to sample representative pod size from "
             "(default: gpu-operator). Falls back to kube-system.",
    )
    sampling.add_argument(
        "--size-tolerance", type=float, default=15.0, metavar="PCT",
        help="Max allowed %% size difference between real node and KWOK node "
             "before aborting (default: 15.0).",
    )

    # ── Measurement ──────────────────────────────────────────────────────────
    meas = p.add_argument_group("measurement (validity requirements V2, V3)")
    meas.add_argument(
        "--warmup", type=int, default=60, metavar="SECONDS",
        help="Warmup duration per scenario in seconds (V2, default: 60). "
             "Metrics collected during warmup are discarded.",
    )
    meas.add_argument(
        "--duration", type=int, default=300, metavar="SECONDS",
        help="Measurement window per scenario in seconds (default: 300).",
    )
    meas.add_argument(
        "--repetitions", type=int, default=3, metavar="N",
        help="Number of repetitions per scenario (V3, default: 3). "
             "Results report median and min/max spread.",
    )
    meas.add_argument(
        "--snapshot-interval", type=int, default=15, metavar="SECONDS",
        help="Prometheus scrape interval for metric samples (default: 15).",
    )

    # ── Output & ingestion ────────────────────────────────────────────────────
    out = p.add_argument_group("output and metrics ingestion")
    out.add_argument(
        "--output", default="results", metavar="DIR",
        help="Directory for JSON result files (default: results/). Created if absent.",
    )
    out.add_argument(
        "--version", default="unknown", metavar="VER",
        help="NVSentinel version string to embed in results (e.g. v1.15.0).",
    )
    out.add_argument(
        "--pushgateway", default="", metavar="URL",
        help="Prometheus Pushgateway URL for ingesting benchmark summary metrics "
             "(e.g. http://pushgateway:9091). "
             "Metrics are pushed after each scenario with labels: "
             "component, scenario, version, stat (median/min/max). "
             "Leave empty to skip (default).",
    )
    out.add_argument(
        "--openmetrics", default="", metavar="FILE",
        help="Write benchmark results in OpenMetrics text format to FILE "
             "(e.g. results/metrics.prom). "
             "This file can be scraped by Prometheus node_exporter textfile "
             "collector or shipped via remote_write. Leave empty to skip (default).",
    )
    out.add_argument(
        "--label", action="append", default=[], metavar="KEY=VALUE",
        help="Extra label to attach to all pushed/written metrics. "
             "Can be repeated. Example: --label cluster=aws-prod --label env=staging",
    )

    # ── Safety ───────────────────────────────────────────────────────────────
    safety = p.add_argument_group("safety")
    safety.add_argument(
        "--dry-run", action="store_true",
        help="Validate prerequisites and print scenario plan, then exit.",
    )
    safety.add_argument(
        "--skip-verify", action="store_true",
        help="Skip KWOK node size verification step. "
             "NOT RECOMMENDED — size mismatch makes memory results wrong.",
    )
    safety.add_argument(
        "--no-cleanup", action="store_true",
        help="Leave KWOK nodes and benchmark pods in place after run "
             "(useful for debugging). Default: clean up after each scenario.",
    )
    safety.add_argument(
        "--pin-node", default="", metavar="NODE",
        help="Node name to pin the component under test to (V4 requirement). "
             "Patches the deployment's nodeSelector before benchmarking. "
             "Example: --pin-node gke-benchmark-pool-001",
    )

    # ── Subcommands (one per issue) ───────────────────────────────────────────
    sub = p.add_subparsers(dest="command", required=True, title="benchmarks")

    # kom — Issue #1512
    kom = sub.add_parser("kom", help="Kubernetes Object Monitor (issue #1512)")
    kom.add_argument(
        "--skip", default="", metavar="STEPS",
        help="Comma-separated steps to skip: memory,churn,restart,policy,resync",
    )

    # Memory sweep counts
    mem = kom.add_argument_group("memory sweep counts")
    mem.add_argument(
        "--node-counts",
        default="100,1000,5000,10000,25000,50000,75000,100000",
        metavar="N1,N2,...",
        help="Exact node counts for the node memory sweep "
             "(default: 100,1000,5000,10000,25000,50000,75000,100000). "
             "Each value creates cumulative KWOK nodes and snapshots KOM heap. "
             "Example for a small cluster: --node-counts 100,500,1000,5000",
    )
    mem.add_argument(
        "--pod-counts",
        default="0,1000,10000,50000,100000",
        metavar="N1,N2,...",
        help="Exact pod counts for the pod memory sweep "
             "(default: 0,1000,10000,50000,100000). "
             "Example: --pod-counts 0,1000,5000,10000",
    )
    mem.add_argument(
        "--node-parallelism", type=int, default=80, metavar="N",
        help="Number of parallel threads for KWOK node creation (default: 80). "
             "Reduce if the API server returns 429s.",
    )
    mem.add_argument(
        "--pod-parallelism", type=int, default=80, metavar="N",
        help="Number of parallel threads for pod creation (default: 80).",
    )

    # Churn sweep
    churn = kom.add_argument_group("churn sweep")
    churn.add_argument(
        "--churn-rates", default="10,50,200", metavar="R1,R2,...",
        help="Comma-separated pods/s churn rates (default: 10,50,200). "
             "Example: --churn-rates 10,50,100,200,500",
    )
    churn.add_argument(
        "--churn-node-count", type=int, default=100, metavar="N",
        help="Number of KWOK nodes to spread churn pods across (default: 100).",
    )

    # Restart storm (K12)
    restart = kom.add_argument_group("restart storm")
    restart.add_argument(
        "--restart-node-counts", default="100,10000", metavar="N1,N2,...",
        help="Node counts at which to measure restart/startup time (K12 bottleneck). "
             "Default: 100,10000. Each triggers a KOM rollout restart.",
    )

    # fq — Issue #1518
    fq = sub.add_parser("fq", help="Fault Quarantine Module (issue #1518)")
    fq.add_argument(
        "--skip", default="", metavar="STEPS",
        help="Comma-separated steps to skip: "
             "throughput,ruleset,cordon,informer,concurrency,circuit_breaker "
             "(default: run all).",
    )
    fq.add_argument(
        "--event-rates", default="10,50,200,500", metavar="RATES",
        help="Comma-separated health event rates in events/s (default: 10,50,200,500).",
    )
    fq.add_argument(
        "--ruleset-counts", default="6,25,100", metavar="COUNTS",
        help="Comma-separated ruleset counts to sweep (default: 6,25,100).",
    )
    fq.add_argument(
        "--cordon-node-counts", default="10,100,500", metavar="COUNTS",
        help="Comma-separated node counts for cordon throughput sweep "
             "(default: 10,100,500).",
    )
    fq.add_argument(
        "--processing-strategy", default="STORE_ONLY",
        choices=["STORE_ONLY", "EXECUTE_REMEDIATION", "STORE_AND_ANALYSE"],
        help="Processing strategy for injected events (default: STORE_ONLY). "
             "Use EXECUTE_REMEDIATION for end-to-end cordon benchmarks — "
             "this will cordon real nodes if using real node names.",
    )

    # nd — Issue #1517
    nd = sub.add_parser("nd", help="Node Drainer (issue #1517)")
    nd.add_argument(
        "--pod-counts", default="100,1000,10000", metavar="COUNTS",
        help="Comma-separated pod counts per node for drain sweep (default: 100,1000,10000).",
    )
    nd.add_argument(
        "--drain-node-counts", default="10,100,500", metavar="COUNTS",
        help="Comma-separated number of nodes to drain simultaneously (default: 10,100,500).",
    )

    # fr — Issue #1519
    sub.add_parser("fr", help="Fault Remediation Module (issue #1519)")

    # ha — Issue #1523
    ha = sub.add_parser("ha", help="Health Events Analyzer (issue #1523)")
    ha.add_argument(
        "--scenario", default="throughput",
        choices=["throughput", "rule-sweep", "historical", "index",
                 "window-sweep", "distribution", "coload"],
        help="Which HEA scenario to run (default: throughput).",
    )
    ha.add_argument(
        "--event-rates", default="30,150,500", metavar="RATES",
        help="Comma-separated event rates in events/s for throughput scenario "
             "(default: 30,150,500).",
    )
    ha.add_argument(
        "--rule-counts", default="1,5,22", metavar="COUNTS",
        help="Comma-separated enabled rule counts for rule-sweep scenario (default: 1,5,22).",
    )
    ha.add_argument(
        "--historical-sizes", default="100000,1000000", metavar="SIZES",
        help="Comma-separated collection sizes for historical scenario "
             "(default: 100000,1000000).",
    )
    ha.add_argument(
        "--windows", default="300,1000,3600,86400,604800,2592000", metavar="SECONDS",
        help="Comma-separated time windows in seconds for window-sweep scenario "
             "(default: 300,1000,3600,86400,604800,2592000 = 5min/16min/1h/24h/7d/30d).",
    )
    ha.add_argument(
        "--seed-docs", type=int, default=1000000, metavar="N",
        help="Documents to pre-seed for window-sweep and distribution scenarios "
             "(default: 1000000).",
    )
    ha.add_argument(
        "--metrics-url", default="http://localhost:2113/metrics", metavar="URL",
        help="HEA Prometheus metrics endpoint "
             "(default: http://localhost:2113/metrics). "
             "Start with: kubectl port-forward -n nvsentinel "
             "deployment/health-events-analyzer 2113:2112",
    )
    ha.add_argument(
        "--mongo-port", type=int, default=27018, metavar="PORT",
        help="Local port for MongoDB primary port-forward (default: 27018). "
             "Start with: kubectl port-forward -n nvsentinel "
             "pod/mongodb-rs0-0 27018:27017",
    )
    ha.add_argument(
        "--measure-s", type=int, default=60, metavar="S",
        help="Measurement window per data point in seconds (default: 60).",
    )
    ha.add_argument(
        "--warmup-s", type=int, default=10, metavar="S",
        help="Warmup duration in seconds before each throughput measurement (default: 10).",
    )

    # ee — Issue #1514
    sub.add_parser("ee", help="Event Exporter (issue #1514)")

    # janitor — Issue #1524
    janitor = sub.add_parser("janitor", help="Janitor (issue #1524)")
    janitor.add_argument(
        "--retained-counts", default="0,100,500,1000,5000,10000", metavar="N1,N2,...",
        help="Comma-separated completed CR counts to sweep for admission webhook benchmark "
             "(default: 0,100,500,1000,5000,10000).",
    )
    janitor.add_argument(
        "--probes-per-point", type=int, default=15, metavar="N",
        help="Number of admission probes per retained_count point (default: 15).",
    )
    janitor.add_argument(
        "--scenario", default="admission", choices=["admission", "concurrent-reconcile", "ttl-cleanup", "concurrent-maintenance", "lock-contention", "reboot-workflow", "gpureset-workflow", "cold-recovery"],
        help="Which scenario to run (default: admission).",
    )
    janitor.add_argument(
        "--concurrency-levels", default="1,10,50,100", metavar="N1,N2,...",
        help="Concurrent active CR counts for concurrent-reconcile scenario (default: 1,10,50,100).",
    )
    janitor.add_argument(
        "--measurement-s", type=int, default=60, metavar="S",
        help="Measurement window in seconds per concurrency level (default: 60).",
    )
    janitor.add_argument(
        "--metrics-url", default="http://localhost:2112/metrics", metavar="URL",
        help="Janitor Prometheus metrics endpoint (default: http://localhost:2112/metrics).",
    )

    # pc — Issue #1525
    pc = sub.add_parser("pc", help="Platform Connector Kubernetes API load (issue #1525)")
    pc.add_argument(
        "--event-rates", default="10,50,200", metavar="RATES",
        help="Health event injection rates in events/s (default: 10,50,200).",
    )

    # mongodb-bitnami — Issue #1515
    mdb_b = sub.add_parser("mongodb-bitnami", help="Bitnami MongoDB datastore (issue #1515)")
    mdb_b.add_argument(
        "--event-rates", default="10,50,200,500", metavar="RATES",
        help="Insert rates in events/s (default: 10,50,200,500).",
    )

    # mongodb-percona — Issue #1516
    mdb_p = sub.add_parser("mongodb-percona", help="Percona MongoDB datastore (issue #1516)")
    mdb_p.add_argument(
        "--event-rates", default="10,50,200,500", metavar="RATES",
        help="Insert rates in events/s (default: 10,50,200,500).",
    )

    # preflight — Issue #1522
    pf = sub.add_parser("preflight", help="Preflight (issue #1522)")
    pf.add_argument("--scenario", default="admission",
        choices=["admission", "gang", "pod-size", "check-count", "restart",
                 "pod-population"],
        help="Which scenario to run (default: admission).")

    # all — Epic #1511
    all_cmd = sub.add_parser("all", help="Run all benchmarks in sequence (Epic #1511)")
    all_cmd.add_argument(
        "--skip-components", default="", metavar="NAMES",
        help="Comma-separated component names to skip "
             "(e.g. --skip-components preflight,janitor).",
    )

    return p


# ── Prerequisite checks ───────────────────────────────────────────────────────

def check_prerequisites(args: argparse.Namespace) -> tuple[ClusterClient, PrometheusClient]:
    log("Checking prerequisites...")

    client = ClusterClient(proxy=args.proxy)
    prom = PrometheusClient(url=args.prometheus)

    # kubectl proxy
    try:
        client.get("/api/v1", timeout=5)
        log(f"  ✅ kubectl proxy reachable at {args.proxy}")
    except Exception as e:
        log(f"  ❌ kubectl proxy not reachable at {args.proxy}: {e}")
        log("     Start with: kubectl proxy --port=8001 &")
        sys.exit(1)

    # Prometheus (optional — benchmarks degrade gracefully without it)
    if not prom.healthy():
        log(f"  ⚠️  Prometheus not reachable at {args.prometheus} — skipping server-side metrics")
        log("     Start with: kubectl port-forward -n prometheus <pod> 9090:9090 &")
    else:
        log(f"  ✅ Prometheus reachable at {args.prometheus}")

    # KWOK
    setup = ClusterSetup(client)
    try:
        setup.verify_kwok()
        log("  ✅ KWOK controller running")
    except RuntimeError as e:
        log(f"  ❌ {e}")
        sys.exit(1)

    return client, prom


# ── KOM benchmark (Issue #1512) ───────────────────────────────────────────────

def _make_ingester(args: argparse.Namespace) -> MetricsIngester:
    extra = {}
    for kv in args.label:
        if "=" in kv:
            k, v = kv.split("=", 1)
            extra[k.strip()] = v.strip()
    return MetricsIngester(
        pushgateway=args.pushgateway or None,
        openmetrics_path=args.openmetrics or None,
        extra_labels=extra,
    )


def run_kom(args: argparse.Namespace, client: ClusterClient, prom: PrometheusClient) -> None:
    from benchmarks.kom import KOMBenchmark
    bench = KOMBenchmark(
        client=client,
        prom=prom,
        namespace=args.namespace,
        pod_sample_namespace=args.pod_namespace,
        size_tolerance_pct=args.size_tolerance,
        warmup_s=args.warmup,
        duration_s=args.duration,
        repetitions=args.repetitions,
        snapshot_interval_s=args.snapshot_interval,
        skip=set(s.strip() for s in args.skip.split(",") if s.strip()),
        node_counts=[int(n) for n in args.node_counts.split(",") if n.strip()],
        pod_counts=[int(n) for n in args.pod_counts.split(",") if n.strip()],
        node_parallelism=args.node_parallelism,
        pod_parallelism=args.pod_parallelism,
        churn_rates=[int(r) for r in args.churn_rates.split(",") if r.strip()],
        churn_node_count=args.churn_node_count,
        restart_node_counts=[int(n) for n in args.restart_node_counts.split(",") if n.strip()],
        skip_verify=args.skip_verify,
        no_cleanup=args.no_cleanup,
        dry_run=args.dry_run,
        version=args.version,
        output=args.output,
        ingester=_make_ingester(args),
    )
    bench.run()


# ── FQ benchmark (Issue #1518) ────────────────────────────────────────────────

def run_fq(args: argparse.Namespace, client: ClusterClient, prom: PrometheusClient) -> None:
    from benchmarks.fault_quarantine import FQBenchmark
    bench = FQBenchmark(
        client=client,
        prom=prom,
        socket_path=args.socket,
        namespace=args.namespace,
        pod_sample_namespace=args.pod_namespace,
        size_tolerance_pct=args.size_tolerance,
        warmup_s=args.warmup,
        duration_s=args.duration,
        repetitions=args.repetitions,
        snapshot_interval_s=args.snapshot_interval,
        skip=set(s.strip() for s in args.skip.split(",") if s.strip()),
        event_rates=[int(r) for r in args.event_rates.split(",")],
        ruleset_counts=[int(r) for r in args.ruleset_counts.split(",")],
        cordon_node_counts=[int(r) for r in args.cordon_node_counts.split(",")],
        processing_strategy=args.processing_strategy,
        skip_verify=args.skip_verify,
        no_cleanup=args.no_cleanup,
        dry_run=args.dry_run,
        version=args.version,
        output=args.output,
    )
    bench.run()


def run_janitor(
    args: argparse.Namespace,
    client: ClusterClient,
    prom: PrometheusClient,
) -> None:
    from benchmarks.janitor import JanitorBenchmark
    bench = JanitorBenchmark(
        client=client,
        prom=prom,
        retained_counts=[int(n) for n in args.retained_counts.split(",")],
        probes_per_point=args.probes_per_point,
        version=args.version,
        output=args.output,
    )
    scenario = getattr(args, "scenario", "admission")
    if scenario == "concurrent-reconcile":
        bench.run_concurrent_reconcile(
            concurrency_levels=[int(n) for n in args.concurrency_levels.split(",")],
            measurement_s=args.measurement_s,
            metrics_url=args.metrics_url,
        )
    elif scenario == "ttl-cleanup":
        bench.run_ttl_cleanup(
            cr_counts=[int(n) for n in args.retained_counts.split(",")],
            metrics_url=args.metrics_url,
        )
    elif scenario == "concurrent-maintenance":
        bench.run_concurrent_maintenance(
            concurrency_levels=[int(n) for n in args.concurrency_levels.split(",")],
            metrics_url=args.metrics_url,
        )
    elif scenario == "lock-contention":
        bench.run_lock_contention(
            sweep=[int(n) for n in args.concurrency_levels.split(",")],
            metrics_url=args.metrics_url,
        )
    elif scenario == "reboot-workflow":
        bench.run_reboot_workflow(
            repetitions=getattr(args, "probes_per_point", 5),
            metrics_url=args.metrics_url,
        )
    elif scenario == "gpureset-workflow":
        bench.run_gpureset_workflow(
            concurrency_levels=[int(n) for n in args.concurrency_levels.split(",")],
            metrics_url=args.metrics_url,
        )
    elif scenario == "cold-recovery":
        bench.run_cold_recovery(
            completed_sweep=[int(n) for n in args.retained_counts.split(",")],
            n_active=[int(n) for n in args.concurrency_levels.split(",")][0],
            metrics_url=args.metrics_url,
        )
    else:
        bench.run_admission_webhook()


def run_preflight(
    args: argparse.Namespace,
    client: ClusterClient,
    prom: PrometheusClient,
) -> None:
    from benchmarks.preflight import PreflightBenchmark
    bench = PreflightBenchmark(
        client=client,
        prom=prom,
        version=args.version,
        output=args.output,
    )
    scenario = getattr(args, "scenario", "admission")
    if scenario == "gang":
        bench.run_gang_coordination()
    elif scenario == "pod-population":
        bench.run_cluster_pod_population(
            metrics_url=getattr(args, "metrics_url",
                                "http://localhost:8080/metrics"))
    elif scenario == "pod-size":
        bench.run_pod_spec_size()
    elif scenario == "check-count":
        bench.run_check_count()
    elif scenario == "restart":
        bench.run_restart()
    else:
        bench.run_admission_throughput()


# ── HEA benchmark (Issue #1523) ───────────────────────────────────────────────

def run_ha(
    args: argparse.Namespace,
    client: ClusterClient,
    prom: PrometheusClient,
) -> None:
    from benchmarks.hea import HEABenchmark
    bench = HEABenchmark(
        client=client,
        prom=prom,
        metrics_url=args.metrics_url,
        mongo_port=args.mongo_port,
        version=args.version,
        output=args.output,
    )
    scenario = getattr(args, "scenario", "throughput")
    if scenario == "rule-sweep":
        bench.run_rule_sweep(
            rule_counts=[int(n) for n in args.rule_counts.split(",") if n.strip()],
            measure_s=args.measure_s,
        )
    elif scenario == "historical":
        bench.run_historical(
            sizes=[int(n) for n in args.historical_sizes.split(",") if n.strip()],
            measure_s=args.measure_s,
        )
    elif scenario == "index":
        bench.run_index_comparison(measure_s=args.measure_s)
    elif scenario == "window-sweep":
        bench.run_window_sweep(
            windows_s=[int(w) for w in args.windows.split(",") if w.strip()],
            seed_docs=args.seed_docs,
        )
    elif scenario == "distribution":
        bench.run_event_distribution(seed_docs=args.seed_docs)
    elif scenario == "coload":
        bench.run_coload(
            inject_rate=float(args.event_rates.split(",")[0]),
            n_events=6000,
            seed_docs=args.seed_docs,
        )
    else:
        bench.run_throughput(
            rates=[int(r) for r in args.event_rates.split(",") if r.strip()],
            warmup_s=args.warmup_s,
            measure_s=args.measure_s,
        )


# ── Dispatch ──────────────────────────────────────────────────────────────────

RUNNERS = {
    "kom": run_kom,
    "fq":  run_fq,
    # Remaining components follow the same pattern — stubs for now
    "nd":               lambda a, c, p: _not_implemented("nd", "#1517"),
    "fr":               lambda a, c, p: _not_implemented("fr", "#1519"),
    "ha":               run_ha,
    "ee":               lambda a, c, p: _not_implemented("ee", "#1514"),
    "janitor":          run_janitor,
    "pc":               lambda a, c, p: _not_implemented("pc", "#1525"),
    "mongodb-bitnami":  lambda a, c, p: _not_implemented("mongodb-bitnami", "#1515"),
    "mongodb-percona":  lambda a, c, p: _not_implemented("mongodb-percona", "#1516"),
    "preflight":        run_preflight,
}


def _not_implemented(name: str, issue: str) -> None:
    log(f"  {name} benchmark not yet implemented (see GitHub issue {issue})")
    log(f"  Contributions welcome — see tests/scale-tests/benchmarks/README.md")


def run_all(args: argparse.Namespace, client: ClusterClient, prom: PrometheusClient) -> None:
    skip = set(s.strip() for s in args.skip_components.split(",") if s.strip())
    order = ["kom", "fq", "nd", "fr", "ha", "ee", "janitor", "pc",
             "mongodb-bitnami", "mongodb-percona", "preflight"]
    for name in order:
        if name in skip:
            log(f"Skipping {name} (--skip-components)")
            continue
        log(f"\n{'='*60}")
        log(f"  Running: {name}")
        log(f"{'='*60}")
        RUNNERS[name](args, client, prom)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    log(f"NVSentinel Benchmark Runner  |  component={args.command}  "
        f"version={args.version}")
    log(f"proxy={args.proxy}  prom={args.prometheus}  output={args.output}")
    if args.dry_run:
        log("DRY RUN — will validate prerequisites only")

    client, prom = check_prerequisites(args)

    if args.dry_run:
        log("Prerequisites OK. Exiting (--dry-run).")
        return

    t0 = time.time()
    try:
        if args.command == "all":
            run_all(args, client, prom)
        else:
            RUNNERS[args.command](args, client, prom)
    except KeyboardInterrupt:
        log("\nInterrupted by user")
    finally:
        elapsed = time.time() - t0
        log(f"\nTotal elapsed: {elapsed/60:.1f} min")


if __name__ == "__main__":
    main()
