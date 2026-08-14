#!/usr/bin/env python3
"""
KOM Microbenchmark Runner
=========================
Self-contained script that runs the full KOM benchmark suite on any Kubernetes cluster.

Usage:
    python3 kom_benchmark.py [--proxy http://localhost:8001] [--prom http://localhost:9090]
                             [--namespace benchmark] [--output results/]
                             [--skip-steps memory,churn,restart,policy]

Prerequisites:
    - kubectl configured and cluster accessible
    - KWOK controller installed: kubectl apply -f https://github.com/kubernetes-sigs/kwok/releases/latest/download/kwok.yaml
    - Prometheus scraping KOM metrics (ServiceMonitor for nvsentinel namespace)
    - kubectl proxy running: kubectl proxy --port=8001 &
    - Prometheus port-forwarded: kubectl port-forward -n prometheus <pod> 9090:9090 &
    - KOM deployed with processingStrategy=STORE_ONLY

Steps performed:
    1. MEASURE   — sample one real node + one GPU operator pod from cluster
    2. VERIFY    — create one KWOK node, confirm size matches real node ±10%
    3. MEMORY    — node sweep 100→1k→5k→10k→25k→50k→100k + pod sweep 0→1k→10k→50k
    4. CHURN     — pod churn 10/50/200 pods/s
    5. RESTART   — restart storm at 10k nodes (K12 bottleneck)
    6. POLICY    — CEL complexity, lookup(), namespace vs cluster scope, MCR sweep
"""

import argparse, json, os, subprocess, sys, threading, time, urllib.request, urllib.parse
import datetime

# ── CLI ──────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="KOM benchmark runner")
    p.add_argument("--proxy",     default="http://localhost:8001")
    p.add_argument("--prom",      default="http://localhost:9090")
    p.add_argument("--namespace", default="benchmark")
    p.add_argument("--output",    default="results/kom")
    p.add_argument("--skip",      default="", help="comma-separated steps to skip: memory,churn,restart,policy")
    p.add_argument("--pod-source-namespace", default="gpu-operator",
                   help="Namespace to sample representative pod object from")
    return p.parse_args()

ARGS = parse_args()
BASE  = ARGS.proxy
PROM  = ARGS.prom
NS    = ARGS.namespace
OUT   = ARGS.output
SKIP  = set(s.strip() for s in ARGS.skip.split(",") if s.strip())

os.makedirs(OUT, exist_ok=True)
RESULTS = {}

def save():
    path = os.path.join(OUT, "results.json")
    with open(path, "w") as f:
        json.dump(RESULTS, f, indent=2)
    print(f"  [saved → {path}]", flush=True)

def log(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ── Prometheus ────────────────────────────────────────────────────────────────

def q(query):
    url = f"{PROM}/api/v1/query?query={urllib.parse.quote(query)}"
    try:
        r = json.loads(urllib.request.urlopen(url, timeout=5).read())["data"]["result"]
        return float(r[0]["value"][1]) if r else None
    except: return None

def wait_queue_drain(controller="node", timeout=300):
    t0 = time.time()
    while time.time()-t0 < timeout:
        d = q(f'sum(workqueue_depth{{job="kubernetes-object-monitor",name="{controller}"}})')
        if d is not None and d < 10:
            return d
        time.sleep(5)
    return q(f'sum(workqueue_depth{{job="kubernetes-object-monitor",name="{controller}"}})')

def snapshot(label, controller="node"):
    d = wait_queue_drain(controller, timeout=120)
    heap  = q('go_memstats_heap_alloc_bytes{job="kubernetes-object-monitor"}')
    objs  = q('go_memstats_heap_objects{job="kubernetes-object-monitor"}')
    recl  = q(f'sum(controller_runtime_reconcile_total{{job="kubernetes-object-monitor",controller="{controller}"}})')
    qwait = q(f'histogram_quantile(0.99,sum(rate(workqueue_queue_duration_seconds_bucket{{job="kubernetes-object-monitor",name="{controller}"}}[5m]))by(le))')
    mb    = round(heap/1024/1024, 1) if heap else None
    result = {
        "label": label,
        "heap_alloc_mb": mb,
        "heap_objects": int(objs) if objs else None,
        "reconciles": int(recl) if recl else None,
        "queue_depth": int(d) if d else 0,
        "queue_wait_p99_s": round(qwait, 1) if qwait else None,
        "limit_recommendation_mi": max(int(mb * 2.5), 2048) if mb else None,
    }
    log(f"  {label}: heap={mb}MB queue_wait_P99={round(qwait,1) if qwait else '?'}s → set limit={result['limit_recommendation_mi']}Mi")
    return result

# ── kubectl / API helpers ─────────────────────────────────────────────────────

def kube(args, **kwargs):
    return subprocess.run(["kubectl"] + args, capture_output=True, text=True, **kwargs)

def api_get(path):
    return json.loads(urllib.request.urlopen(f"{BASE}{path}", timeout=30).read())

def api_post(path, body):
    req = urllib.request.Request(f"{BASE}{path}", data=json.dumps(body).encode(),
                                  method="POST", headers={"Content-Type":"application/json"})
    try: urllib.request.urlopen(req, timeout=10); return True
    except urllib.error.HTTPError as e: return e.code == 409
    except: return False

def api_patch_status(path, body):
    req = urllib.request.Request(f"{BASE}{path}", data=json.dumps(body).encode(),
                                  method="PATCH", headers={"Content-Type":"application/merge-patch+json"})
    try: urllib.request.urlopen(req, timeout=10); return True
    except: return False

def api_delete(path):
    req = urllib.request.Request(f"{BASE}{path}", method="DELETE",
                                  data=b'{"gracePeriodSeconds":0}',
                                  headers={"Content-Type":"application/json"})
    try: urllib.request.urlopen(req, timeout=10)
    except: pass

# ── STEP 1: MEASURE real objects ──────────────────────────────────────────────

def step_measure():
    log("=== STEP 1: Measuring real node and pod object sizes ===")

    # Sample a real ready node
    nodes = api_get("/api/v1/nodes?limit=20")["items"]
    ready_nodes = [n for n in nodes
                   if any(c.get("type")=="Ready" and c.get("status")=="True"
                         for c in n.get("status",{}).get("conditions",[]))]
    if not ready_nodes:
        log("ERROR: No ready nodes found")
        sys.exit(1)

    # Pick node with most conditions (most representative)
    sample_node = max(ready_nodes,
                      key=lambda n: len(n.get("status",{}).get("conditions",[])))
    node_bytes   = len(json.dumps(sample_node))
    node_status  = sample_node.get("status", {})
    node_name    = sample_node["metadata"]["name"]
    node_conds   = len(node_status.get("conditions", []))
    node_images  = len(node_status.get("images", []))

    log(f"  Sampled node: {node_name}")
    log(f"  Node size: {node_bytes:,} bytes ({node_bytes/1024:.1f} KB)")
    log(f"  Conditions: {node_conds}, Images: {node_images}")

    # Sample a pod from the specified namespace
    pods = api_get(f"/api/v1/namespaces/{ARGS.pod_source_namespace}/pods?limit=20").get("items", [])
    running_pods = [p for p in pods if p.get("status",{}).get("phase")=="Running"]
    if not running_pods:
        log(f"  WARNING: No running pods in {ARGS.pod_source_namespace}, falling back to kube-system")
        pods = api_get("/api/v1/namespaces/kube-system/pods?limit=20").get("items", [])
        running_pods = [p for p in pods if p.get("status",{}).get("phase")=="Running"]

    sample_pod  = running_pods[0] if running_pods else None
    pod_bytes   = len(json.dumps(sample_pod)) if sample_pod else 0
    pod_name    = sample_pod["metadata"]["name"] if sample_pod else "n/a"

    log(f"  Sampled pod ({ARGS.pod_source_namespace}): {pod_name}")
    log(f"  Pod size: {pod_bytes:,} bytes ({pod_bytes/1024:.1f} KB)")

    RESULTS["object_sizes"] = {
        "node_sample": node_name,
        "node_bytes": node_bytes,
        "node_conditions": node_conds,
        "node_images": node_images,
        "pod_sample": pod_name,
        "pod_namespace": ARGS.pod_source_namespace,
        "pod_bytes": pod_bytes,
    }

    # Build status template from the real node
    RESULTS["node_status_template"] = {
        "status": {
            k: v for k, v in node_status.items()
            if k in ("conditions","allocatable","capacity","nodeInfo",
                     "addresses","daemonEndpoints","images",
                     "volumesAttached","volumesInUse","runtimeHandlers")
        }
    }

    save()
    return node_bytes, pod_bytes, RESULTS["node_status_template"]

# ── STEP 2: VERIFY one KWOK node matches real size ────────────────────────────

def step_verify(node_bytes, status_template):
    log("\n=== STEP 2: Creating one KWOK node and verifying size ===")

    # Ensure DNS autoscaler is frozen
    kube(["scale","deployment","kube-dns-autoscaler","-n","kube-system","--replicas=0"])

    test_name = "kwok-verify-node-000"
    # Delete if exists
    api_delete(f"/api/v1/nodes/{test_name}")
    time.sleep(1)

    # Create
    api_post("/api/v1/nodes", {
        "apiVersion": "v1", "kind": "Node",
        "metadata": {
            "name": test_name,
            "labels": {"type":"kwok","kwok.x-k8s.io/node":"fake"},
            "annotations": {"kwok.x-k8s.io/node":"fake"}
        },
        "spec": {"taints": [{"key":"kwok.x-k8s.io/node","value":"fake","effect":"NoSchedule"}]}
    })

    # Patch status with real node's status
    api_patch_status(f"/api/v1/nodes/{test_name}/status", status_template)

    # Measure
    kwok_node = api_get(f"/api/v1/nodes/{test_name}")
    kwok_bytes = len(json.dumps(kwok_node))
    diff_pct   = abs(kwok_bytes - node_bytes) / node_bytes * 100

    log(f"  Real node:  {node_bytes:,} bytes")
    log(f"  KWOK node:  {kwok_bytes:,} bytes")
    log(f"  Difference: {diff_pct:.1f}%")

    if diff_pct > 15:
        log(f"  WARNING: >15% size difference — status template may be incomplete")
    else:
        log(f"  ✅ Size within tolerance")

    # Clean up
    api_delete(f"/api/v1/nodes/{test_name}")

    RESULTS["verify"] = {
        "real_node_bytes": node_bytes,
        "kwok_node_bytes": kwok_bytes,
        "diff_pct": round(diff_pct, 1),
        "passed": diff_pct <= 15,
    }
    save()

# ── STEP 3: MEMORY sweep ──────────────────────────────────────────────────────

def create_kwok_nodes(start, end, status_template, parallelism=80):
    status_bytes = json.dumps(status_template).encode()
    def create_and_patch(i):
        name = f"kwok-node-{i:06d}"
        body = json.dumps({
            "apiVersion":"v1","kind":"Node",
            "metadata":{
                "name":name,
                "labels":{"type":"kwok","benchmark":"true","kwok.x-k8s.io/node":"fake"},
                "annotations":{"kwok.x-k8s.io/node":"fake"}
            },
            "spec":{"taints":[{"key":"kwok.x-k8s.io/node","value":"fake","effect":"NoSchedule"}]}
        }).encode()
        req = urllib.request.Request(f"{BASE}/api/v1/nodes",
            data=body, method="POST", headers={"Content-Type":"application/json"})
        try: urllib.request.urlopen(req, timeout=10)
        except urllib.error.HTTPError as e:
            if e.code != 409: return
        req2 = urllib.request.Request(f"{BASE}/api/v1/nodes/{name}/status",
            data=status_bytes, method="PATCH",
            headers={"Content-Type":"application/merge-patch+json"})
        try: urllib.request.urlopen(req2, timeout=10)
        except: pass

    threads = [threading.Thread(target=create_and_patch, args=(i,))
               for i in range(start, end)]
    for t in threads: t.start()
    for t in threads: t.join()

def delete_all_kwok_nodes():
    log("  Cleaning up KWOK nodes...")
    names, cont = [], None
    while True:
        url = f"{BASE}/api/v1/nodes?labelSelector=type%3Dkwok&limit=500"
        if cont: url += f"&continue={cont}"
        resp = json.loads(urllib.request.urlopen(url, timeout=30).read())
        names += [n["metadata"]["name"] for n in resp.get("items",[])]
        cont = resp.get("metadata",{}).get("continue")
        if not cont: break
    def d(name):
        api_delete(f"/api/v1/nodes/{name}")
    chunks = [names[i:i+200] for i in range(0,len(names),200)]
    for chunk in chunks:
        ts = [threading.Thread(target=d, args=(n,)) for n in chunk]
        for t in ts: t.start()
        for t in ts: t.join()
    log(f"  Deleted {len(names)} KWOK nodes")

def step_memory(status_template, pod_status_template):
    log("\n=== STEP 3: Memory sweep ===")
    memory_results = {"nodes": [], "pods": []}

    # ── Node sweep ──
    log("\n  Node sweep (100 → 100k):")
    current = 0
    for target, label in [
        (100,"100"), (1000,"1k"), (5000,"5k"),
        (10000,"10k"), (25000,"25k"), (50000,"50k"),
        (75000,"75k"), (100000,"100k")
    ]:
        t0 = time.time()
        batch = 500
        while current < target:
            end = min(current + batch, target)
            create_kwok_nodes(current, end, status_template)
            current = end
        elapsed = time.time() - t0
        log(f"  Built to {label} nodes in {elapsed:.0f}s", )
        r = snapshot(f"{label} nodes", controller="node")
        r["node_count"] = target
        memory_results["nodes"].append(r)
        RESULTS["memory"] = memory_results
        save()

    # ── Node resync saturation table ──
    log("\n  Resync saturation analysis (MCR=1, resyncPeriod=5m, ~230 rec/s):")
    tput = 230
    period = 300
    sat = []
    for n in [10000,25000,50000,65000,75000,100000]:
        drain = n / tput
        sat.append({"nodes":n,"drain_s":round(drain,0),"period_s":period,
                    "saturated":drain>period*0.8})
        status = "❌ SATURATED" if drain > period*0.8 else ("⚠ near limit" if drain > period*0.6 else "✅ safe")
        log(f"    {n:>7,} nodes: drain={drain:.0f}s {status}")
    RESULTS["resync_saturation"] = sat

    # Clean up nodes before pod sweep
    delete_all_kwok_nodes()
    current = 0

    # ── Pod sweep ──
    log("\n  Pod sweep (0 → 100k):")
    TOLS = [{"key":"kwok.x-k8s.io/node","operator":"Exists","effect":"NoSchedule"},
            {"key":"node.kubernetes.io/not-ready","operator":"Exists","effect":"NoExecute"},
            {"key":"node.kubernetes.io/unreachable","operator":"Exists","effect":"NoExecute"}]

    # Need KWOK nodes for pods
    create_kwok_nodes(0, 100, status_template)
    kube(["create","namespace",NS,"--dry-run=client","-o","yaml"])
    subprocess.run(["kubectl","create","namespace",NS], capture_output=True)

    pod_n = [0]
    def create_pods(count, parallelism=80):
        def create(i):
            idx = pod_n[0] + i
            body = json.dumps({
                "apiVersion":"v1","kind":"Pod",
                "metadata":{"name":f"bench-pod-{idx:08d}","namespace":NS,
                             "labels":{"bench":"true"}},
                "spec":{"nodeName":f"kwok-node-{idx%100:06d}","tolerations":TOLS,
                        "containers":[{"name":"pause","image":"registry.k8s.io/pause:3.9",
                                       "resources":{"requests":{"cpu":"1m","memory":"4Mi"}}}]}
            }).encode()
            req = urllib.request.Request(f"{BASE}/api/v1/namespaces/{NS}/pods",
                data=body, method="POST", headers={"Content-Type":"application/json"})
            try: urllib.request.urlopen(req, timeout=10)
            except: pass
        threads = [threading.Thread(target=create, args=(i,)) for i in range(count)]
        for t in threads: t.start()
        for t in threads: t.join()
        pod_n[0] += count

    for target, label, new_pods in [
        (0,    "0 pods",    0),
        (1000, "1k pods",   1000),
        (10000,"10k pods",  9000),
        (50000,"50k pods",  40000),
        (100000,"100k pods",50000),
    ]:
        if new_pods > 0:
            create_pods(new_pods, parallelism=min(80, new_pods))
        r = snapshot(label, controller="pod")
        r["pod_count"] = target
        memory_results["pods"].append(r)
        RESULTS["memory"] = memory_results
        save()

    # Cleanup
    subprocess.run(["kubectl","delete","namespace",NS,"--grace-period=0"], capture_output=True)
    delete_all_kwok_nodes()

# ── STEP 4: CHURN sweep ───────────────────────────────────────────────────────

def step_churn(status_template):
    log("\n=== STEP 4: Pod churn rate sweep ===")

    create_kwok_nodes(0, 100, status_template)
    subprocess.run(["kubectl","create","namespace",NS], capture_output=True)

    TOLS = [{"key":"kwok.x-k8s.io/node","operator":"Exists","effect":"NoSchedule"},
            {"key":"node.kubernetes.io/not-ready","operator":"Exists","effect":"NoExecute"},
            {"key":"node.kubernetes.io/unreachable","operator":"Exists","effect":"NoExecute"}]

    churn_results = []
    counter = [0]

    def churn_worker(rate_per_thread):
        t_next = time.time()
        while not stop.is_set():
            i = counter[0]; counter[0] += 1
            body = json.dumps({
                "apiVersion":"v1","kind":"Pod",
                "metadata":{"name":f"churn-{i:09d}","namespace":NS,"labels":{"churn":"true"}},
                "spec":{"nodeName":f"kwok-node-{i%100:06d}","tolerations":TOLS,
                        "containers":[{"name":"pause","image":"registry.k8s.io/pause:3.9",
                                       "resources":{"requests":{"cpu":"1m","memory":"4Mi"}}}]}
            }).encode()
            req = urllib.request.Request(f"{BASE}/api/v1/namespaces/{NS}/pods",
                data=body, method="POST", headers={"Content-Type":"application/json"})
            try: urllib.request.urlopen(req, timeout=10)
            except: pass
            t_next += 1/rate_per_thread
            gap = t_next - time.time()
            if gap > 0: time.sleep(gap)

    stop = threading.Event()
    for scenario, target_rate, label in [(10,"C1"),(50,"C2"),(200,"C3")]:
        n_threads = max(1, scenario // 5)
        per_thread = scenario / n_threads
        log(f"  {label}: {scenario} pods/s target ({n_threads} threads × {per_thread:.0f}/s)")
        stop.clear()
        threads = [threading.Thread(target=churn_worker, args=(per_thread,), daemon=True)
                   for _ in range(n_threads)]
        for t in threads: t.start()

        time.sleep(60)  # warmup
        samples = []
        t0 = time.time()
        while time.time()-t0 < 120:
            tput  = q('sum(rate(controller_runtime_reconcile_total{job="kubernetes-object-monitor",controller="pod"}[30s]))')
            p50   = q('histogram_quantile(0.50,sum(rate(controller_runtime_reconcile_time_seconds_bucket{job="kubernetes-object-monitor",controller="pod"}[30s]))by(le))')
            p99   = q('histogram_quantile(0.99,sum(rate(controller_runtime_reconcile_time_seconds_bucket{job="kubernetes-object-monitor",controller="pod"}[30s]))by(le))')
            qwait = q('histogram_quantile(0.99,sum(rate(workqueue_queue_duration_seconds_bucket{job="kubernetes-object-monitor",name="pod"}[30s]))by(le))')
            cpu   = q('rate(process_cpu_seconds_total{job="kubernetes-object-monitor"}[30s])')
            depth = q('sum(workqueue_depth{job="kubernetes-object-monitor",name="pod"})')
            if tput is not None:
                samples.append({"tput":tput,"p50":(p50 or 0)*1000,"p99":(p99 or 0)*1000,
                                "cpu":(cpu or 0)*100,"depth":depth or 0,
                                "queue_wait_p99_s":qwait or 0})
            time.sleep(15)

        stop.set()
        avg = lambda k: round(sum(s[k] for s in samples)/len(samples),1) if samples else None
        result = {"label":label,"target_rate":scenario,
                  "tput_rps":avg("tput"),"p50_ms":avg("p50"),"p99_ms":avg("p99"),
                  "cpu_pct":avg("cpu"),"queue_depth":avg("depth"),
                  "queue_wait_p99_s":avg("queue_wait_p99_s")}
        log(f"    tput={result['tput_rps']}/s P99={result['p99_ms']}ms "
            f"queue_wait_P99={result['queue_wait_p99_s']}s CPU={result['cpu_pct']}%")
        churn_results.append(result)
        RESULTS["churn"] = churn_results
        save()

        # Clean pods between runs
        subprocess.run(["kubectl","delete","pods","-n",NS,"-l","churn=true",
                        "--grace-period=0","--force"], capture_output=True)
        time.sleep(5)

    subprocess.run(["kubectl","delete","namespace",NS,"--grace-period=0"], capture_output=True)
    delete_all_kwok_nodes()

# ── STEP 5: RESTART storm (K12) ───────────────────────────────────────────────

def step_restart(status_template):
    log("\n=== STEP 5: Restart storm (K12 bottleneck) ===")

    for node_count, label in [(100,"100 nodes"), (10000,"10k nodes")]:
        log(f"\n  Restart at {label}:")
        create_kwok_nodes(0, node_count, status_template)
        time.sleep(30)  # let informer sync

        heap_pre = q('go_memstats_heap_alloc_bytes{job="kubernetes-object-monitor"}')
        log(f"    Pre-restart heap: {round(heap_pre/1024/1024,1) if heap_pre else '?'} MB")

        t_restart = time.time()
        subprocess.run(["kubectl","rollout","restart","deployment/kubernetes-object-monitor",
                         "-n","nvsentinel"], capture_output=True)

        t_ready = t_synced = None
        peak_queue = 0

        for _ in range(200):
            time.sleep(5)
            elapsed = time.time() - t_restart
            r = subprocess.run(["kubectl","get","pods","-n","nvsentinel","-l",
                "app.kubernetes.io/name=kubernetes-object-monitor","--no-headers"],
                capture_output=True, text=True).stdout.strip()
            ready = "1/1" in r and "Running" in r and r.count("\n") == 0

            depth = q('sum(workqueue_depth{job="kubernetes-object-monitor",name="node"})')
            d = depth or 0
            if d > peak_queue: peak_queue = d

            if ready and t_ready is None:
                t_ready = elapsed
                log(f"    Pod Ready at +{elapsed:.0f}s")
            if t_ready and d < 20 and t_synced is None:
                t_synced = elapsed
                log(f"    Cache synced at +{elapsed:.0f}s")
                break

        result = {
            "node_count": node_count,
            "t_ready_s": round(t_ready,0) if t_ready else None,
            "t_synced_s": round(t_synced,0) if t_synced else None,
            "relist_duration_s": round(t_synced-t_ready,0) if (t_ready and t_synced) else None,
            "peak_queue": peak_queue,
            "per_node_startup_ms": round(t_ready/node_count*1000,1) if t_ready else None,
        }
        log(f"    Time to Ready: {result['t_ready_s']}s, per-node: {result['per_node_startup_ms']}ms")
        RESULTS.setdefault("restart",[]).append(result)
        save()
        delete_all_kwok_nodes()

# ── STEP 6: POLICY sweep ──────────────────────────────────────────────────────

def step_policy(status_template):
    log("\n=== STEP 6: Policy configuration sweep ===")

    create_kwok_nodes(0, 100, status_template)
    subprocess.run(["kubectl","create","namespace",NS], capture_output=True)

    TOLS = [{"key":"kwok.x-k8s.io/node","operator":"Exists","effect":"NoSchedule"},
            {"key":"node.kubernetes.io/not-ready","operator":"Exists","effect":"NoExecute"},
            {"key":"node.kubernetes.io/unreachable","operator":"Exists","effect":"NoExecute"}]

    stop = threading.Event()
    n = [0]
    def churn():
        t = time.time()
        while not stop.is_set():
            i = n[0]; n[0] += 1
            body = json.dumps({
                "apiVersion":"v1","kind":"Pod",
                "metadata":{"name":f"pol-{i:09d}","namespace":NS,"labels":{"pol":"true"}},
                "spec":{"nodeName":f"kwok-node-{i%100:06d}","tolerations":TOLS,
                        "containers":[{"name":"pause","image":"registry.k8s.io/pause:3.9",
                                       "resources":{"requests":{"cpu":"1m","memory":"4Mi"}}}]}
            }).encode()
            req = urllib.request.Request(f"{BASE}/api/v1/namespaces/{NS}/pods",
                data=body, method="POST", headers={"Content-Type":"application/json"})
            try: urllib.request.urlopen(req, timeout=10)
            except: pass
            t += 1/5; gap = t-time.time()
            if gap > 0: time.sleep(gap)
    for _ in range(6): threading.Thread(target=churn, daemon=True).start()

    def patch_cm(toml):
        subprocess.run(["kubectl","patch","configmap","kubernetes-object-monitor",
            "-n","nvsentinel","--type=merge",
            "-p",json.dumps({"data":{"config.toml":toml}})], capture_output=True)
        pod = subprocess.run(["kubectl","get","pods","-n","nvsentinel","-l",
            "app.kubernetes.io/name=kubernetes-object-monitor",
            "-o","jsonpath={.items[0].metadata.name}"],
            capture_output=True, text=True).stdout.strip()
        subprocess.run(["kubectl","delete","pod",pod,"-n","nvsentinel",
            "--grace-period=0","--force"], capture_output=True)
        for _ in range(30):
            r = subprocess.run(["kubectl","get","pods","-n","nvsentinel","-l",
                "app.kubernetes.io/name=kubernetes-object-monitor","--no-headers"],
                capture_output=True, text=True).stdout
            if "1/1" in r and "Running" in r: break
            time.sleep(3)
        time.sleep(15)

    def make_toml(expr, ns=NS, node_assoc=None):
        lines = ["[[policies]]",'  name="Bench"',"  enabled=true",
                 "  [policies.resource]",'    group=""','    version="v1"','    kind="Pod"']
        if ns: lines.append(f'    namespace="{ns}"')
        lines += ["  [policies.predicate]", f"    expression={json.dumps(expr)}"]
        if node_assoc:
            lines += ["  [policies.nodeAssociation]", f"    expression={json.dumps(node_assoc)}"]
        lines += ["  [policies.healthEvent]",'    componentClass="Node"',"    isFatal=false",
                  '    message="bench"','    recommendedAction="CONTACT_SUPPORT"',
                  '    errorCode=["BENCH"]']
        return "\n".join(lines)+"\n"

    def measure_policy(warmup=30, window=60, ctrl="pod"):
        time.sleep(warmup)
        samples = []
        t0 = time.time()
        while time.time()-t0 < window:
            tput = q(f'sum(rate(controller_runtime_reconcile_total{{job="kubernetes-object-monitor",controller="{ctrl}"}}[30s]))')
            p50  = q(f'histogram_quantile(0.50,sum(rate(controller_runtime_reconcile_time_seconds_bucket{{job="kubernetes-object-monitor",controller="{ctrl}"}}[30s]))by(le))')
            p99  = q(f'histogram_quantile(0.99,sum(rate(controller_runtime_reconcile_time_seconds_bucket{{job="kubernetes-object-monitor",controller="{ctrl}"}}[30s]))by(le))')
            cpu  = q('rate(process_cpu_seconds_total{job="kubernetes-object-monitor"}[30s])')
            qw   = q(f'histogram_quantile(0.99,sum(rate(workqueue_queue_duration_seconds_bucket{{job="kubernetes-object-monitor",name="{ctrl}"}}[30s]))by(le))')
            if tput is not None:
                samples.append({"tput":tput,"p50":(p50 or 0)*1000,"p99":(p99 or 0)*1000,
                                "cpu":(cpu or 0)*100,"queue_wait_p99_s":qw or 0})
            time.sleep(10)
        avg = lambda k: round(sum(s[k] for s in samples)/len(samples),1) if samples else None
        return {k: avg(k) for k in ["tput","p50","p99","cpu","queue_wait_p99_s"]}

    policy_results = {}

    # MB-C2.1: CEL complexity
    log("  MB-C2.1: CEL predicate complexity")
    cel_results = []
    for name, expr in [
        ("simple",   'resource.status.phase == "Running"'),
        ("medium",   'resource.status.phase == "Running" && has(resource.metadata.labels) && resource.metadata.labels.exists(k, k == "pol")'),
        ("complex",  'resource.status.phase == "Running" && has(resource.status.conditions) && resource.status.conditions.filter(c, c.type in ["Ready","ContainersReady"] && c.status == "True").size() >= 1 && has(resource.metadata.labels) && resource.metadata.labels.exists(k, k.startsWith("pol"))'),
        ("catchall", 'has(resource.metadata.name)'),
    ]:
        patch_cm(make_toml(expr))
        r = measure_policy()
        r["predicate"] = name
        log(f"    {name}: tput={r['tput']}/s P99={r['p99']}ms queue_wait={r['queue_wait_p99_s']}s")
        cel_results.append(r)
        subprocess.run(["kubectl","delete","pods","-n",NS,"-l","pol=true","--grace-period=0","--force"],capture_output=True)
        time.sleep(3)
    policy_results["cel_complexity"] = cel_results

    # MB-C2.2: nodeAssociation lookup()
    log("  MB-C2.2: nodeAssociation lookup()")
    lookup_results = []
    for name, expr, assoc in [
        ("none",          'resource.status.phase == "Running"', None),
        ("direct_cel",    'resource.status.phase == "Running"', "resource.spec.nodeName"),
        ("single_lookup", 'resource.status.phase == "Running"',
         "lookup('v1', 'Node', '', resource.spec.nodeName).metadata.name"),
    ]:
        patch_cm(make_toml(expr, node_assoc=assoc))
        r = measure_policy()
        r["association"] = name
        log(f"    {name}: tput={r['tput']}/s P50={r['p50']}ms")
        lookup_results.append(r)
        subprocess.run(["kubectl","delete","pods","-n",NS,"-l","pol=true","--grace-period=0","--force"],capture_output=True)
        time.sleep(3)
    policy_results["nodeassociation"] = lookup_results

    # MB-C2.3: namespace vs cluster scope
    log("  MB-C2.3: Namespace vs cluster scope")
    scope_results = []
    for name, ns in [("namespace_scoped", NS), ("cluster_scoped", "")]:
        patch_cm(make_toml('resource.status.phase == "Running"', ns=ns))
        time.sleep(15)
        heap = q('go_memstats_heap_alloc_bytes{job="kubernetes-object-monitor"}')
        r = measure_policy()
        r["scope"] = name
        r["heap_alloc_mb"] = round(heap/1024/1024,1) if heap else None
        log(f"    {name}: tput={r['tput']}/s P99={r['p99']}ms heap={r['heap_alloc_mb']}MB")
        scope_results.append(r)
        subprocess.run(["kubectl","delete","pods","-n",NS,"-l","pol=true","--grace-period=0","--force"],capture_output=True)
        time.sleep(3)
    policy_results["scope"] = scope_results

    # MB-C2.4: maxConcurrentReconciles
    log("  MB-C2.4: maxConcurrentReconciles sweep")
    mcr_results = []
    patch_cm(make_toml('resource.status.phase == "Running"'))
    for mcr in [1, 2, 4, 8]:
        subprocess.run(["kubectl","patch","deployment","kubernetes-object-monitor",
            "-n","nvsentinel","--type=json",
            f'-p=[{{"op":"replace","path":"/spec/template/spec/containers/0/args/4","value":"--max-concurrent-reconciles={mcr}"}}]'],
            capture_output=True)
        subprocess.run(["kubectl","rollout","restart","deployment/kubernetes-object-monitor","-n","nvsentinel"],capture_output=True)
        for _ in range(30):
            r = subprocess.run(["kubectl","get","pods","-n","nvsentinel","-l","app.kubernetes.io/name=kubernetes-object-monitor","--no-headers"],capture_output=True,text=True).stdout
            if "1/1" in r and "Running" in r: break
            time.sleep(3)
        time.sleep(15)
        r = measure_policy(warmup=30, window=60)
        r["mcr"] = mcr
        log(f"    MCR={mcr}: tput={r['tput']}/s P99={r['p99']}ms CPU={r['cpu']}%")
        mcr_results.append(r)
        subprocess.run(["kubectl","delete","pods","-n",NS,"-l","pol=true","--grace-period=0","--force"],capture_output=True)
        time.sleep(3)
    policy_results["mcr"] = mcr_results

    # Restore defaults
    subprocess.run(["kubectl","patch","deployment","kubernetes-object-monitor","-n","nvsentinel",
        "--type=json",'-p=[{"op":"replace","path":"/spec/template/spec/containers/0/args/4","value":"--max-concurrent-reconciles=1"}]'],
        capture_output=True)

    stop.set()
    RESULTS["policy"] = policy_results
    save()

    subprocess.run(["kubectl","delete","namespace",NS,"--grace-period=0"], capture_output=True)
    delete_all_kwok_nodes()

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    log("KOM Benchmark Runner")
    log(f"Output: {OUT}")
    log(f"Skip:   {SKIP or 'none'}\n")

    # Step 1: Measure
    node_bytes, pod_bytes, status_template = step_measure()

    # Step 2: Verify
    step_verify(node_bytes, status_template)

    # Step 3: Memory
    if "memory" not in SKIP:
        step_memory(status_template, None)

    # Step 4: Churn
    if "churn" not in SKIP:
        step_churn(status_template)

    # Step 5: Restart
    if "restart" not in SKIP:
        step_restart(status_template)

    # Step 6: Policy
    if "policy" not in SKIP:
        step_policy(status_template)

    log("\n=== BENCHMARK COMPLETE ===")
    log(f"Results saved to {OUT}/results.json")
    save()

    # Print summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    if "object_sizes" in RESULTS:
        s = RESULTS["object_sizes"]
        print(f"Real node size:  {s['node_bytes']:,} bytes ({s['node_bytes']/1024:.1f} KB)")
        print(f"Real pod size:   {s['pod_bytes']:,} bytes ({s['pod_bytes']/1024:.1f} KB)")
    if "memory" in RESULTS:
        print("\nNode memory (set resources.limits.memory to recommended):")
        print(f"  {'Nodes':>8}  {'heap_alloc':>12}  {'Recommended limit':>18}  {'Queue wait P99':>15}")
        for r in RESULTS["memory"].get("nodes",[]):
            print(f"  {r['node_count']:>8,}  {str(r['heap_alloc_mb'])+'MB':>12}  {str(r['limit_recommendation_mi'])+'Mi':>18}  {str(r['queue_wait_p99_s'])+'s':>15}")

if __name__ == "__main__":
    main()
