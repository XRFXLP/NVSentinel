"""
fq_oplog_stress.py — FQ oplog resume token stress test.

Simulates a fleet-wide health monitor rollout storm:
- N nodes come online simultaneously
- Each sends M non-fatal healthy events (heartbeats, system info)
- A fatal event for one target node is buried in the middle of the crowd
- Measures: time from fatal event insertion to node cordon
            backlog growth during the burst
            whether FQ falls behind (ChangeStreamHistoryLost)

Run inside fq-injector pod:
    python3 /tmp/fq_oplog_stress.py --nodes 1000 --events-per-node 10 --fatal-at 500

The fatal event is inserted after `fatal_at` total events, so it's
buried under (fatal_at) events in the change stream queue.
"""
import argparse
import os
import sys
import time
import uuid
from datetime import datetime, timezone

from pymongo import MongoClient, InsertOne

CERT_DIR  = "/etc/ssl/client-certs"
DB_NAME   = "HealthEventsDatabase"
COLL_NAME = "HealthEvents"


def get_uri() -> str:
    return os.environ.get("MONGO_URI") or (
        f"mongodb://mongodb-0.mongodb-headless.nvsentinel.svc.cluster.local:27017/{DB_NAME}"
        f"?directConnection=true"
        f"&authMechanism=MONGODB-X509"
        f"&authSource=$external"
        f"&tls=true"
        f"&tlsCAFile={CERT_DIR}/ca.crt"
        f"&tlsCertificateKeyFile={CERT_DIR}/creds.pem"
        f"&tlsAllowInvalidHostnames=true"
    )


def make_event(node_name: str, is_fatal: bool, agent: str,
               check: str, error_code: str = "0", entity_val: str = "0") -> dict:
    now = datetime.now(timezone.utc)
    return {
        "createdAt": now,
        "healthevent": {
            "version":            1,
            "agent":              agent,
            "componentclass":     "GPU" if is_fatal else "System",
            "checkname":          check,
            "isfatal":            is_fatal,
            "ishealthy":          not is_fatal,
            "message":            f"XID {error_code}" if is_fatal else "heartbeat",
            "recommendedaction":  2 if is_fatal else 0,
            "errorcode":          [error_code] if is_fatal else [],
            "entitiesimpacted":   [{"entitytype": "gpu", "entityvalue": entity_val}],
            "metadata":           {"trace_id": uuid.uuid4().hex},
            "generatedtimestamp": {"seconds": int(now.timestamp()), "nanos": 0},
            "nodename":           node_name,
            "quarantineoverrides":  None,
            "drainoverrides":       None,
            "processingstrategy":   0,
            "id":                   "",
            "customrecommendedaction": "",
        },
        "healtheventstatus": {
            "nodequarantined":           "",
            "quarantinefinishtimestamp": None,
            "userpodsevictionstatus":    {"status": "", "message": ""},
            "drainfinishtimestamp":      None,
            "faultremediated":           None,
            "lastremediationtimestamp":  None,
            "spanids":                   {},
        },
    }


def wait_for_cordon(coll, node_name: str, timeout_s: int = 300) -> float | None:
    """Poll until FQ marks the node as quarantined. Returns seconds elapsed."""
    t0 = time.time()
    while time.time() - t0 < timeout_s:
        doc = coll.find_one({
            "healthevent.nodename":        node_name,
            "healthevent.isfatal":         True,
            "healtheventstatus.nodequarantined": {"$ne": ""},
        })
        if doc:
            return time.time() - t0
        time.sleep(1)
    return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--nodes",           type=int, default=100,
                   help="Number of distinct nodes in the storm")
    p.add_argument("--events-per-node", type=int, default=20,
                   help="Non-fatal events per node (simulates monitor startup heartbeats)")
    p.add_argument("--fatal-at",        type=int, default=None,
                   help="Insert fatal event after this many total non-fatal events (default: middle)")
    p.add_argument("--fatal-node",      type=str, default=None,
                   help="Node name to target with fatal event (default: first node in list)")
    p.add_argument("--batch-size",      type=int, default=500,
                   help="MongoDB insertMany batch size")
    p.add_argument("--dry-run",         action="store_true",
                   help="Build docs but don't insert")
    args = p.parse_args()

    client = MongoClient(get_uri(), serverSelectionTimeoutMS=5000)
    coll   = client[DB_NAME][COLL_NAME]

    # Get actual KWOK node names from MongoDB (events already present)
    # Fall back to synthetic names if needed
    existing_nodes = list(coll.distinct("healthevent.nodename",
                                        {"healthevent.agent": "kubernetes-object-monitor"}))
    kwok_nodes = sorted([n for n in existing_nodes if n.startswith("kwok-node-")])

    if len(kwok_nodes) < args.nodes:
        # Discover KWOK nodes via in-cluster K8s API
        import json
        from urllib.request import urlopen, Request
        token_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"
        ca_path    = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        with open(token_path) as f:
            token = f.read().strip()
        import ssl, urllib.request
        ctx = ssl.create_default_context(cafile=ca_path)
        req = Request(
            "https://kubernetes.default.svc/api/v1/nodes?labelSelector=type%3Dkwok&limit=500",
            headers={"Authorization": f"Bearer {token}"}
        )
        resp = json.loads(urllib.request.urlopen(req, context=ctx, timeout=15).read())
        kwok_nodes = sorted([n["metadata"]["name"] for n in resp.get("items", [])])

    node_names  = kwok_nodes[:args.nodes]
    fatal_node  = args.fatal_node or node_names[0]
    total_nonfatal = args.nodes * args.events_per_node
    fatal_at    = args.fatal_at if args.fatal_at is not None else total_nonfatal // 2

    print(f"\n=== FQ Oplog Resume Token Stress Test ===")
    print(f"Nodes:            {args.nodes}")
    print(f"Events per node:  {args.events_per_node} (non-fatal)")
    print(f"Total non-fatal:  {total_nonfatal:,}")
    print(f"Fatal event at:   #{fatal_at:,} (buried under {fatal_at:,} events)")
    print(f"Fatal node:       {fatal_node}")
    print(f"Batch size:       {args.batch_size}")
    print()

    # ── Build all documents ───────────────────────────────────────────────────
    checks = ["GpuHealth", "SystemInfo", "NICHealth", "NVSwitchHealth"]
    batch:  list[dict] = []
    fatal_doc: dict | None = None
    fatal_inserted = False

    t_build_start = time.time()
    inserted_total = 0
    t_fatal_insert: float | None = None

    def flush_batch(b: list[dict]) -> int:
        if not b:
            return 0
        if args.dry_run:
            return len(b)
        coll.insert_many(b, ordered=False)
        return len(b)

    print("Building and inserting events...", flush=True)
    t0 = time.time()

    for i, node in enumerate(node_names):
        for j in range(args.events_per_node):
            # Check if we should insert the fatal event here
            if not fatal_inserted and inserted_total + len(batch) >= fatal_at:
                # Flush current batch first
                inserted_total += flush_batch(batch)
                batch = []

                # Insert fatal event
                fatal_doc = make_event(fatal_node, is_fatal=True,
                                       agent="gpu-health-monitor",
                                       check="GpuXidError",
                                       error_code="79", entity_val="0")
                if not args.dry_run:
                    coll.insert_one(fatal_doc)
                t_fatal_insert = time.time()
                fatal_inserted = True
                print(f"  ⚡ Fatal event inserted at position #{inserted_total:,} "
                      f"(t={t_fatal_insert - t0:.3f}s)", flush=True)

            # Non-fatal heartbeat event
            batch.append(make_event(
                node, is_fatal=False, agent="gpu-health-monitor",
                check=checks[j % len(checks)],
            ))

            if len(batch) >= args.batch_size:
                inserted_total += flush_batch(batch)
                batch = []
                if inserted_total % 5000 == 0:
                    print(f"  {inserted_total:,}/{total_nonfatal:,} non-fatal events "
                          f"({time.time()-t0:.1f}s)", flush=True)

    # Insert fatal event at end if never triggered (fatal_at > total)
    if not fatal_inserted:
        inserted_total += flush_batch(batch)
        batch = []
        fatal_doc = make_event(fatal_node, is_fatal=True,
                               agent="gpu-health-monitor",
                               check="GpuXidError", error_code="79")
        if not args.dry_run:
            coll.insert_one(fatal_doc)
        t_fatal_insert = time.time()
        fatal_inserted = True
        print(f"  ⚡ Fatal event inserted at END (position #{inserted_total:,})", flush=True)

    inserted_total += flush_batch(batch)
    t_inject_done = time.time()

    print(f"\nAll events inserted:")
    print(f"  Non-fatal: {inserted_total:,}")
    print(f"  Fatal:     1")
    print(f"  Rate:      {inserted_total/(t_inject_done-t0):.0f} events/s")
    print(f"  Duration:  {t_inject_done-t0:.1f}s")

    if args.dry_run:
        print("\n[dry-run] Skipping FQ wait.")
        return

    # ── Wait for FQ to cordon the fatal node ─────────────────────────────────
    print(f"\nWaiting for FQ to cordon {fatal_node}...")
    t_cordon = wait_for_cordon(coll, fatal_node, timeout_s=600)

    if t_cordon is not None:
        detection_lag = t_fatal_insert + t_cordon - t_fatal_insert
        # Actually measure from when fatal event was inserted
        print(f"\n✅ Node cordoned!")
        print(f"  Fatal event inserted at: t+{t_fatal_insert - t0:.1f}s")
        print(f"  Cordon completed at:     t+{t_fatal_insert - t0 + t_cordon:.1f}s")
        print(f"  Detection lag:           {t_cordon:.1f}s")
        print(f"  Events ahead in queue:   ~{fatal_at:,}")
        print(f"  Implied per-event cost:  {t_cordon*1000/max(fatal_at,1):.1f}ms")
    else:
        print(f"\n❌ TIMEOUT: Node {fatal_node} was NOT cordoned within 600s")
        print(f"   This may indicate ChangeStreamHistoryLost — fatal event was skipped")

    # Cleanup
    print("\nCleaning up inserted events...")
    result = coll.delete_many({
        "healthevent.agent": "gpu-health-monitor",
        "healthevent.metadata.trace_id": {"$exists": True},
        "healthevent.nodename": {"$in": node_names + [fatal_node]},
    })
    print(f"Deleted {result.deleted_count:,} events")
    client.close()


if __name__ == "__main__":
    main()
