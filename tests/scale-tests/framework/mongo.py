"""
framework/mongo.py — Direct MongoDB injection for FQ benchmarks.

Bypasses platform-connector to isolate the DB → FQ → API server path.
Requires port-forward to mongodb-0:27017 and TLS certs from
mongo-app-client-cert-secret.
"""
from __future__ import annotations

import os
import subprocess
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator

from pymongo import MongoClient
from pymongo.collection import Collection


DB_NAME   = "HealthEventsDatabase"
COLL_NAME = "HealthEvents"
NAMESPACE = "nvsentinel"
SECRET    = "mongo-app-client-cert-secret"


@dataclass
class MongoInjector:
    """
    Injects HealthEvent documents directly into MongoDB, triggering
    FQ's change stream without going through platform-connector.

    Usage:
        with MongoInjector.from_cluster() as inj:
            inj.inject_fleet_storm(node_names=["kwok-node-000000", ...])
    """
    uri:      str
    cert_dir: str
    _client:  MongoClient = field(default=None, init=False, repr=False)
    _coll:    Collection  = field(default=None, init=False, repr=False)

    def __enter__(self) -> "MongoInjector":
        self._client = MongoClient(self.uri)
        self._coll   = self._client[DB_NAME][COLL_NAME]
        return self

    def __exit__(self, *_):
        if self._client:
            self._client.close()

    @classmethod
    def in_cluster(cls, cert_dir: str = "/etc/ssl/client-certs") -> "MongoInjector":
        """
        Use when running inside the cluster (fq-injector pod).
        Connects directly to MongoDB without port-forward.
        """
        import os
        uri = os.environ.get("MONGO_URI") or (
            f"mongodb://mongodb-0.mongodb-headless.nvsentinel.svc.cluster.local:27017/{DB_NAME}"
            f"?directConnection=true"
            f"&authMechanism=MONGODB-X509"
            f"&authSource=$external"
            f"&tls=true"
            f"&tlsCAFile={cert_dir}/ca.crt"
            f"&tlsCertificateKeyFile={cert_dir}/creds.pem"
            f"&tlsAllowInvalidHostnames=true"
        )
        return cls(uri=uri, cert_dir=cert_dir)

    @classmethod
    def from_cluster(cls, local_port: int = 27017) -> "MongoInjector":
        """Extract certs from cluster secret and build MongoInjector."""
        cert_dir = tempfile.mkdtemp(prefix="fq-mongo-certs-")

        def extract(key: str, path: str) -> None:
            data = subprocess.check_output([
                "kubectl", "get", "secret", SECRET, "-n", NAMESPACE,
                "-o", f"jsonpath={{.data.{key}}}"
            ])
            with open(path, "wb") as f:
                import base64
                f.write(base64.b64decode(data))
            os.chmod(path, 0o600)

        extract("ca\\.crt",  os.path.join(cert_dir, "ca.crt"))
        extract("tls\\.crt", os.path.join(cert_dir, "tls.crt"))
        extract("tls\\.key", os.path.join(cert_dir, "tls.key"))

        creds = os.path.join(cert_dir, "creds.pem")
        with open(creds, "wb") as f:
            for part in ("tls.crt", "tls.key"):
                with open(os.path.join(cert_dir, part), "rb") as p:
                    f.write(p.read())
        os.chmod(creds, 0o600)

        uri = (
            f"mongodb://localhost:{local_port}/{DB_NAME}"
            f"?directConnection=true"
            f"&authMechanism=MONGODB-X509"
            f"&authSource=$external"
            f"&tls=true"
            f"&tlsCAFile={cert_dir}/ca.crt"
            f"&tlsCertificateKeyFile={cert_dir}/creds.pem"
            f"&tlsAllowInvalidHostnames=true"
        )
        return cls(uri=uri, cert_dir=cert_dir)

    # ── Document builder ──────────────────────────────────────────────────────

    def _make_event(
        self,
        node_name:   str,
        agent:       str  = "gpu-health-monitor",
        is_fatal:    bool = True,
        check_name:  str  = "GpuXidError",
        component:   str  = "GPU",
        error_code:  str  = "79",
        entity_type: str  = "gpu",
        entity_val:  str  = "0",
    ) -> dict:
        now = datetime.now(timezone.utc)
        ts_seconds = int(now.timestamp())
        ts_nanos   = now.microsecond * 1000
        return {
            "createdAt": now,
            "healthevent": {
                "version":          1,
                "agent":            agent,
                "componentclass":   component,
                "checkname":        check_name,
                "isfatal":          is_fatal,
                "ishealthy":        not is_fatal,
                "message":          f"XID {error_code} - benchmark injection",
                "recommendedaction": 2,          # COMPONENT_RESET
                "errorcode":        [error_code],
                "entitiesimpacted": [{"entitytype": entity_type, "entityvalue": entity_val}],
                "metadata":         {"trace_id": uuid.uuid4().hex},
                "generatedtimestamp": {"seconds": ts_seconds, "nanos": ts_nanos},
                "nodename":         node_name,
                "quarantineoverrides": None,
                "drainoverrides":   None,
                "processingstrategy": 0,         # EXECUTE_REMEDIATION
                "id":               "",
                "customrecommendedaction": "",
            },
            "healtheventstatus": {
                "nodequarantined":         "",
                "quarantinefinishtimestamp": None,
                "userpodsevictionstatus":  {"status": "", "message": ""},
                "drainfinishtimestamp":    None,
                "faultremediated":         None,
                "lastremediationtimestamp": None,
                "spanids":                 {},
            },
        }

    # ── Injection patterns ────────────────────────────────────────────────────

    def inject_fleet_storm(
        self,
        node_names: list[str],
        agent:      str = "gpu-health-monitor",
    ) -> list[str]:
        """
        1 fatal event per distinct node — all inserted at once.
        Tests FQ's cordon throughput: how fast can it cordon N nodes
        when they all fail simultaneously?

        Returns list of inserted _id strings for latency tracking.
        """
        docs = [self._make_event(n, agent=agent) for n in node_names]
        result = self._coll.insert_many(docs, ordered=False)
        return [str(i) for i in result.inserted_ids]

    def inject_noisy_node_backlog(
        self,
        node_groups: list[tuple[str, int]],
        agent:       str = "gpu-health-monitor",
    ) -> dict[str, list[str]]:
        """
        Sequential per-node burst: for each (node_name, count) in node_groups,
        insert `count` events for that node before moving to the next.

        e.g. node_groups = [("kwok-node-000000", 1000),
                             ("kwok-node-000001", 1000),
                             ("kwok-node-000002", 1000)]

        The first event for each node is fatal (triggers cordon).
        Subsequent events vary entity/errorcode to avoid HealthEventKey dedup,
        ensuring each event is processed on the handleAlreadyQuarantinedNode path.

        Returns {node_name: [inserted_ids]} for latency tracking.
        The timestamp of the FIRST insert for each node is the injection time.
        """
        results = {}
        gpu_count = 8  # typical GPU node
        error_codes = [str(x) for x in [79, 80, 81, 74, 92, 48, 31, 63]]

        for node_name, count in node_groups:
            docs = []
            for i in range(count):
                docs.append(self._make_event(
                    node_name,
                    agent=agent,
                    is_fatal=True,
                    error_code=error_codes[i % len(error_codes)],
                    entity_val=str(i % gpu_count),
                ))
            result = self._coll.insert_many(docs, ordered=True)
            results[node_name] = [str(i) for i in result.inserted_ids]

        return results

    def inject_sustained_rate(
        self,
        node_names:  list[str],
        rate_per_s:  float,
        duration_s:  int,
        agent:       str = "gpu-health-monitor",
        is_fatal:    bool = True,
    ) -> int:
        """
        Continuous distinct events across node_names at `rate_per_s`.
        Round-robins through nodes and varies error codes to avoid dedup.
        Used to find the rate at which FQ's backlog grows unboundedly.

        Returns total events injected.
        """
        interval  = 1.0 / rate_per_s
        deadline  = time.time() + duration_s
        count     = 0
        error_codes = [str(x) for x in [79, 80, 81, 74, 92, 48, 31, 63]]

        while time.time() < deadline:
            t0   = time.time()
            node = node_names[count % len(node_names)]
            ec   = error_codes[count % len(error_codes)]
            ev   = str(count % 8)
            self._coll.insert_one(
                self._make_event(node, agent=agent, is_fatal=is_fatal,
                                 error_code=ec, entity_val=ev)
            )
            count += 1
            elapsed = time.time() - t0
            sleep   = interval - elapsed
            if sleep > 0:
                time.sleep(sleep)

        return count

    def inject_post_cordon_flood(
        self,
        node_names: list[str],
        events_per_node: int,
        agent: str = "gpu-health-monitor",
    ) -> int:
        """
        After nodes are already cordoned, flood them with more events.
        Tests handleAlreadyQuarantinedNode overhead + updateNodeQuarantineStatus
        MongoDB write rate. Each event has a distinct HealthEventKey
        (different entity/errorcode) to flow through the full already-quarantined path.

        Returns total events injected.
        """
        docs = []
        error_codes = [str(x) for x in [79, 80, 81, 74, 92, 48, 31, 63]]
        for i, node in enumerate(node_names):
            for j in range(events_per_node):
                docs.append(self._make_event(
                    node, agent=agent,
                    error_code=error_codes[j % len(error_codes)],
                    entity_val=str(j % 8),
                ))
        self._coll.insert_many(docs, ordered=False)
        return len(docs)

    def count_cordoned(self, node_names: list[str]) -> int:
        """Count how many of the given nodes are marked as cordoned in MongoDB."""
        return self._coll.count_documents({
            "healthevent.nodename": {"$in": node_names},
            "healtheventstatus.nodequarantined": {"$ne": ""},
        })

    def clear_test_events(self, node_names: list[str]) -> int:
        """Delete all injected events for given nodes (cleanup between reps)."""
        result = self._coll.delete_many(
            {"healthevent.nodename": {"$in": node_names},
             "healthevent.agent": "gpu-health-monitor",
             "healthevent.metadata.trace_id": {"$exists": True}}
        )
        return result.deleted_count
