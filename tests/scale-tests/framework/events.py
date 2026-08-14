"""
events.py — Health event generator.

Injects synthetic HealthEvents into NVSentinel via:
  - gRPC to platform-connector Unix socket (mirrors real monitors)
  - Direct MongoDB/PostgreSQL insert (bypasses platform-connector for
    datastore-layer benchmarks)

The gRPC path is the primary injection method for end-to-end latency
measurements (V1 requirement: per-event ID from injector to API effect).
"""
from __future__ import annotations

import json
import socket
import struct
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Callable

from .log import log


@dataclass
class HealthEventSpec:
    """
    Parameters for a synthetic health event.

    Mirrors the proto fields in data-models/protobufs/health_event.proto.
    """
    node_name: str
    agent: str = "benchmark-injector"
    component_class: str = "GPU"
    check_name: str = "BenchmarkCheck"
    is_fatal: bool = True
    is_healthy: bool = False
    message: str = "Synthetic fault for benchmarking"
    recommended_action: str = "CONTACT_SUPPORT"   # enum string
    error_codes: list[str] = field(default_factory=lambda: ["BENCHMARK_FAULT"])
    processing_strategy: str = "STORE_ONLY"       # safe default; override for E2E tests
    metadata: dict[str, str] = field(default_factory=dict)

    def to_dict(self, event_id: str | None = None) -> dict:
        return {
            "version": 1,
            "id": event_id or str(uuid.uuid4()),
            "agent": self.agent,
            "componentClass": self.component_class,
            "checkName": self.check_name,
            "isFatal": self.is_fatal,
            "isHealthy": self.is_healthy,
            "message": self.message,
            "recommendedAction": self.recommended_action,
            "errorCode": self.error_codes,
            "nodeName": self.node_name,
            "processingStrategy": self.processing_strategy,
            "metadata": self.metadata,
        }


class GrpcInjector:
    """
    Sends HealthEvents to a platform-connector over its gRPC Unix socket.

    Uses a raw socket with length-prefixed protobuf framing — this avoids
    the grpcio dependency while still being compatible with the gRPC wire
    format for unary calls.

    For benchmarks that need the full end-to-end path
    (monitor → platform-connector → datastore → FQ):
      - Use processingStrategy="EXECUTE_REMEDIATION"
      - Use agent="gpu-health-monitor" so FQ rulesets match
      - Set is_fatal=True

    For datastore/FQ-only benchmarks (bypass platform-connector):
      - Use DirectStoreInjector below
    """

    def __init__(self, socket_path: str = "/var/run/nvsentinel/nvsentinel.sock"):
        self.socket_path = socket_path
        self._lock = threading.Lock()
        self._sock: socket.socket | None = None

    def connect(self) -> None:
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.connect(self.socket_path)
        log(f"Connected to platform-connector at {self.socket_path}")

    def close(self) -> None:
        if self._sock:
            self._sock.close()
            self._sock = None

    def inject(self, events: list[HealthEventSpec]) -> list[str]:
        """
        Inject a batch of health events. Returns list of injected event IDs
        for downstream latency tracking (V1 requirement).
        """
        ids = []
        payloads = []
        for spec in events:
            eid = str(uuid.uuid4())
            ids.append(eid)
            payloads.append(spec.to_dict(event_id=eid))

        # Encode as JSON-over-gRPC (compatible with Connect protocol)
        body = json.dumps({"version": 1, "events": payloads}).encode()
        # gRPC frame: 1 byte compression flag + 4 byte length
        frame = b"\x00" + struct.pack(">I", len(body)) + body

        with self._lock:
            if self._sock is None:
                self.connect()
            self._sock.sendall(frame)

        return ids

    def inject_one(self, spec: HealthEventSpec) -> str:
        """Convenience wrapper for single-event injection."""
        return self.inject([spec])[0]


class EventRateController:
    """
    Drives event injection at a target rate (events/second).

    Spawns worker threads that call inject_fn at the configured rate.
    Tracks injected event IDs and injection timestamps for V1 latency
    measurement (harness-side per-event timing).
    """

    def __init__(
        self,
        inject_fn: Callable[[HealthEventSpec], str],
        rate_per_second: float,
        spec_factory: Callable[[int], HealthEventSpec],
        parallelism: int = 1,
    ):
        self.inject_fn = inject_fn
        self.rate = rate_per_second
        self.spec_factory = spec_factory
        self.parallelism = parallelism

        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._counter = 0
        self._injected: dict[str, float] = {}  # event_id → injection_timestamp

    def start(self) -> None:
        self._stop.clear()
        interval = 1.0 / (self.rate / self.parallelism)
        for _ in range(self.parallelism):
            threading.Thread(target=self._worker, args=(interval,), daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def _worker(self, interval: float) -> None:
        t_next = time.monotonic()
        while not self._stop.is_set():
            with self._lock:
                idx = self._counter
                self._counter += 1
            spec = self.spec_factory(idx)
            t_inject = time.time()
            try:
                eid = self.inject_fn(spec)
                with self._lock:
                    self._injected[eid] = t_inject
            except Exception:
                pass
            t_next += interval
            gap = t_next - time.monotonic()
            if gap > 0:
                time.sleep(gap)

    @property
    def injected_count(self) -> int:
        with self._lock:
            return len(self._injected)

    def drain_injected(self) -> dict[str, float]:
        """Return and clear the injected event map for latency measurement."""
        with self._lock:
            result = dict(self._injected)
            self._injected.clear()
        return result


def make_fatal_gpu_event_factory(
    node_names: list[str],
    agent: str = "gpu-health-monitor",
    processing_strategy: str = "EXECUTE_REMEDIATION",
) -> Callable[[int], HealthEventSpec]:
    """
    Factory for fatal GPU XID-79 events — the default FQ trigger.

    Uses agent='gpu-health-monitor' so FQ's default rulesets match.
    Use processing_strategy='STORE_ONLY' for non-destructive benchmarks.
    """
    def factory(idx: int) -> HealthEventSpec:
        return HealthEventSpec(
            node_name=node_names[idx % len(node_names)],
            agent=agent,
            component_class="GPU",
            check_name="XIDError",
            is_fatal=True,
            is_healthy=False,
            message="XID 79: GPU has fallen off the bus",
            recommended_action="CONTACT_SUPPORT",
            error_codes=["XID_79"],
            processing_strategy=processing_strategy,
            metadata={"xid": "79", "gpu_index": str(idx % 8)},
        )
    return factory


def make_healthy_event_factory(
    node_names: list[str],
    agent: str = "gpu-health-monitor",
) -> Callable[[int], HealthEventSpec]:
    """Factory for healthy (recovery) events — used to test uncordon flows."""
    def factory(idx: int) -> HealthEventSpec:
        return HealthEventSpec(
            node_name=node_names[idx % len(node_names)],
            agent=agent,
            component_class="GPU",
            check_name="XIDError",
            is_fatal=False,
            is_healthy=True,
            message="GPU recovered",
            recommended_action="NONE",
            error_codes=[],
            processing_strategy="EXECUTE_REMEDIATION",
        )
    return factory
