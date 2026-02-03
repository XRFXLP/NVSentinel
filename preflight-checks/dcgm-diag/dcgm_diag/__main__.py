# Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import sys

from .config import Config
from .diag import DCGMDiagnostic, DiagResult
from .health import HealthReporter
from .protos import health_event_pb2 as pb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y/%m/%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _build_message(results: list[DiagResult]) -> tuple[list[str], str]:
    """Extract UUIDs and build message from diagnostic results."""
    uuids = [r.gpu_uuid for r in results if r.gpu_uuid]
    message = "; ".join(f"{r.test_name} (GPU {r.gpu_index}): {r.error_message}" for r in results)
    return uuids, message


def main() -> None:
    try:
        cfg = Config.from_env()
    except ValueError as e:
        log.error(f"Configuration error: {e}")
        sys.exit(1)

    log.info(
        f"Starting preflight dcgm-diag check "
        f"level={cfg.diag_level} processing_strategy={pb.ProcessingStrategy.Name(cfg.processing_strategy)}"
    )

    reporter = HealthReporter(
        socket_path=cfg.connector_socket,
        node_name=cfg.node_name,
        processing_strategy=cfg.processing_strategy,
    )

    diag = DCGMDiagnostic(hostengine_addr=cfg.hostengine_addr)

    try:
        results = diag.run(cfg.diag_level)
    except Exception as e:
        log.error(f"DCGM diagnostic failed: {e}")
        reporter.send_event(gpu_uuids=[], is_healthy=False, is_fatal=False, message=str(e))
        sys.exit(1)

    failures = [r for r in results if r.status == "fail"]
    warnings = [r for r in results if r.status == "warn"]

    for r in results:
        log.info(f"Test result test={r.test_name} status={r.status} gpu={r.gpu_index} error={r.error_message}")

    log.info(
        f"Diagnostic summary passed={len(results) - len(failures) - len(warnings)} "
        f"failed={len(failures)} warned={len(warnings)} total={len(results)}"
    )

    if failures:
        uuids, message = _build_message(failures)
        log.error(f"DCGM diagnostic failed: {message}")
        reporter.send_event(gpu_uuids=uuids, is_healthy=False, is_fatal=True, message=message)
        sys.exit(1)

    if warnings:
        uuids, message = _build_message(warnings)
        log.warning(f"DCGM diagnostic warnings: {message}")
        reporter.send_event(gpu_uuids=uuids, is_healthy=False, is_fatal=False, message=message)
    else:
        uuids = diag.get_all_gpu_uuids()
        reporter.send_event(gpu_uuids=uuids, is_healthy=True, is_fatal=False, message="DCGM diagnostic passed")

    log.info("DCGM diagnostic check passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
