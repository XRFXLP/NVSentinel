#!/usr/bin/env python3
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

"""Entrypoint script for NCCL all-reduce preflight check.

This script:
1. Waits for gang formation (all peers registered in ConfigMap)
2. Extracts coordination info (rank, master address, etc.)
3. Execs torchrun with the appropriate arguments

The ConfigMap is mounted at GANG_CONFIG_DIR (default: /etc/preflight) and
contains:
    - expected_count: Number of pods in the gang
    - gang_id: Unique identifier for the gang
    - master_addr: IP address of rank 0 pod
    - master_port: Port for PyTorch distributed
    - peers: List of "pod_name;pod_ip;rank" lines

Environment variables:
    Required:
        - POD_NAME: This pod's name (injected by webhook)

    Optional:
        - GANG_CONFIG_DIR: ConfigMap mount path (default: /etc/preflight)
        - GANG_TIMEOUT_SECONDS: Timeout for gang formation (default: 600)
        - NPROCS_PER_NODE: GPUs per node (default: auto-detect)
        - BW_THRESHOLD_GBPS: Bandwidth threshold (default: 100)
        - MESSAGE_SIZES: Sizes to test (default: 4G)
        - LOG_LEVEL: Logging level (default: info)
"""

import logging
import os
import subprocess
import sys

# Add parent directory to path for imports when running as script
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nccl_allreduce.errors import NCCLError
from nccl_allreduce.gang import GangWaiter
from nccl_allreduce.health import HealthReporter
from nccl_allreduce.logger import set_default_structured_logger
from nccl_allreduce.protos import health_event_pb2 as pb

log = logging.getLogger(__name__)

# Default values
DEFAULT_GANG_CONFIG_DIR = "/etc/preflight"
DEFAULT_GANG_TIMEOUT = 600
DEFAULT_NPROCS_PER_NODE = 8


def detect_gpu_count() -> int:
    """Detect the number of GPUs using nvidia-smi.

    Returns:
        Number of GPUs visible to this container.
    """
    try:
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name", "--format=csv,noheader"],
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
        lines = [line for line in result.stdout.strip().split("\n") if line]
        return len(lines)
    except (subprocess.SubprocessError, FileNotFoundError) as err:
        log.warning(
            "Failed to detect GPU count, using default",
            extra={"error": str(err), "default": DEFAULT_NPROCS_PER_NODE},
        )
        return DEFAULT_NPROCS_PER_NODE


def main() -> int:
    """Main entrypoint function.

    Returns:
        Exit code.
    """
    log_level = os.getenv("LOG_LEVEL", "info")
    set_default_structured_logger("preflight-nccl-allreduce", "0.1.0", log_level)

    # Get configuration from environment
    pod_name = os.getenv("POD_NAME", "")
    if not pod_name:
        log.error("POD_NAME environment variable is required")
        return NCCLError.GANG_CONFIG_ERROR.value.exit_code

    gang_config_dir = os.getenv("GANG_CONFIG_DIR", DEFAULT_GANG_CONFIG_DIR)
    gang_timeout = int(os.getenv("GANG_TIMEOUT_SECONDS", str(DEFAULT_GANG_TIMEOUT)))

    nprocs_env = os.getenv("NPROCS_PER_NODE", "")
    if nprocs_env:
        nprocs_per_node = int(nprocs_env)
    else:
        nprocs_per_node = detect_gpu_count()

    log.info(
        "Starting NCCL all-reduce preflight check",
        extra={
            "pod_name": pod_name,
            "gang_config_dir": gang_config_dir,
            "gang_timeout": gang_timeout,
            "nprocs_per_node": nprocs_per_node,
        },
    )

    # Wait for gang formation
    waiter = GangWaiter(gang_config_dir)

    try:
        gang_config = waiter.wait(pod_name, gang_timeout)
    except TimeoutError as err:
        log.error("Gang formation timeout", extra={"error": str(err)})
        _report_error(NCCLError.GANG_TIMEOUT, str(err))
        return NCCLError.GANG_TIMEOUT.value.exit_code
    except ValueError as err:
        log.error("Invalid gang configuration", extra={"error": str(err)})
        _report_error(NCCLError.GANG_CONFIG_ERROR, str(err))
        return NCCLError.GANG_CONFIG_ERROR.value.exit_code

    # Validate configuration
    if gang_config.my_rank < 0:
        error_msg = f"Pod {pod_name} not found in peers list"
        log.error(error_msg)
        _report_error(NCCLError.GANG_CONFIG_ERROR, error_msg)
        return NCCLError.GANG_CONFIG_ERROR.value.exit_code

    if not gang_config.master_addr:
        error_msg = "Master address not set in ConfigMap"
        log.error(error_msg)
        _report_error(NCCLError.GANG_CONFIG_ERROR, error_msg)
        return NCCLError.GANG_CONFIG_ERROR.value.exit_code

    log.info(
        "Gang formation complete, launching torchrun",
        extra={
            "gang_id": gang_config.gang_id,
            "expected_count": gang_config.expected_count,
            "my_rank": gang_config.my_rank,
            "master_addr": gang_config.master_addr,
            "master_port": gang_config.master_port,
        },
    )

    # Build torchrun command
    cmd = gang_config.get_torchrun_args(nprocs_per_node, "-m")
    cmd.append("nccl_allreduce")

    log.info("Executing torchrun", extra={"command": " ".join(cmd)})

    # Export NPROCS_PER_NODE for the benchmark
    os.environ["NPROCS_PER_NODE"] = str(nprocs_per_node)

    # Exec torchrun (replaces this process)
    os.execvp(cmd[0], cmd)

    # Should never reach here
    return NCCLError.GANG_CONFIG_ERROR.value.exit_code


def _report_error(error: NCCLError, message: str) -> None:
    """Report an error as a health event.

    Args:
        error: The NCCL error type.
        message: Error message.
    """
    skip_report = os.getenv("SKIP_HEALTH_REPORT", "false").lower() in (
        "true",
        "1",
        "yes",
    )
    if skip_report:
        log.info("Skipping health report (SKIP_HEALTH_REPORT=true)")
        return

    # Don't try to send events for errors that don't support them
    if error.value.error_code is None:
        return

    connector_socket = os.getenv("PLATFORM_CONNECTOR_SOCKET", "")
    node_name = os.getenv("NODE_NAME", "")

    if not connector_socket or not node_name:
        log.warning(
            "Cannot send health event: missing PLATFORM_CONNECTOR_SOCKET or NODE_NAME"
        )
        return

    strategy_str = os.getenv("PROCESSING_STRATEGY", "EXECUTE_REMEDIATION")
    try:
        processing_strategy = pb.ProcessingStrategy.Value(strategy_str)
    except ValueError:
        processing_strategy = pb.ProcessingStrategy.EXECUTE_REMEDIATION

    try:
        reporter = HealthReporter(
            socket_path=connector_socket,
            node_name=node_name,
            processing_strategy=processing_strategy,
        )
        reporter.send_failure(error, message)
    except RuntimeError as err:
        log.error(
            "Failed to send health event",
            extra={"error": str(err)},
        )


if __name__ == "__main__":
    sys.exit(main())
