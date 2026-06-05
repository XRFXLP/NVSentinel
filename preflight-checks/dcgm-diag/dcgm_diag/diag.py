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
import re
from dataclasses import dataclass
from time import sleep

import dcgm_agent
import dcgm_structs
import pydcgm

from .gpu import GPUDiscovery

log = logging.getLogger(__name__)

DEFAULT_STATUS_RETRY_TIMEOUT_SECONDS = 300.0
DEFAULT_STATUS_RETRY_INTERVAL_SECONDS = 10.0
DCGM_STATUS_PATTERN = re.compile(r"DCGM_ST_[A-Z0-9_]+")


@dataclass
class DiagResult:
    test_name: str
    status: str
    gpu_index: int
    gpu_uuid: str
    error_code: int
    error_message: str


class DiagnosticStatusError(RuntimeError):
    """Raised when a DCGM_ST_* status prevents diagnostics from completing."""

    def __init__(self, status_name: str, message: str) -> None:
        super().__init__(message)
        self.status_name = status_name


class DCGMDiagnostic:
    DIAG_LEVELS = {
        1: dcgm_structs.DCGM_DIAG_LVL_SHORT,
        2: dcgm_structs.DCGM_DIAG_LVL_MED,
        3: dcgm_structs.DCGM_DIAG_LVL_LONG,
        4: dcgm_structs.DCGM_DIAG_LVL_XLONG,
    }

    def __init__(
        self,
        hostengine_addr: str,
        status_retry_timeout_seconds: float = DEFAULT_STATUS_RETRY_TIMEOUT_SECONDS,
        status_retry_interval_seconds: float = DEFAULT_STATUS_RETRY_INTERVAL_SECONDS,
    ) -> None:
        self._hostengine_addr = hostengine_addr
        self._status_retry_timeout_seconds = status_retry_timeout_seconds
        self._status_retry_interval_seconds = status_retry_interval_seconds
        self._handle: pydcgm.DcgmHandle | None = None
        self._gpu_discovery = GPUDiscovery()

    def run(self, level: int) -> list[DiagResult]:
        self._connect()
        try:
            return self._run_diagnostic(level)
        finally:
            self._disconnect()

    def get_all_gpu_uuids(self) -> list[str]:
        return self._gpu_discovery.get_all_uuids()

    def _connect(self) -> None:
        log.info(f"Connecting to DCGM hostengine at {self._hostengine_addr}")
        self._handle = pydcgm.DcgmHandle(
            ipAddress=self._hostengine_addr,
            opMode=dcgm_structs.DCGM_OPERATION_MODE_AUTO,
        )

    def _disconnect(self) -> None:
        if self._handle:
            try:
                self._handle.Shutdown()
            except Exception as e:
                log.warning(f"Failed to shutdown DCGM handle: {e}")
            self._handle = None

    def _run_diagnostic(self, level: int) -> list[DiagResult]:
        gpu_indices = self._gpu_discovery.get_allocated_gpus()
        if not gpu_indices:
            raise RuntimeError("No GPUs allocated to this container")

        log.info(f"Running DCGM diagnostic level={level} gpus={gpu_indices}")

        group = self._create_gpu_group(gpu_indices)
        try:
            diag_level = self.DIAG_LEVELS.get(level, dcgm_structs.DCGM_DIAG_LVL_SHORT)
            response = self._run_dcgm_diagnostic_with_retries(group, diag_level)
            return self._parse_response(response, gpu_indices)
        finally:
            group.Delete()

    def _run_dcgm_diagnostic_with_retries(
        self, group: pydcgm.DcgmGroup, diag_level: int
    ) -> dcgm_structs.c_dcgmDiagResponse_v12:
        waited_seconds = 0.0
        attempt = 1

        while True:
            try:
                return self._run_dcgm_diagnostic_once(group, diag_level)
            except Exception as err:
                status_name = get_dcgm_status_name(err)
                if not status_name:
                    raise

                remaining_seconds = self._status_retry_timeout_seconds - waited_seconds
                if remaining_seconds <= 0:
                    final_err = err
                    final_status_name = status_name

                    if is_diag_already_running_status(status_name):
                        log.warning(
                            "DCGM diagnostic stayed already-running; stopping stale diagnostic and retrying once",
                            extra={
                                "attempt": attempt,
                                "waited_seconds": waited_seconds,
                                "retry_timeout_seconds": self._status_retry_timeout_seconds,
                                "dcgm_status": status_name,
                                "error": str(err),
                            },
                        )
                        self.stop_diagnostic()
                        try:
                            return self._run_dcgm_diagnostic_once(group, diag_level)
                        except Exception as retry_err:
                            retry_status_name = get_dcgm_status_name(retry_err)
                            if not retry_status_name:
                                raise
                            final_err = retry_err
                            final_status_name = retry_status_name

                    raise DiagnosticStatusError(
                        final_status_name,
                        f"DCGM diagnostic failed with {final_status_name} after waiting "
                        f"{waited_seconds:.1f}s: {final_err}",
                    ) from final_err

                sleep_seconds = min(self._status_retry_interval_seconds, remaining_seconds)
                log.warning(
                    "DCGM diagnostic returned a DCGM status error; waiting to retry",
                    extra={
                        "attempt": attempt,
                        "waited_seconds": waited_seconds,
                        "retry_delay_seconds": sleep_seconds,
                        "retry_timeout_seconds": self._status_retry_timeout_seconds,
                        "dcgm_status": status_name,
                        "error": str(err),
                    },
                )
                sleep(sleep_seconds)
                waited_seconds += sleep_seconds
                attempt += 1

    def _run_dcgm_diagnostic_once(
        self, group: pydcgm.DcgmGroup, diag_level: int
    ) -> dcgm_structs.c_dcgmDiagResponse_v12:
        return group.action.RunDiagnostic(diag_level)

    def stop_diagnostic(self) -> None:
        if not self._handle:
            return

        try:
            dcgm_agent.dcgmStopDiagnostic(self._handle.handle)
        except Exception:  # noqa: BLE001
            log.debug("Failed to stop DCGM diagnostic", exc_info=True)

    def _create_gpu_group(self, gpu_indices: list[int]) -> pydcgm.DcgmGroup:
        group = pydcgm.DcgmGroup(
            self._handle,
            groupName="nvsentinel-preflight-diag",
            groupType=dcgm_structs.DCGM_GROUP_EMPTY,
        )
        for idx in gpu_indices:
            group.AddGpu(idx)
        return group

    def _parse_response(
        self, response: dcgm_structs.c_dcgmDiagResponse_v12, gpu_indices: list[int]
    ) -> list[DiagResult]:
        """Parse DCGM v12 diagnostic response structure.

        v12 structure:
        - tests[0..numTests-1]: c_dcgmDiagTestRun_v2 with name, result, resultIndices
        - results[0..numResults-1]: c_dcgmDiagEntityResult_v1 with entity.entityId, result, testId
        - errors[0..numErrors-1]: c_dcgmDiagError_v1 with entity, msg, testId
        """
        results = []
        gpu_set = set(gpu_indices)

        error_lookup: dict[tuple[int, int], tuple[int, str]] = {}
        for i in range(response.numErrors):
            err = response.errors[i]
            key = (err.testId, err.entity.entityId)
            error_lookup[key] = (err.code, self._decode_string(err.msg))

        for test_idx in range(response.numTests):
            test = response.tests[test_idx]
            test_name = self._decode_string(test.name)

            for j in range(test.numResults):
                result_idx = test.resultIndices[j]
                entity_result = response.results[result_idx]
                gpu_idx = entity_result.entity.entityId

                if gpu_idx not in gpu_set:
                    continue

                status = self._status_to_string(entity_result.result)
                error_code, error_msg = error_lookup.get((entity_result.testId, gpu_idx), (0, ""))

                results.append(
                    DiagResult(
                        test_name=test_name,
                        status=status,
                        gpu_index=gpu_idx,
                        gpu_uuid=self._gpu_discovery.get_uuid(gpu_idx),
                        error_code=error_code,
                        error_message=error_msg,
                    )
                )

        return results

    @staticmethod
    def _decode_string(value) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8").strip("\x00")
        if isinstance(value, str):
            return value.strip("\x00")
        return str(value) if value else ""

    @staticmethod
    def _status_to_string(status: int) -> str:
        status_map = {
            dcgm_structs.DCGM_DIAG_RESULT_PASS: "pass",
            dcgm_structs.DCGM_DIAG_RESULT_SKIP: "skip",
            dcgm_structs.DCGM_DIAG_RESULT_WARN: "warn",
            dcgm_structs.DCGM_DIAG_RESULT_FAIL: "fail",
            dcgm_structs.DCGM_DIAG_RESULT_NOT_RUN: "not_run",
        }
        return status_map.get(status, "unknown")


def get_dcgm_status_name(err: Exception) -> str:
    value = getattr(err, "value", None)
    if isinstance(value, int) and value < 0:
        for name in dir(dcgm_structs):
            constant_value = getattr(dcgm_structs, name)
            if name.startswith("DCGM_ST_") and constant_value == value:
                return name

    message = str(err)
    match = DCGM_STATUS_PATTERN.search(message.upper())
    if match:
        return match.group(0)

    message_lower = message.lower()
    if "affected resource is in use" in message_lower or "resource is in use" in message_lower:
        return "DCGM_ST_IN_USE"
    if "diag instance is already running" in message_lower or "new diag until the current one finishes" in message_lower:
        return "DCGM_ST_DIAG_ALREADY_RUNNING"

    return ""


def is_diag_already_running_status(status_name: str) -> bool:
    return status_name == "DCGM_ST_DIAG_ALREADY_RUNNING"
