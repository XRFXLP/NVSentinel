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

import signal
from unittest.mock import MagicMock

import pytest

from dcgm_diag.__main__ import _install_shutdown_handlers, _run_diagnostic
from dcgm_diag.config import Config
from dcgm_diag.diag import DiagnosticStatusError
from dcgm_diag.protos import health_event_pb2 as pb


def test_dcgm_status_error_reports_nonfatal_and_does_not_block() -> None:
    cfg = Config(
        diag_level=2,
        hostengine_addr="localhost:5555",
        connector_socket="/var/run/nvsentinel.sock",
        node_name="test-node",
        processing_strategy=pb.ProcessingStrategy.EXECUTE_REMEDIATION,
        status_retry_max_attempts=4,
        status_retry_interval_seconds=2,
    )
    reporter = MagicMock()
    diag = MagicMock()
    diag.run.side_effect = DiagnosticStatusError(
        "DCGM_ST_IN_USE",
        "DCGM diagnostic returned DCGM_ST_IN_USE after 4 attempts; diagnostic did not complete",
    )

    exit_code = _run_diagnostic(cfg, reporter, diag)

    assert exit_code == 0
    reporter.send_event.assert_called_once_with(
        gpu_uuid="",
        is_healthy=False,
        is_fatal=False,
        message="DCGM diagnostic returned DCGM_ST_IN_USE after 4 attempts; diagnostic did not complete",
        error_code_name="DCGM_ST_IN_USE",
        recommended_action=pb.RecommendedAction.NONE,
    )


def test_shutdown_handler_stops_active_diagnostic() -> None:
    diag = MagicMock()
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    previous_sigint = signal.getsignal(signal.SIGINT)

    try:
        _install_shutdown_handlers(diag)
        handler = signal.getsignal(signal.SIGTERM)

        with pytest.raises(SystemExit) as exc_info:
            handler(signal.SIGTERM, None)

        assert exc_info.value.code == 128 + signal.SIGTERM
        diag.stop_diagnostic.assert_called_once()
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)
