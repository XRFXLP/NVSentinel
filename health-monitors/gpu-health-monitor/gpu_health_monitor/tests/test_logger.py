# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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

"""Tests for the structlog-based structured logging module."""

import json
import logging

import pytest
import structlog

from gpu_health_monitor.logger import set_default_structured_logger_with_level


@pytest.fixture(autouse=True)
def reset_logging() -> None:
    """Reset structlog and logging configuration before and after each test."""
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(logging.WARNING)
    yield
    structlog.reset_defaults()
    structlog.contextvars.clear_contextvars()
    root.handlers.clear()
    root.setLevel(logging.WARNING)


def test_log_output_format(capsys) -> None:
    """Verify JSON output has required fields and values."""
    set_default_structured_logger_with_level("gpu-health-monitor", "v1.0.0", "info")

    logging.info("Test message", extra={"gpu_count": 8})

    captured = capsys.readouterr()
    log_entry = json.loads(captured.err.strip())

    assert log_entry["event"] == "Test message"
    assert log_entry["module"] == "gpu-health-monitor"
    assert log_entry["version"] == "v1.0.0"
    assert log_entry["gpu_count"] == 8
    assert "timestamp" in log_entry
    assert "level" in log_entry


def test_log_level_filtering(capsys) -> None:
    """Verify log level filtering works correctly."""
    set_default_structured_logger_with_level("test", "v1.0.0", "warning")

    logging.debug("Debug message")
    logging.info("Info message")
    logging.warning("Warning message")
    logging.error("Error message")

    captured = capsys.readouterr()
    assert captured.err.strip(), "No log output captured"
    lines = [line for line in captured.err.strip().split("\n") if line]

    # Only warning and error should be logged
    assert len(lines) == 2
