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

"""Test configuration and fixtures for dcgm-diag tests."""

import os
import sys
from unittest.mock import MagicMock

import pytest

# ============================================================================
# Mock DCGM Modules (must be installed before any dcgm imports)
# ============================================================================


class MockDCGMStructs:
    """Mock dcgm_structs module for testing."""

    # Diagnostic level constants
    DCGM_DIAG_LVL_SHORT = 1
    DCGM_DIAG_LVL_MED = 2
    DCGM_DIAG_LVL_LONG = 3
    DCGM_DIAG_LVL_XLONG = 4

    # Diagnostic result constants
    DCGM_DIAG_RESULT_PASS = 0
    DCGM_DIAG_RESULT_SKIP = 1
    DCGM_DIAG_RESULT_WARN = 2
    DCGM_DIAG_RESULT_FAIL = 3
    DCGM_DIAG_RESULT_NOT_RUN = 4

    # Group constants
    DCGM_GROUP_EMPTY = 0
    DCGM_OPERATION_MODE_AUTO = 0

    class c_dcgmDiagResponse_v12:
        """Mock diagnostic response structure."""

        def __init__(self) -> None:
            self.numTests = 0
            self.numResults = 0
            self.numErrors = 0
            self.tests: list = []
            self.results: list = []
            self.errors: list = []


class MockDCGMTestRun:
    """Mock c_dcgmDiagTestRun_v2 structure."""

    def __init__(self, name: str = "", num_results: int = 0, result_indices: list | None = None) -> None:
        self.name = name.encode() if isinstance(name, str) else name
        self.numResults = num_results
        self.resultIndices = result_indices or []


class MockDCGMEntityResult:
    """Mock c_dcgmDiagEntityResult_v1 structure."""

    def __init__(self, entity_id: int = 0, result: int = 0, test_id: int = 0) -> None:
        self.entity = MagicMock()
        self.entity.entityId = entity_id
        self.result = result
        self.testId = test_id


class MockDCGMError:
    """Mock c_dcgmDiagError_v1 structure."""

    def __init__(self, test_id: int = 0, entity_id: int = 0, code: int = 0, msg: str = "") -> None:
        self.testId = test_id
        self.entity = MagicMock()
        self.entity.entityId = entity_id
        self.code = code
        self.msg = msg.encode() if isinstance(msg, str) else msg


class MockPyNVML:
    """Mock pynvml module for testing."""

    class NVMLError(Exception):
        pass

    @staticmethod
    def nvmlInit() -> None:
        pass

    @staticmethod
    def nvmlShutdown() -> None:
        pass

    @staticmethod
    def nvmlDeviceGetCount() -> int:
        return 2

    @staticmethod
    def nvmlDeviceGetHandleByIndex(index: int) -> MagicMock:
        return MagicMock()

    @staticmethod
    def nvmlDeviceGetUUID(handle: MagicMock) -> str:
        # Return different UUIDs for different handles
        return f"GPU-test-uuid-{id(handle) % 100}"


class MockPyDCGM:
    """Mock pydcgm module for testing."""

    class DcgmHandle:
        def __init__(self, ipAddress: str | None = None, opMode: int | None = None) -> None:
            self.ipAddress = ipAddress
            self.opMode = opMode

        def Shutdown(self) -> None:
            pass

    class DcgmGroup:
        def __init__(
            self, handle: "MockPyDCGM.DcgmHandle | None" = None, groupName: str = "", groupType: int = 0
        ) -> None:
            self.handle = handle
            self.groupName = groupName
            self.groupType = groupType
            self.gpus: list[int] = []
            self.action = MagicMock()

        def AddGpu(self, gpu_idx: int) -> None:
            self.gpus.append(gpu_idx)

        def Delete(self) -> None:
            pass


# Install mocks in sys.modules before any imports
dcgm_structs_mock = MockDCGMStructs()
pydcgm_mock = MockPyDCGM()
pynvml_mock = MockPyNVML()

sys.modules["dcgm_structs"] = dcgm_structs_mock
sys.modules["pydcgm"] = pydcgm_mock
sys.modules["pynvml"] = pynvml_mock


# ============================================================================
# Pytest Fixtures
# ============================================================================


@pytest.fixture(scope="session", autouse=True)
def mock_dcgm_modules() -> None:
    """Ensure DCGM modules are mocked for all tests."""
    # Already installed above, but this fixture documents the dependency
    pass


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove all DCGM-related env vars."""
    for key in list(os.environ.keys()):
        if key.startswith(("DCGM_", "PLATFORM_", "NODE_", "PROCESSING_")):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture
def valid_env(monkeypatch: pytest.MonkeyPatch, clean_env: None) -> None:
    """Set minimum valid environment."""
    monkeypatch.setenv("PLATFORM_CONNECTOR_SOCKET", "/var/run/nvsentinel.sock")
    monkeypatch.setenv("NODE_NAME", "test-node")


@pytest.fixture
def mock_gpu_discovery() -> MagicMock:
    """Create a mock GPUDiscovery instance."""
    discovery = MagicMock()
    discovery.get_allocated_gpus.return_value = [0, 1]
    discovery.get_uuid.side_effect = lambda idx: f"GPU-uuid-{idx}"
    discovery.get_all_uuids.return_value = ["GPU-uuid-0", "GPU-uuid-1"]
    return discovery


@pytest.fixture
def mock_diag_response_pass() -> MockDCGMStructs.c_dcgmDiagResponse_v12:
    """Create a mock diagnostic response with all tests passing."""
    response = MockDCGMStructs.c_dcgmDiagResponse_v12()
    response.numTests = 2
    response.numResults = 4
    response.numErrors = 0

    # Test 1: Memory test
    response.tests = [
        MockDCGMTestRun(name="memory", num_results=2, result_indices=[0, 1]),
        MockDCGMTestRun(name="diagnostic", num_results=2, result_indices=[2, 3]),
    ]

    # Results: all pass
    response.results = [
        MockDCGMEntityResult(entity_id=0, result=dcgm_structs_mock.DCGM_DIAG_RESULT_PASS, test_id=0),
        MockDCGMEntityResult(entity_id=1, result=dcgm_structs_mock.DCGM_DIAG_RESULT_PASS, test_id=0),
        MockDCGMEntityResult(entity_id=0, result=dcgm_structs_mock.DCGM_DIAG_RESULT_PASS, test_id=1),
        MockDCGMEntityResult(entity_id=1, result=dcgm_structs_mock.DCGM_DIAG_RESULT_PASS, test_id=1),
    ]

    response.errors = []
    return response


@pytest.fixture
def mock_diag_response_fail() -> MockDCGMStructs.c_dcgmDiagResponse_v12:
    """Create a mock diagnostic response with a failure."""
    response = MockDCGMStructs.c_dcgmDiagResponse_v12()
    response.numTests = 1
    response.numResults = 2
    response.numErrors = 1

    response.tests = [
        MockDCGMTestRun(name="memory", num_results=2, result_indices=[0, 1]),
    ]

    response.results = [
        MockDCGMEntityResult(entity_id=0, result=dcgm_structs_mock.DCGM_DIAG_RESULT_PASS, test_id=0),
        MockDCGMEntityResult(entity_id=1, result=dcgm_structs_mock.DCGM_DIAG_RESULT_FAIL, test_id=0),
    ]

    response.errors = [
        MockDCGMError(test_id=0, entity_id=1, code=100, msg="Memory error detected on GPU 1"),
    ]
    return response


@pytest.fixture
def mock_diag_response_warn() -> MockDCGMStructs.c_dcgmDiagResponse_v12:
    """Create a mock diagnostic response with a warning."""
    response = MockDCGMStructs.c_dcgmDiagResponse_v12()
    response.numTests = 1
    response.numResults = 2
    response.numErrors = 1

    response.tests = [
        MockDCGMTestRun(name="pcie", num_results=2, result_indices=[0, 1]),
    ]

    response.results = [
        MockDCGMEntityResult(entity_id=0, result=dcgm_structs_mock.DCGM_DIAG_RESULT_WARN, test_id=0),
        MockDCGMEntityResult(entity_id=1, result=dcgm_structs_mock.DCGM_DIAG_RESULT_PASS, test_id=0),
    ]

    response.errors = [
        MockDCGMError(test_id=0, entity_id=0, code=50, msg="PCIe replay rate elevated"),
    ]
    return response
