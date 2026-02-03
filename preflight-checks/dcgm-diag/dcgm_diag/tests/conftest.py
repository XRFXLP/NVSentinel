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

import os

import pytest


@pytest.fixture
def clean_env(monkeypatch):
    """Remove all DCGM-related env vars."""
    for key in list(os.environ.keys()):
        if key.startswith(("DCGM_", "PLATFORM_", "NODE_", "PROCESSING_")):
            monkeypatch.delenv(key, raising=False)


@pytest.fixture
def valid_env(monkeypatch, clean_env):
    """Set minimum valid environment."""
    monkeypatch.setenv("PLATFORM_CONNECTOR_SOCKET", "/var/run/nvsentinel.sock")
    monkeypatch.setenv("NODE_NAME", "test-node")
