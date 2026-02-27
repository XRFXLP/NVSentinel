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

import sys
from unittest.mock import MagicMock

import pytest

# benchmark.py imports torch at module level; mock it for CPU-only test envs.
sys.modules.setdefault("torch", MagicMock())
sys.modules.setdefault("torch.distributed", MagicMock())

from nccl_allreduce.benchmark import format_size, parse_size  # noqa: E402


class TestParseSize:
    def test_megabytes_suffix_m(self) -> None:
        assert parse_size("512M") == 512 * 1024**2

    def test_megabytes_suffix_mb(self) -> None:
        assert parse_size("512MB") == 512 * 1024**2

    def test_gigabytes_suffix_g(self) -> None:
        assert parse_size("4G") == 4 * 1024**3

    def test_gigabytes_suffix_gb(self) -> None:
        assert parse_size("4GB") == 4 * 1024**3

    def test_lowercase(self) -> None:
        assert parse_size("1g") == 1 * 1024**3

    def test_whitespace(self) -> None:
        assert parse_size("  256M  ") == 256 * 1024**2

    def test_fractional(self) -> None:
        assert parse_size("0.5G") == int(0.5 * 1024**3)

    def test_invalid_suffix(self) -> None:
        with pytest.raises(ValueError, match="Invalid size format"):
            parse_size("100")

    def test_invalid_string(self) -> None:
        with pytest.raises(ValueError):
            parse_size("not-a-size")


class TestFormatSize:
    def test_megabytes(self) -> None:
        assert format_size(512 * 1024**2) == "512.00 MB"

    def test_gigabytes(self) -> None:
        assert format_size(4 * 1024**3) == "4.00 GB"

    def test_sub_megabyte(self) -> None:
        result = format_size(1024)
        assert "MB" in result

    def test_exact_one_gb(self) -> None:
        assert format_size(1024**3) == "1.00 GB"
