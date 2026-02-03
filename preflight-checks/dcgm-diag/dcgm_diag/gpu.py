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

import pynvml

log = logging.getLogger(__name__)


class GPUDiscovery:
    def __init__(self):
        self._index_to_uuid: dict[int, str] = {}
        self._discover()

    def get_allocated_gpus(self) -> list[int]:
        return list(self._index_to_uuid.keys())

    def get_uuid(self, index: int) -> str:
        return self._index_to_uuid.get(index, "")

    def get_all_uuids(self) -> list[str]:
        return list(self._index_to_uuid.values())

    def _discover(self) -> None:
        try:
            pynvml.nvmlInit()
            count = pynvml.nvmlDeviceGetCount()

            for i in range(count):
                handle = pynvml.nvmlDeviceGetHandleByIndex(i)
                uuid = pynvml.nvmlDeviceGetUUID(handle)
                self._index_to_uuid[i] = uuid
                log.debug(f"Discovered GPU index={i} uuid={uuid}")

        except pynvml.NVMLError as e:
            log.error(f"Failed to discover GPUs: {e}")
            raise RuntimeError(f"NVML initialization failed: {e}")
        finally:
            try:
                pynvml.nvmlShutdown()
            except pynvml.NVMLError:
                pass
