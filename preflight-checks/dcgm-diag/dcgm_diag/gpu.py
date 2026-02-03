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
    """Discovers GPUs allocated to this container using NVML.

    We use pynvml (NVML) instead of `dcgmi discovery -l` because NVML respects
    the NVIDIA container runtime's GPU isolation. When a container is allocated
    specific GPUs via NVIDIA_VISIBLE_DEVICES or Kubernetes device plugins, NVML
    only returns those allocated GPUs.

    Example on an 8-GPU node with a container allocated 2 GPUs:

        # Embedded mode (local hostengine inside container):
        $ dcgmi discovery -l
        2 GPUs found.
        GPU 0: GPU-bfd72471-...  (PCI 0000000D:00:00.0)
        GPU 1: GPU-60ef74d0-...  (PCI 0000000E:00:00.0)

        # Standalone mode (node's hostengine):
        $ dcgmi discovery -l --host nvidia-dcgm.gpu-operator.svc:5555
        8 GPUs found.
        GPU 0-5: ... (other tenants' GPUs)
        GPU 6: GPU-bfd72471-...  (PCI 0000000D:00:00.0)  <- same GPU, different index!
        GPU 7: GPU-60ef74d0-...  (PCI 0000000E:00:00.0)

    Notice the same physical GPUs have different indices: container sees them as
    GPU 0,1 while the node sees them as GPU 6,7. If we used the standalone
    hostengine and ran diagnostics on "GPU 0", we'd diagnose a completely
    different GPU belonging to another tenant!

    NVML (pynvml) always returns container-local indices, so we use it to
    discover allocated GPUs, then create a targeted DCGM group with those
    indices for diagnostics.
    """

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
