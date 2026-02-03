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
from time import sleep

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from .protos import health_event_pb2 as pb
from .protos import health_event_pb2_grpc as pb_grpc

log = logging.getLogger(__name__)

MAX_RETRIES = 5
INITIAL_DELAY = 2.0
BACKOFF_FACTOR = 1.5
RPC_TIMEOUT = 30.0


class HealthReporter:
    AGENT = "preflight-dcgm-diag"
    COMPONENT_CLASS = "GPU"
    CHECK_NAME = "DCGM_DIAGNOSTIC"

    def __init__(
        self,
        socket_path: str,
        node_name: str,
        processing_strategy: pb.ProcessingStrategy,
    ) -> None:
        self._socket_path = socket_path.removeprefix("unix://")
        self._node_name = node_name
        self._processing_strategy = processing_strategy

    def send_event(
        self,
        gpu_uuids: list[str],
        is_healthy: bool,
        is_fatal: bool,
        message: str,
    ) -> None:
        event = self._build_event(gpu_uuids, is_healthy, is_fatal, message)
        health_events = pb.HealthEvents(version=1, events=[event])

        log.info(
            f"Sending health event is_healthy={is_healthy} is_fatal={is_fatal} "
            f"gpu_count={len(gpu_uuids)} processing_strategy={self._processing_strategy} message={message}"
        )

        if not self._send_with_retries(health_events):
            raise RuntimeError(f"Failed to send health event after {MAX_RETRIES} retries")

    def _build_event(
        self,
        gpu_uuids: list[str],
        is_healthy: bool,
        is_fatal: bool,
        message: str,
    ) -> pb.HealthEvent:
        entities = [pb.Entity(entityType="GPU_UUID", entityValue=uuid) for uuid in gpu_uuids]

        recommended_action = pb.NONE if is_healthy else pb.CONTACT_SUPPORT

        timestamp = Timestamp()
        timestamp.GetCurrentTime()

        return pb.HealthEvent(
            version=1,
            agent=self.AGENT,
            componentClass=self.COMPONENT_CLASS,
            checkName=self.CHECK_NAME,
            isFatal=is_fatal,
            isHealthy=is_healthy,
            message=message,
            recommendedAction=recommended_action,
            entitiesImpacted=entities,
            generatedTimestamp=timestamp,
            nodeName=self._node_name,
            processingStrategy=self._processing_strategy,
        )

    def _send_with_retries(self, health_events: pb.HealthEvents) -> bool:
        delay = INITIAL_DELAY

        for attempt in range(MAX_RETRIES):
            try:
                with grpc.insecure_channel(f"unix://{self._socket_path}") as channel:
                    stub = pb_grpc.PlatformConnectorStub(channel)
                    stub.HealthEventOccurredV1(health_events, timeout=RPC_TIMEOUT)
                    log.info("Health event sent successfully")
                    return True
            except grpc.RpcError as e:
                log.warning(f"Failed to send health event (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    sleep(delay)
                    delay *= BACKOFF_FACTOR

        return False
