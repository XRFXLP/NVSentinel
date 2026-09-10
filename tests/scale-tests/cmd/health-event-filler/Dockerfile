# Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build from the repository root so the data-models replace directive resolves:
#   docker build -f tests/scale-tests/cmd/health-event-filler/Dockerfile -t health-event-filler .
FROM golang:1.26 AS builder

WORKDIR /src

COPY data-models/ ./data-models/
COPY tests/scale-tests/cmd/health-event-filler/ ./tests/scale-tests/cmd/health-event-filler/

WORKDIR /src/tests/scale-tests/cmd/health-event-filler

ENV GOWORK=off
RUN CGO_ENABLED=0 go build -trimpath -o /out/health-event-filler .

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/health-event-filler /health-event-filler

ENTRYPOINT ["/health-event-filler"]
