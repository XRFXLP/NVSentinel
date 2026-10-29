#!/bin/bash
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

# Usage: remove-pending-helm-releases.sh <namespace>/<release>...
#
# Removes each given Helm release that is in a pending state.
#
# When tilt ci stops an attempt while Helm installs or upgrades a release, the release stays in a
# pending state. Each later helm upgrade of that release then fails with "another operation
# (install/upgrade/rollback) is in progress", so the next attempt fails too.
set -euo pipefail

for arg in "$@"; do
    namespace=${arg%%/*}
    release=${arg#*/}

    pending=$(helm list -n "$namespace" --pending --short --filter "^${release}\$")
    if [ -n "$pending" ]; then
        echo "Removing Helm release $release in namespace $namespace, which is in a pending state"
        helm uninstall "$release" -n "$namespace" --wait
    fi
done
