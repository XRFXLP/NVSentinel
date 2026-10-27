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

# Prints the go test -skip pattern for one E2E CI job: the tests that e2e-shards.txt gives to the
# other jobs. Prints nothing if no test is skipped.
#
# Usage: e2e-skip-pattern.sh <E2E_JOB_ID>
#
# The script fails if the job or a job in e2e-shards.txt is not in the E2E workflow matrix. Otherwise
# a misspelled job name in e2e-shards.txt makes every job skip that test.

set -euo pipefail

job=${1:?usage: $0 <E2E_JOB_ID>}
dir=$(cd "$(dirname "$0")" && pwd)
shards="$dir/e2e-shards.txt"
workflow="$dir/../.github/workflows/e2e-test.yml"

# Same expression as E2E_JOB_ID in the workflow.
jobs=$(yq '.jobs["e2e-test"].strategy.matrix.include[] | [.arch, .datastore, .platform_connector // ""] | join(" ")' "$workflow" |
    awk '{ print $1 "-" $2 ($3 == "deployment" ? "-deployment-pc" : "") }')

if ! grep -qxF "$job" <<< "$jobs"; then
    echo "Error: job $job is not in the matrix of $workflow" >&2
    exit 1
fi

awk -v jobs="$jobs" -v job="$job" '
    BEGIN { n = split(jobs, list, "\n"); for (i = 1; i <= n; i++) known[list[i]] = 1 }
    /^[[:space:]]*(#|$)/ { next }
    NF != 2 || !($2 in known) {
        printf "Error: %s line %d: expected a test name and a job in the E2E matrix: %s\n", FILENAME, FNR, $0 > "/dev/stderr"
        failed = 1
        exit 1
    }
    $2 != job { skip = skip (skip == "" ? "" : "|") $1 }
    END { if (!failed && skip != "") print "^(" skip ")$" }
' "$shards"
