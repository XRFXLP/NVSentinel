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

# Prints the go test -skip pattern for one E2E CI job: the tests in e2e-shards.yaml that do not name
# this job. Prints nothing if no test is skipped.
#
# Usage: e2e-skip-pattern.sh <E2E_JOB_ID>
#
# The script fails if e2e-shards.yaml is not complete and correct: each test function in tests/ is
# listed once, each listed test exists, and each job is in the E2E workflow matrix. Otherwise a new
# test runs in every job, and a misspelled job name makes that job skip the test.

set -euo pipefail

job=${1:?usage: $0 <E2E_JOB_ID>}
dir=$(cd "$(dirname "$0")" && pwd)
shards="$dir/e2e-shards.yaml"
workflow="$dir/../.github/workflows/e2e-test.yml"

# Same expression as E2E_JOB_ID in the workflow.
jobs=$(yq '.jobs["e2e-test"].strategy.matrix.include[] | [.arch, .datastore, .platform_connector // ""] | join(" ")' "$workflow" |
    awk '{ print $1 "-" $2 ($3 == "deployment" ? "-deployment-pc" : "") }')

if ! grep -qxF "$job" <<< "$jobs"; then
    echo "Error: job $job is not in the matrix of $workflow" >&2
    exit 1
fi

# The top-level test functions in all packages of tests/.
tests=$(grep -rhoE '^func Test[A-Za-z0-9_]+\(t \*testing\.T\)' --include='*_test.go' "$dir" |
    sed -E 's/^func (Test[A-Za-z0-9_]+).*/\1/')

# One line for each test: the test name, then its jobs or "all". yq keeps a duplicate key, so the
# check below finds it.
entries=$(yq 'to_entries[] | [.key] + ([.value] | flatten) | join(" ")' "$shards")

awk -v jobs="$jobs" -v tests="$tests" -v job="$job" -v shards="$shards" '
    function fail(msg) {
        printf "Error: %s: %s\n", shards, msg > "/dev/stderr"
        failed = 1
        exit 1
    }
    BEGIN {
        n = split(jobs, list, "\n"); for (i = 1; i <= n; i++) known[list[i]] = 1
        ntests = split(tests, defined, "\n"); for (i = 1; i <= ntests; i++) exists[defined[i]] = 1
    }
    {
        if (!($1 in exists)) fail($1 " is not a test function in tests/")
        if ($1 in seen) fail($1 " is listed twice")
        if (NF < 2) fail($1 " has no job")
        seen[$1] = 1
        runs = (NF == 2 && $2 == "all")
        if (!runs) {
            for (i = 2; i <= NF; i++) {
                if (!($i in known)) fail($1 ": " $i " is not a job in the E2E matrix")
                if ($i == job) runs = 1
            }
        }
        if (!runs) skip = skip (skip == "" ? "" : "|") $1
    }
    END {
        if (failed) exit 1
        for (i = 1; i <= ntests; i++) {
            if (!(defined[i] in seen)) fail(defined[i] " is not listed. Add it with its jobs, or with \"all\"")
        }
        if (skip != "") print "^(" skip ")$"
    }
' <<< "$entries"
