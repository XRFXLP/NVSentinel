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

# Prints the go test -skip pattern for one E2E CI job: the tests in e2e-shards.yaml that are not in
# the list of this job or in "all". Prints nothing if no test is skipped.
#
# Usage: e2e-skip-pattern.sh <E2E_JOB_ID>
#
# The script fails if e2e-shards.yaml is not complete and correct: each key is "all" or a job in the
# E2E workflow matrix, each test function in tests/ is listed, each listed test exists, and no test
# is listed twice for a job. Otherwise a new test runs in every job, and a misspelled job name makes
# that job skip its tests.

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

# One "job <key> <type>" line for each key, and one "test <key> <number of items that are not
# names> <tests>" line for each list. yq keeps a duplicate key, so the checks below find it.
entries=$(yq 'to_entries[] | (("job " + .key + " " + (.value | tag)), (select(.value | tag == "!!seq") | "test " + .key + " " + (.value | map(select(tag != "!!str")) | length | tostring) + " " + (.value | map(select(tag == "!!str")) | join(" "))))' "$shards")

awk -v jobs="$jobs" -v tests="$tests" -v job="$job" -v shards="$shards" '
    function fail(msg) {
        printf "Error: %s: %s\n", shards, msg > "/dev/stderr"
        failed = 1
        exit 1
    }
    BEGIN {
        n = split(jobs, list, "\n"); for (i = 1; i <= n; i++) known[list[i]] = 1
        known["all"] = 1
        ntests = split(tests, defined, "\n"); for (i = 1; i <= ntests; i++) exists[defined[i]] = 1
    }
    $1 == "job" {
        if (!($2 in known)) fail($2 " is not \"all\" or a job in the E2E matrix")
        if ($2 in keys) fail($2 " is listed twice")
        if ($3 != "!!seq") fail($2 " is not a list of tests")
        keys[$2] = 1
    }
    $1 == "test" {
        if ($3 != 0) fail($2 " has an item that is not a test name")
        for (i = 4; i <= NF; i++) {
            if (!($i in exists)) fail($i " in " $2 " is not a test function in tests/")
            if (($2, $i) in pairs) fail($i " is listed twice in " $2)
            pairs[$2, $i] = 1
            if ($2 == "all") inall[$i] = 1; else injob[$i] = 1
            if ($2 == "all" || $2 == job) runs[$i] = 1
        }
    }
    END {
        if (failed) exit 1
        for (i = 1; i <= ntests; i++) {
            t = defined[i]
            if (!(t in inall) && !(t in injob)) fail(t " is not listed. Add it to its jobs, or to \"all\"")
            if ((t in inall) && (t in injob)) fail(t " is in \"all\" and in a job. Keep it in one of them")
            if (!(t in runs)) skip = skip (skip == "" ? "" : "|") t
        }
        if (skip != "") print "^(" skip ")$"
    }
' <<< "$entries"
