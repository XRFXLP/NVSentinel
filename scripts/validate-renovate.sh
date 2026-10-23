#!/usr/bin/env bash

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

# Checks the Renovate setup for .versions.yaml:
#   1. Every pin has a "# renovate:" annotation on the line above it, whose depType is the
#      pin's section, unless the pin is in UNTRACKED. Renovate skips a pin without one silently.
#   2. Every annotated section has a group rule in .github/renovate.json5.
#   3. renovate-config-validator accepts .github/renovate.json5 as repository config, using the
#      pinned Renovate image. Given no path, the validator finds the file and does not treat it
#      as global config.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VERSIONS_FILE="${REPO_ROOT}/.versions.yaml"
CONFIG_FILE="${REPO_ROOT}/.github/renovate.json5"

# Pins that Renovate does not update, as section.key:
#   - go, python: toolchain upgrades are their own change.
#   - docker_buildx: 'latest' is not a version.
#   - renovate: tag@digest, which the annotation regex does not read.
UNTRACKED="languages.go languages.python container_tools.docker_buildx container_tools.renovate"

errors=0

echo "Checking Renovate annotations in .versions.yaml..."

annotation_errors=$(awk -v untracked="${UNTRACKED}" '
  BEGIN {
    n = split(untracked, list, " ")
    for (i = 1; i <= n; i++) skip[list[i]] = 1
  }
  /^[a-z_]+:[[:space:]]*$/ { section = $1; sub(/:$/, "", section); prev = ""; next }
  /^  [a-z_]+:[[:space:]]*["\x27]/ {
    key = $1; sub(/:$/, "", key)
    id = section "." key
    annotated = (prev ~ /^  # renovate: datasource=[^ ]+ depName=[^ ]+ depType=[^ ]+$/)
    if (skip[id]) {
      if (annotated) printf "%s:%d: %s is in UNTRACKED but has an annotation\n", FILENAME, NR, id
    } else if (!annotated) {
      printf "%s:%d: %s has no \"# renovate:\" annotation on the line above\n", FILENAME, NR, id
    } else {
      want = "depType=" section
      if (substr(prev, length(prev) - length(want) + 1) != want)
        printf "%s:%d: %s annotation must end with %s\n", FILENAME, NR, id, want
      if ($0 ~ /^  [a-z_]+:[[:space:]]*["\x27][^"\x27]*:/)
        printf "%s:%d: %s value contains \":\", which the annotation regex does not read\n", FILENAME, NR, id
    }
  }
  prev ~ /^  # renovate:/ && $0 !~ /^  [a-z_]+:[[:space:]]*["\x27]/ {
    printf "%s:%d: annotation is not followed by a quoted pin\n", FILENAME, NR - 1
  }
  { prev = $0 }
' "${VERSIONS_FILE}")

if [ -n "${annotation_errors}" ]; then
  echo "${annotation_errors}" >&2
  errors=1
fi

echo "Checking that every annotated section has a group rule..."

while read -r dep_type; do
  if ! grep -qF "matchDepTypes: [\"${dep_type}\"]" "${CONFIG_FILE}"; then
    echo "${CONFIG_FILE}: no packageRules entry with matchDepTypes: [\"${dep_type}\"]" >&2
    errors=1
  fi
done < <(grep -oE 'depType=[a-z_]+' "${VERSIONS_FILE}" | sort -u | cut -d= -f2)

echo "Validating .github/renovate.json5 with renovate-config-validator..."

RENOVATE_VERSION="$(yq '.container_tools.renovate' "${VERSIONS_FILE}")"

if ! docker run --rm \
  -v "${REPO_ROOT}/.github:/usr/src/app/.github:ro" \
  "ghcr.io/renovatebot/renovate:${RENOVATE_VERSION}" \
  renovate-config-validator --strict; then
  errors=1
fi

if [ "${errors}" -ne 0 ]; then
  echo "Renovate configuration check failed." >&2
  exit 1
fi

echo "Renovate configuration is valid."
