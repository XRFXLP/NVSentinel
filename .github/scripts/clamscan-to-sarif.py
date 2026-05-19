#!/usr/bin/env python3
#
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

"""Convert ClamAV stdout into a minimal SARIF 2.1.0 report.

Contract:
- argv[1] is the workflow/job automation id used in SARIF automationDetails.
- argv[2] is a clamscan output file generated with infected lines enabled.
- argv[3] is the SARIF output path consumed by upload-sarif.
"""

import json
import sys
from pathlib import Path


def result_from_line(line):
    if " FOUND" not in line:
        return None

    path_text, finding = line.rsplit(": ", 1)
    finding = finding.removesuffix(" FOUND").strip()
    path = path_text.removeprefix("./")

    return {
        "ruleId": "clamav-malware",
        "level": "error",
        "message": {"text": f"ClamAV detected {finding}"},
        "locations": [
            {
                "physicalLocation": {
                    "artifactLocation": {"uri": path},
                    "region": {"startLine": 1},
                }
            }
        ],
    }


def main():
    if len(sys.argv) != 4:
        print("usage: clamscan-to-sarif.py <job-name> <clamscan-output> <sarif-output>", file=sys.stderr)
        return 2

    job_name, input_path, output_path = sys.argv[1:]
    lines = Path(input_path).read_text(encoding="utf-8", errors="replace").splitlines()
    results = [result for line in lines if (result := result_from_line(line)) is not None]

    sarif = {
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "ClamAV",
                        "informationUri": "https://www.clamav.net/",
                        "rules": [
                            {
                                "id": "clamav-malware",
                                "name": "ClamAV malware finding",
                                "shortDescription": {"text": "ClamAV detected malware or an unwanted file"},
                                "defaultConfiguration": {"level": "error"},
                            }
                        ],
                    }
                },
                "automationDetails": {"id": job_name},
                "results": results,
            }
        ],
    }

    Path(output_path).write_text(json.dumps(sarif, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
