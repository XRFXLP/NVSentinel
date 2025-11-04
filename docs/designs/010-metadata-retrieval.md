# ADR-010: Design - Metadata Retrieval

## Context

In the current system, while sending health events we're only sending following informations from:
1. GPU health monitor:
    - GPU ID
2. Syslog health monitor
   - GPU ID (not always)
   - PCI ID
   - NVSwitch number
   - Link number

Other than this from platform connector, we attach the node name associated with the node.

Problem with this are:
1. With increasing number of clusters, it gets hard to disintguish nodes on basis of names along as in case of OCI and AWS, the names are distinguish by IPs which can repeat across fleet.
2. Information is not always consistent, for instance: GPU health monitor sends GPU ID for the event, but syslog health monitor sends PCI ID for a XID error. 
3. Furthermore, in environments that have multiple interconnected racks, comparing health events across racks becomes cumbersome as manual lookup is needed to determine the which rack the health event originated from.
4. We need more information for instance like chassis number, its GB an B specific thing I believe so won't be available on H100s and such, but in case of NVL-lrealted issues (e.g. Switch) such label can aid users in avoiding the issues during workloads scheduling.

## Solution Proposed

We can classify missing data into two categories:
1. Node level information
2. Entity level information like GPU ID / UUID.

We