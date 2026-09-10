# End-to-end run at 50,000 nodes

Full fault-handling pipeline exercised against a 50,113-node fleet at production
object shape, 2026-09-02.

## Fleet

| | |
|---|---|
| Nodes | 50,113 (50,000 simulated + 113 real) |
| Node object size | 52,696 B median, production shape: 180 labels, 40 conditions, 50 images |
| Pods | 551,326 (11.0 per node, DaemonSet fan-out) |
| Node creation rate | 6,243 nodes/s, zero failures |
| Kubernetes | EKS v1.34, control plane tier 4XL |

## Event injection

Events are written to the `HealthEvents` collection by `cmd/mongo-connection-pool`,
one goroutine per simulated connector, each holding its own MongoDB client. 200
connectors write for nodes `pb-860000` through `pb-860199` at 0.05 events/s each,
about 10 events/s in total.

Distributions are taken from production. The generated mix matched the targets
within half a percentage point on every action:

| RecommendedAction | target | generated |
|---|---|---|
| COMPONENT_RESET (2) | 62.38% | 61.9% |
| CONTACT_SUPPORT (5) | 34.52% | 34.8% |
| RESTART_VM (15) | 2.37% | 2.2% |
| RESTART_BM (24) | 0.73% | 0.8% |
| REPLACE_VM (25) | 0% | 0% |

Fatal share 16.7% against a 16.75% target. Fatal events are drawn from the
observed check distribution, dominated by `SysLogsXIDError` with error code 95
at 61.31% and `GpuThermalMarginWatch` at 30.07%.

## Result

The pipeline ran end to end. All 200 injected nodes were cordoned, node-drainer
evicted pods, and fault-remediation labelled nodes and created maintenance CRs.

| stage | evidence |
|---|---|
| Ingestion | ~10 events/s, no unmarshal or processing errors |
| fault-quarantine | 200 of 200 injected nodes cordoned |
| node-drainer | 3,339 pod evictions |
| fault-remediation | node labelling and maintenance CR creation |

## Latency

Read from each component's own histograms. Prometheus was unavailable during the
run, so these are cumulative since each pod started rather than rate-windowed,
and the quantiles land on bucket boundaries rather than being interpolated.

| component | metric | samples | p50 | p99 | mean |
|---|---|---|---|---|---|
| fault-quarantine | event handling | 89,403 | 5 ms | 100 ms | 4.7 ms |
| fault-quarantine | node quarantine | 200 | 50 ms | 100 ms | 32 ms |
| node-drainer | event handling | 46,197 | 10 ms | 250 ms | 14.1 ms |
| node-drainer | pod eviction | 3,339 | 100 ms | 400 ms | 57 ms |
| fault-remediation | event handling | 63,511 | 100 ms | 1.0 s | 137 ms |
| fault-remediation | reconcile | 63,511 | 100 ms | 600 ms | 137 ms |

Quarantine completes in 50 ms at the median and 100 ms at p99 on a 50,000-node
fleet, so the cordon path does not degrade with fleet size.

fault-remediation is the slowest stage at 137 ms mean and 1.0 s p99, roughly ten
times the others. It runs with `MAX_CONCURRENT_RECONCILES` fixed at 1, and its
work queue is keyed per event rather than per node, so raising that value is not
safe without first changing the key scope and the inline resume-token
checkpointing.

## kubernetes-object-monitor memory

Measured on the same fleet, both arms with a fresh pod and eight minutes to fill,
sampled every two minutes to distinguish a plateau from a filling cache.

| build | resident |
|---|---|
| v1.21.0 released | 25.31 GB |
| xrfxlp/1718 | 10.35 GB |
| reduction | 2.45x, 14.96 GB |

The 1718 build carries two changes: cache transforms derived from the policy CEL,
and serving watched objects from the informer cache. The second matters as much
as the first. controller-runtime bypasses the cache for unstructured reads unless
`Cache.Unstructured` is set, so before the change every reconcile of every object
issued a live GET. Measured on the new build, 380,239 reconciles produced 33 API
GETs, all at startup; the previous behaviour would have produced roughly 380,000.
At the measured 179 reconciles per second that is the difference between 179 GETs
per second of steady API load from this component and none.

The 2.45x figure is specific to this policy set. These policies read
`status.conditions`, `status.phase` and `spec.nodeName`; a policy set touching
more fields prunes less.

## Schema requirements for synthetic events

Two mismatches each stopped the pipeline silently, and both are easy to hit when
generating events outside platform-connector.

The document is the shape `model.HealthEventWithStatus` serialises to, not a flat
event. The event nests under `healthevent`, and the protobuf types carry no bson
tags, so field names are their Go names lowercased.

`generatedtimestamp` decodes into `*timestamppb.Timestamp` and must be written as
a nested document of `seconds` and `nanos`. A BSON datetime there fails to
unmarshal, and fault-quarantine logs "Event processing failed, but still marking
as processed to proceed ahead", so every event is dropped while the component
looks healthy.

`agent` must match the deployed ruleset. The ruleset used here matches
`event.agent == 'gpu-health-monitor' && event.componentClass == 'GPU' &&
event.isFatal == true`. Events with any other agent decode correctly and match
nothing, which from outside is indistinguishable from a pipeline that is not
running.

`healtheventstatus.userpodsevictionstatus` must be an empty document rather than
null. node-drainer promotes an event with `$set` on
`healtheventstatus.userpodsevictionstatus.status`, which cannot create a field
inside a null.

## Limitations

Repeated events for the same node are de-duplicated: after the first drain,
node-drainer logs "Full drain previously completed for node as part of old event,
skipping drain". Each node received roughly 75 events during the run, so the
eviction figures represent one drain per node rather than sustained drain
throughput. Measuring that needs a larger node range rather than a higher rate.

No ExternalRemediationRequest resources were created. On this path
fault-remediation creates maintenance CRs instead, and the actions that would
drive external remediation are 3% of the mix by design.

fault-remediation logs a bare `Reconciler error {}` with no detail, so failures
there are silent and retried.

Prometheus could not stay up at this fleet size on a 64Gi limit and was raised to
96Gi during the run, so rate-windowed quantiles are not available for this run.
