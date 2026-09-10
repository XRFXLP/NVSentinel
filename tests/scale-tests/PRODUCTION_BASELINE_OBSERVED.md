# Production Baselines and the Representative Model

Measured characteristics of four production clusters across three cloud providers, and the representative node, pod and workload model derived from them for scale testing.

Section 6 is the model. Sections 1-5 are the measurements it comes from.

Surveyed 2026-09-01, read-only. All figures are medians unless stated.

## Clusters measured

| | Azure | AWS | OCI-1 | OCI-2 |
|---|---|---|---|---|
| Kubernetes | v1.33 | v1.34 | v1.33 | v1.33 |
| Nodes | 228 | 29 | 164 | 400 |
| Pods | 6,871 | 675 | 5,357 | 12,835 |
| Namespaces | 38 | 45 | 41 | 178 |
| Pods per node | 29.7 | 16.4 | 32.7 | 32.1 |

The AWS cluster was drained when sampled, so it contributes object shapes but not workload density.

---

## 1. Node shape

| Field | Azure | AWS | OCI-1 | OCI-2 |
|---|---|---|---|---|
| total | 52,839 B | 27,184 B | 54,816 B | 49,940 B |
| `managedFields` | 20,099 (38%) | 7,425 (27%) | 17,996 (33%) | 15,924 (32%) |
| `status` | 19,688 (37%) | 11,128 (41%) | 22,630 (41%) | 19,780 (40%) |
| ↳ `status.images` | 8,608 | 7,760 | 13,152 | 12,172 |
| ↳ `status.conditions` | 10,336 | 2,249 | 8,527 | 5,825 |
| `labels` | 8,926 (17%) | 3,904 (14%) | 9,230 (17%) | 9,248 (19%) |
| `annotations` | 3,458 | 2,213 | 4,452 | 4,350 |
| labels / conditions / images (count) | 183 / 48 / 50 | 76 / 10 / 36 | 180 / 39 / 50 | 182 / 28 / 50 |

A GPU worker node is ~50 KB, carrying 180 labels, 40 conditions and 50 cached images. Bytes are spread across three roughly equal thirds: `managedFields`, `status`, and `metadata` labels plus annotations.

## 2. Pod shape

| Namespace class | Cluster | total | `managedFields` | `spec` | `status` | `annotations` |
|---|---|---|---|---|---|---|
| user workload | OCI-1 | 388,355 B | 27,148 | 357,377 (92%) | 2,348 | 514 |
| user workload | AWS | 65,045 | 5,487 | 57,574 (88%) | 485 | 547 |
| user workload | OCI-2 | 51,493 | 7,152 | 40,282 (78%) | 2,287 | 724 |
| user workload | Azure | 33,542 | 12,390 | 13,087 | 6,611 | 513 |
| platform services | AWS | 17,995 | 6,019 | 6,306 | 5,122 | 2 |
| `kube-system` | AWS | 14,494 | 5,109 | 5,080 | 3,503 | 2 |
| `gpu-operator` | Azure | 9,971 | 3,362 | 3,520 | 2,381 | 2 |

Two populations. **System and platform pods** are 10-18 KB, dominated by `managedFields` and a modest `spec`. **User workload pods** are 33-65 KB with 78-92% of bytes in `spec` — large container definitions, env and volumes. Annotations are near-empty on both, commonly 2 bytes.

OCI-1's 388 KB is one workload's shape; the 33-65 KB band covers the rest.

## 3. Workload density

node-drainer evicts pods outside `systemNamespaces` that are not DaemonSet-owned. On a GPU node that set is the workload pods, bounded by GPUs per node — 8 on H100-generation, 4 on newer generations. An occupied node carries 4-8; an idle node carries none.

| | Azure | AWS *(drained)* | OCI-1 | OCI-2 |
|---|---|---|---|---|
| Pods/node | 29.7 | 16.4 | 32.7 | 32.1 |
| Evictable p25 / median / p75 / p90 | 1 / 1 / 1 / 2 | 0 / 0 / 5 / 14 | 4 / 4 / 4 / 5 | 0 / 0 / 1 / 3 |
| Nodes with zero evictable | 32 / 228 | 17 / 29 | 0 / 164 | 287 / 400 |

OCI-1 is fully occupied and sits at 4 — one workload pod per GPU on a 4-GPU node. The lower medians elsewhere are idle capacity: 72% of OCI-2's nodes had nothing scheduled at sample time. Topology is constant across the fleet; occupancy is what varies.

Workload composition, which determines whether a node can drain at all:

- Azure: bare pods, no owner references
- OCI-1: bare pods 661, ReplicaSet 54, alongside DaemonSet 809
- OCI-2: MPIJob 1,342, Workflow 212, ReplicaSet 111, alongside DaemonSet 2,364

MPIJob and Workflow are distributed training jobs; bare pods are long-lived by construction. Neither completes on its own, so under the default `AllowCompletion` eviction mode those nodes do not finish draining.

## 4. Retained bytes per component

Each component's informer transform applied to real production objects.

| Component | Azure | AWS | OCI-1 | OCI-2 |
|---|---|---|---|---|
| kubernetes-object-monitor (no transform) | 52,839 | 27,184 | 54,816 | 49,940 |
| fault-quarantine | 33,062 (63%) | 13,830 (51%) | 32,054 (58%) | 30,000 (60%) |
| janitor *(conditional, see below)* | 9,259 (18%) | 4,235 (16%) | 9,535 (17%) | 9,600 (19%) |
| node-drainer | 123 (0.2%) | 122 (0.4%) | 88 (0.2%) | 89 (0.2%) |

fault-quarantine retains 30-33 KB per node, about 3.3 GiB at 100k nodes. Its retained set is fixed by the CEL surface: `NodeRuleEvaluator` is declared as `cel.Variable(nodeObjKey, cel.AnyType)` and evaluated against the cached node, so a rule may reference any field in metadata or spec, resolved at runtime. `status` sits outside that surface and is the part it strips.

node-drainer keeps identity plus one allow-listed annotation, at 0.2%. Pod transforms are effective across the board because user pods keep their bytes in `spec`, which every pod transform discards.

janitor's figure is what its transform would retain, not what it holds. janitor registers a Node cache with a transform that keeps labels, but no controller watches Nodes: all four watch custom resources, and the only Node read in the component is a single cached `Get` in the TerminateNode reconciler. controller-runtime starts an informer on first use, so until a TerminateNode resource reconciles, janitor caches no nodes at all and its memory is independent of both fleet size and node size.

Measured on a 5,105-node fleet, janitor sat at 135-150 MB across node sizes from 29.7 KB to 69.2 KB, and had issued 16 API GETs in total with `rebootnode` as its only active workqueue. A node LIST at that fleet size would be unmistakable in both figures.

The TerminateNode controller is registered unconditionally, so this is a step rather than a permanent saving: the first TerminateNode reconcile starts the informer and moves janitor from the idle figure to the full retained figure. Both branches belong in sizing. The initial LIST that starts the informer is not a concern -- the API server serves nodes via streaming watch-list on v1.34, and it happens once per process.

## 5. Convergence

| Parameter | Azure / OCI-1 / OCI-2 | Spread |
|---|---|---|
| Node object size | 52.8 / 54.8 / 49.9 KB | ±5% |
| fault-quarantine retained/node | 33.1 / 32.1 / 30.0 KB | ±5% |
| Labels per node | 183 / 180 / 182 | ±1% |
| Pods per node | 29.7 / 32.7 / 32.1 | ±5% |
| Evictable pods per node | 1 / 4 / 0 | occupancy-dependent |

Four parameters agree within 5% across two cloud providers, so they are fixed values in the model. Evictable density tracks occupancy rather than topology, so it is a swept axis.

---

## 6. The representative model

Fixture parameters for scale testing, derived from sections 1-5.

**Node** — target 50 KB:

| Property | Value | Placement in the object |
|---|---|---|
| Total size | 50 KB | |
| `managedFields` | 18 KB | synthesised, ~8 entries |
| `status.images` | 10 KB | 50 image entries |
| `status.conditions` | 8 KB | 40 conditions |
| `labels` | 9 KB | 180 labels |
| `annotations` | 4 KB | ~14 annotations |
| `spec` | <1 KB | taints, unschedulable |

**Pod** — two profiles:

| Profile | Total | Placement |
|---|---|---|
| User workload | 50 KB | ~80% `spec` (containers, env, volumes), 7 KB `managedFields`, negligible annotations |
| System / platform | 14 KB | 5 KB `managedFields`, 5 KB `spec`, 3.5 KB `status` |

**Workload density** — 30 pods per node, of which:

| Class | Count | Evicted by node-drainer |
|---|---|---|
| DaemonSet and system-namespace | ~24 | no |
| User workload | **4-8 occupied, 0 idle** | yes |

Sweep evictable at **4 and 8**; use **0** for the idle case.

**Derived component sizing** at 100k nodes, from section 4:

| Component | Retained/node | 100k nodes |
|---|---|---|
| kubernetes-object-monitor | 50 KB | ≈21.6 GB |
| fault-quarantine | 32 KB | ≈5.4 GB |
| janitor, before any TerminateNode reconcile | 0 | ≈150 MB, flat |
| janitor, after the first TerminateNode reconcile | 9.5 KB | ≈1.1 GB |
| node-drainer | 0.1 KB | ≈3.6 GB |

The 100k column is measured against node count, not derived from retained bytes. Each figure is `base + slope x nodes`, fitted to resident memory at 5,105 and 40,105 nodes with the fleet at production node size:

| Component | base | per node | 100k |
|---|---|---|---|
| kubernetes-object-monitor | 2.16 GB | 194 KB | 21.6 GB |
| fault-quarantine | 0.22 GB | 51 KB | 5.4 GB |
| node-drainer | 0.07 GB | 36 KB | 3.6 GB |
| janitor | 0.09 GB | 10 KB | 1.1 GB |

An earlier version of this table projected 60 GB for kubernetes-object-monitor. That came from measuring how memory grew with node object *size* at a fixed fleet of 5,000 nodes, which gave 9.95 bytes per byte of node, and then multiplying by node count. At a small fleet much of resident memory is baseline and per-object bookkeeping, and that weight sits inside the size slope, so multiplying by count charges it once per node. A slope measured along the size axis is only valid along the size axis. Every component projected the same way was high by a similar factor.

Wire bytes; Go in-memory representation is larger by the `inflation_factor` term, which the padding sweep measures. Measured inflation on a 5,105-node fleet is 9.95x for kubernetes-object-monitor and 2.59x for fault-quarantine, per byte of node object.

## 7. Applying the model

1. Set node fixtures to 50 KB with the field placement in section 6 — labels and `managedFields` carry the bulk, not annotations.
2. Set user pod fixtures to 50 KB with ~80% in `spec`.
3. Synthesise `managedFields` on both; it is a third of a node and a third of a system pod.
4. Sweep evictable pods/node at 4 and 8.
5. Budget ~3.3 GiB for fault-quarantine and ~5 GiB for kubernetes-object-monitor at 100k nodes.
6. Cover `AllowCompletion` with non-completing pods, since MPIJob, Workflow and bare pods are the dominant production workload shapes.

## 8. Method notes

- Read-only: `kubectl get --raw` with `limit` and `continue`, plus `kubectl get ns/nodes`.
- Collection totals come from `metadata.remainingItemCount` on a `limit=1` request, avoiding a full list against a production API server.
- `remainingItemCount` does not work with `fieldSelector` — filtering is applied after pagination, so `limit=1` returns 1 regardless. Per-node pod counts need a full per-node fetch, or better, scan non-system namespaces and group by `spec.nodeName`.
- `?limit=N` returns etcd key order, not a random sample. Sample per namespace.
- Evictable density needs a census or large stratified sample. Six-node samples on two clusters gave 9.7 and 0.8 evictable/node against census values of 2.97 and 1.54.
- Byte counts are compact JSON, i.e. wire size.

## 9. Scope

- Evictable figures are censuses; object-size figures are samples of 29-40 nodes and 60 pods per namespace.
- Bare-metal and GCP deployments were not measured.
- One point in time; workload mix drifts.

---

## 10. Control plane ceiling, measured on the benchmark cluster

The figures in sections 1-9 come from production clusters. This section is different: it records what happened when the representative model was driven to 90,000 nodes on an EKS v1.34 cluster at control plane tier XL, and it sets the fleet size for the end-to-end test.

### The ceiling

The test ramped in 10,000-node steps to 90,105 nodes carrying 991,237 pods, all at the production node shape of ≈53.6 KB. etcd's allocated file cycled between 12.8 GB and 19.7 GB, with EKS compacting and defragmenting on its own. Steady state was sustainable: node heartbeats at one write per node per 300 seconds produce 13.5 MB/s, or ≈4 GB of revisions per five-minute compaction window.

It failed when a single additional workload was added on top. The labeler began its first pass across the fleet, writing a label to every node. Each write creates a new revision of a 53.6 KB object, so one full pass is ≈4.8 GB of fresh revisions arriving faster than compaction could reclaim. etcd raised its NOSPACE alarm and the cluster went read-only:

```
etcdserver: mvcc: database space exceeded
```

The quota is above 16 GB, since a 19.66 GB allocated peak was sustained without an alarm, but not far above. All scaling tiers carry the same database limit, so raising the tier does not move this ceiling.

### The alarm is not observable from inside the cluster

No metric the API server exposes reports it. There is no `etcd_server_quota_backend_bytes` and no alarm gauge, and on a managed control plane `etcdctl alarm list` is unreachable. The health endpoints report the opposite of the truth:

```
[+]etcd ok
[+]etcd-readiness ok
```

while every write is rejected. The only in-cluster detection is to attempt a write and read the error. `apiserver_storage_size_bytes` is useful as a growth signal, but it reports the file etcd has allocated rather than the quota figure, so it runs above the number that matters.

### Recovery

Reads and deletes continued working throughout; only creates and updates were rejected. Deleting 60,000 nodes freed enough keys for etcd to compact and defragment, allocated size fell from 17.5 GB to 12.9 GB, and the alarm disarmed on its own. Total write outage was about ten minutes, with roughly two minutes between freeing space and writes resuming. No support intervention was needed.

### Fleet size for the end-to-end test: 80,000 nodes

| nodes | pods | estimated etcd | |
|---|---|---|---|
| 25,000 | 275,000 | ≈7.1 GB | comfortable |
| 50,000 | 550,000 | ≈8.0 GB | comfortable |
| **80,000** | **880,000** | **≈12.1 GB** | **test ceiling** |
| 90,000 | 990,000 | ≈13.4 GB | reached, then failed under added write load |
| 100,000 | 1,100,000 | ≈14.8 GB | not attempted |

80,000 is the last size measured with margin for a component to do a full pass over the fleet without exhausting the quota. The estimate uses `DB_GB = 1.21 + 136 KB x nodes`, fitted to two settled CloudWatch readings and accurate to within a few percent at every step of the ramp.

### Supporting systems fail before NVSentinel does

Five components broke at this scale while NVSentinel's own workqueues stayed at zero depth and the API server served 5,137 requests per second with no throttling and no errors.

| component | failure | scale |
|---|---|---|
| kube-state-metrics | scrape stopped completing, all `kube_*` series lost | ≈80k nodes |
| CloudWatch control plane metrics | publication stopped for all three series | ≈75k nodes |
| vpc-resource-controller | webhook refusing connections, admission failing open | ≈90k nodes |
| kwok-controller | OOMKilled, 44 restarts, whole fleet went NotReady | ≈20k nodes at 8Gi |
| Prometheus | readiness failing during WAL replay, 30 restarts | ≈90k nodes |

Only the per-node kubelet summary API, read through the API server proxy, remained usable for measurement. Any sizing exercise at this scale should establish that path before it needs it.

### Pods stranded on deleted nodes are not reclaimed at scale

Draining nodes leaves their pods behind. Kubernetes garbage collection is supposed to reclaim them, and on a small fleet it does. After deleting 60,000 nodes it did not: the pod population sat at roughly 927,000 against 30,113 nodes, and a clean three-minute measurement with nothing else running showed zero reclamation.

The consequences are not obviously connected to pods, which is what makes this expensive to diagnose:

- The scheduler stopped placing new pods at all. Pods were created with no `PodScheduled` condition of any kind, not even `Unschedulable`, and had to be bound by hand through the Binding API.
- The StatefulSet controller's informer cache diverged from etcd. It reported `readyReplicas: 3` for a StatefulSet whose three pods had been deleted, emitted `FailedDelete` for pods that no longer existed, and would not recreate them. `observedGeneration` tracked correctly throughout, so the object looked healthy.
- etcd held roughly 6 GB for the stranded objects.

None of this appears in events, conditions or health endpoints. It presents as a workload that silently refuses to start.

Deleting them directly is far faster than waiting. Pods are indexed on `spec.nodeName`, so one request per dead node finds them without listing the full collection:

| method | rate |
|---|---|
| Kubernetes garbage collection | 13/s falling to 0/s |
| `orphan-reaper`, 200 workers, in-cluster | **1,768/s** |

`tests/scale-tests/cmd/orphan-reaper` cleared 589,149 pods in 5 minutes 35 seconds with 45 failures, returning the fleet to exactly 11.0 pods per node. The scheduler resumed placing pods within a minute of it finishing.

Run it from inside the cluster. An earlier attempt at 120 workers through `kubectl proxy` saturated that single process and starved every other client, including the queries used to watch progress, which made a working approach look like a failing one.

### Harness requirements

| component | default | required at 90k |
|---|---|---|
| kwok-controller memory | 8Gi | **64Gi**, measured 22-33 GB in use |
| kwok-controller `nodeLeaseParallelism` | 4 | **64**, for ≈3,000 lease renewals/s |
| kwok-controller `nodePlayStageParallelism` | 4 | **64**, for ≈300 node heartbeats/s |
| node-heartbeat stage `jitterDurationMilliseconds` | must exceed `durationMilliseconds` | an inverted interval collapses the delay and produced a 29 s heartbeat instead of 300 s |

KWOK also retains stage and lease work for deleted nodes indefinitely. After several drain cycles, 32% of node writes were 404s against nodes that no longer existed; restarting the controller clears it.
