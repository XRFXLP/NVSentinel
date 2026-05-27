# Node Drainer Flood Queue Results

## Summary

We tested node-drainer behavior under a grouped event flood intended to reproduce the production symptom where nodes stayed `Quarantined` for a long time before node-drainer marked them `draining`.

The most useful result is:

- **Priority-only is the best immediate mitigation for `Quarantined -> draining` latency.**
- **Combined priority + coalescing reduces queue depth the most, but it moves fewer nodes to `draining` by T+25m than priority-only.**
- **Coalescing-only helps compared with baseline, but its follower bookkeeping still competes with owner progress.**

## Test Setup

```text
Events:       50,000
Nodes:        50 KWOK nodes
Events/node:  1,000
Duration:     2 minutes
Event order:  grouped by node
Workload:     one long-running AllowCompletion pod per target node
Checkpoint:   about T+25 minutes after event injection completed
```

Grouped ordering means all events for one node are sent before moving to the next node. This reproduces queue-position pressure: later nodes' first actionable events are buried behind many earlier-node events.

The benchmark variants were:

| Variant | Priority queue | Coalescing |
|---|---:|---:|
| Baseline | Off | Off |
| Priority-only | On | Off |
| Coalescing-only | Off | On |
| Combined | On | On |

## Variant Definitions

**Priority queue** means node-drainer changes the order of ready queue items so work for nodes that have not reached `draining` is processed ahead of work for nodes already in `draining`. In the priority-only run, the effective priority model used two classes:

```text
high priority: event for a node not yet known as draining
low priority: event for a node already marked draining
```

This targets the incident symptom directly: later nodes should not wait behind repeated work for nodes that are already draining.

**Coalescing** means node-drainer tries to identify overlapping built-in drain events for the same drain scope. One event becomes the owner for that node/scope, and duplicate events become followers instead of running the full drain evaluator path independently. In the tested implementation, followers were still requeued and processed, but their work was cheaper than running full pod/evaluator logic.

**Combined** means both mechanisms are enabled. Coalescing first classifies overlapping events as owners or followers, then the priority queue orders ready work as owner/first-unowned node-scope work, follower bookkeeping, and already-draining work. Followers are still requeued and processed in this model; they are just placed behind owner/unowned work so they compete less with first-time node progress.

## Raw Results

### Baseline

```text
events:       50,000
nodes:        50
duration:     2m
node_order:   grouped
draining:     5
quarantined:  45
queue_depth:  ~49,970
```

Baseline reproduced the production failure shape. Node-drainer had a very large ready queue and only a small fraction of nodes reached `draining` by T+25m.

### Priority-only

Script sample:

```text
events:       50,000
nodes:        50
wall_seconds: 1231
draining:     16
quarantined:  15
blank:        19
queue_depth:  15,085
```

T+25m sample:

```text
draining:     21
quarantined:  29
queue_depth:  20,172
```

Priority queue metrics:

```text
node_drainer_priority_queue_enabled 1
node_drainer_priority_queue_items_total{priority="1",reason="node_draining"} 24960
node_drainer_priority_queue_items_total{priority="3",reason="node_not_draining"} 155
```

Priority-only produced the best `draining` count at T+25m among the tested variants.

### Coalescing-only

Script sample:

```text
events:          50,000
nodes:           50
wall_seconds:    1062
draining:        10
quarantined:     15
blank:           25
queue_depth:     16,779
owners:          13
followers:       12,928
event_attempts:  36,781
```

T+25m sample:

```text
draining:     16
quarantined:  30
blank:        4
queue_depth:  31,125
```

Coalescing lowered per-attempt cost and improved over baseline, but still left substantial queue depth and did not match priority-only for node transition progress.

### Combined

When priority queueing was combined with coalescing, the final combined run used three priority classes:

```text
priority=3: owner / first unowned event for a node-scope
priority=2: follower bookkeeping
priority=1: work for a node already marked draining
```

Initial combined run:

```text
events:       50,000
successes:    49,021
draining:     11
blank:        39
queue_depth:  7,441
owners:       11
followers:    10,270
```

Rerun after priority classes were adjusted:

Script sample:

```text
events:          50,000
nodes:           50
duration:        2m
node_order:      grouped
wall_seconds:    1049
draining:        11
quarantined:     23
blank:           16
queue_depth:     8,556
owners:          11
followers:       3,309
event_attempts:  27,010
```

T+25m sample:

```text
draining:     15
quarantined:  35
queue_depth:  13,507
```

Priority queue metrics at T+25m:

```text
node_drainer_priority_queue_enabled 1
node_drainer_priority_queue_items_total{priority="1",reason="node_draining"} 12294
node_drainer_priority_queue_items_total{priority="2",reason="follower"} 35875
node_drainer_priority_queue_items_total{priority="3",reason="owner_or_unowned_scope"} 2215
```

Combined produced the lowest queue depth, but it did not transition as many nodes to `draining` as priority-only by T+25m.

## T+25m Comparison

| Variant | Draining | Quarantined | Blank | Queue Depth | Draining % |
|---|---:|---:|---:|---:|---:|
| Baseline | 5 | 45 | 0 | ~49,970 | 10% |
| Priority-only | 21 | 29 | 0 | 20,172 | 42% |
| Coalescing-only | 16 | 30 | 4 | 31,125 | 32% total, 34.8% of non-blank |
| Combined | 15 | 35 | 0 | 13,507 | 30% |

Queue-depth reduction versus baseline:

| Variant | Queue Depth | Reduction vs Baseline |
|---|---:|---:|
| Baseline | ~49,970 | 0% |
| Priority-only | 20,172 | ~60% |
| Coalescing-only | 31,125 | ~38% |
| Combined | 13,507 | ~73% |

## Interpretation

### Baseline

Baseline reproduces the production symptom: a grouped event flood leaves most nodes stuck in `quarantined` while node-drainer processes a large ready queue.

### Priority-only

Priority-only is the strongest immediate mitigation for the `Quarantined -> draining` transition. It gets the most nodes to `draining` by T+25m.

The effective model is:

```text
high priority: event for a node not yet known as draining
low priority: event for a node already marked draining
```

This directly targets the incident symptom: later nodes should not wait behind repeated work for nodes already in `draining`.

### Coalescing-only

Coalescing makes duplicate work cheaper, but follower work still competes with owner progress. It improves over baseline, but not enough to justify the extra lifecycle complexity as the first mitigation.

### Combined

Combined reduces queue depth the most, but it does not improve `draining` progress over priority-only. The large number of follower assignments shows that coalescing is active, but follower bookkeeping remains significant.

Combined is useful for queue backlog reduction, but not currently the best choice for the node transition SLO.

## Recommendation

Ship **priority-only** first as the immediate mitigation.

Reasons:

- Best `draining` count at T+25m: `21/50`.
- Significantly lower queue depth than baseline: `~60%` reduction.
- Much simpler than owner/follower coalescing.
- Does not introduce follower lifecycle, cancellation, cold-start ownership, or metadata-preservation edge cases.

Do **not** ship the current owner/follower coalescing implementation as the first mitigation. It should remain a follow-up optimization only if queue depth remains a critical problem after priority scheduling.

## Follow-up Ideas

The final combined run tested a three-class priority model:

```text
owner / first unowned event for node-scope: highest priority
follower bookkeeping for not-yet-draining node: lower priority
follower / retry for already-draining node: lowest priority
```

That improved queue depth, but it still allowed followers to cycle through the queue. The combined run still had significant follower traffic:

```text
priority=2 follower: 35,875
```

That follower churn likely explains why combined reduces queue depth but does not improve `draining` progress over priority-only. If coalescing is revisited, the next design should park followers more aggressively instead of repeatedly requeueing them while the owner is still active.

## Notes

- These results are from a local kind/KWOK development environment, not production.
- The test uses grouped event order because round-robin event order did not reproduce the production lag.
- The relevant success metric is node transition to `dgxc.nvidia.com/nvsentinel-state=draining`, not total event ingestion success.
- Fault-quarantine can also contribute to blank/unlabeled nodes; for node-drainer-specific comparison, prefer `draining / non_blank` when upstream progress differs across runs.
