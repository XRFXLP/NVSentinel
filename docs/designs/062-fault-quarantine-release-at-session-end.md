# ADR-062: Fault Quarantine — Release Quarantine Actions Only at Session End

Status: Proposed

## Context

A quarantine session starts when fault-quarantine quarantines a node, and it ends when the last event that it tracks for the node recovers. A session can track several unhealthy health events at the same time. Fault-quarantine records them in the `quarantineHealthEvent` node annotation. Each event matches its own rule sets, and each rule set can require a taint, a cordon, or a label ([ADR-003](003-rule-based-node-quarantine.md)).

Fault-quarantine applies the taints and the cordon of the event that starts a session. A later event in the same session adds its rule labels, but not its taints or its cordon. Fault-quarantine releases the taints and the cordon of a session in one step, when every event in the session has recovered. Until then, a recovered event's taint and cordon stay on the node, even when no remaining event requires them. [Issue #1439](https://github.com/NVIDIA/NVSentinel/issues/1439) asks for two changes: apply the actions of later events, and release a recovered event's actions on partial recovery. For example, event E1 requires a cordon and taint A, and event E2 requires taint B only. After E1 recovers, the node stays cordoned and keeps taint A until E2 also recovers.

No earlier record states why fault-quarantine releases actions only when the session ends. Node-drainer, fault-remediation, and [ADR-049](049-node-validation.md) depend on this behaviour.

## Decision

Fault-quarantine applies the taints and the cordon that each event in a session requires when the event arrives, including events that arrive after the node is quarantined. It keeps every taint and the cordon that it applied in the session until the last event in the session recovers. It does not release an action when one event recovers and other events remain.

## Implementation

- **Apply the actions of later events.** When an event arrives for a node that is already quarantined, `handleQuarantinedNode` (`fault-quarantine/pkg/reconciler/reconciler.go`) evaluates the event's rule sets and applies the taints and the cordon that they require, in the same node update that adds the rule labels:
  - A taint that the session already recorded is not applied again. A taint that the node already has is recorded as pre-existing, so the release does not remove it.
  - The node is cordoned only if no action already cordoned it. If the node is already unschedulable, the cordon is recorded as pre-existing.
  - The circuit breaker counts a new cordon, and dry run records the actions without applying them, as for the event that starts a session.
- **Release at session end.** This is the current behaviour. When a healthy event leaves other events in the session, `handleQuarantinedNode` removes the recovered entities and keeps the node quarantined. When the last event recovers, it removes the taints, the cordon, and the rule labels, sets the event status to `UnQuarantined`, and creates the post-remediation `ValidationRequest` if validation is enabled.
- Update the fault-quarantine configuration reference (`docs/configuration/fault-quarantine.md`) to state that the taints and the cordon of a session are released only when the session ends.

## Rationale

- **Every active fault gets its isolation.** If a later event requires a cordon or a taint, the node gets it, even when the event that started the session did not require it. Otherwise pods keep landing on a node with an active fault that its rules isolate.

The node must stay unschedulable for the whole session, because node-drainer and fault-remediation assume that no new pods land on it after a drain:

- **Node-drainer trusts an earlier drain.** When a later event arrives, node-drainer marks it `AlreadyDrained` if another event in the session already finished a drain (`IsNodeDrained` in `commons/pkg/drain/drain.go`). If a release lets pods land, the later event evicts nothing, and fault-remediation then reboots or resets the node with those pods on it.
- **Work for a recovered event can still run.** A drain that started for an event continues after the event recovers, and fault-remediation remediates the event when the drain finishes. If the cordon comes off during this, new pods land and are then evicted or interrupted by the reboot.
- **A GPU reset needs a GPU that no process holds.** Pods that land after a release can open the device, so the reset fails and fault-remediation escalates to a reboot.
- **Cancelling the recovered event does not solve this.** Fault-remediation treats `Cancelled` as the end of the session: `handleCancellationEvent` clears all remediation state of the node, which is correct for a manual uncordon, the only writer of `Cancelled` today. A per-event cancellation needs a new event status in node-drainer and fault-remediation and a time-aware drain proof. It still cannot stop a reset or reboot that has started. That changes the contract of three modules to return capacity on a node that still has an active fault.
- **One release point keeps the session simple.** The node returns to service at one point in time, and ADR-049 validates it once, at that point.

## Consequences

### Positive

- A later fault that requires a cordon or a taint gets it, so the node's isolation covers every active fault.
- Node-drainer and fault-remediation keep their current contract: the node is unschedulable from the first cordon until the session ends.
- The node returns to service at one point, after one validation.

### Negative

- A recovered fault's cordon and taints stay on the node until every other fault in the session recovers. The node stays out of service longer than its active faults require.
- A session that started with taints only can now become cordoned when a later event requires a cordon. More nodes can be cordoned than before, and the circuit breaker counts them.

### Mitigations

- Operators choose which faults require a cordon and which require only taints. A fault that should not take the node out of service can use taints only.
- An operator who decides that the node is fit can uncordon it manually. This ends the session and cancels its events.

## Alternatives Considered

### Release a recovered event's actions on partial recovery

When an event recovers, fault-quarantine evaluates the rule sets against the remaining events and removes each taint and the cordon that no remaining event requires, behind an opt-in setting.

**Rejected** because: removing the cordon in the middle of a session lets pods land while node-drainer and fault-remediation still act on the node, as described in the Rationale. Keeping the cordon whenever a remaining event will be drained or remediated would avoid this, but then the release helps only sessions whose remaining events need no drain or remediation.

### Release on partial recovery and cancel the recovered event

Fault-quarantine marks the recovered event `Cancelled`, as a manual uncordon does. Node-drainer stops its drain, fault-remediation skips it, and node-drainer stops trusting an earlier drain after a release.

**Rejected** because: fault-remediation treats `Cancelled` as the end of the session, so the remaining events would lose their remediation state. A new per-event status would change node-drainer and fault-remediation, and a reset or reboot that has started cannot be cancelled. A drain would no longer mean that the node stays empty until remediation, which makes its intent unclear.

### Record the owners of each action in a new annotation

**Rejected** because: owners are needed only to release actions per event, which this record rejects. The annotation would also be a new public interface that every later version must read.

## Notes

- A session whose rules require only taints does not cordon the node, so pods that tolerate the taints can land during the session. Node-drainer's `AlreadyDrained` check has the same assumption there. That gap exists independently of this record and is tracked separately.
- A later record can revisit partial release if node-drainer and fault-remediation gain per-event cancellation and a drain proof that accounts for pods that land after a drain.

## References

- [Issue #1439](https://github.com/NVIDIA/NVSentinel/issues/1439)
- [ADR-003: Rule-Based Node Quarantine](003-rule-based-node-quarantine.md)
- [ADR-049: Node Validation](049-node-validation.md)
- [PR #2010: review discussion of partial release](https://github.com/NVIDIA/NVSentinel/pull/2010)
