# ADR-062: Fault Quarantine — Release Quarantine Actions on Partial Recovery

Status: Proposed

## Context

A quarantine session starts when fault-quarantine quarantines a node, and it ends when the last event that it tracks for the node recovers. A session can track several unhealthy health events at the same time. Fault-quarantine records them in the `quarantineHealthEvent` node annotation. Each event matches its own rule sets, and each rule set can require a taint, a cordon, or a label ([ADR-003](003-rule-based-node-quarantine.md)). Fault-quarantine applies these actions for every event in the session, including events that arrive after the node is quarantined. Before the fix for [issue #1439](https://github.com/NVIDIA/NVSentinel/issues/1439), it applied only the actions of the event that started the session.

The actions are not recorded per event. The `quarantineHealthEventAppliedTaints` annotation is a flat list of taints, and `quarantineHealthEventIsCordoned` is a single flag. Fault-quarantine releases all of them in one step, when every event in the session has recovered. Until then, a recovered event's taint and cordon stay on the node, even when no remaining event requires them. [Issue #1439](https://github.com/NVIDIA/NVSentinel/issues/1439) describes four scenarios. For example, event E1 requires a cordon and taint A, and event E2 requires taint B only. After E1 recovers, the node stays cordoned and keeps taint A until E2 also recovers.

No earlier record decides when actions are released. [ADR-049](049-node-validation.md) depends on the current behaviour: it creates the post-remediation `ValidationRequest` when the session ends, and fault-quarantine keeps its cordon and taints for lifecycle-manager to remove when validation passes.

## Decision

When an event in a quarantine session recovers and other events remain, fault-quarantine removes each taint and the cordon that it applied and that no remaining event requires. It finds the required actions by evaluating the current rule sets against the remaining events in `quarantineHealthEvent`, so it does not store on the node which event requires each action. It releases nothing until the session ends if any evaluation fails, or if a post-remediation validation is pending for the session. The behaviour is opt-in. By default, fault-quarantine keeps the current behaviour and releases every action only when the last event in the session recovers.

## Implementation

- Add the setting `releaseActionsOnPartialRecovery` (Helm value `fault-quarantine.releaseActionsOnPartialRecovery`), default `false`.
- In `handleQuarantinedNode` (`fault-quarantine/pkg/reconciler/reconciler.go`), when a healthy event leaves other events in the session:
  1. Evaluate the rule sets against each remaining event in `quarantineHealthEvent`, which stores each full `HealthEvent`, through the same path as a new event. Collect the required taints and whether any event requires a cordon.
  2. Release nothing if an evaluation fails or the node has the `quarantineValidationHealthEvent` annotation.
  3. Remove each taint in `quarantineHealthEventAppliedTaints` that is not pre-existing and that no remaining event requires.
  4. If fault-quarantine applied the cordon (`quarantineHealthEventIsCordoned` is set and `quarantineHealthEventCordonPreExisting` is not) and no remaining event requires one, uncordon the node and set the uncordon labels, as the session-end path does.

  Steps 3 and 4 remove the matching annotation entry in the same node update. Otherwise the node informer reads the change as a manual untaint or uncordon.
- These stay unchanged:
  - Rule labels. Label priority is resolved across the session, so a recompute could change a label value.
  - `quarantineHealthEvent`, the `nvsentinel-state` label, and the event statuses. The node stays in its session until the last event recovers.
  - node-drainer. Draining is shared by every event on the node, and an eviction cannot be reversed for one event. A drain in progress for a recovered event runs to completion.
  - Fault-remediation. It already handles a recovery in the middle of a session (#1421). A maintenance request in progress for a recovered event runs to completion.
- In dry run, the release step updates only the annotations, as the rest of dry run does.

## Rationale

- **No change to persisted state.** The release step reads the annotations that already exist and removes entries from them. During a rolling upgrade, an old fault-quarantine pod releases nothing early, which is the current behaviour, and it reads the annotations that a new pod writes without a change.
- **It matches an existing pattern.** Fault-remediation already derives node state from the remaining events in the same annotation.
- **It fails closed.** Every uncertain case keeps the current isolation: an evaluation error, a pending validation, a pre-existing action, and the default setting.
- **It frees capacity that is held for no reason.** A node with one minor remaining fault no longer stays cordoned because of a fault that has already recovered.

## Consequences

### Positive

- The node's isolation matches the events that are still active. This completes #1439.
- Operators choose when to enable the behaviour, and nothing changes for an install that does not set the value.

### Negative

- The release step uses the current rule sets and the current node, not those that applied when each event arrived. If an operator changes a rule set, or a node label that a `Node` rule reads, during a session, a recovery can release an action that the original rule required.
- When the setting is on, a node can accept workloads while a fault is still active, if the remaining events require only taints that the workloads tolerate. This is the purpose of the change, but it is a new state: the node is in a quarantine session and is schedulable.
- Sessions that request validation do not benefit. They keep the current behaviour.

### Mitigations

- The default is off. The configuration reference describes the rule-change effect, and recommends that operators change rule sets only when no sessions are active, or accept the effect.
- A release step logs each released action with the event that recovered, and counts it in a metric, so an unexpected release is visible.
- The state label and `quarantineHealthEvent` continue to show the session, so automation that keys off them sees no change.

## Alternatives Considered

### Keep releasing every action when the session ends

**Rejected** because: a recovered fault holds the node's cordon and taints until every unrelated fault recovers, which keeps nodes out of service for longer than any active fault requires. It stays the default until operators enable the setting.

### Record the owners of each action in a new annotation

**Rejected** because: it adds a persisted node annotation, which is a public interface that every later version must read. During a rolling upgrade, an old pod writes actions without owners, so the new pod still needs a fallback for actions with no recorded owner. The manual uncordon, manual untaint, and stale-state paths would also have to keep the owners consistent. Its advantage, immunity to rule changes during a session, does not justify that cost while recomputing can fail closed.

### Release actions on partial recovery by default

**Rejected** because: it changes when nodes return to service for every existing install, including during the upgrade that introduces it. An opt-in setting follows the repository rule that new behaviour keeps the current behaviour by default.

### Create a validation request for each partially recovered event

**Rejected** because: ADR-049 defines one validation session per quarantine session, created when the session ends. Validating each event separately would change that decision and the interface between fault-quarantine and lifecycle-manager. This record keeps the current behaviour for sessions that request validation instead.

## Notes

- The setting can become the default in a later release, after field experience, through a new record.

## References

- [Issue #1439](https://github.com/NVIDIA/NVSentinel/issues/1439)
- [ADR-003: Rule-Based Node Quarantine](003-rule-based-node-quarantine.md)
- [ADR-049: Node Validation](049-node-validation.md)
- [PR #1421: nvsentinel-state label handling on partial recovery](https://github.com/NVIDIA/NVSentinel/pull/1421)

