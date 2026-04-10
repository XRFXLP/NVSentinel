# ADR-034: Data Model — Custom Remediation Actions

## Context

NVSentinel's `RecommendedAction` is a protobuf enum with a fixed set of values (`COMPONENT_RESET`, `RESTART_VM`, `RESTART_BM`, `REPLACE_VM`, etc.). The fault-remediation module uses this enum's string representation as the key to look up which maintenance CR to create.

This works for built-in GPU-centric workflows, but breaks down when operators want to extend NVSentinel to custom component classes (disks, NICs, DPUs, custom accelerators) or define organization-specific remediation playbooks. The enum is a closed set—adding a new action today requires a proto change, code regeneration, and a new release.

The fault-remediation module's *output* side is already generic: it uses Go templates and the dynamic Kubernetes client to create arbitrary CRs, keyed by a string in Helm config. The bottleneck is the *input* side: the health event can only express actions that exist in the enum.

## Decision

Add a `CUSTOM` value to the `RecommendedAction` enum and a `string customRecommendedAction` field to the `HealthEvent` message. When `recommendedAction` is `CUSTOM`, consumers read the string field for the action name. Built-in actions continue using the enum directly.

## Implementation

### Proto Change

`data-models/protobufs/health_event.proto`:

```protobuf
enum RecommendedAction {
  NONE = 0;
  COMPONENT_RESET = 2;
  CONTACT_SUPPORT = 5;
  RUN_FIELDDIAG = 6;
  RESTART_VM = 15;
  RESTART_BM = 24;
  REPLACE_VM = 25;
  RUN_DCGMEUD = 26;
  UNKNOWN = 99;
  CUSTOM = 100;  // Action name is in customRecommendedAction field
}

message HealthEvent {
  // ... existing fields 1-17 ...
  string customRecommendedAction = 18;  // Only read when recommendedAction == CUSTOM
}
```

### Resolution Helper

Add a shared helper in `data-models/pkg/model/` (or `fault-remediation/pkg/common/`):

```go
func GetEffectiveActionName(he *protos.HealthEvent) string {
    if he.RecommendedAction == protos.RecommendedAction_CUSTOM {
        return he.CustomRecommendedAction
    }
    return he.RecommendedAction.String()
}
```

### Fault-Remediation Code Changes

Three call sites switch from `healthEvent.RecommendedAction.String()` to `GetEffectiveActionName(healthEvent)`:

1. **`fault-remediation/pkg/common/equivalence_groups.go`** — `GetGroupConfigForEvent` action lookup
2. **`fault-remediation/pkg/remediation/remediation.go`** — `CreateMaintenanceResource` action routing
3. **`fault-remediation/pkg/reconciler/reconciler.go`** — `shouldSkipEvent` must treat `CUSTOM` with an empty `customRecommendedAction` as unsupported

### Validation

- **`fault-remediation/pkg/config/config.go`**: `validateResourceImpactedEntityScope` currently hardcodes `protos.RecommendedAction_COMPONENT_RESET.String()` as the only action allowed to have an `ImpactedEntityScope`. This should be relaxed to also allow custom actions that declare an `ImpactedEntityScope` in config, or kept restricted with a clear error message explaining the limitation.
- **Platform Connector**: If `recommendedAction == CUSTOM` and `customRecommendedAction` is empty, log a warning and treat as `UNKNOWN`.

### Operator Usage

A custom health monitor emitting a disk remediation event:

```go
event := &protos.HealthEvent{
    Agent:                     "disk-health-monitor",
    ComponentClass:            "Disk",
    RecommendedAction:         protos.RecommendedAction_CUSTOM,
    CustomRecommendedAction:   "REPLACE_DISK",
    // ...
}
```

Operator adds to Helm values:

```yaml
maintenance:
  actions:
    REPLACE_DISK:
      apiGroup: "storage.example.com"
      version: "v1"
      kind: "ReplaceDisk"
      scope: "Cluster"
      completeConditionType: "DiskReplaced"
      templateFileName: "replace-disk.yaml"
      equivalenceGroup: "disk-replace"
```

No code changes needed beyond the initial implementation—new custom actions are purely configuration.

### Event Exporter

`event-exporter/pkg/transformer/cloudevents.go` should include `customRecommendedAction` in exported events when set, so external monitoring systems see the effective action name.

### Health Event Overrides

The existing CEL-based override system (ADR-021) should support setting `recommendedAction: CUSTOM` and `customRecommendedAction: "MY_ACTION"` in override rules, enabling operators to reroute built-in events to custom actions without modifying health monitors.

## Rationale

- **Backward compatible**: Existing events and consumers are unaffected; `CUSTOM` is purely additive
- **Minimal code change**: Three call-site swaps plus one helper function in fault-remediation
- **Type-safe for built-in actions**: The enum retains compile-time safety for known actions
- **Open-ended for custom actions**: Operators can define arbitrary actions through config
- **Clear contract**: The enum value `CUSTOM` is an unambiguous signal to read the string field—no guessing which field to consult

## Consequences

### Positive
- Custom health monitors can specify any remediation action without proto changes
- NVSentinel becomes extensible to non-GPU component classes (disks, NICs, etc.)
- Leverages the existing template-based CR creation system with zero changes to the output path
- Operators can add new actions entirely through Helm configuration

### Negative
- Two fields encode the same concept (enum + string) for custom actions
- Consumers must use the resolution helper consistently; direct `.RecommendedAction.String()` calls will return `"CUSTOM"` instead of the actual action name

### Mitigations
- Provide the `GetEffectiveActionName` helper in a shared package so all consumers have a single correct path
- Add a linter or code review guideline to flag direct `.RecommendedAction.String()` usage in remediation paths
- Validate at Platform Connector ingress that `CUSTOM` events have a non-empty `customRecommendedAction`

## Alternatives Considered

### Replace enum with string field
**Rejected** because: Changing a proto field's type on the same field number is a wire-breaking change. Adding a new string field and deprecating the enum would create the same two-field duplication but without the clear discriminator signal. Built-in actions would lose compile-time type safety.

### CEL-based action routing in fault-remediation config
**Rejected** because: Requires operators to write and maintain CEL expressions that enumerate error conditions (e.g., XIDs). Doesn't scale when new error codes are constantly added—the health monitor already knows the right action, it just needs an open-ended way to express it.

### Extend the enum for every new action
**Rejected** because: Every new action requires a proto change, code regeneration, and release. Custom/organization-specific actions can never be added this way without forking the proto.

## Notes

- The `CUSTOM` enum value uses `100` to leave room for future built-in actions
- RBAC for custom CRD kinds must be configured separately by the operator; the Helm chart's auto-generated RBAC only covers built-in kinds
- This change pairs well with ADR-021 (Health Event Property Overrides) for rerouting built-in events to custom actions

## References

- [GitHub Issue #1141](https://github.com/NVIDIA/NVSentinel/issues/1141) — Support for custom remediation actions
- [GitHub Issue #182](https://github.com/NVIDIA/NVSentinel/issues/182) — Support for arbitrary 3rd party CRD remediation actions
- [ADR-009: Fault Remediation Triggering](./009-fault-remediation-triggering.md)
- [ADR-021: Health Event Property Overrides](./021-health-event-property-overrides.md)
