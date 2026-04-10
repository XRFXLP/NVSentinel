# ADR-036: Data Model — Custom Remediation Actions

## Context

NVSentinel's `RecommendedAction` is a protobuf enum with a fixed set of values (`COMPONENT_RESET`, `RESTART_VM`, `RESTART_BM`, `REPLACE_VM`, etc.). The fault-remediation module uses this enum's string representation as the key to look up which maintenance CR to create.

This works for built-in GPU-centric workflows, but breaks down when operators want to extend NVSentinel to custom component classes (disks, NICs, DPUs, custom accelerators) or define organization-specific remediation playbooks. The enum is a closed set—adding a new action today requires a proto change, code regeneration, and a new release.

The fault-remediation module's *output* side is already generic: it uses Go templates and the dynamic Kubernetes client to create arbitrary CRs, keyed by a string in Helm config. The bottleneck is the *input* side: the health event can only express actions that exist in the enum.

## Decision

Use a protobuf `oneof` to express the remediation action as either a built-in `RecommendedAction` enum or an arbitrary `string`. Moving an existing field into a **new** `oneof` is a [wire-safe change in proto3](https://protobuf.dev/programming-guides/proto3/#updating). This gives custom health monitors an open-ended way to specify actions while preserving type safety for built-in actions.

## Implementation

### Proto Change

`data-models/protobufs/health_event.proto`:

```protobuf
message HealthEvent {
  // ... existing fields 1-7, 9-17 ...

  oneof action {
    RecommendedAction recommendedAction = 8;  // existing field, same number
    string customRecommendedAction = 18;      // new field for custom actions
  }
}
```

The `RecommendedAction` enum is unchanged — no new values needed. The `oneof` enforces that *at most one* of the two fields is set at any time (zero or one). Since neither field being set is a valid wire state, explicit validation must be added at the Platform Connector gRPC boundary to reject events where neither variant is populated (see Validation section).

**Wire compatibility:** Old consumers that don't know about the `oneof` will still read field 8 (`recommendedAction`) normally for built-in actions. For custom actions (field 18), old consumers will see `recommendedAction` at its default value (`NONE`) and treat the event as a no-op — which is the correct fallback behavior.

### Resolution Helper

Add a shared helper in `data-models/pkg/model/`:

```go
func GetEffectiveActionName(he *protos.HealthEvent) string {
    switch v := he.Action.(type) {
    case *protos.HealthEvent_RecommendedAction:
        return v.RecommendedAction.String()
    case *protos.HealthEvent_CustomRecommendedAction:
        return v.CustomRecommendedAction
    default:
        return protos.RecommendedAction_NONE.String()
    }
}
```

All consumers that need the action name as a string use this helper instead of accessing the fields directly.

### Fault-Remediation Code Changes

Two call sites switch from `healthEvent.RecommendedAction.String()` to `GetEffectiveActionName(healthEvent)`:

1. **`fault-remediation/pkg/common/equivalence_groups.go`** — `GetGroupConfigForEvent` action lookup.
2. **`fault-remediation/pkg/remediation/remediation.go`** — `CreateMaintenanceResource` action routing.

All other call sites that read `RecommendedAction` directly (e.g., `shouldSkipEvent` checking for `NONE`) must switch to using `he.GetRecommendedAction()` — the generated getter returns the enum value if set, or the zero value (`NONE`) if the custom string variant is set. This is correct behavior: custom actions should not be skipped by the `NONE` check.

### Broader Codebase Migration

Since the `oneof` changes the generated Go field access pattern, every direct reference to `he.RecommendedAction` across the codebase must be updated to use `he.GetRecommendedAction()` or the `GetEffectiveActionName` helper. Affected modules include:

- `fault-quarantine` — CEL rule evaluation, reconciler
- `fault-remediation` — reconciler, remediation client, config validation
- `platform-connectors` — event processing, overrides
- `event-exporter` — CloudEvent transformation
- `health-events-analyzer` — rule publisher
- `store-client` — document utilities

The generated Python code (`gpu-health-monitor`, `dcgm-diag`, `nccl-allreduce`) will also change but Python protobuf access via `HasField()` and `WhichOneof()` handles this naturally.

### Validation

#### `fault-remediation/pkg/config/config.go`

`validateResourceImpactedEntityScope` currently hardcodes `protos.RecommendedAction_COMPONENT_RESET.String()` as the only action allowed to have an `ImpactedEntityScope`. Update to also allow custom actions (action names that don't match any built-in enum value):

```go
_, isBuiltinAction := protos.RecommendedAction_value[actionName]
if isBuiltinAction && actionName != protos.RecommendedAction_COMPONENT_RESET.String() {
    return fmt.Errorf(
        "built-in action '%s' cannot have an ImpactedEntityScope; "+
            "only COMPONENT_RESET and custom actions support this", actionName)
}
```

#### Platform Connector

In the gRPC handler, reject events where `customRecommendedAction` is set but empty:

```go
for _, event := range he.Events {
    if _, ok := event.Action.(*pb.HealthEvent_CustomRecommendedAction); ok &&
        event.GetCustomRecommendedAction() == "" {
        return nil, status.Errorf(codes.InvalidArgument,
            "customRecommendedAction is set but empty (node=%s, agent=%s)",
            event.NodeName, event.Agent)
    }
}
```

### Operator Usage

A custom health monitor emitting a disk remediation event:

```go
event := &protos.HealthEvent{
    Agent:          "disk-health-monitor",
    ComponentClass: "Disk",
    IsFatal:        true,
    Action: &protos.HealthEvent_CustomRecommendedAction{
        CustomRecommendedAction: "REPLACE_DISK",
    },
}
```

Built-in actions stay unchanged:

```go
event := &protos.HealthEvent{
    Agent:          "syslog-health-monitor",
    ComponentClass: "GPU",
    IsFatal:        true,
    Action: &protos.HealthEvent_RecommendedAction{
        RecommendedAction: protos.RecommendedAction_RESTART_BM,
    },
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

Once implemented, adding new custom actions requires no further code changes—only Helm configuration.

### Event Exporter

`event-exporter/pkg/transformer/cloudevents.go` should use the `GetEffectiveActionName` helper to emit the resolved action name, and include `customRecommendedAction` as a separate field when set:

```go
healthEventData["recommendedAction"] = model.GetEffectiveActionName(event)
if custom := event.GetCustomRecommendedAction(); custom != "" {
    healthEventData["customRecommendedAction"] = custom
}
```

### Health Event Overrides

The existing CEL-based override system (ADR-021) currently supports overriding `isFatal`, `isHealthy`, and `recommendedAction`. To enable rerouting built-in events to custom actions, the override `Override` struct in `platform-connectors/pkg/overrides/` must also accept a `customRecommendedAction` string field. When set, the override replaces the `oneof` variant with `HealthEvent_CustomRecommendedAction`. This is an additive change to ADR-021's implementation scope.

## Rationale

- **Wire-safe**: Moving an existing field into a new `oneof` is explicitly safe per the [proto3 spec](https://protobuf.dev/programming-guides/proto3/#updating)
- **No enum pollution**: No `CUSTOM` sentinel value needed in the enum
- **Type-safe**: The `oneof` enforces exactly one variant is set — the compiler prevents accidentally setting both
- **Open-ended**: Custom actions are arbitrary strings, extensible purely through config
- **Clean API**: Consumers use a type switch or the helper function, no "check this field, then maybe that field" logic

## Consequences

### Positive
- Custom health monitors can specify any remediation action without proto changes
- NVSentinel becomes extensible to non-GPU component classes (disks, NICs, etc.)
- The `oneof` makes the "built-in or custom" distinction explicit in the type system
- Operators can add new actions entirely through Helm configuration

### Negative
- Larger migration: every direct `he.RecommendedAction` access across the codebase must change to `he.GetRecommendedAction()` or the helper
- Generated Go code changes field access patterns (struct field → oneof interface)
- Old consumers that receive custom action events will see `RecommendedAction` at its default (`NONE`)

### Mitigations
- Platform Connector rejects custom actions with empty strings at the gRPC boundary
- Provide `GetEffectiveActionName` in a shared package so all consumers have a single correct path
- The typed Go migration is mechanical (search-and-replace `he.RecommendedAction` → `he.GetRecommendedAction()`) and missed usages will be caught by the compiler as build errors. However, CEL override rules, dynamic Kubernetes client access, and export/serialization/transformer paths use string-based or dynamic field access that the compiler cannot catch — these must be validated with targeted integration tests
- Old consumers defaulting to `NONE` for custom actions is safe (event is skipped, not mishandled)

## Alternatives Considered

### CUSTOM enum value + separate string field
**Rejected** because: Adds a sentinel `CUSTOM` value to the enum and a separate `customRecommendedAction` string field. Consumers must check the enum first, then conditionally read the string — two fields encoding the same concept. The `oneof` approach is cleaner and enforced by the type system. While the `CUSTOM` enum approach has a smaller diff, it introduces long-term ambiguity about which field to consult.

### Replace enum with string field
**Rejected** because: Changing a proto field's type on the same field number is a wire-breaking change. Adding a new string field and deprecating the enum would create the same two-field duplication but without the clear discriminator signal. Built-in actions would lose compile-time type safety.

### CEL-based action routing in fault-remediation config
**Rejected** because: Requires operators to write and maintain CEL expressions that enumerate error conditions (e.g., XIDs). Doesn't scale when new error codes are constantly added—the health monitor already knows the right action, it just needs an open-ended way to express it.

### Extend the enum for every new action
**Rejected** because: Every new action requires a proto change, code regeneration, and release. Custom/organization-specific actions can never be added this way without forking the proto.

## Notes

- RBAC for custom CRD kinds must be configured separately by the operator; the Helm chart's auto-generated RBAC only covers built-in kinds
- This change pairs well with ADR-021 (Health Event Property Overrides) for rerouting built-in events to custom actions
- The broader codebase migration (updating all `he.RecommendedAction` references) is mechanical but should be done in a single PR to avoid intermediate states where some code uses the old pattern

## References

- [GitHub Issue #1141](https://github.com/NVIDIA/NVSentinel/issues/1141) — Support for custom remediation actions
- [GitHub Issue #182](https://github.com/NVIDIA/NVSentinel/issues/182) — Support for arbitrary 3rd party CRD remediation actions
- [Proto3 Updating A Message Type](https://protobuf.dev/programming-guides/proto3/#updating) — Wire-safety of moving fields into a new oneof
- [ADR-009: Fault Remediation Triggering](./009-fault-remediation-triggering.md)
- [ADR-021: Health Event Property Overrides](./021-health-event-property-overrides.md)
