# ADR-043: Labeler - Expected Device Inventory Labels

## Context

NVSentinel needs a way to detect missing GPUs and NICs even when the kernel or driver does not emit a useful XID, SXID, or fallen-off-bus syslog line. A common failure mode is that a node reports fewer devices than its platform is expected to have; if no error log is produced, Kubernetes Object Monitor needs a normalized Kubernetes signal to consume.

Today the expected inventory is implicit. Operators may know that a class of nodes should have eight GPUs or eight RoCE interfaces, but NVSentinel does not persist that expectation in a normalized form that Kubernetes Object Monitor policies can consume.

This ADR defines that normalized inventory contract.

## Decision

Extend `labeler` to normalize device inventory into NVSentinel-owned node labels that express both current and expected device counts. Kubernetes Object Monitor can then compare these labels and emit health events when the current count is lower than the expected count.

The labels are derived from the best available source for the deployment mode:

- device-plugin/GFD labels for legacy GPU deployments;
- DRA `ResourceSlice` device inventory for GPU and NIC DRA deployments.

`labeler` owns the derived labels. Kubernetes Object Monitor consumes them.

## Inventory Class Contract

`labeler` is configured with inventory classes. Each class declares:

- the NVSentinel-owned labels to write;
- the grouping labels that define the hardware class;
- a CEL expression that returns the current count for the node.

The CEL environment is intentionally small and read-only. `labeler` builds the input context and the expression only counts from that context:

- `node`: the Kubernetes `Node` object being reconciled;
- `resourceSlices`: all raw `ResourceSlice` objects associated with that node.

Inventory class expressions can use either legacy node labels or DRA inventory:

- device-plugin deployments can read labels such as `nvidia.com/gpu.count` and `nvidia.com/gpu.product` from `node`;
- DRA deployments can count matching `ResourceSlice.spec.devices[]` entries from `resourceSlices`;
- DRA device semantics are driver-specific, so the expression is responsible for matching the right driver and device attributes for that inventory class.

The expression must return an integer. If it errors or returns a non-integer, `labeler` skips the current-count update for that class and records the evaluation failure. The first implementation will support standard CEL list macros such as `filter`, `map`, and `size`, and will register a `sum(list<int>) -> int` helper so DRA counts can be expressed across multiple `ResourceSlice` objects.

Example shape:

```yaml
labeler:
  expectedInventory:
    enabled: true
    classes:
      - name: gpu
        labels:
          current: nvsentinel.dgxc.nvidia.com/gpu.count.current
          expected: nvsentinel.dgxc.nvidia.com/gpu.count.expected
        groupingLabels:
          - node.kubernetes.io/instance-type
          - karpenter.sh/nodepool
          - nvidia.com/gpu.product
          - nvidia.com/gpu.sharing-strategy
        currentExpression: |
          int(node.metadata.labels['nvidia.com/gpu.count'])
      - name: nic.roce
        labels:
          current: nvsentinel.dgxc.nvidia.com/nic.roce.count.current
          expected: nvsentinel.dgxc.nvidia.com/nic.roce.count.expected
        groupingLabels:
          - node.kubernetes.io/instance-type
          - karpenter.sh/nodepool
        currentExpression: |
          sum(resourceSlices
            .filter(rs, rs.spec.driver == 'dra.networking.k8s.aws')
            .map(rs, rs.spec.devices
              .filter(d, d.attributes['dra.vpc.amazonaws.com/deviceType'].string == 'roce')
              .size()
            ))
```

Initial labels are current/expected pairs:

```text
nvsentinel.dgxc.nvidia.com/gpu.count.current
nvsentinel.dgxc.nvidia.com/gpu.count.expected
nvsentinel.dgxc.nvidia.com/nic.roce.count.current
nvsentinel.dgxc.nvidia.com/nic.roce.count.expected
```

- `current` is the count visible in the selected inventory source.
- `expected` is the stable baseline for the node's hardware class.
- `expected` may rise automatically when a higher count is observed in the same class, but it must not fall automatically when `current` drops.

The CEL environment will not provide arbitrary Kubernetes lookup/list functions. `labeler` controls the available inventory context so expressions remain deterministic, cheap to evaluate, and easy to reason about.

## Expected Count And Grouping

Expected counts are learned per inventory class and hardware-class partition. For each inventory class, the configured `groupingLabels` form the partition key. Nodes with the same values for that key are compared with each other, and nodes with different values are evaluated independently:

```text
partition = (inventory class, grouping label values)
expected = max(current count for nodes in the same partition)
```

This supports heterogeneous clusters by preventing unrelated hardware from influencing each other's expected counts. For GPU inventory, grouping labels can include values such as `nvidia.com/gpu.product`, `nvidia.com/gpu.sharing-strategy`, instance type, and nodepool. For RoCE inventory, grouping labels can include instance type and nodepool, while the inventory class expression itself selects the DRA driver and device type.

For example, AWS RoCE DRA advertises RoCE interfaces as devices:

```yaml
spec:
  driver: dra.networking.k8s.aws
  nodeName: worker-1.example.internal
  devices:
  - name: rocep172s0
    attributes:
      dra.vpc.amazonaws.com/deviceType:
        string: roce
      dra.vpc.amazonaws.com/networkDevName:
        string: ens164
```

`expected` may decrease only through an explicit relearn path, such as:

- an admin removing the expected-count label from a node or hardware class;
- an explicit labeler command or configuration flag to relearn a class;
- a future configuration object that declares expected inventory per class.

Cold start behavior should be conservative. If no expected label exists and only one node exists in a class, `labeler` may initialize expected from current, but that provides no protection against a device that was already missing before NVSentinel started. The ADR accepts this limitation for auto-learned baselines and leaves admin-provided expected inventory as a future hardening path.

## Consumer Policies

Kubernetes Object Monitor can consume the normalized labels with a simple Node policy.

GPU example:

```cel
'nvsentinel.dgxc.nvidia.com/gpu.count.current' in resource.metadata.labels &&
'nvsentinel.dgxc.nvidia.com/gpu.count.expected' in resource.metadata.labels &&
resource.metadata.labels['nvsentinel.dgxc.nvidia.com/gpu.count.current'].matches('^[0-9]+$') &&
resource.metadata.labels['nvsentinel.dgxc.nvidia.com/gpu.count.expected'].matches('^[0-9]+$') &&
int(resource.metadata.labels['nvsentinel.dgxc.nvidia.com/gpu.count.current']) <
int(resource.metadata.labels['nvsentinel.dgxc.nvidia.com/gpu.count.expected'])
```

The `matches` guards prevent malformed labels from producing CEL conversion errors. A malformed current or expected label should be treated as a labeler bug and surfaced through labeler metrics/logs, not as a device-missing health event.

```mermaid
flowchart TD
    devicePlugin["Device Plugin and GFD"] --> labeler["Labeler"]
    gpuDRA["GPU ResourceSlices"] --> labeler
    nicDRA["NIC ResourceSlices"] --> labeler
    labeler --> nodeLabels["NVSentinel Node Labels"]
    nodeLabels --> kom["Kubernetes Object Monitor"]
    kom --> healthEvent["Health Event"]
```

## Implementation

### CEL Evaluation

`labeler` compiles each inventory class `currentExpression` at startup. The environment includes the `node` and `resourceSlices` variables described above, plus one non-standard helper:

```text
sum(list<int>) -> int
```

`sum` returns the total of the integer values in the list and returns `0` for an empty list. It exists so expressions can count devices across multiple `ResourceSlice` objects without adding broader Kubernetes query helpers.

### RBAC

The labeler ClusterRole currently grants access to pods and nodes. DRA support requires read access to `ResourceSlice` objects:

```yaml
- apiGroups:
    - resource.k8s.io
  resources:
    - resourceslices
  verbs:
    - get
    - list
    - watch
```

Node patch/update permissions remain necessary for writing labels.

### Observability

Add metrics and structured logs for:

- current inventory count by node and device kind;
- expected inventory count by hardware class and device kind;
- label update success and failure counts;
- skipped updates due to missing labels, malformed source data, or CEL evaluation errors.

## Rationale

- `labeler` is already the component that watches node metadata and writes derived node labels.
- Normalizing inventory into labels keeps Kubernetes Object Monitor simple. It can evaluate one Node object instead of listing and aggregating all `ResourceSlice` objects for a node.
- Current and expected counts make the failure condition explicit and easy to audit from the Node object.

## Consequences

### Positive

- Enables missing-device detection without requiring XID, SXID, or syslog evidence.
- Supports both legacy device-plugin and DRA deployments through one label contract.
- Lets Kubernetes Object Monitor express missing-inventory detection as a small CEL policy.

### Negative

- Adds cross-node aggregation behavior to `labeler`.
- Requires new RBAC to watch `ResourceSlice` objects.
- Auto-learned expected counts are heuristic and depend on at least one healthy peer in the same hardware class.

### Mitigations

- Do not set current count to `0` when the inventory source is missing.
- Add metrics and logs for skipped inventory.
- Document admin relearn procedures before enabling fatal remediation from these labels by default.

## Alternatives Considered

### Compute Counts Directly in Kubernetes Object Monitor

**Rejected** because Kubernetes Object Monitor evaluates one object at a time and does not list or aggregate all `ResourceSlice` objects for a node. Adding list and reduce helpers to CEL would make it a cross-object aggregation engine and would complicate policy evaluation, caching, RBAC, and performance.

### Use `nvidia.com/gpu.count` Everywhere

**Rejected** because `nvidia.com/gpu.count` is not guaranteed in DRA deployments. It is also affected by MIG and sharing configuration, so it is not a universal representation of physical GPU count.

### Require Admin-Provided Expected Counts Only

**Rejected** as the only mechanism because it increases setup burden and makes the feature harder to adopt. Admin-provided expected counts remain a useful future enhancement for fleets where auto-learning is not strong enough.

## Notes

- The first implementation should treat `nic.roce.count.*` as RoCE interface count, not physical NIC card count.
- Fatal remediation should be enabled carefully. Operators may want STORE_ONLY behavior until expected-count learning has been validated in their fleet.

## References

- [Kubernetes Object Monitor](../kubernetes-object-monitor.md)
- [ADR-011: Kubernetes Object Monitor](./011-kubernetes-object-monitor.md)
