# ADR-028: Janitor — Generic Bare-Metal Reboot Provider

## Context

The janitor-provider supports node reboots through CSP APIs (AWS EC2, GCP Compute, Azure, OCI, Nebius). If the `CSP` environment variable is set to an unrecognized value, the provider returns an error and refuses to start.

This doesn't work for environments without a CSP reboot API:

- On-premises bare-metal clusters
- Infrastructure providers that do not expose a reboot API
- Self-managed Kubernetes clusters on physical hardware

### Current Architecture

```mermaid
flowchart TD
    JC[Janitor Controller] -->|"gRPC: SendRebootSignal / IsNodeReady"| JP[Janitor Provider]
    JP -->|"CSP env var selects provider"| Factory{Provider Factory}
    Factory --> AWS[AWS Provider]
    Factory --> GCP[GCP Provider]
    Factory --> Azure[Azure Provider]
    Factory --> OCI[OCI Provider]
    Factory --> Nebius[Nebius Provider]
    Factory --> Kind[Kind Provider]
    Factory -->|"Unknown CSP"| ERR["Error: unsupported CSP"]
    AWS -->|"EC2 RebootInstances"| Cloud["CSP Control Plane"]
    GCP -->|"Compute Stop/Start"| Cloud
    Azure -->|"VM BeginRestart"| Cloud
    OCI -->|"InstanceAction Softreset"| Cloud
    Nebius -->|"Instance Stop/Start"| Cloud
    Kind -->|"Simulated sleep"| Sim["No-op Simulation"]
```

## Decision

Add a **generic provider** that reboots nodes via a privileged Kubernetes Job running `nsenter ... /sbin/reboot`, following the Job-based pattern from GPU Reset ([ADR-019](019-janitor-gpu-reset.md)).

The generic provider is selected via an explicit opt-in: a new `csp.rebootMethod` configuration field. When set to `"host-reboot"`, the factory returns the generic provider regardless of the `CSP` value. The `CSP` name is preserved for logging and metrics. Unknown `CSP` values without an explicit opt-in continue to produce an error, preventing accidental privileged reboots from typos.

## Implementation

### 1. Provider Factory — Explicit Opt-In

```mermaid
flowchart TD
    Start["Read CSP and REBOOT_METHOD env vars"] --> CheckMethod{"REBOOT_METHOD = host-reboot?"}
    CheckMethod -->|Yes| GEN["generic.NewClient"]
    CheckMethod -->|No| Switch{"Match known CSP?"}
    Switch -->|aws| AWS[aws.NewClient]
    Switch -->|gcp| GCP[gcp.NewClient]
    Switch -->|azure| AZR[azure.NewClient]
    Switch -->|oci| OCI[oci.NewClient]
    Switch -->|nebius| NEB[nebius.NewClient]
    Switch -->|kind| KND[kind.NewClient]
    Switch -->|unknown| ERR["Error: unsupported CSP"]
```

When `rebootMethod=host-reboot` is set, the generic provider is used regardless of the `CSP` value. When `rebootMethod` is empty (the default), the factory routes to the CSP-specific provider or errors on unknown values.

### 2. Generic Provider — Reboot via Privileged Job

```mermaid
sequenceDiagram
    participant JC as Janitor Controller
    participant GP as Generic Provider
    participant K8s as Kubernetes API
    participant Job as Reboot Job Pod
    participant Node as Target Node

    JC->>GP: SendRebootSignal(node)
    GP->>K8s: Read node.Status.NodeInfo.BootID
    GP->>K8s: Create privileged Job with nodeName=target
    GP-->>JC: requestID = pre-reboot bootID

    K8s->>Job: Schedule pod on target node
    Job->>Node: nsenter --target 1 ... /sbin/reboot
    Note over Node: Node reboots, pod is killed

    Note over Node: Node boots back up, new bootID assigned

    loop Poll every 60s
        JC->>GP: IsNodeReady(node, requestID)
        GP->>K8s: Get current bootID and Ready condition
        GP-->>JC: bootID changed AND Ready=True?
    end

    Note over JC: Sets CompletionTime, releases lock
```

#### SendRebootSignal

Records the node's current `bootID` from `node.Status.NodeInfo.BootID`, creates a privileged Job on the target node, and returns the pre-reboot `bootID` as the `requestID`.

**Job specification:**

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: reboot-<node-name>-<short-hash>
  namespace: <configured-namespace>
  labels:
    nvsentinel.nvidia.com/reboot-job: "true"
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 3600
  template:
    spec:
      nodeName: <target-node>
      hostPID: true
      restartPolicy: Never
      tolerations:
        - operator: "Exists"
      containers:
        - name: reboot
          image: busybox:1.37
          command:
            - nsenter
            - --target
            - "1"
            - --mount
            - --uts
            - --ipc
            - --net
            - --
            - /sbin/reboot
          securityContext:
            privileged: true
```

**Design choices (mirroring GPU Reset):**

| Choice | Value | Rationale |
|--------|-------|-----------|
| `hostPID` | `true` | Required for `nsenter` to access PID 1 |
| `privileged` | `true` | Required to enter host namespaces |
| `backoffLimit` | `0` | Reboot kills the pod — retrying would double-reboot |
| `ttlSecondsAfterFinished` | `3600` | Auto-cleanup after 1h |
| `tolerations` | `[{operator: Exists}]` | Target node is likely cordoned/tainted |
| `restartPolicy` | `Never` | Do not restart after reboot |
| Image | `busybox:1.37` | Only needs `nsenter` and `/sbin/reboot` |

#### IsNodeReady

The `requestID` is the pre-reboot `bootID`. A changed `bootID` is definitive proof that the node rebooted — unlike `lastTransitionTime`, it cannot change due to network blips or other non-reboot events.

```mermaid
flowchart TD
    Start["IsNodeReady called with requestID (pre-reboot bootID)"] --> GetBoot["Get current node.Status.NodeInfo.BootID"]
    GetBoot --> CheckBoot{"Current bootID != requestID?"}
    CheckBoot -->|No| RetFalse["Return false (not yet rebooted)"]
    CheckBoot -->|Yes| CheckReady{"Ready condition = True?"}
    CheckReady -->|No| RetFalse
    CheckReady -->|Yes| RetTrue["Return true"]
```

If the reboot never happened, `bootID` stays the same and the janitor controller will eventually time out.

Once `IsNodeReady` confirms a successful reboot, it deletes the reboot Job before returning `true`. This avoids a lingering "Failed" Job (the pod is killed by the reboot, so the Job always ends in a failed state).

### 3. Helm Configuration

```yaml
# distros/kubernetes/nvsentinel/charts/janitor-provider/values.yaml
csp:
  provider: "kind"              # existing field, unchanged
  rebootMethod: ""              # NEW — "" (default, use CSP API) or "host-reboot"

  generic:                      # config for the generic provider (when rebootMethod=host-reboot)
    rebootImage: "busybox:1.37"
    rebootJobNamespace: ""      # defaults to the janitor-provider's own namespace
    rebootJobTTLSeconds: 3600
```

```yaml
# deployment.yaml env injection
env:
  - name: CSP
    value: {{ .Values.csp.provider | default "kind" | quote }}
  - name: REBOOT_METHOD
    value: {{ .Values.csp.rebootMethod | quote }}
  - name: GENERIC_REBOOT_IMAGE
    value: {{ .Values.csp.generic.rebootImage | default "busybox:1.37" | quote }}
  - name: GENERIC_REBOOT_JOB_NAMESPACE
    value: {{ .Values.csp.generic.rebootJobNamespace | quote }}
  - name: GENERIC_REBOOT_JOB_TTL
    value: {{ .Values.csp.generic.rebootJobTTLSeconds | default 3600 | quote }}
```

### 4. RBAC

Today the janitor-provider is a read-only gRPC service — its ClusterRole only grants `get`/`list`/`watch` on `nodes`, and all Kubernetes write operations (creating RebootNode CRs, managing Leases) belong to the janitor controller. The generic provider changes this boundary: the janitor-provider now creates and deletes Jobs.

This is scoped as tightly as possible. The janitor-provider retains its existing ClusterRole for read-only node access. Job permissions are granted via a separate namespaced **Role**, limiting writes to the provider's own namespace:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ include "provider.fullname" . }}-jobs
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["create", "get", "list", "watch", "delete"]
```

The janitor controller's RBAC is unchanged — it continues to own RebootNode lifecycle and distributed locking. The provider's new write scope is limited to ephemeral reboot Jobs in a single namespace.

### 5. File Locations

| File | Change |
|------|--------|
| `janitor-provider/pkg/csp/generic/generic.go` | New — generic provider implementation |
| `janitor-provider/pkg/csp/generic/generic_test.go` | New — unit tests |
| `janitor-provider/pkg/csp/client.go` | Modified — check `REBOOT_METHOD` before CSP switch |
| `janitor-provider/pkg/csp/client_test.go` | Modified — tests for opt-in routing |
| `distros/.../charts/janitor-provider/values.yaml` | Modified — add `csp.rebootMethod` and `csp.generic` |
| `distros/.../charts/janitor-provider/templates/deployment.yaml` | Modified — inject `REBOOT_METHOD` and generic env vars |
| `distros/.../charts/janitor-provider/templates/role.yaml` | New — namespaced Role for `batch/jobs` permissions |
| `distros/.../charts/janitor-provider/templates/rolebinding.yaml` | New — RoleBinding for the above |

## Rationale

- **Proven pattern**: Same Job-based architecture as GPU Reset ([ADR-019](019-janitor-gpu-reset.md)), validated in production
- **No custom image**: `busybox` with `nsenter` is sufficient
- **Safe by default**: Running `sudo reboot` via a privileged pod requires explicit opt-in. A CSP name typo produces an error, not an accidental bare-metal reboot.
- **CSP identity preserved**: The `CSP` name remains available for logging and metrics even when using the generic reboot method

## Consequences

### Positive
- Enables automated reboot remediation on bare-metal and non-cloud environments
- Consistent remediation workflow regardless of infrastructure type
- New providers can be onboarded with `CSP=<name>` + `rebootMethod=host-reboot` — no code needed
- Kubernetes-native (Jobs, RBAC, tolerations)

### Negative
- Requires **privileged pod with hostPID**
- No `SendTerminateSignal` support for bare-metal nodes
- Extra configuration field (`rebootMethod`)

### Mitigations
- **Security**: Ephemeral Job with TTL cleanup, RBAC-restricted RebootNode creation. Matches GPU Reset security model.
- **No terminate**: Bare-metal deployments should set `terminateNodeController.enabled=false`.

## Alternatives Considered

### Implicit Fallback (Unknown CSP → Generic)
**Rejected**: A CSP name typo (e.g., `"awss"`) would silently create a privileged pod and reboot a cloud node via `sudo reboot` instead of using the CSP API. Running `sudo reboot` is a high-impact action that should require explicit intent.

### SSH-Based Reboot
**Rejected**: Requires SSH key management and network access to all nodes.

### Privileged DaemonSet Agent
**Rejected**: Larger security surface than an ephemeral Job, wastes resources while idle. Also rejected in [ADR-019](019-janitor-gpu-reset.md).


## Testing

- **Unit**: Job spec generation, `IsNodeReady` bootID comparison logic, factory opt-in routing, config loading
- **Integration**: Mock K8s client, verify Job creation with correct spec, verify `IsNodeReady` for various node conditions

## Notes

- `busybox:1.37` is overridable via Helm for custom image registries
- Job TTL (1h default) is tunable per-environment
- Future providers (IPMI, Redfish) can be added as named factory cases

## References

- [ADR-019: Janitor GPU Reset](019-janitor-gpu-reset.md) — Job-based pattern for host-level operations
- [ADR-020: NVSentinel GPU Reset](020-nvsentinel-gpu-reset.md) — End-to-end GPU reset in NVSentinel
- [ADR-008: Cloud Provider Integration](008-cloud-provider-integration.md) — Current CSP architecture
- [Kubernetes Jobs](https://kubernetes.io/docs/concepts/workloads/controllers/job/)
- [nsenter](https://man7.org/linux/man-pages/man1/nsenter.1.html)
