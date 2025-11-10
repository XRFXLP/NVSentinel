# ADR-011: Kubernetes Object Monitor

## Problem

Although NVSentinel has a pluggable architecture, the barrier to adding new monitors is high. Users must write, test, and deploy a complete health monitor to integrate existing health checks that don't fall into current monitor categories (e.g., storage checks). Many of these existing checks already manifest issues by modifying Kubernetes objects such as node conditions, taints, labels, and annotations.

## Solution 

Develop a policy-driven Kubernetes health monitor that is configurable via CEL policies and publishes health events based on rule matches. This allows users to integrate existing health checks without writing new health monitors.


An example of such configuration would be:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kubernetes-health-monitor
  namespace: nvsentinel
data:
  policies.yaml: |
    policies:
      # Node condition-based policy
      # Node not ready for more than 2 hours
      - name: GPUNodeNotReady
        enabled: true
        resourceType: Node
        predicate: |
          has(node.metadata.labels['nvidia.com/gpu.present']) &&
          node.metadata.labels['nvidia.com/gpu.present'] == 'true' &&
          node.status.conditions.exists(c, 
            c.type == 'Ready' && 
            (c.status == 'Unknown' || c.status == 'False')
          )
        timeThreshold: 2h
        healthEvent:
          componentClass: "Node"
          isFatal: true
          message: "GPU node has been NotReady for more than 2 hours"
          recommendedAction: "REBOOT_NODE"
      
      # Event-based policy (GPU container creation failures from Pods)
      # Note: These are Pod events, but indicate Node-level GPU issues
      # Events don't support timeThreshold - immediate evaluation
      - name: NVMLError
        enabled: true
        resourceType: Event
        predicate: |
          event.type == 'Warning' &&
          event.reason == 'Failed' &&
          event.involvedObject.kind == 'Pod' &&
          event.source.component == 'kubelet' &&
          event.message.contains('nvidia-container-cli') &&
          event.message.contains('nvml error') &&
          event.count > 2
        healthEvent:
          componentClass: "GPU"
          isFatal: true
          message: "GPU container runtime failures detected - likely node GPU driver issue"
          recommendedAction: "REBOOT_NODE"
```

### Design

#### Architecture Overview

The Kubernetes Object Monitor is implemented as a **Deployment** that uses **Kubernetes informers** with a **work-queue** for efficient cluster-wide monitoring. It evaluates CEL-based policies against Kubernetes objects (Nodes, Events) and publishes HealthEvents when policies match.

**Key Components:**
- **Informer-based watch**: Efficiently monitors Nodes and Events using Kubernetes client-go informers
- **Work-queue**: Provides retry logic, rate-limiting, and deduplication
- **CEL Policy Engine**: Evaluates user-defined predicates against Kubernetes objects


**Watched Resources:**
- **Nodes**: Primary resource for condition, taint, label, and annotation checks
- **Events**: Kubernetes events (Node, Pod, etc.) for detecting specific conditions

#### Policy Structure

Policies consist of three key components:

1. **Resource Type**: The Kubernetes resource to watch (`Node` or `Event`)
2. **Predicate** (CEL expression): Boolean condition evaluated against the resource object
3. **Time Threshold** (optional): Duration the predicate must remain true before triggering an event
   - **Only supported for Node condition-based predicates** (Node conditions with `LastTransitionTime`)
   - Not supported for labels, taints, annotations, or events

If no time threshold is specified, events are sent immediately when the predicate matches.


**CEL Variables:**
- `node` - Available in Node-type policies (corev1.Node object)
- `event` - Available in Event-type policies (corev1.Event object)
- `now` - Current timestamp (time.Time)

#### Three-Stage State Machine

Each **Node condition-based policy** (with time thresholds) transitions through three stages:

**Note:** Label, taint, annotation, and event-based policies skip Stage 2 and go directly from Stage 1 → Stage 3 (immediate evaluation).

**Stage 1: Unmatched**
- Predicate evaluates to `false`
- No state tracking required
- If transitioning from Stage 3 (Matched), send a healthy/recovery event

**Stage 2: Matched but Waiting**
- Predicate evaluates to `true` but time threshold not yet met
- Controller tracks when predicate first became true
- Reconciliation is requeued to check again when threshold will be reached
- If predicate becomes false, transition back to Stage 1

**Stage 3: Matched**
- Predicate evaluates to `true` AND time threshold met (or no threshold configured)
- HealthEvent is published (once per condition instance)
- State remains in Stage 3 until predicate becomes false

```
┌─────────────┐
│  Unmatched  │
│  (Stage 1)  │
└──────┬──────┘
       │ Predicate becomes true
       ↓
┌─────────────────────┐
│  Matched but        │
│  Waiting (Stage 2)  │◄────┐ Requeue while waiting
│                     │     │
└──────┬──────────────┘─────┘
       │ Time threshold met (or no threshold)
       ↓
┌─────────────┐
│   Matched   │──→ Send HealthEvent (once)
│  (Stage 3)  │
└──────┬──────┘
       │ Predicate becomes false
       ↓
┌─────────────┐
│  Unmatched  │──→ Send recovery event
│  (Stage 1)  │
└─────────────┘
```

**Event-based policies** use a simplified flow with recovery:
- When event matches predicate, publish unhealthy event immediately (no time threshold support)
- Track when last matching event was seen and requeue after grace period (10 minutes)
- **Recovery detection**: If requeue triggers and no new matching events, publish recovery event
- State tracking: Last event timestamp per node-policy combination


#### Reconciliation Triggers

The controller reconciles when:

**1. Resource changes (event-driven):**
- **Node changes**: Node informer detects Node create/update/delete
  - Triggers reconciliation for all Node-based policies for that specific Node
- **Event changes**: Event informer detects Event create/update
  - Triggers reconciliation for all Event-based policies for that specific Event

**2. Time-based requeue (waiting for thresholds):**
- **Node condition policies**: When in Stage 2 (Matched but Waiting)
  - Controller calculates when time threshold will be met
  - Requeues at that exact time: `ctrl.Result{RequeueAfter: duration}`
- **Event-based policies**: When event matches
  - Records last seen timestamp
  - Requeues after grace period (10 minutes): `ctrl.Result{RequeueAfter: 10m}`
  - If another matching event arrives before requeue, timestamp updates and requeue is rescheduled
  - If requeue happens with no new events, publishes recovery event

#### Reconciliation Logic

**For Node-based policies:**
1. Retrieve the Node object (from informer cache)
2. For each Node policy, evaluate the CEL predicate
3. If condition-based with timeThreshold:
   - Use 3-stage state machine (check `LastTransitionTime` against threshold)
   - Requeue if in "Matched but Waiting" stage
4. If label/taint/annotation-based:
   - Immediate evaluation (Stage 1 → Stage 3 directly)
5. Multiple reconciliations run concurrently in separate goroutines

**For Event-based policies:**
1. Evaluate Event policies against the event (from informer)
2. CEL predicate determines which events to act on
3. If predicate matches:
   - Immediately publish unhealthy event (or update if already sent)
   - Record last seen timestamp for this node-policy combination
   - Requeue reconciliation after grace period (10 minutes)
4. On requeue (after grace period):
   - Check if new matching events arrived (updated timestamp)
   - If yes: Reschedule requeue for another grace period
   - If no: Publish recovery event, clear state
5. For Pod events indicating Node issues:
   - Look up the Pod to determine which Node it's scheduled on
   - Associate the health event with that Node



**Time evaluation strategy:**
- Prefer built-in timestamps (NodeCondition.LastTransitionTime) over tracked timestamps
- For Events, use event.firstTimestamp and event.count for frequency analysis

#### State Tracking

**State Persistence:**

State is written to disk as JSON at `/var/run/nvsentinel/k8s-monitor-state.json`. State tracks:
- Current stage for each node-policy combination (Stage 1, 2, or 3) - for Node condition policies
- Whether events have been sent (to prevent duplicates)
- **For Event-based policies**: Last seen timestamp per node-policy combination

**Example state file:**
```json
{
  "version": 1,
  "nodeStates": {
    "worker-gpu-01": {
      "GPUNodeNotReady": {
        "stage": 3,
        "eventSent": true,
        "lastEvaluated": "2025-11-10T14:30:00Z"
      }
    },
    "worker-gpu-02": {
      "GPUNodeNotReady": {
        "stage": 2,
        "eventSent": false,
        "lastEvaluated": "2025-11-10T14:25:00Z"
      }
    }
  },
  "eventStates": {
    "worker-gpu-01": {
      "NVMLError": {
        "lastSeenTimestamp": "2025-11-10T14:28:00Z",
        "eventSent": true
      }
    }
  }
}
```

**Note on timestamps:**
- Node condition-based policies use built-in Kubernetes timestamps (`LastTransitionTime` from API)
- Event-based policies track last seen event timestamp in state (for recovery detection)
- Label/taint/annotation policies don't support time thresholds (immediate evaluation)


**Recovery Event Handling:**

Recovery events depend on the **resource type being watched**:

**1. Node-based policies** (watch Node objects):
- ✅ **Full lifecycle support** - can detect recovery
- Condition changes: `Ready=False` → `Ready=True`
- Taints: Taint removed
- Labels: Label removed or changed
- Uses 3-stage state machine (or immediate for labels/taints)

**2. Event-based policies** (watch Event objects):
- ✅ **Automatic recovery detection** based on event absence
- When matching events appear: Publish unhealthy event, requeue after grace period
- When requeue triggers with no new events: Publish recovery event
- Grace period configurable (default: 10 minutes)
- Example: Container failures stop → automatic recovery after 10 minutes
- Uses time-based requeue mechanism (same as Node condition waiting)
