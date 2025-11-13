# Metadata Enrichment: Enhancing GPU Infrastructure Observability

## Why Metadata Enrichment?

Modern GPU infrastructure faces three critical challenges:

1. **Debuggability**: When errors occur, operators need to quickly identify *which specific hardware* is failing
2. **Visibility**: Infrastructure spans multiple clusters, clouds, and physical locations - correlation is manual and time-consuming  
3. **Remediation Efficiency**: Without precise identification, remediation is overly broad (draining entire nodes instead of specific GPUs)

**The Problem**: Health monitors detect failures using disparate identifiers - PCI addresses, device indexes, node names, IP addresses. These identifiers:
- Don't map to each other without external lookups
- Aren't unique across clusters
- Don't provide hardware lifecycle context

**The Solution**: Metadata enrichment bridges these gaps through two complementary approaches:

| Enrichment Type | Purpose | Enables |
|----------------|---------|---------|
| **Entity-Level** | Hardware identification | GPU UUID, chassis serial, PCI mapping |
| **Node-Level** | Infrastructure context | Cloud provider ID, cluster, rack, topology |

Together, these create a complete identification and correlation framework from error detection → remediation → lifecycle tracking.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  Metadata Sources                                               │
│                                                                 │
│  ┌──────────────────────┐        ┌──────────────────────────┐   │
│  │ GPU Metadata         │        │ Node Metadata            │   │
│  │ Collector            │        │ (Kubernetes API)         │   │
│  │                      │        │                          │   │
│  │ Source: go-nvml      │        │ Source: K8s API Server   │   │
│  │ Runs: Init container │        │ Accessed by: Platform    │   │
│  │       per node       │        │              Connector   │   │
│  │                      │        │                          │   │
│  │ Provides:            │        │ Provides:                │   │
│  │ • GPU UUID           │        │ • Provider ID            │   │
│  │ • PCI address        │        │ • Node labels            │   │
│  │ • Chassis serial     │        │ • Topology info          │   │
│  │ • Device index       │        │ • Instance type          │   │
│  └──────────────────────┘        └──────────────────────────┘   │
│           │                                   │                 │
│           │                                   │                 │
└───────────┼───────────────────────────────────┼─────────────────┘
            │                                   │
            ▼                                   ▼
┌─────────────────────┐            ┌─────────────────────────────┐
│ Health Monitors     │            │ Platform Connector          │
│                     │            │                             │
│ Enrich with:        │   gRPC     │ Further enriches with:      │
│ • GPU UUID          │───────────▶│ • Provider ID               │
│ • Chassis serial    │            │ • Cluster name              │
│                     │            │ • Rack ID                   │
└─────────────────────┘            │ • Topology labels           │
                                   └─────────────────────────────┘
                                                │
                                                ▼
                                   ┌─────────────────────────────┐
                                   │ MongoDB (events)            │
                                   │                             │
                                   │ Complete enriched events:   │
                                   │ • gpu_uuid (entity)         │
                                   │ • chassis_serial (entity)   │
                                   │ • provider_id (node)        │
                                   │ • cluster/rack (node)       │
                                   └─────────────────────────────┘
                                                │
                                                ▼
                                   ┌─────────────────────────────┐
                                   │ Fault Management            │
                                   │                             │
                                   │ • Correlate by gpu_uuid     │
                                   │ • Query by cluster/rack     │
                                   │ • Precise GPU-level actions │
                                   └─────────────────────────────┘
```

---

## Part 1: Entity-Level Metadata (GPU Hardware)

### Problem: Identifier Fragmentation

Different health monitors use different identifiers for the same GPU:

```
Syslog Monitor:  "XID on PCI 0000:17:00.0"
DCGM Monitor:    "Error on GPU device 0"
Operator:        "Which GPU UUID is this?"
```

**Impact**: Cannot correlate events across monitors → Cannot confirm if it's the same GPU failing

### Solution: GPU Metadata Collector

A DaemonSet init container collects and maps all GPU identifiers:

```json
// /var/lib/nvsentinel/gpu_metadata.json
{
  "chassis_serial": "DGX-A100-SN123456",
  "gpus": [
    {
      "uuid": "GPU-abc123-def456-789012-345678",
      "pci_address": "0000:17:00.0",
      "device_index": 0,
      "model": "A100-SXM4-40GB"
    }
  ]
}
```

Health monitors read this file and enrich events with GPU UUID as the universal correlation key.

### Benefits

| Before | After |
|--------|-------|
| XID on PCI `0000:17:00.0` | XID on GPU `GPU-abc123...` |
| DCGM error on device `0` | DCGM error on GPU `GPU-abc123...` |
| Manual correlation needed | Automatic: both events share `gpu_uuid` |
| Drain entire node (8 GPUs) | Quarantine specific GPU |
| No RMA tracking | Chassis serial enables lifecycle tracking |

---

## Part 2: Node-Level Metadata (Infrastructure Context)

> **Related**: [GitHub Issue #119](https://github.com/NVIDIA/NVSentinel/issues/119)

### Problem: Infrastructure Correlation

As fleets grow, node names alone are insufficient:

**Challenge 1: Node uniqueness across clusters**
```
Cluster A: node-name = "10.0.1.5"
Cluster B: node-name = "10.0.1.5"  ← Collision!

Query: db.events.find({ node_name: "10.0.1.5" })
Result: Events from BOTH clusters mixed ❌
```

**Challenge 2: Physical topology visibility**
```
Event: { node_name: "node-05" }
Question: Which rack? Which datacenter? Which cluster?
Answer: Requires manual lookup 😓
```

### Solution: Node Metadata Enrichment

Platform Connector queries Kubernetes API and enriches events:

```go
// Platform Connector enrichment
node, _ := k8s.Nodes().Get(event.NodeName)

event.Metadata["provider_id"] = node.Spec.ProviderID
// Examples:
//   "aws:///us-west-2a/i-abc123..."        (globally unique)
//   "azure:///subscriptions/.../..."        (globally unique)

// Add allow-listed node labels
for _, label := range allowList {
    event.Metadata[label] = node.Labels[label]
}
```

**Configuration** (platform-connector):
```yaml
node_metadata:
  allowed_labels:
    - topology.kubernetes.io/region
    - topology.kubernetes.io/zone
    - cluster-name           # Custom label
    - rack-id                # Custom label
    - datacenter             # Custom label
```

### Benefits

| Before | After |
|--------|-------|
| Node name only (may collide) | Provider ID (globally unique) |
| Unknown cluster | `metadata.cluster-name` |
| Unknown rack | `metadata.rack-id` |
| Manual topology lookup | Automatic via labels |
| 30 min manual correlation | < 1 sec MongoDB query |

---

## Complete Event Structure

With both entity and node enrichment, events contain full context:

```json
{
  "_id": ObjectId("..."),
  "timestamp": "2025-11-13T10:30:00Z",
  "agent": "syslog-health-monitor",
  "check_name": "xid_critical",
  "is_healthy": false,
  
  // Entity-level (from GPU Metadata Collector)
  "gpu_uuid": "GPU-abc123...",           // ← Correlation key
  "pci_address": "0000:17:00.0",
  "chassis_serial": "DGX-A100-SN123456",
  "device_index": 0,
  
  // Node-level (from Platform Connector)
  "node_name": "10.0.1.5",
  "provider_id": "aws:///us-west-2a/i-abc123...",  // ← Globally unique
  "metadata": {
    "cluster-name": "prod-ml-01",
    "rack-id": "rack-12",
    "datacenter": "aws-us-west-2",
    "topology.kubernetes.io/zone": "us-west-2a"
  }
}
```

---

## Use Cases

### 1. Cross-Monitor Correlation (Entity-Level)

**Scenario**: Same GPU fails, detected by two monitors

```javascript
// Query: Events for specific GPU in 5-minute window
db.events.find({
  gpu_uuid: "GPU-abc123...",
  timestamp: { $gte: ISODate("2025-11-13T10:30:00Z") }
})

// Result: Correlated events
[
  {
    agent: "syslog-health-monitor",
    timestamp: "10:30:00Z",
    message: "XID 63 detected"
  },
  {
    agent: "gpu-health-monitor", 
    timestamp: "10:30:05Z",      // 5 seconds later
    message: "Memory error count: 1024"
  }
]

// Insight: Memory failure caused XID → Confirmed hardware issue
```

### 2. Fleet-Wide Analytics (Node-Level)

**Scenario**: Which clusters have the most issues?

```javascript
// Cross-cluster GPU health summary
db.events.aggregate([
  {
    $match: {
      component_class: "GPU",
      is_healthy: false,
      timestamp: { $gte: ISODate("2025-11-01T00:00:00Z") }
    }
  },
  {
    $group: {
      _id: {
        cluster: "$metadata.cluster-name",
        zone: "$metadata.topology.kubernetes.io/zone"
      },
      failure_count: { $sum: 1 },
      unique_gpus: { $addToSet: "$gpu_uuid" }
    }
  }
])

// Output:
[
  { cluster: "prod-ml-01", zone: "us-west-2a", failures: 156, gpus: 32 },
  { cluster: "prod-training-02", zone: "us-east-1b", failures: 89, gpus: 18 }
]
```

### 3. Rack-Level Pattern Detection (Node-Level)

**Scenario**: Detect systemic issues in a rack

```javascript
// Events per rack in last 24h
db.events.aggregate([
  {
    $match: {
      "metadata.datacenter": "aws-us-west-2",
      is_healthy: false,
      timestamp: { $gte: ISODate("2025-11-13T00:00:00Z") }
    }
  },
  {
    $group: {
      _id: "$metadata.rack-id",
      event_count: { $sum: 1 },
      affected_nodes: { $addToSet: "$node_name" },
      error_types: { $addToSet: "$check_name" }
    }
  }
])

// Output:
[
  {
    rack_id: "rack-12",
    event_count: 47,              // High event count
    affected_nodes: 6,            // Multiple nodes
    error_types: ["xid_critical", "thermal_threshold"]
    // ⚠️ Likely cooling issue in rack-12
  }
]
```

### 4. Surgical Remediation (Entity-Level)

**Scenario**: Quarantine only the failing GPU

```javascript
// Node Drainer watches MongoDB
db.events.watch([
  { $match: { "fullDocument.is_fatal": true } }
])

// Receives event:
{
  gpu_uuid: "GPU-abc123...",
  node_name: "dgx-node-03",
  pci_address: "0000:17:00.0"
}

// Action: Taint node with GPU-specific key
// → Evicts ONLY pods using GPU-abc123
// → Other 7 GPUs remain in service ✅
```

---

## Data Flow

### End-to-End: Error → Remediation

```
T+0s:  Kernel XID on PCI 0000:17:00.0

T+1s:  Syslog Monitor
       ├─ Reads /var/lib/nvsentinel/gpu_metadata.json
       ├─ Maps PCI → GPU-abc123...
       └─ Publishes enriched event (entity metadata)

T+2s:  Platform Connector
       ├─ Receives event via gRPC
       ├─ Queries K8s API for node metadata
       ├─ Enriches with provider_id, cluster, rack
       └─ Stores to MongoDB (entity + node metadata)

T+3s:  Health Events Analyzer
       ├─ Queries: gpu_uuid = GPU-abc123...
       ├─ Finds correlated DCGM event (same UUID)
       └─ Confirms: hardware failure

T+10s: Node Drainer
       ├─ Detects fatal event
       ├─ Taints node: gpu-xid=GPU-abc123...
       └─ Drains ONLY affected GPU

Result:
✅ 1 GPU quarantined
✅ 7 GPUs remain in service (87.5% capacity preserved)
✅ Full audit trail in MongoDB
✅ RMA tracking enabled via chassis serial
```

---

## MongoDB Indexes

Optimized for both entity and node queries:

```javascript
// Entity-level queries
db.events.createIndex({ gpu_uuid: 1, timestamp: -1 })
db.events.createIndex({ chassis_serial: 1 })

// Node-level queries  
db.events.createIndex({ provider_id: 1, timestamp: -1 })
db.events.createIndex({ "metadata.cluster-name": 1, timestamp: -1 })
db.events.createIndex({ "metadata.rack-id": 1, is_healthy: 1 })

// Combined
db.events.createIndex({ 
  gpu_uuid: 1, 
  "metadata.cluster-name": 1,
  timestamp: -1 
})
```

---

## Configuration Examples

### GPU Metadata Collector (Entity-Level)

```yaml
# DaemonSet init container
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: gpu-metadata-collector
spec:
  template:
    spec:
      initContainers:
      - name: collect-metadata
        image: gpu-metadata-collector:latest
        volumeMounts:
        - name: metadata
          mountPath: /var/lib/nvsentinel
        - name: nvidia
          mountPath: /usr/local/nvidia
      
      containers:
      - name: pause
        image: gcr.io/google_containers/pause:latest
      
      volumes:
      - name: metadata
        hostPath:
          path: /var/lib/nvsentinel
      - name: nvidia
        hostPath:
          path: /usr/local/nvidia
```

### Platform Connector (Node-Level)

```yaml
# ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: platform-connector-config
data:
  config.yaml: |
    node_metadata:
      enabled: true
      include_provider_id: true
      
      allowed_labels:
        # Kubernetes standard
        - topology.kubernetes.io/region
        - topology.kubernetes.io/zone
        - node.kubernetes.io/instance-type
        
        # GPU-specific
        - nvidia.com/gpu.product
        - nvidia.com/gpu.count
        
        # Custom organizational
        - cluster-name
        - datacenter
        - rack-id
        - environment
      
      cache:
        ttl: 5m
        max_size: 10000
```

---

## Impact Summary

### Debuggability

| Metric | Before | After |
|--------|--------|-------|
| Identify failing GPU | Manual PCI → UUID lookup | Automatic in event |
| Correlate across monitors | Manual log analysis | Query by `gpu_uuid` |
| Find cluster/rack | Manual lookup | In event metadata |
| Time to root cause | 15-30 minutes | < 1 minute |

### Visibility

| Metric | Before | After |
|--------|--------|-------|
| Cross-cluster queries | Not possible | By `cluster-name` |
| Fleet-wide health | Manual aggregation | MongoDB analytics |
| Rack pattern detection | Not possible | Automatic via `rack-id` |
| Provider comparison | Not tracked | By `provider_id` |

### Remediation Efficiency

| Metric | Before | After |
|--------|--------|-------|
| GPUs affected per failure | 8 (entire node) | 1 (specific GPU) |
| Capacity preserved | 0% | 87.5% |
| Action granularity | Node-level | GPU-level |
| False positive cost | High | Minimal |

---

## Key Takeaways

1. **Two-Tier Enrichment**:
   - Entity-level (GPU UUID, chassis) enables precise hardware identification
   - Node-level (provider ID, labels) enables infrastructure correlation

2. **Universal Correlation Key**: GPU UUID connects events across health monitors

3. **Fleet-Scale Visibility**: Provider ID and labels enable multi-cluster analytics

4. **Surgical Remediation**: Precise identification allows GPU-level actions, not node-level

5. **Complete Audit Trail**: MongoDB stores enriched events for debugging, RMA tracking, and compliance

The metadata enrichment system transforms GPU infrastructure from opaque black boxes into fully observable, precisely remediable, fleet-scale intelligent systems.
