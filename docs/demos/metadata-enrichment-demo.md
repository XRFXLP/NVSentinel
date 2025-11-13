# Metadata Enrichment

## Overview

NVSentinel's metadata enrichment transforms fragmented GPU error detection into precise, fleet-scale observability.

**Journey**: Raw kernel logs → Isolated events → Enriched hardware context → Complete infrastructure visibility

---

## Part 1: The Baseline (Before Enrichment)

### Architecture

```
┌────────────────────────────────────────────────────────────────┐
│ GPU Node                                                       │
└────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────┐
    │ Kernel                                                   │
    │                                                          │
    │ NVRM: Xid (PCI:0000:17:00): 79, pid='<unknown>',         │
    │       name=<unknown>, GPU has fallen off the bus.        │
    └────────────────────────┬─────────────────────────────────┘
                             │ syslog/dmesg
                             ▼
    ┌──────────────────────────────────────────────────────────┐
    │ Syslog Health Monitor                                    │
    │ (DaemonSet container)                                    │
    │                                                          │
    │ • Parses XID from kernel log                             │
    │ • Extracts PCI address                                   │
    │ • Creates HealthEvent                                    │
    │                                                          │
    │ ❌ No GPU UUID (can't correlate with other monitors)     │
    │ ❌ No chassis serial (can't track RMA)                   │
    └────────────────────────┬─────────────────────────────────┘
                             │
                             │ gRPC
                             │
                ┌────────────▼────────────────┐
                │ Event Structure:            │
                │                             │
                │ {                           │
                │   "agent": "syslog-...",    │
                │   "checkName": "SysLogs...",│
                │   "isFatal": true,          │
                │   "entitiesImpacted": [     │
                │     {                       │
                │       "entityType": "PCI",  │
                │       "entityValue":        │
                │         "0000:17:00.0"      │
                │     }                       │
                │   ],                        │
                │   "metadata": {},           │
                │   "nodeName": "gpu-node-42" │
                │ }                           │
                └────────────┬────────────────┘
                             │
────────────────────────────────────────────────────────────────

┌────────────────────────────────────────────────────────────────┐
│ Platform Connector                                             │
└────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────┐
    │ Event Receiver                                           │
    │                                                          │
    │ • Validates event schema                                 │
    │ • Queues for MongoDB insertion                           │
    │                                                          │
    │ ❌ No K8s API query                                      │
    │ ❌ No node metadata enrichment                           │
    └────────────────────────┬─────────────────────────────────┘
                             │
                             │ Insert
                             │
                ┌────────────▼────────────────┐
                │ Event (unchanged):          │
                │                             │
                │ {                           │
                │   "entitiesImpacted": [     │
                │     { "entityType": "PCI",  │
                │       "entityValue":        │
                │         "0000:17:00.0" }    │
                │   ],                        │
                │   "metadata": {},           │
                │   "nodeName": "gpu-node-42" │
                │ }                           │
                └────────────┬────────────────┘
                             │
────────────────────────────────────────────────────────────────

┌────────────────────────────────────────────────────────────────┐
│ MongoDB                                                        │
└────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────┐
    │ health_events collection                                 │
    │                                                          │
    │ {                                                        │
    │   "_id": ObjectId("..."),                                │
    │   "createdAt": ISODate("..."),                           │
    │   "healthevent": {                                       │
    │     "agent": "syslog-health-monitor",                    │
    │     "checkName": "SysLogsXIDError",                      │
    │     "isFatal": true,                                     │
    │     "entitiesImpacted": [                                │
    │       { "entityType": "PCI",                             │
    │         "entityValue": "0000:17:00.0" }                  │
    │     ],                                                   │
    │     "metadata": {},  ← Empty!                            │
    │     "nodeName": "gpu-node-42"                            │
    │   }                                                      │
    │ }                                                        │
    │                                                          │
    │ ❌ Cannot correlate events across monitors               │
    │ ❌ Cannot distinguish clusters                           │
    │ ❌ Cannot detect rack patterns                           │
    │ ❌ Cannot track hardware lifecycle                       │
    └──────────────────────────────────────────────────────────┘
```

---

## Part 2: The Problems

### Problem 1: Identifier Fragmentation

Same GPU fails, detected by two monitors:

```javascript
// Syslog: Uses PCI address
{ "entitiesImpacted": [{ "entityType": "PCI", "entityValue": "0000:17:00.0" }] }

// DCGM: Uses GPU index
{ "entitiesImpacted": [{ "entityType": "GPU", "entityValue": "0" }] }

// Question: Same GPU? Answer: Can't tell! PCI ≠ GPU index
```

**Impact**: Cannot correlate → Must drain entire node (8 GPUs) instead of 1 GPU

### Problem 2: Node Name Collisions

```javascript
// Cluster A
{ "nodeName": "10.0.1.5" }

// Cluster B
{ "nodeName": "10.0.1.5" }  // ← Collision!

// Result: Events from BOTH clusters mixed ❌
```

### Problem 3: Missing Infrastructure Context

```javascript
{ "nodeName": "gpu-node-42" }

// Questions: Which cluster? 🤷 Which rack? 🤷 Which datacenter? 🤷
// Answer: Manual lookup
```

### Problem 4: No Hardware Lifecycle Tracking

```javascript
{ "entitiesImpacted": [{ "entityType": "PCI", "entityValue": "0000:17:00.0" }] }

// For RMA: What's the chassis serial? 🤷
// Cannot track through RMA process
```

---

## Part 3: The Solution

### New Architecture

```
┌────────────────────────────────────────────────────────────────┐
│ GPU Node                                                       │
└────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────┐
    │ GPU Metadata Collector                                   │
    │ (DaemonSet init container)                               │
    │                                                          │
    │ • Uses go-nvml to query GPU info                         │
    │ • Collects: UUID, PCI address, chassis serial            │
    └────────────────────────┬─────────────────────────────────┘
                             │
                             │ Writes to disk
                             ▼
    ┌──────────────────────────────────────────────────────────┐
    │ /var/lib/nvsentinel/gpu_metadata.json                    │
    │                                                          │
    │ {                                                        │
    │   "chassis_serial": "DGX-A100-SN123456",                 │
    │   "gpus": [                                              │
    │     {                                                    │
    │       "gpu_id": 0,                                       │
    │       "uuid": "GPU-abc123-def456-789012-345678",         │
    │       "pci_address": "0000:17:00.0"                      │
    │     }                                                    │
    │   ]                                                      │
    │ }                                                        │
    └────────────────────────┬─────────────────────────────────┘
                             │
                             │ Read by
                             ▼
    ┌──────────────────────────────────────────────────────────┐
    │ Syslog Health Monitor                                    │
    │ (DaemonSet container)                                    │
    │                                                          │
    │ 1. Parses kernel XID from syslog                         │
    │ 2. Reads metadata file                                   │
    │ 3. Maps PCI address → GPU UUID                           │
    │ 4. Enriches event with GPU UUID + chassis serial         │
    └────────────────────────┬─────────────────────────────────┘
                             │
                             │ gRPC
                             │
                ┌────────────▼─────────────────┐
                │ Event (entity-enriched):     │
                │                              │
                │ {                            │
                │   "entitiesImpacted": [      │
                │     { "entityType": "PCI",   │
                │       "entityValue":         │
                │         "0000:17:00.0" },    │
                │     { "entityType":          │
                │         "GPU_UUID",          │
                │       "entityValue":         │
                │         "GPU-abc123..." } ✅ │
                │   ],                         │
                │   "metadata": {              │
                │     "chassis_serial":        │
                │       "DGX-A100-SN..." ✅    │
                │   },                         │
                │   "nodeName": "gpu-node-42"  │
                │ }                            │
                └────────────┬─────────────────┘
                             │
────────────────────────────────────────────────────────────────

┌────────────────────────────────────────────────────────────────┐
│ Platform Connector                                             │
└────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────┐
    │ Node Metadata Processor                                  │
    │                                                          │
    │ 1. Receives entity-enriched event from monitor           │
    └────────────────────────┬─────────────────────────────────┘
                             │
                             │ Query for node metadata
                             ▼
            ┌────────────────────────────────────────┐
            │ Kubernetes API Server                  │
            │                                        │
            │ GET /api/v1/nodes/{nodeName}           │
            │                                        │
            │ Returns:                               │
            │   • node.Spec.ProviderID               │
            │   • node.Labels (cluster, rack, zone)  │
            └────────────────┬───────────────────────┘
                             │
                             │ Node metadata response
                             ▼
    ┌──────────────────────────────────────────────────────────┐
    │ Node Metadata Processor                                  │
    │                                                          │
    │ 2. Extracts providerID from node.Spec                    │
    │ 3. Extracts allowed labels (cluster, rack, zone)         │
    │ 4. Enriches event.metadata with node context             │
    └────────────────────────┬─────────────────────────────────┘
                             │
                             │ Insert
                             │
                ┌────────────▼─────────────────┐
                │ Event (fully enriched):      │
                │                              │
                │ {                            │
                │   "entitiesImpacted": [      │
                │     { "entityType": "PCI",   │
                │       "..." },               │
                │     { "entityType":          │
                │       "GPU_UUID", "..." }    │
                │   ],                         │
                │   "metadata": {              │
                │     "chassis_serial": "...", │
                │     "providerID":            │
                │       "aws:///.../i-..." ✅  │
                │     "cluster-name":          │
                │       "prod-ml-01", ✅       │
                │     "rack-id":               │
                │       "rack-12", ✅          │
                │     "topology...zone":       │
                │       "us-west-2a" ✅        │
                │   }                          │
                │ }                            │
                └────────────┬─────────────────┘
                             │
────────────────────────────────────────────────────────────────

┌────────────────────────────────────────────────────────────────┐
│ MongoDB                                                        │
└────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────────────┐
    │ health_events collection                                 │
    │                                                          │
    │ {                                                        │
    │   "_id": ObjectId("..."),                                │
    │   "healthevent": {                                       │
    │     "agent": "syslog-health-monitor",                    │
    │     "checkName": "SysLogsXIDError",                      │
    │     "isFatal": true,                                     │
    │     "entitiesImpacted": [                                │
    │       { "entityType": "PCI",                             │
    │         "entityValue": "0000:17:00.0" },                 │
    │       { "entityType": "GPU_UUID",                        │
    │         "entityValue": "GPU-abc123..." } ✅              │
    │     ],                                                   │
    │     "metadata": {                                        │
    │       "chassis_serial": "DGX-A100-SN123456", ✅          │
    │       "providerID": "aws:///.../i-0abc123...", ✅        │
    │       "cluster-name": "prod-ml-01", ✅                   │
    │       "rack-id": "rack-12", ✅                           │
    │       "topology.kubernetes.io/zone": "us-west-2a" ✅     │
    │     },                                                   │
    │     "nodeName": "gpu-node-42"                            │
    │   }                                                      │
    │ }                                                        │
    │                                                          │
    │ ✅ Can correlate events across monitors (GPU UUID)       │
    │ ✅ Can distinguish clusters (providerID)                 │
    │ ✅ Can detect rack patterns (rack-id)                    │
    │ ✅ Can track hardware lifecycle (chassis_serial)         │
    └──────────────────────────────────────────────────────────┘
```

---

## Part 4: Entity-Level Enrichment (GPU Hardware)

### Metadata Collector Output

**File**: `/var/lib/nvsentinel/gpu_metadata.json`

```json
{
  "chassis_serial": "DGX-A100-SN123456",
  "gpus": [
    {
      "gpu_id": 0,
      "uuid": "GPU-abc123-def456-789012-345678",
      "pci_address": "0000:17:00.0"
    }
  ]
}
```

### How It Works

Syslog monitor reads metadata file and enriches events:

```go
// Maps PCI → GPU UUID
uuid := metadataReader.GetGPUByPCI("0000:17:00.0").UUID

// Adds GPU_UUID to entities
entities = append(entities, &pb.Entity{
    EntityType: "GPU_UUID", 
    EntityValue: uuid,
})

// Adds chassis serial to metadata
metadata["chassis_serial"] = metadataReader.GetChassisSerial()
```

**Result**: Events now have GPU UUID for correlation and chassis serial for RMA tracking.

---

## Part 5: Node-Level Enrichment (Infrastructure Context)

### Configuration

Platform connector queries Kubernetes API and enriches with allowed labels:

```yaml
nodeMetadata:
  enabled: true
  allowedLabels:
    - topology.kubernetes.io/region
    - topology.kubernetes.io/zone
    - cluster-name
    - rack-id
    - datacenter
```

**Result**: Events enriched with providerID (globally unique) and organizational labels (cluster, rack, zone).

---

## Part 6: Use Cases Enabled

### 1. Cross-Monitor Correlation

```javascript
// Find all events for specific GPU
db.events.find({
  "healthevent.entitiesImpacted.entityValue": "GPU-abc123..."
})

// Result: Correlated events from different monitors
[
  { "agent": "syslog-health-monitor", "message": "XID 79" },
  { "agent": "gpu-health-monitor", "message": "Memory error" }
]
// Insight: Memory failure caused XID → Same GPU confirmed
```

### 2. Cluster-Wide Health

```javascript
// Failures by cluster
db.events.aggregate([
  { $match: { "healthevent.isHealthy": false } },
  { $group: {
      _id: "$healthevent.metadata.cluster-name",
      count: { $sum: 1 }
  }}
])

// Output:
// prod-ml-01: 156 failures
// prod-training-02: 89 failures
```

### 3. Rack-Level Pattern Detection

```javascript
// Events by rack
db.events.aggregate([
  { $match: { "healthevent.isHealthy": false } },
  { $group: {
      _id: "$healthevent.metadata.rack-id",
      count: { $sum: 1 },
      nodes: { $addToSet: "$healthevent.nodeName" }
  }}
])

// Output:
// rack-12: 47 events across 6 nodes (likely cooling issue)
```

---

## Part 7: Complete Flow

```
T+0s:   Kernel XID
T+1s:   Syslog Monitor
        ├─ Maps PCI → GPU UUID (entity enrichment)
        └─ Adds chassis serial
T+2s:   Platform Connector
        ├─ Queries K8s API (node enrichment)
        └─ Adds providerID, cluster, rack, zone
T+3s:   MongoDB (complete enriched event)
T+10s:  Node Drainer
        └─ Drains only affected GPU

Result:
✅ 1 GPU quarantined, 7 remain (87.5% capacity preserved)
✅ Full context: GPU-abc123, cluster prod-ml-01, rack-12
✅ RMA tracking via chassis DGX-A100-SN123456
```

---
