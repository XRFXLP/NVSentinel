# ADR-012: Observability — Health Events Exporter

## Context

NVSentinel stores health events in per-cluster MongoDB. This works well for local operations (fault quarantine, node draining) but creates fleet-wide visibility challenges: isolated data cannot be queried across clusters; Prometheus handles aggregate metrics but not high-cardinality event data (198B+ time series); no centralized event store for detailed search and analysis.

---

## Problem Statement

Health events are trapped in per-cluster MongoDB instances (100s clusters). Operations teams need centralized access for fleet-wide analytics: querying events by GPU serial number, analyzing failure patterns across clusters, investigating timelines before node failures.

**Current:** Each cluster's MongoDB is isolated → no cross-cluster visibility

**Needed:** Export events to centralized event store → enable fleet-wide search and analysis

**Note:** Existing Prometheus/Grafana handle aggregate metrics and alerting. This exporter addresses a different use case: detailed event-level queries that would cause cardinality explosion in Prometheus.

---

## Goals and Non-Goals

### Goals

- Export health events from MongoDB to HTTP sinks in CloudEvents format
- Support OIDC authentication for secure sink communication
- Automatically backfill historical events on first deployment
- Provide operational observability (metrics, logging, health checks)

### Non-Goals

- Building centralized storage or analytics (users provide sink)
- Supporting multiple sinks per exporter instance

---

## Design

### Decision

Implement a new component - health event exporter that continuously exports health events from the in-cluster MongoDB data store to external event stores for centralized analytics and detailed querying. The exporter transforms MongoDB documents into standardized [CloudEvents](https://cloudevents.io/) format and publishes them to HTTP-based sinks with OIDC authentication support.

The exporter is deployed as a single-replica Deployment in the `nvsentinel` namespace, using the existing NVSentinel service account. Configuration is loaded from ConfigMap; sensitive values come from Secrets.

### Health Events

This design focuses on exporting **Health Events** - hardware and cluster health status changes from NVSentinel's health monitors (GPU, Syslog, CSP, K8s Object monitors). Events stored in MongoDB `healthevents` collection; delivery guarantee is at-least-once; use case is centralized analytics and pattern detection.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     Health Events Exporter Architecture                 │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │                    Exporter Core                               │     │
│  │                                                                │     │
│  │   On First Start: Automatic Backfill (historical events)       │     │
│  │   Then: Streaming (real-time events)                           │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │              EventSource Interface                             │     │
│  │              (Database Agnostic)                               │     │
│  │                                                                │     │
│  │    Events() <-chan Event                                       │     │
│  │    MarkProcessed(token) error                                  │     │
│  └────────────────────────────────────────────────────────────────┘     │
│           │                      │                      │               │
│           ↓                      ↓                      ↓               │
│  ┌──────────────┐      ╔══════════════╗      ╔══════════════╗           │
│  │   MongoDB    │      ║  PostgreSQL  ║      ║    Kafka     ║           |
│  │ ChangeStream │      ║    NOTIFY    ║      ║   Consumer   ║           │
│  │ (IMPLEMENTED)│      ║ (extensible) ║      ║ (extensible) ║           │
│  └──────────────┘      ╚══════════════╝      ╚══════════════╝           │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │          EventTransformer (CloudEvents 1.0)                    │     │
│  │                                                                │     │
│  │    Transform(Event) -> CloudEvent                              │     │
│  └────────────────────────────────────────────────────────────────┘     │
│                                                                         │
│  ┌────────────────────────────────────────────────────────────────┐     │
│  │                 EventSink Interface                            │     │
│  │                 (Pluggable Destinations)                       │     │
│  │                                                                │     │
│  │    Publish(event) error                                        │     │
│  └────────────────────────────────────────────────────────────────┘     │
│           │                      │                      │               │
│           ↓                      ↓                      ↓               │
│  ┌──────────────┐      ╔══════════════╗      ╔══════════════╗           │
│  │  HTTP Sink   │      ║  Kafka Sink  ║      ║  gRPC Sink   ║           │
│  │  + OIDC      │      ║  + ACLs      ║      ║  + mTLS      ║           │
│  │(IMPLEMENTED) │      ║ (extensible) ║      ║ (extensible) ║           │
│  └──────────────┘      ╚══════════════╝      ╚══════════════╝           │
│           │                      │                      │               │
│           └──────────────────────┴──────────────────────┘               │
│                                  │                                      │
└──────────────────────────────────┼──────────────────────────────────────┘
                                   │
                                   ↓
         ┌─────────────────────────────────────────────────┐
         │       External Destinations                     │
         │  (Elasticsearch, Kafka, SIEM, Custom APIs, ...) │
         └─────────────────────────────────────────────────┘

Legend:
  Solid boxes (─) = Implemented in this design
  Double boxes (═) = Extensible via interfaces (not implemented)
```

### Component Responsibilities

- **ChangeStream Watcher:** Watches MongoDB `healthevents` collection; reuses existing `store-client` infrastructure and resume token pattern from fault-quarantine
- **CloudEvents Transformer:** Converts health events to CloudEvents 1.0 format; uses cluster name as `source` field
- **HTTP Publisher:** Publishes to HTTP sink with retry logic and OIDC authentication
- **Resume Token Manager:** Tracks last processed position for at-least-once delivery

---

## Implementation Details

### 1. Core Interfaces

The exporter uses database-agnostic interfaces from the `store-client` SDK, enabling future migration to different datastores (PostgreSQL, CockroachDB, etc.) without code changes.

#### Event Source Interface

```go
// ChangeStreamWatcher provides database-agnostic event streaming
// Currently implemented by MongoDB, but can be backed by PostgreSQL LISTEN/NOTIFY,
// Kafka, or any other change data capture mechanism
type ChangeStreamWatcher interface {
    // Start begins watching for events
    Start(ctx context.Context)
    
    // Events returns a channel of database change events
    Events() <-chan Event
    
    // MarkProcessed updates the resume token after successful processing
    MarkProcessed(ctx context.Context, token []byte) error
    
    // Close shuts down the watcher
    Close(ctx context.Context) error
}

// Event represents a generic database change event
type Event interface {
    // GetDocumentID returns the unique document identifier
    GetDocumentID() (string, error)
    
    // GetNodeName extracts the node name from the event
    GetNodeName() (string, error)
    
    // GetResumeToken returns the position for resumable streaming
    GetResumeToken() []byte
    
    // UnmarshalDocument deserializes the event data into a struct
    UnmarshalDocument(v interface{}) error
}
```

#### Sink Interface

```go
// EventSink abstracts the destination for exported events
// Implementations: HTTPSink, KafkaSink, GRPCSink, etc.
type EventSink interface {
    // Publish sends a single event to the sink
    Publish(ctx context.Context, event *CloudEvent) error
    
    // Close flushes pending events and releases resources
    Close(ctx context.Context) error
}
```

#### Transformer Interface

```go
// EventTransformer converts datastore events to CloudEvents
type EventTransformer interface {
    // Transform converts a database event to CloudEvents format
    Transform(event Event) (*CloudEvent, error)
}
```

#### Exporter Architecture

```go
// HealthEventsExporter is the main component
// Completely decoupled from specific database or sink implementations
type HealthEventsExporter struct {
    source      ChangeStreamWatcher  // Database-agnostic event source
    transformer EventTransformer     // Format converter
    sink        EventSink            // Destination (HTTP, Kafka, etc.)
    config      ExporterConfig
}

func NewHealthEventsExporter(
    source ChangeStreamWatcher,
    transformer EventTransformer,
    sink EventSink,
    config ExporterConfig,
) *HealthEventsExporter {
    return &HealthEventsExporter{
        source:      source,
        transformer: transformer,
        sink:        sink,
        config:      config,
    }
}
```

### 2. CloudEvents Schema

Following the [CloudEvents 1.0 specification](https://github.com/cloudevents/spec/blob/v1.0/spec.md), health events are transformed into the following format:

```json
{
  "specversion": "1.0",
  "id": "fe5302cf-1887-48a9-8e6e-117d366a7344",
  "time": "2024-11-18T10:30:00.123456Z",
  "source": "us-west-1-prod-cluster",
  "type": "health-event",
  "data": {
    "version": "1",
    "agent": "syslog-health-monitor",
    "componentClass": "GPU",
    "checkName": "XID_ERROR_48",
    "isFatal": true,
    "isHealthy": false,
    "message": "GPU 3 reported XID 48: Double Bit ECC Error",
    "recommendedAction": "REPLACE_GPU",
    "errorCode": ["XID_48"],
    "entitiesImpacted": [
      {
        "entityType": "PCI",
        "entityValue": "0000:17:00.0"
      },
      {
        "entityType": "GPU_UUID",
        "entityValue": "GPU-a1b2c3d4-e5f6-7890-abcd-ef1234567890"
      }
    ],
    "metadata": {
      "chassis_serial": "SN123456789",
      "providerID": "aws:///us-west-2a/i-1234567890abcdef0",
      "topology.kubernetes.io/zone": "us-west-2a",
      "topology.kubernetes.io/region": "us-west-2"
    },
    "generatedTimestamp": "2024-11-18T10:30:00.123456Z",
    "nodeName": "gpu-node-42",
    "quarantineOverrides": {
      "force": true,
      "skip": false
    },
    "drainOverrides": {
      "force": false,
      "skip": false
    }
  }
}
```

**CloudEvents Top-Level Attributes:**

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
| `specversion` | String | Yes | CloudEvents version (always "1.0") |
| `id` | String (UUID) | Yes | Unique event ID; from MongoDB `_id` |
| `time` | Timestamp (RFC3339) | Yes | Event generation time |
| `source` | String | Yes | Cluster identifier |
| `type` | String | Yes | Event type (always "health-event") |

**Data Payload Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | Number | Yes | Schema version |
| `agent` | String | Yes | NVSentinel health monitor that generated the event (`gpu-health-monitor`, `syslog-health-monitor`, `csp-health-monitor`, `kubernetes-object-monitor`) |
| `componentClass` | String | Yes | Component type (GPU, CPU, Memory, CSP, etc.) |
| `checkName` | String | Yes | Name of the health check (e.g., `XID_ERROR_48`, `SXID_ERROR_12`) |
| `isFatal` | Boolean | Yes | Whether error is fatal |
| `isHealthy` | Boolean | Yes | Overall health status |
| `message` | String | No | Human-readable description |
| `recommendedAction` | String | Yes | Suggested remediation (`NONE`, `COMPONENT_RESET`, `CONTACT_SUPPORT`, `RESTART_VM`, `RESTART_BM`, `REPLACE_VM`) |
| `errorCode` | Array[String] | No | Specific error codes (e.g., `["XID_48"]`) |
| `entitiesImpacted` | Array[Entity] | Yes | Affected entities (e.g., GPU, PCI, NVSWITCH) |
| `entitiesImpacted[].entityType` | String | Yes | Entity type (`GPU`, `PCI`, `GPU_UUID`, `NVSWITCH`, `NVLINK`, etc.) |
| `entitiesImpacted[].entityValue` | String | Yes | Entity identifier (GPU index, PCI address, UUID, etc.) |
| `metadata` | Map[String, String] | No | Key-value metadata (enriched by platform connector); common keys: `chassis_serial`, `providerID`, `topology.kubernetes.io/zone`, `topology.kubernetes.io/region` |
| `generatedTimestamp` | Timestamp (RFC3339) | Yes | Event generation timestamp |
| `nodeName` | String | Yes | Kubernetes node name |
| `quarantineOverrides.force` | Boolean | No | Force node cordoning regardless of rules |
| `quarantineOverrides.skip` | Boolean | No | Skip node cordoning |
| `drainOverrides.force` | Boolean | No | Force pod eviction regardless of rules |
| `drainOverrides.skip` | Boolean | No | Skip pod eviction |

**Field Mapping from Datastore:**

The exporter transforms NVSentinel's `HealthEvent` protobuf (already enriched by platform connector) to CloudEvents format:

```go
// Mapping from protobuf HealthEvent to CloudEvents
CloudEvent := map[string]interface{}{
    "specversion": "1.0",
    "id":          uuid.New().String(),  // Generate new UUID for CloudEvents ID
    "time":        time.Now().Format(time.RFC3339Nano),
    "source":      config.ClusterName,  // Kubernetes cluster name (from kubeconfig context or env var)
    "type":        "health-event",
    "data": map[string]interface{}{
        "version":           healthEvent.Version,
        "agent":             healthEvent.Agent,
        "componentClass":    healthEvent.ComponentClass,
        "checkName":         healthEvent.CheckName,
        "isFatal":           healthEvent.IsFatal,
        "isHealthy":         healthEvent.IsHealthy,
        "message":           healthEvent.Message,
        "recommendedAction": healthEvent.RecommendedAction.String(),
        "errorCode":         healthEvent.ErrorCode,  // Already an array
        "entitiesImpacted":  transformEntities(healthEvent.EntitiesImpacted),
        "metadata":          healthEvent.Metadata,  // Already enriched by platform connector (providerID, topology labels, chassis_serial)
        "generatedTimestamp": healthEvent.GeneratedTimestamp.AsTime().Format(time.RFC3339Nano),
        "nodeName":          healthEvent.NodeName,
        "quarantineOverrides": map[string]interface{}{
            "force": healthEvent.QuarantineOverrides.Force,
            "skip":  healthEvent.QuarantineOverrides.Skip,
        },
        "drainOverrides": map[string]interface{}{
            "force": healthEvent.DrainOverrides.Force,
            "skip":  healthEvent.DrainOverrides.Skip,
        },
    },
}

// transformEntities converts protobuf entities to JSON-friendly format
func transformEntities(entities []*pb.Entity) []map[string]string {
    result := make([]map[string]string, len(entities))
    for i, entity := range entities {
        result[i] = map[string]string{
            "entityType":  entity.EntityType,
            "entityValue": entity.EntityValue,
        }
    }
    return result
}
```


### 3. Event Stream Pipeline

The exporter watches all insert operations on the health events collection:

```go
// BuildHealthEventsExportPipeline creates a pipeline for health events
// Works with any datastore provider (MongoDB, PostgreSQL, etc.)
func BuildHealthEventsExportPipeline() interface{} {
    return EventFilter{
        OperationType: "insert",  // Watch all new health event inserts
    }
}

// Provider-specific implementation:
// MongoDB: db.healthevents.watch([{$match: {operationType: "insert"}}])
// PostgreSQL: LISTEN health_events WHERE operation = 'INSERT'
// Kafka: Consume from health-events topic
```

### 4. Sink Implementations

#### HTTP Sink

```go
type HTTPPublisher interface {
    // Publish single event
    Publish(ctx context.Context, event CloudEvent) error
    
    // Close publisher and flush pending events
    Close(ctx context.Context) error
}

type HTTPPublisherConfig struct {
    // Sink configuration
    Endpoint            string        // HTTP endpoint URL
    MaxRetries          int           // Number of retries on failure
    RetryBackoff        time.Duration // Initial backoff duration
    Timeout             time.Duration // Request timeout
    
    // Authentication
    OIDCTokenProvider   TokenProvider // OIDC token provider
    
    // HTTP client settings
    MaxIdleConns        int
    MaxIdleConnsPerHost int
    IdleConnTimeout     time.Duration
}
```

#### Authentication

HTTP sink uses **OAuth2 Client Credentials Flow** (OIDC): client credentials configured via secrets, tokens auto-refreshed and cached, included in requests as `Authorization: Bearer <token>`.

### 5. Event Processing Flow

Pipeline: Receive → Transform → Publish → Update resume token → Repeat

Errors: Transform/publish failures logged and skipped (with HTTP retry); resume token failures trigger restart to maintain consistency.

### 6. Automatic Backfill (Bootstrap Phase)

On first deployment (when no resume token exists), the exporter automatically exports historical events before streaming real-time events. Subsequent startups resume from the last checkpoint.

```go
type BackfillConfig struct {
    MaxAge         time.Duration // How far back to backfill (e.g., 24h, 7d, 30d)
    MaxEvents      int           // Optional: safety limit
    RateLimit      int           // Events per second
}
```

---

## Configuration

Configuration is managed via **ConfigMap** (structured TOML) and **Secrets** (sensitive values only). This follows existing NVSentinel patterns and makes config easier to review and maintain.

### ConfigMap: `health-events-exporter-config`

```toml
[exporter]
enabled = true
cluster_name = "nvsentinel-prod"  # Optional: auto-detected from kubeconfig context if not set

[exporter.sink]
endpoint = "https://events.example.com/api/v1/events"
timeout = "30s"
max_retries = 3
retry_backoff = "1s"

[exporter.oidc]
token_url = "https://auth.example.com/oauth2/token"
client_id = "nvsentinel-exporter"
scopes = ["events:write"]
# client_secret comes from Kubernetes Secret (not in ConfigMap)

[exporter.backfill]
# Backfill is automatic on first deployment (when no resume token exists)
# Optional: limit how far back to backfill
# max_age = "720h"      # Uncomment to limit backfill window (24h, 168h=7d, 720h=30d)
# max_events = 1000000  # Optional: safety limit
rate_limit = 1000       # events per second, don't overwhelm sink

[datastore]
# Reuse existing NVSentinel datastore config
provider = "mongodb"
uri = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/"
database = "nvsentinel"
collection = "healthevents"
token_collection = "resumetokens"

[exporter.resume_token]
client_name = "health-events-exporter"
```

### Secret: health-events-exporter-secret

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: health-events-exporter-secret
  namespace: nvsentinel
type: Opaque
stringData:
  oidc-client-secret: "<your-secret-here>"
```

---

## Rationale

### Why Build a Custom Exporter?

**Existing tools evaluated:**
- **Debezium/MongoDB Kafka Connector:** Requires Kafka infrastructure; no direct HTTP sink or CloudEvents support
- **MongoDB Realm Triggers:** Atlas-only (not self-hosted); vendor lock-in; limited control over retry/auth
- **Generic event exporters:** Don't support MongoDB change streams

**Why custom solution:** HTTP-first (Issue #128 requirement), CloudEvents native, OIDC auth, automatic backfill, lightweight (single binary), reuses NVSentinel infrastructure (store-client SDK, resume tokens, metadata enrichment).

### Key Design Decisions

- **CloudEvents format:** Industry standard (CNCF), widely supported
- **HTTP sink:** Universal, no infrastructure dependency (Issue #128)
- **At-least-once delivery:** Simpler than exactly-once; idempotent sinks handle duplicates
- **Database-agnostic:** Interfaces enable future PostgreSQL/CockroachDB migration
- **ConfigMap config:** Structured, reviewable (follows NVSentinel pattern)
- **Reuse metadata enrichment:** Platform connector already adds providerID, topology labels, chassis_serial
- **Single sink:** Simplifies config; multi-sink achievable via multiple instances
- **No event filtering:** Defer to downstream tools (Elasticsearch, Kibana)
- **Complement Prometheus:** Exporter handles detailed events; Prometheus handles aggregate metrics
- **Single replica:** Events are idempotent; no coordination needed

---

## Observability

### Metrics

```go
// Events processed
health_events_exporter_events_received_total{cluster="..."}
health_events_exporter_events_published_total{cluster="...",status="success|failure"}

// Latency
health_events_exporter_publish_duration_seconds{cluster="...",quantile="0.5|0.9|0.99"}

// Errors
health_events_exporter_transform_errors_total{cluster="..."}
health_events_exporter_publish_errors_total{cluster="...",error_type="..."}
health_events_exporter_token_refresh_errors_total{cluster="..."}

// Queue/Backlog
health_events_exporter_event_backlog_size{cluster="..."}
health_events_exporter_batch_size{cluster="..."}

// Resume token
health_events_exporter_resume_token_update_timestamp{cluster="..."}

// Backfill
health_events_exporter_backfill_in_progress{cluster="..."}
health_events_exporter_backfill_events_processed_total{cluster="..."}
health_events_exporter_backfill_duration_seconds{cluster="..."}
```

## References

- [CloudEvents Specification v1.0](https://github.com/cloudevents/spec/blob/v1.0/spec.md)
- [CloudEvents HTTP Protocol Binding](https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md)
- [OAuth 2.0 Client Credentials Grant](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4)
- [GitHub Issue #128](https://github.com/NVIDIA/NVSentinel/issues/128)
- NVSentinel: `store-client/pkg/datastore/interfaces.go` (database-agnostic interfaces)
- NVSentinel: `store-client/pkg/client/interfaces.go` (change stream abstractions)
- NVSentinel: `docs/DATA_FLOW.md`
- NVSentinel: `fault-quarantine/pkg/eventwatcher/event_watcher.go` (reference implementation)

