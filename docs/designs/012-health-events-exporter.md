# ADR-012: Observability — Health Events Exporter

## Context

NVSentinel currently stores all health events within its in-cluster MongoDB. While this works well for per-cluster operations (fault quarantine, node draining, remediation), it creates challenges for fleet-wide event visibility:

- **Data Isolation:** Each cluster's MongoDB is isolated; health event data cannot be queried across clusters
- **Wrong Tool for Events:** While Prometheus + Grafana provide centralized metrics visibility, Prometheus is designed for time series data, not events
- **Cardinality Explosion:** Attempting to export high-cardinality event data (550 XID codes × 6,000 nodes × 8 GPUs × metadata fields) as Prometheus metrics creates 198+ billion time series, which is unsustainable
- **Limited Event Queries:** Prometheus/PromQL cannot efficiently answer event-oriented queries like "Show me all XID-48 errors with GPU serial X123 across the fleet" or "Timeline of events for node gpu-node-42"
- **Missing Event Store:** No centralized platform for detailed event search, analysis, and long-term retention

---

## Problem Statement

### Current State

```
┌──────────────────────────────────────────────────────────────────┐
│ Cluster 1 (us-west-1)                                            │
│                                                                  │
│ MongoDB: 10,000 health events                                    │
│ ├─ Detailed event data (XID codes, GPU serials, metadata)        │
│ └─ ❌ Isolated, not queryable across clusters                    │
│                                                                  │
│ Prometheus: Aggregate metrics                                    │
│ ├─ ✅ nvsentinel_total_xid_errors{cluster="us-west-1"}           │
│ ├─ ✅ nvsentinel_nodes_cordoned_total{cluster="us-west-1"}       │
│ └─ ❌ CANNOT export per-GPU events (cardinality explosion)       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘

... (150 clusters total)

┌──────────────────────────────────────────────────────────────────┐
│ Centralized Prometheus + Grafana                                 │
│                                                                  │
│ ✅ Works well for: Aggregate metrics across fleet                │
│ ❌ Cannot handle: Individual event queries                       │
│                                                                  │
│ Example queries that DON'T work:                                 │
│ • "Show me all events for GPU serial 1234567890"                 │
│ • "Timeline of events leading to node-42 failure"                │
│ • "All XID-48 errors with temperature > 80°C"                    │
│ • "Events with specific metadata field values"                   │
│                                                                  │
│ Cardinality problem:                                             │
│ 550 XIDs × 6,000 nodes × 8 GPUs × metadata = 198B time series    │
└──────────────────────────────────────────────────────────────────┘

Operations Team Needs:
❌ Cannot query detailed event data across fleet
❌ Cannot search by GPU serial number, error metadata, etc.
❌ Cannot perform event-based analysis (event sequences, patterns)
✅ Can view aggregate metrics (but not individual events)
```

### Target State: Complementary Observability

```
┌──────────────────────────────────────────────────────────────────┐
│ NVSentinel Clusters (150 clusters)                               │
│                                                                  │
│ MongoDB → Event Exporter ──┬──→ Prometheus (metrics)             │
│                             │    ✅ Aggregate counters/gauges    │
│                             │    ✅ Alerting on thresholds       │
│                             │    ✅ Real-time dashboards         │
│                             │                                    │
│                             └──→ Event Store (events)            │
│                                  ✅ Detailed event data          │
│                                  ✅ Search by any field          │
│                                  ✅ Event sequences/patterns     │
│                                  ✅ Long-term retention          │
└──────────────────────────────────────────────────────────────────┘

Use Case Split:
┌────────────────────────────────────────────────────────────────┐
│ Prometheus + Grafana (Time Series Metrics)                     │
├────────────────────────────────────────────────────────────────┤
│ • How many XID errors per cluster? (counters)                  │
│ • What's the error rate trend? (rate over time)                │
│ • Which clusters have highest error rates? (top-k)             │
│ • Alert when errors > threshold (alerting)                     │
└────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────┐
│ Event Store + Kibana/etc (Detailed Events)                     │
├────────────────────────────────────────────────────────────────┤
│ • Show all events for GPU serial X123 (search)                 │
│ • Timeline of events before node-42 failed (event sequence)    │
│ • All XID-48 with temp > 80°C (complex filtering)              │
│ • Pattern: XID-48 → XID-31 → node reboot (analytics)           │
└────────────────────────────────────────────────────────────────┘

Both are needed for complete observability!
```

### Requirements (from [Issue #128](https://github.com/NVIDIA/NVSentinel/issues/128))

1. **Continuous Export:** Stream data from MongoDB as events occur
2. **CloudEvents Format:** Use standardized event schema
3. **HTTP Sink Support:** Push to HTTP-based endpoints
4. **OIDC Authentication:** Secure communication with OAuth2/OIDC
5. **Fleet-Wide Analytics:** Enable event-based querying and analysis across multiple clusters

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

**Note:** This exporter **complements** the existing Prometheus metrics export. Prometheus remains the primary solution for aggregate metrics, real-time dashboards, and alerting. The event exporter addresses the need for detailed event-level querying that would cause cardinality explosion in Prometheus (e.g., searching by GPU serial number, analyzing event sequences, complex filtering on metadata fields).

### Health Events

This design focuses on exporting **Health Events** - hardware and cluster health status changes detected by NVSentinel's health monitors.

**Health Event Characteristics:**

- **CloudEvents type:** `"health-event"`
- **Delivery Guarantee:** At-least-once
- **Source:** NVSentinel Health Monitors
- **Stored in:** MongoDB `healthevents` collection
- **Volume:** High (thousands of events per day per cluster)
- **Examples:** GPU XID-48 errors, ECC errors, scheduled maintenance events, pod evictions
- **Use Case:** Centralized analytics, dashboarding, ML-based pattern detection

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
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐           │
│  │   MongoDB    │      │  PostgreSQL  │      │    Kafka     │           |
│  │ ChangeStream │      │    NOTIFY    │      │   Consumer   │           │
│  │ (resumable)  │      │ (resumable)  │      │  (offsets)   │           │
│  └──────────────┘      └──────────────┘      └──────────────┘           │
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
│  │    PublishBatch(events) error                                  │     │
│  └────────────────────────────────────────────────────────────────┘     │
│           │                      │                      │               │
│           ↓                      ↓                      ↓               │
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐           │
│  │  HTTP Sink   │      │  Kafka Sink  │      │  gRPC Sink   │           │
│  │  + OIDC      │      │  + ACLs      │      │  + mTLS      │           │
│  └──────────────┘      └──────────────┘      └──────────────┘           │
│           │                      │                      │               │
│           └──────────────────────┴──────────────────────┘               │
│                                  │                                      │
└──────────────────────────────────┼──────────────────────────────────────┘
                                   │
                                   ↓
         ┌─────────────────────────────────────────────────┐
         │       External Destinations (Examples)          │
         │                                                 │
         │  • Elasticsearch (HTTP API)                     │
         │  • Kafka Topics (via Kafka Sink or REST Proxy)  │
         │  • Cloud Storage (S3/GCS via HTTP)              │
         │  • SIEM platforms (Datadog, Splunk)             │
         │  • Custom services (your API)                   │
         └─────────────────────────────────────────────────┘

Key Design Principles:
  ✓ Database agnostic: Swap MongoDB → PostgreSQL with import change
  ✓ Sink agnostic: Add new sinks by implementing EventSink interface
  ✓ Automatic bootstrap: Backfill historical data on first start, then stream
  ✓ Clean separation: Source → Transform → Sink
  ✓ Resumable: All sources support position tracking
```

### Component Responsibilities

#### 1. ChangeStream Watcher
**Purpose:** Monitor MongoDB for new health events

- Reuses existing `store-client/pkg/datastore/providers/mongodb/watcher` infrastructure
- Watches `healthevents` collection using MongoDB change streams
- Manages resume tokens for reliable delivery

#### 2. CloudEvents Transformer
**Purpose:** Convert MongoDB documents to CloudEvents format

- Transforms health event documents to CloudEvents 1.0 specification
- Adds cluster context (cluster ID, region, environment)
- Supports both structured (JSON) and binary content modes
- Extensible for custom attributes

#### 3. HTTP Publisher
**Purpose:** Publish events to HTTP sink

- HTTP/HTTPS client with connection pooling
- Retry logic with exponential backoff
- Batch support (configurable)
- Circuit breaker for sink failures

#### 4. OIDC Token Provider
**Purpose:** Manage authentication tokens

- OAuth2 client credentials flow
- Automatic token refresh before expiry
- Thread-safe token caching
- Support for multiple OIDC providers

#### 5. Resume Token Manager
**Purpose:** Ensure reliable delivery

- Stores last processed position in MongoDB
- Enables restart without data loss
- At-least-once delivery semantics
- Same pattern as fault-quarantine module

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
    
    // PublishBatch sends multiple events atomically (optional)
    PublishBatch(ctx context.Context, events []*CloudEvent) error
    
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
    "errorCode": "XID_48",
    "entity": {
      "type": "GPU",
      "value": "3",
      "serialNumber": "1234567890"
    },
    "metadata": {
      "env": "prod",
      "csp": "aws",
      "region": "us-west-1"
    },
    "timestamp": "2024-11-18T10:30:00.123456Z",
    "nodeName": "gpu-node-42",
    "forceDrain": false,
    "forceQuarantine": true
  }
}
```

**CloudEvents Top-Level Attributes:**

| Attribute | Type | Required | Description |
|-----------|------|----------|-------------|
| `specversion` | String | Yes | CloudEvents version (always "1.0") |
| `id` | String (UUID) | Yes | Unique event ID; generated or from MongoDB `_id` |
| `time` | Timestamp (RFC3339) | Yes | Event generation time |
| `source` | String | Yes | Cluster identifier |
| `type` | String | Yes | Event type (always "health-event") |

**Data Payload Fields:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | String | Yes | Schema version |
| `agent` | String | Yes | NVSentinel health monitor that generated the event (`gpu-health-monitor`, `syslog-health-monitor`, `csp-health-monitor`, `kubernetes-object-monitor`) |
| `componentClass` | String | Yes | Component type (GPU, CPU, Memory, CSP, etc.) |
| `checkName` | String | Yes | Name of the health check |
| `isFatal` | Boolean | Yes | Whether error is fatal |
| `isHealthy` | Boolean | Yes | Overall health status |
| `message` | String | No | Human-readable description |
| `recommendedAction` | String | No | Suggested remediation |
| `errorCode` | String | No | Specific error code (e.g., XID codes) |
| `entity.type` | String | Yes | Entity type |
| `entity.value` | String | Yes | Entity identifier (GPU index, etc.) |
| `entity.serialNumber` | String | No | Hardware serial number |
| `metadata.env` | String | Yes | Environment (prod, non-prod) |
| `metadata.csp` | String | Yes | Cloud service provider |
| `metadata.region` | String | Yes | Geographic region |
| `timestamp` | Timestamp (RFC3339) | Yes | Original event timestamp |
| `nodeName` | String | Yes | Kubernetes node name |
| `forceDrain` | Boolean | Yes | Force pod eviction |
| `forceQuarantine` | Boolean | Yes | Force node cordoning |

**Field Mapping from Datastore:**

The exporter transforms NVSentinel's `HealthEventWithStatus` documents to this schema:

```go
// Mapping from datastore event to CloudEvents
CloudEvent := map[string]interface{}{
    "specversion": "1.0",
    "id":          event.ID,  // Event ID
    "time":        time.Now().Format(time.RFC3339Nano),
    "source":      clusterID,
    "type":        "health-event",
    "data": map[string]interface{}{
        "version":          "1",
        "agent":            healthEvent.Agent,
        "componentClass":   healthEvent.ComponentClass,
        "checkName":        healthEvent.CheckName,
        "isFatal":          healthEvent.IsFatal,
        "isHealthy":        healthEvent.IsHealthy,
        "message":          healthEvent.Message,
        "recommendedAction": healthEvent.RecommendedAction,
        "errorCode":        healthEvent.ErrorCode,
        "entity": map[string]interface{}{
            "type":         healthEvent.Entity.Type,
            "value":        healthEvent.Entity.Value,
            "serialNumber": healthEvent.Entity.SerialNumber,
        },
        "metadata": map[string]interface{}{
            "env":    config.Environment,
            "csp":    config.CSP,
            "region": config.Region,
        },
        "timestamp":        healthEvent.Timestamp,
        "nodeName":         healthEvent.NodeName,
        "forceDrain":       healthEvent.ForceDrain,
        "forceQuarantine":  healthEvent.ForceQuarantine,
    },
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
    
    // Publish batch of events
    PublishBatch(ctx context.Context, events []CloudEvent) error
    
    // Close publisher and flush pending events
    Close(ctx context.Context) error
}

type HTTPPublisherConfig struct {
    // Sink configuration
    Endpoint            string        // HTTP endpoint URL
    MaxRetries          int           // Number of retries on failure
    RetryBackoff        time.Duration // Initial backoff duration
    Timeout             time.Duration // Request timeout
    
    // Batching configuration
    BatchSize           int           // Events per batch
    BatchTimeout        time.Duration // Max time to wait for batch
    
    // Authentication
    OIDCTokenProvider   TokenProvider // OIDC token provider
    
    // HTTP client settings
    MaxIdleConns        int
    MaxIdleConnsPerHost int
    IdleConnTimeout     time.Duration
}
```

#### Authentication

HTTP sink uses **OAuth2 Client Credentials Flow** (OIDC) for authentication:

- Client ID and secret configured via environment variables
- Access tokens cached and automatically refreshed before expiry
- Tokens included in HTTP requests: `Authorization: Bearer <token>`

### 5. Event Processing Flow

The exporter follows a simple pipeline:

1. **Receive** events from source (via change stream channel)
2. **Transform** each event to CloudEvents format
3. **Publish** to sink (HTTP endpoint)
4. **Update** resume token after successful publish
5. **Repeat** continuously

**Error Handling:**
- Transform errors: Log and skip event
- Publish errors: Log and skip event (with retry in HTTP client)
- Resume token errors: Fatal - restart exporter to maintain consistency

### 6. Automatic Backfill (Bootstrap Phase)

The exporter automatically exports historical events on first deployment.

#### How It Works

When the exporter starts:

1. **Check for resume token**
   - If exists → Resume streaming from last position
   - If doesn't exist → **Bootstrap phase**: Export all historical events

2. **Bootstrap/Backfill**
   - Query all events from earliest (or MaxAge) to "now"
   - Export in batches with rate limiting
   - Track progress with checkpoint

3. **Transition to streaming**
   - After bootstrap completes, save resume token
   - Start change stream from current position
   - Continue with real-time events

**Note:** This is not a separate "mode" - it's an automatic initialization step that happens once on first deployment.

#### Configuration

```go
type BackfillConfig struct {
    MaxAge         time.Duration // How far back to backfill (e.g., 24h, 7d, 30d)
    MaxEvents      int           // Optional: safety limit
    BatchSize      int           // Events per batch
    RateLimit      int           // Events per second
}
```

#### Startup Flow

1. **Check for resume token** - if exists, resume from last position; if not, enter bootstrap phase
2. **Bootstrap (first deployment only)**
   - Query historical events within `MaxAge` window (or all history if unset)
   - Export in batches with rate limiting
   - Save resume token after completion
3. **Stream real-time events** - start change stream and process continuously

**Behavior:**
- **First deployment**: Bootstrap (export historical) → Stream (real-time)
- **Restart**: Resume streaming from last position (no bootstrap)
- **After downtime**: Change streams automatically handle gaps

---

## Configuration

Configuration is managed via **ConfigMap** (structured TOML) and **Secrets** (sensitive values only). This follows existing NVSentinel patterns and makes config easier to review and maintain.

### ConfigMap: `health-events-exporter-config`

```toml
[exporter]
enabled = true
cluster_id = "us-west-1-prod-cluster"
csp = "aws"
region = "us-west-1"
environment = "prod"

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
batch_size = 500
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

### Secret: `health-events-exporter-secret`

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

### Deployment

The exporter is deployed as a single-replica Deployment in the `nvsentinel` namespace, using the existing NVSentinel service account. Configuration is loaded from ConfigMap; sensitive values come from Secrets.

---

## Rationale

### Why Build a Custom Exporter?

**Existing Open-Source Alternatives:**

1. **Debezium MongoDB Connector**
   - Industry-standard CDC tool
   - Exports MongoDB change streams → Kafka topics
   - **Why not suitable:**
     - Requires Kafka infrastructure (many users don't have it)
     - No direct HTTP sink support (would need Kafka Connect HTTP Sink separately)
     - No CloudEvents format out-of-box
     - Heavier operational overhead (Kafka cluster, Zookeeper/KRaft, Debezium connector management)

2. **MongoDB Kafka Connector (Official)**
   - MongoDB's official Kafka connector
   - Tight integration with MongoDB Atlas
   - **Why not suitable:**
     - Same Kafka dependency as Debezium
     - Proprietary to MongoDB ecosystem
     - No CloudEvents transformation
     - GitHub Issue #128 explicitly requests HTTP sink, not Kafka

3. **MongoDB Realm Triggers / Atlas App Services**
   - Cloud-native MongoDB serverless functions
   - Can trigger HTTP webhooks on data changes
   - **Why not suitable:**
     - Only available in MongoDB Atlas (not self-hosted)
     - Vendor lock-in
     - Limited control over retry logic, batching, authentication
     - Not suitable for open-source, on-prem deployments

4. **Generic Webhook/Event Exporters (e.g., Kubernetes Event Exporter)**
   - Export Kubernetes events, not MongoDB data
   - **Why not suitable:**
     - Don't support MongoDB change streams
     - NVSentinel events are stored in MongoDB, not Kubernetes events

**Why Custom Solution:**

- **HTTP-First Design:** Direct HTTP sink support without Kafka dependency (GitHub Issue #128 requirement)
- **CloudEvents Native:** Built-in CloudEvents transformation, not a bolt-on
- **NVSentinel-Specific:** Tailored to `HealthEventWithStatus` schema, cluster context injection
- **OIDC Support:** OAuth2 Client Credentials Flow for enterprise auth (Debezium connectors typically use SASL/TLS)
- **Automatic Backfill:** Zero-config historical export on first deployment (not standard in CDC tools)
- **Lightweight:** Single binary, no Kafka/Zookeeper overhead
- **Reuses Existing Infrastructure:** Leverages NVSentinel's `store-client` SDK, resume token management from fault-quarantine module

### Design Rationale

1. **CloudEvents Format**
   - **Why:** Industry standard (CNCF spec), widely supported by event stores and SIEM platforms
   - **Alternative:** Custom JSON format → Rejected: reinventing the wheel, poor interoperability

2. **HTTP Sink (Not Kafka)**
   - **Why:** Universal protocol, no infrastructure dependency, aligns with Issue #128
   - **Trade-off:** Lower throughput than Kafka, but health events are ~550 events across 150 clusters (manageable)
   - **Future:** Can add Kafka sink later for high-volume users

3. **At-Least-Once Delivery**
   - **Why:** Simpler than exactly-once, idempotent sinks (Elasticsearch) handle duplicates naturally
   - **Trade-off:** Potential duplicate events on failures
   - **Acceptable because:** Health events are naturally idempotent (same event ID, sink deduplicates)

4. **Database-Agnostic Interfaces**
   - **Why:** Future-proof migration to PostgreSQL/CockroachDB without rewriting exporter
   - **Cost:** Additional abstraction layer
   - **Benefit:** NVSentinel's datastore strategy is not finalized; exporter remains independent

5. **ConfigMap > Environment Variables**
   - **Why:** Structured config easier to review, version control, and validate
   - **Alternative:** 20+ env vars → Rejected: verbose, error-prone, hard to review in PRs
   - **Follows:** Existing NVSentinel pattern (other components use ConfigMaps)

6. **Single HTTP Sink (Not Multi-Sink)**
   - **Why:** Simplifies configuration and error handling
   - **User workaround:** Deploy multiple exporter instances with different configs, or use sink-side routing (e.g., Elasticsearch ingest pipeline)
   - **Future:** Can add multi-sink if requested

7. **No Event Filtering in Exporter**
   - **Why:** Filtering logic belongs in downstream analytics tools (Elasticsearch queries, Kibana filters)
   - **Alternative:** CEL expressions in exporter → Rejected: adds complexity, duplicates functionality available in event stores
   - **User benefit:** All events available in sink for ad-hoc analysis; filtering at query time is more flexible

### Operational Rationale

- **Complement Prometheus (Don't Replace):** Prometheus excels at aggregate metrics and alerting; exporter addresses orthogonal need for event-level queries
- **Zero-Config Backfill:** Automatic historical export on first deployment reduces operational friction
- **Resume Tokens:** Proven pattern from fault-quarantine module ensures reliability
- **Single Replica:** Health events are idempotent; no need for leader election or distributed coordination

---

## Observability

### Metrics

```go
// Events processed
health_events_exporter_events_received_total{cluster_id="..."}
health_events_exporter_events_published_total{cluster_id="...",status="success|failure"}

// Latency
health_events_exporter_publish_duration_seconds{cluster_id="...",quantile="0.5|0.9|0.99"}

// Errors
health_events_exporter_transform_errors_total{cluster_id="..."}
health_events_exporter_publish_errors_total{cluster_id="...",error_type="..."}
health_events_exporter_token_refresh_errors_total{cluster_id="..."}

// Queue/Backlog
health_events_exporter_event_backlog_size{cluster_id="..."}
health_events_exporter_batch_size{cluster_id="..."}

// Resume token
health_events_exporter_resume_token_update_timestamp{cluster_id="..."}

// Backfill
health_events_exporter_backfill_in_progress{cluster_id="..."}
health_events_exporter_backfill_events_processed_total{cluster_id="..."}
health_events_exporter_backfill_duration_seconds{cluster_id="..."}
```

---

## Future Enhancements

### Additional Sink Types

- **Kafka Native:** Direct Kafka producer as alternative to HTTP
- **Cloud Storage:** S3, GCS, Azure Blob for archival
- **gRPC:** For lower latency streaming

### Performance Optimizations

- **HTTP/2 Batching:** Batch multiple events per HTTP request
  - *When needed:* If sink latency becomes bottleneck (e.g., >100ms per request) and event rate exceeds ~100/sec per cluster
  - *Trade-off:* Adds complexity to resume token management and error handling
  - *Current approach:* Not needed for typical health event volumes (~550 events across 150 clusters)

- **Parallel Backfill:** Multi-threaded backfill for faster initial bootstrap
  - *When needed:* If initial backfill takes >10 minutes (e.g., millions of historical events)
  - *Current approach:* Sequential backfill sufficient for typical deployments

### Reliability Improvements

- **Dead Letter Queue:** Failed events sent to separate storage for manual inspection
  - *When needed:* If sink has frequent failures and manual investigation is required
  - *Current approach:* Skip failed events with logging; retry handled by HTTP client

### Advanced Observability

- **Distributed Tracing:** OpenTelemetry spans for end-to-end visibility
  - *When needed:* If debugging complex latency issues across multiple systems
  - *Current approach:* Structured logging and Prometheus metrics sufficient for troubleshooting

---

## References

- [CloudEvents Specification v1.0](https://github.com/cloudevents/spec/blob/v1.0/spec.md)
- [CloudEvents HTTP Protocol Binding](https://github.com/cloudevents/spec/blob/v1.0/http-protocol-binding.md)
- [OAuth 2.0 Client Credentials Grant](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4)
- [GitHub Issue #128](https://github.com/NVIDIA/NVSentinel/issues/128)
- NVSentinel: `store-client/pkg/datastore/interfaces.go` (database-agnostic interfaces)
- NVSentinel: `store-client/pkg/client/interfaces.go` (change stream abstractions)
- NVSentinel: `docs/DATA_FLOW.md`
- NVSentinel: `fault-quarantine/pkg/eventwatcher/event_watcher.go` (reference implementation)

---

## Appendix: Example Use Cases

### Use Case 1: Elasticsearch + Kibana (Complements Prometheus)

```
NVSentinel Clusters ──┬──→ Prometheus (existing)
                      │    └─ Grafana: Aggregate metrics
                      │       • Total errors per cluster
                      │       • Error rate trends
                      │       • Alerting on thresholds
                      │
                      └──→ HTTP Exporter → Elasticsearch
                                              ↓
                                          Kibana Dashboards
                                          • Detailed event search
                                          • GPU serial number lookup
                                          • Event timeline analysis
                                          • Complex filtering

Use both together:
1. Grafana alert: "Cluster X has spike in errors"
2. Kibana investigation: "Show events for affected GPUs"
3. Root cause: Specific GPU serial numbers with pattern
```

### Use Case 2: Cloud Data Lake

```
NVSentinel Clusters → HTTP Exporter → Cloud Storage (S3/GCS)
                                           ↓
                                       Data Pipeline
                                       - Parquet files
                                       - ML training
                                       - Long-term analytics
```

### Use Case 3: Real-Time Alerting

```
NVSentinel Clusters → HTTP Exporter → Custom Service
                                           ↓
                                       - PagerDuty alerts
                                       - Slack notifications
                                       - JIRA ticket creation
```

### Use Case 4: Kafka via REST Proxy

```
NVSentinel Clusters → HTTP Exporter → Kafka REST Proxy
                                           ↓
                                       Kafka Topics
                                           ↓
                                       Multiple Consumers
```

### Use Case 5: Bootstrap Phase on Initial Deployment

```
Scenario 1: First deployment - full history bootstrap (default)
  
  kubectl apply -f health-events-exporter.yaml
  
  Exporter startup:
    1. Check for resume token → NOT FOUND
    2. Detect 100,000 events spanning 6 months
    3. Bootstrap: Export all historical events from earliest
    4. Process in batches at 1000 events/sec (~100 seconds)
    5. Save resume token
    6. Start streaming for new events
  
  Log output:
    INFO: First deployment detected - running bootstrap (backfill historical events)
    INFO: Bootstrap progress: processed=50000, last_timestamp=2024-08-15T...
    INFO: Bootstrap completed: total_processed=100000
    INFO: Bootstrap completed - starting normal streaming operation

Scenario 2: First deployment - limited to last 30 days
  
  # Set environment variable:
  EXPORTER_BACKFILL_MAX_AGE=720h  # 30 days
  
  Exporter startup:
    1. Check for resume token → NOT FOUND
    2. Calculate time window: now - 30 days
    3. Detect 20,000 events in last 30 days
    4. Bootstrap: Export only recent historical events
    5. Save resume token
    6. Start streaming
  
  Log output:
    INFO: First deployment detected - running bootstrap
    INFO: Bootstrap limited by max age: max_age=720h, earliest_event=2024-10-19T...
    INFO: Bootstrap progress: processed=20000
    INFO: Bootstrap completed - starting normal streaming operation

Scenario 3: Restart (normal operation)
  
  Pod restarts after initial deployment
  
  Exporter startup:
    1. Check for resume token → FOUND
    2. Resume streaming from last position
    3. NO BOOTSTRAP (already done on first deployment)
    4. Continue processing new events
  
  Log output:
    INFO: Resume token found - continuing from last position

Result:
  ✓ Zero configuration for full history
  ✓ Simple duration-based limiting (24h, 7d, 30d)
  ✓ No manual date calculations
  ✓ Works consistently across time zones
  ✓ Idempotent restarts
  ✓ Not a "mode" - just automatic initialization
```

