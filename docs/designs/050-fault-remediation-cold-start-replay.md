# ADR-050: Fault Remediation — ID-Only Cold-Start Queue

## Context

Fault Remediation (FR) consumes health events after quarantine and drain have completed, determines the required remediation, creates or reuses the corresponding maintenance resource, and records the outcome. Live events arrive through the datastore change stream. After a restart, cold-start replay recovers eligible events that are still pending in the datastore.

Cold-start replay has four distinct stages:

1. **Discovery**: query the datastore for events that still require remediation or cancellation cleanup.
2. **Scheduling**: place each discovered event on the controller workqueue.
3. **Hydration**: load and decode the event selected by a worker.
4. **Reconciliation**: apply the existing remediation, retry, status-update, and resume-token behavior.

The existing path performs discovery by loading full documents in bounded batches, decodes those documents into generic maps, and retains the maps in the workqueue. The bounded scan limits transient discovery memory, but a large queue can retain full documents for every pending event.

This decision targets the dominant retention point:

- **Discovery** remains the existing bounded full-document scan.
- **Scheduling** retains only the document ID.
- **Hydration** fetches the current full document when a worker dequeues that ID.

Typed datastore decoding and reconciliation semantics are outside the scope of this decision.

## Decision

FR will continue scanning full documents in bounded batches, extract each document ID before enqueueing, and retain only that ID in the controller workqueue. A worker will fetch the current document through the existing map-based query API before invoking the existing reconciliation path.

## Implementation

- Reuse `HealthEventStore.FindHealthEventsByQueryBatched` for cold-start discovery.
- Extract the datastore document ID from each returned `RawEvent`.
- Cold-start controller queue entries contain only the datastore document ID; live entries continue carrying the change-stream event and resume token.
- When a cold-start ID is dequeued, fetch its current document with `FindHealthEventsByQuery` and pass its `RawEvent` to the existing `Reconcile` method.
- Datastore failures remain retryable. IDs whose documents were deleted before processing are treated as terminal.
- No store-client provider or interface changes are required.
- Existing worker concurrency, remediation decisions, status updates, and resume-token behavior remain unchanged.

## Rationale

- ID-only queue entries remove full-document retention from the backlog.
- Reusing existing datastore APIs keeps the change isolated to FR.
- Reusing the existing `Reconcile` method avoids a second decoding and remediation path.
- The implementation mirrors Node Drainer's established ID-only queue and lazy-fetch model.

## Consequences

### Positive
- The workqueue no longer retains full documents for the complete backlog.
- The implementation is small and database-agnostic.
- Live and cold-start events continue through the same parsing and remediation path.

### Negative
- Cold start performs one point read for every queued event.
- Discovery still decodes a bounded batch of full documents.
- Each cold-start document is read once during discovery and again during worker processing.
- Existing map and JSON conversion costs remain during hydration.

### Mitigations
- Keep the discovery batch bounded.
- Preserve the current controller concurrency to avoid changing remediation ordering.
- Treat direct ID projection and typed point reads as follow-up optimizations if the remaining transient allocation is operationally significant.

## Alternatives Considered

### Smaller Full-Document Batches
**Rejected** because: it lowers transient batch memory but still decodes and queues full generic documents.

### ID-Only Scan With Typed Point Fetch
**Deferred** because: it also removes discovery and conversion allocations, but requires new cross-provider APIs, compatibility codecs, and a second ingestion path. The additional complexity is not required to remove the dominant queue-retention cost.

### ID-Only Scan With Map-Based Point Fetch
**Deferred** because: it avoids the initial full-document scan but still requires new provider APIs while preserving the map conversion cost.

## Notes

- This decision applies only to FR cold-start replay. It does not alter live change-stream delivery.
- The queue remains proportional to the number of pending IDs; this decision removes full-document retention, not all transient decoding allocation.

## References

- [ADR-002: Storage Layer Selection](002-storage-layer-selection.md)
- [ADR-009: Fault Remediation Triggering](009-fault-remediation-triggering.md)
- [NVIDIA/NVSentinel#1592](https://github.com/NVIDIA/NVSentinel/issues/1592)
