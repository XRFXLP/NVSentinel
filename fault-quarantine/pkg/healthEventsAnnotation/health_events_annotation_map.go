// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package healthEventsAnnotation

import (
	"encoding/json"
	"fmt"

	"github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

// EventKey represents the identifying fields of a HealthEvent for entity-level matching
// IMPORTANT: This struct is used ONLY for matching/comparison.
// The full event details (including IsFatal, IsHealthy, ErrorCodes, Message) are stored
// in the annotation for visibility, but matching only uses these identifying fields.
//
// ErrorCode is included in the key so that synthetic cancellation events,
// which carry a targeted error code, clear only the matching prior fault. A
// real component-recovery event (e.g. successful GPU reset) carries an empty
// ErrorCode and continues to clear every prior fault on the entity via the
// fallback path in createEventKeys / RemoveEvent.
type HealthEventKey struct {
	Agent          string // e.g., "gpu-health-monitor"
	ComponentClass string // e.g., "GPU"
	CheckName      string // e.g., "GpuXidError"
	NodeName       string // e.g., "node-123"
	// Entity-specific fields for granular tracking
	EntityType  string // e.g., "GPU", "NIC"
	EntityValue string // e.g., "1", "eth0"
	// ErrorCode scopes the key to a specific error code; empty when the event
	// has no ErrorCode (real recovery events) so the legacy entity-only key is
	// used.
	ErrorCode string // e.g., "163"
	// Version is included in the key to distinguish between different versions of the same event
	Version uint32 // e.g., 1
}

// HealthEventsAnnotationMap represents a collection of unique health events
type HealthEventsAnnotationMap struct {
	Events map[HealthEventKey]*protos.HealthEvent
}

// NewHealthEventsAnnotationMap creates a new HealthEventsAnnotationMap instance
func NewHealthEventsAnnotationMap() *HealthEventsAnnotationMap {
	return &HealthEventsAnnotationMap{
		Events: make(map[HealthEventKey]*protos.HealthEvent),
	}
}

// CreateEventKeyForEntity creates a comparable key for a specific entity in a HealthEvent
//
// The ErrorCode field on the returned key is left empty. Callers that want
// per-error-code scoping should set ErrorCode after calling this function or
// use createEventKeys, which fans out one key per (entity, errorCode) pair.
func CreateEventKeyForEntity(
	event *protos.HealthEvent,
	entity *protos.Entity,
) HealthEventKey {
	key := HealthEventKey{
		Agent:          event.Agent,
		ComponentClass: event.ComponentClass,
		CheckName:      event.CheckName,
		NodeName:       event.NodeName,
		Version:        event.Version,
	}

	// Add entity-specific information if provided
	if entity != nil {
		key.EntityType = entity.EntityType
		key.EntityValue = entity.EntityValue
	}

	return key
}

// createEventKeys creates keys for all (entity, errorCode) pairs in a
// HealthEvent.
//
// Behaviour matrix:
//   - No entities, no ErrorCode → one entity-only, error-code-empty key (legacy).
//   - No entities, with ErrorCode(s) → one key per error code (check-level
//     synthetic event scoped to specific error codes).
//   - Entities, no ErrorCode → one entity-only key per entity (legacy: a real
//     recovery event clears every prior error code on the entity).
//   - Entities, with ErrorCode(s) → one key per (entity, errorCode) pair so a
//     synthetic cancellation event clears only its targeted code.
func createEventKeys(event *protos.HealthEvent) []HealthEventKey {
	entities := event.EntitiesImpacted
	errorCodes := event.ErrorCode

	if len(entities) == 0 {
		return createCheckLevelKeys(event, errorCodes)
	}

	return createEntityKeys(event, entities, errorCodes)
}

// createCheckLevelKeys returns keys for an event with no impacted entities.
// One key per error code is emitted, or a single entity-empty key when no
// error codes are present.
func createCheckLevelKeys(event *protos.HealthEvent, errorCodes []string) []HealthEventKey {
	if len(errorCodes) == 0 {
		return []HealthEventKey{CreateEventKeyForEntity(event, nil)}
	}

	keys := make([]HealthEventKey, 0, len(errorCodes))
	for _, code := range errorCodes {
		key := CreateEventKeyForEntity(event, nil)
		key.ErrorCode = code
		keys = append(keys, key)
	}

	return keys
}

// createEntityKeys returns keys for an event with at least one impacted
// entity. When error codes are present, one key per (entity, errorCode) pair
// is emitted; otherwise one entity-only key per entity (legacy behaviour).
func createEntityKeys(
	event *protos.HealthEvent,
	entities []*protos.Entity,
	errorCodes []string,
) []HealthEventKey {
	if len(errorCodes) == 0 {
		keys := make([]HealthEventKey, 0, len(entities))
		for _, entity := range entities {
			keys = append(keys, CreateEventKeyForEntity(event, entity))
		}

		return keys
	}

	keys := make([]HealthEventKey, 0, len(entities)*len(errorCodes))

	for _, entity := range entities {
		for _, code := range errorCodes {
			key := CreateEventKeyForEntity(event, entity)
			key.ErrorCode = code
			keys = append(keys, key)
		}
	}

	return keys
}

// AddOrUpdateEvent adds a health event for each impacted entity
// Returns true if at least one entity was added/updated
func (he *HealthEventsAnnotationMap) AddOrUpdateEvent(event *protos.HealthEvent) bool {
	keys := createEventKeys(event)
	added := false

	for _, key := range keys {
		if _, exists := he.Events[key]; !exists {
			he.Events[key] = event
			added = true
		}
	}

	return added
}

// GetEvent checks if any entity from the event exists in the map
// Returns the stored event for the first matching entity
// If the event has no entities (empty EntitiesImpacted), it performs check-level matching
// to find any stored event with the same Agent/ComponentClass/CheckName/Version
//
// ErrorCode semantics: when the incoming event has no ErrorCode, an entity
// match against any stored key wins regardless of the stored key's ErrorCode
// (preserving the "real recovery clears every code" contract). When the
// incoming event has ErrorCode set, only stored keys whose ErrorCode matches
// one of the incoming codes are considered.
func (he *HealthEventsAnnotationMap) GetEvent(
	event *protos.HealthEvent,
) (*protos.HealthEvent, bool) {
	if storedEvent, ok := he.findFirstMatch(event); ok {
		return storedEvent, true
	}

	// If no entities in incoming event (check-level event), do check-level matching
	// This handles cases where healthy events don't specify entities but mean "all entities for this check are healthy"
	if len(event.EntitiesImpacted) == 0 {
		return he.getEventByCheck(event)
	}

	return nil, false
}

// findFirstMatch returns the first stored event that matches one of the keys
// derived from the incoming event under the asymmetric ErrorCode rule
// described on GetEvent.
func (he *HealthEventsAnnotationMap) findFirstMatch(event *protos.HealthEvent) (*protos.HealthEvent, bool) {
	for storedKey, storedEvent := range he.Events {
		if keyMatchesEvent(storedKey, event) {
			return storedEvent, true
		}
	}

	return nil, false
}

// keyMatchesEvent reports whether storedKey matches the supplied event.
// Identifying fields (Agent, ComponentClass, CheckName, Version, NodeName)
// must always match. Entity matching uses any of the event's entities (or
// matches every key when the event carries no entities — handled by the
// caller). ErrorCode uses the asymmetric rule documented on GetEvent.
func keyMatchesEvent(storedKey HealthEventKey, event *protos.HealthEvent) bool {
	if storedKey.Agent != event.Agent ||
		storedKey.ComponentClass != event.ComponentClass ||
		storedKey.CheckName != event.CheckName ||
		storedKey.NodeName != event.NodeName ||
		storedKey.Version != event.Version {
		return false
	}

	if len(event.EntitiesImpacted) > 0 && !entityInEvent(storedKey, event) {
		return false
	}

	if len(event.ErrorCode) > 0 && !errorCodeMatches(storedKey.ErrorCode, event.ErrorCode) {
		return false
	}

	return true
}

func entityInEvent(key HealthEventKey, event *protos.HealthEvent) bool {
	for _, e := range event.EntitiesImpacted {
		if e.EntityType == key.EntityType && e.EntityValue == key.EntityValue {
			return true
		}
	}

	return false
}

func errorCodeMatches(stored string, incoming []string) bool {
	for _, c := range incoming {
		if c == stored {
			return true
		}
	}

	return false
}

// getEventByCheck finds any stored event matching the check (ignoring entities)
// Used for check-level healthy events that clear all entities for a check
func (he *HealthEventsAnnotationMap) getEventByCheck(
	event *protos.HealthEvent,
) (*protos.HealthEvent, bool) {
	for key, storedEvent := range he.Events {
		if key.Agent == event.Agent &&
			key.ComponentClass == event.ComponentClass &&
			key.CheckName == event.CheckName &&
			key.Version == event.Version &&
			key.NodeName == event.NodeName {
			return storedEvent, true
		}
	}

	return nil, false
}

// HasMatchingEntities checks if the event has any entities that match stored
// events. Honours the same asymmetric ErrorCode rule as GetEvent / RemoveEvent
// so callers see consistent matching across the API.
func (he *HealthEventsAnnotationMap) HasMatchingEntities(event *protos.HealthEvent) bool {
	for storedKey := range he.Events {
		if keyMatchesEvent(storedKey, event) {
			return true
		}
	}

	return false
}

// IsEmpty checks if there are no events in the collection
func (he *HealthEventsAnnotationMap) IsEmpty() bool {
	return len(he.Events) == 0
}

// Count returns the number of events in the collection
func (he *HealthEventsAnnotationMap) Count() int {
	return len(he.Events)
}

// RemoveEvent removes all matching entities from the collection
// This is used when a healthy event clears specific entity failures
// If the event has no entities (empty EntitiesImpacted), it removes ALL entities for that check
// This handles check-level healthy events that mean "all entities for this check are healthy"
//
// ErrorCode semantics: a healthy event with empty ErrorCode removes every
// stored key for the matching entities (preserving real component-recovery
// semantics). A healthy event with ErrorCode set removes only stored keys
// whose ErrorCode is in the supplied list.
func (he *HealthEventsAnnotationMap) RemoveEvent(event *protos.HealthEvent) int {
	// If no entities specified (check-level healthy event), remove all entities for this check
	if len(event.EntitiesImpacted) == 0 {
		return he.removeAllEntitiesForCheck(event)
	}

	keysToRemove := make([]HealthEventKey, 0)

	for storedKey := range he.Events {
		if keyMatchesEvent(storedKey, event) {
			keysToRemove = append(keysToRemove, storedKey)
		}
	}

	for _, key := range keysToRemove {
		delete(he.Events, key)
	}

	return len(keysToRemove)
}

// removeAllEntitiesForCheck removes all entities for a specific check
// Used when a healthy event has no entities, meaning "all entities for this check are healthy"
//
// When the incoming event carries an ErrorCode, the removal is narrowed to
// keys whose ErrorCode appears in the incoming list. An empty ErrorCode
// preserves today's "clear everything for this check" semantic.
func (he *HealthEventsAnnotationMap) removeAllEntitiesForCheck(event *protos.HealthEvent) int {
	removed := 0
	keysToRemove := []HealthEventKey{}

	for key := range he.Events {
		if key.Agent != event.Agent ||
			key.ComponentClass != event.ComponentClass ||
			key.CheckName != event.CheckName ||
			key.Version != event.Version ||
			key.NodeName != event.NodeName {
			continue
		}

		if len(event.ErrorCode) > 0 && !errorCodeMatches(key.ErrorCode, event.ErrorCode) {
			continue
		}

		keysToRemove = append(keysToRemove, key)
		removed++
	}

	for _, key := range keysToRemove {
		delete(he.Events, key)
	}

	return removed
}

// RemoveEntitiesForCheck removes specific entities for a check
func (he *HealthEventsAnnotationMap) RemoveEntitiesForCheck(event *protos.HealthEvent) {
	keys := createEventKeys(event)
	for _, key := range keys {
		delete(he.Events, key)
	}
}

// GetAllCheckNames returns all unique check names from stored events
func (he *HealthEventsAnnotationMap) GetAllCheckNames() []string {
	checkNamesMap := make(map[string]bool)

	for _, event := range he.Events {
		if event != nil && event.CheckName != "" {
			checkNamesMap[event.CheckName] = true
		}
	}

	checkNames := make([]string, 0, len(checkNamesMap))

	for name := range checkNamesMap {
		checkNames = append(checkNames, name)
	}

	return checkNames
}

// MarshalJSON converts the map to a JSON-serializable format (slice of events)
// Deduplicates events since multiple entity keys may point to the same event object
func (he *HealthEventsAnnotationMap) MarshalJSON() ([]byte, error) {
	// Use a map to deduplicate event pointers
	// Multiple entities can reference the same event object, we only want unique events
	seen := make(map[*protos.HealthEvent]bool)
	events := make([]*protos.HealthEvent, 0, len(he.Events))

	for _, event := range he.Events {
		if !seen[event] {
			seen[event] = true

			events = append(events, event)
		}
	}

	return json.Marshal(events)
}

// UnmarshalJSON reconstructs the map from JSON data (slice of events)
func (he *HealthEventsAnnotationMap) UnmarshalJSON(data []byte) error {
	var events []*protos.HealthEvent
	if err := json.Unmarshal(data, &events); err != nil {
		return fmt.Errorf("failed to unmarshal health events: %w", err)
	}

	if he.Events == nil {
		he.Events = make(map[HealthEventKey]*protos.HealthEvent)
	}

	for k := range he.Events {
		delete(he.Events, k)
	}

	for _, event := range events {
		// Re-create entity-level tracking from the stored events
		keys := createEventKeys(event)
		for _, key := range keys {
			he.Events[key] = event
		}
	}

	return nil
}
