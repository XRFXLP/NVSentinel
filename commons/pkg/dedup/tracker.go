// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package dedup tracks recently observed health events by a canonical event key.
package dedup

import (
	"encoding/binary"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

type Option func(*Tracker)

// WithNow injects the clock used by the tracker. It is intended for tests.
func WithNow(now func() time.Time) Option {
	return func(t *Tracker) {
		if now != nil {
			t.now = now
		}
	}
}

// Tracker remembers recently seen health-event keys for one burst window.
type Tracker struct {
	mu   sync.RWMutex
	seen map[uint64]time.Time
	ttl  time.Duration
	now  func() time.Time
}

// NewTracker creates a tracker that treats repeated keys within ttl as duplicates.
func NewTracker(ttl time.Duration, opts ...Option) *Tracker {
	t := &Tracker{
		seen: make(map[uint64]time.Time),
		ttl:  ttl,
		now:  time.Now,
	}

	for _, opt := range opts {
		opt(t)
	}

	return t
}

// Key extracts the canonical key from an event.
func Key(event *pb.HealthEvent) uint64 {
	return keyWithHealthState(event, event.GetIsHealthy())
}

// IsDuplicate is true iff the event's key is already in the tracker and within ttl.
// Side effect: evicts the queried key if expired (lazy eviction).
func (t *Tracker) IsDuplicate(event *pb.HealthEvent) bool {
	k := Key(event)
	now := t.now()

	t.mu.Lock()
	defer t.mu.Unlock()

	firstSeen, ok := t.seen[k]
	if !ok {
		return false
	}

	if now.Sub(firstSeen) >= t.ttl {
		delete(t.seen, k)
		return false
	}

	return true
}

// Mark records the event's key.
func (t *Tracker) Mark(event *pb.HealthEvent) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seen[Key(event)] = t.now()
}

// ClearUnhealthyCounterpart removes the prior unhealthy entry that a healthy
// recovery event resolves. Unhealthy events do not resolve anything, so they are ignored.
func (t *Tracker) ClearUnhealthyCounterpart(event *pb.HealthEvent) {
	if !event.GetIsHealthy() {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.seen, keyWithHealthState(event, false))
}

// EvictExpired walks the entire seen set and removes entries past ttl.
func (t *Tracker) EvictExpired() {
	now := t.now()

	t.mu.Lock()
	defer t.mu.Unlock()

	for k, firstSeen := range t.seen {
		if now.Sub(firstSeen) >= t.ttl {
			delete(t.seen, k)
		}
	}
}

// Clear removes all tracked keys.
func (t *Tracker) Clear() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.seen = make(map[uint64]time.Time)
}

// keyWithHealthState builds the canonical event key while allowing callers to
// evaluate the same event as healthy or unhealthy. Recovery handling uses this
// to clear the unhealthy key that corresponds to an incoming healthy event.
func keyWithHealthState(event *pb.HealthEvent, isHealthy bool) uint64 {
	h := fnv.New64a()

	writeString(h, event.GetNodeName())
	writeString(h, event.GetCheckName())

	entities := canonicalEntities(event.GetEntitiesImpacted())
	writeUint64(h, uint64(len(entities)))
	for _, entity := range entities {
		writeString(h, entity.entityType)
		writeString(h, entity.entityValue)
	}

	errorCodes := append([]string(nil), event.GetErrorCode()...)
	sort.Strings(errorCodes)
	writeUint64(h, uint64(len(errorCodes)))
	for _, errorCode := range errorCodes {
		writeString(h, errorCode)
	}

	if isHealthy {
		_, _ = h.Write([]byte{1})
	} else {
		_, _ = h.Write([]byte{0})
	}

	return h.Sum64()
}

type canonicalEntity struct {
	entityType  string
	entityValue string
}

func canonicalEntities(entities []*pb.Entity) []canonicalEntity {
	canonical := make([]canonicalEntity, 0, len(entities))
	for _, entity := range entities {
		canonical = append(canonical, canonicalEntity{
			entityType:  entity.GetEntityType(),
			entityValue: entity.GetEntityValue(),
		})
	}

	sort.Slice(canonical, func(i, j int) bool {
		if canonical[i].entityType != canonical[j].entityType {
			return canonical[i].entityType < canonical[j].entityType
		}

		return canonical[i].entityValue < canonical[j].entityValue
	})

	return canonical
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func writeString(w byteWriter, value string) {
	writeUint64(w, uint64(len(value)))
	_, _ = w.Write([]byte(value))
}

func writeUint64(w byteWriter, value uint64) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, _ = w.Write(buf[:])
}
