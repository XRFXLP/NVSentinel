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

package queue

import (
	"log/slog"
	"sync"

	"k8s.io/client-go/util/workqueue"

	"github.com/nvidia/nvsentinel/node-drainer/pkg/metrics"
)

type queuePriority string
type nodePriorityStateKind int

const (
	queuePriorityHigh queuePriority = "high"
	queuePriorityLow  queuePriority = "low"

	priorityReasonNodeNotYetDraining     = "node_not_yet_draining"
	priorityReasonNodeAlreadyDraining    = "node_already_draining"
	priorityReasonNodeHighPriorityQueued = "node_high_priority_queued"
)

const (
	nodePriorityStateNone nodePriorityStateKind = iota
	nodePriorityStateDraining
	nodePriorityStateRepresented
)

// nodePriorityState tracks the node-level state needed to classify new ready
// queue items without changing the event lifecycle itself.
type nodePriorityState struct {
	mu sync.Mutex

	nodes map[string]nodePriorityStateEntry
}

type nodePriorityStateEntry struct {
	kind              nodePriorityStateKind
	representativeKey string // only meaningful when kind == nodePriorityStateRepresented
}

// nodeEventPriorityQueue is the ready queue used under Kubernetes' rate
// limiting workqueue. It preserves FIFO order within each priority lane.
type nodeEventPriorityQueue struct {
	state *nodePriorityState
	high  []NodeEvent
	low   []NodeEvent
}

var _ workqueue.Queue[NodeEvent] = (*nodeEventPriorityQueue)(nil)

func newNodePriorityState() *nodePriorityState {
	return &nodePriorityState{
		nodes: make(map[string]nodePriorityStateEntry),
	}
}

func newNodeEventPriorityQueue(state *nodePriorityState) *nodeEventPriorityQueue {
	return &nodeEventPriorityQueue{
		state: state,
	}
}

func (q *nodeEventPriorityQueue) Touch(NodeEvent) {}

// Push assigns priority when an item becomes ready. Only the first queued item
// for a not-yet-draining node gets the high-priority lane.
func (q *nodeEventPriorityQueue) Push(item NodeEvent) {
	priority, reason := q.state.classifyForEnqueue(item)
	if priority == queuePriorityHigh {
		q.high = append(q.high, item)
	} else {
		q.low = append(q.low, item)
	}

	metrics.QueueItemsAssigned.WithLabelValues(string(priority), reason).Inc()
	slog.Debug("Assigned node-drainer queue priority",
		"node", item.NodeName,
		"eventID", item.EventID,
		"priority", priority,
		"reason", reason)
}

func (q *nodeEventPriorityQueue) Len() int {
	return len(q.high) + len(q.low)
}

// Pop always drains high-priority representatives before duplicate or
// already-draining work in the low-priority lane.
func (q *nodeEventPriorityQueue) Pop() NodeEvent {
	if len(q.high) > 0 {
		return popNodeEvent(&q.high)
	}

	return popNodeEvent(&q.low)
}

// classifyForEnqueue decides whether an item can still improve the time to
// first draining transition for its node.
func (s *nodePriorityState) classifyForEnqueue(item NodeEvent) (queuePriority, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.nodes[item.NodeName]
	if entry.kind == nodePriorityStateDraining {
		return queuePriorityLow, priorityReasonNodeAlreadyDraining
	}

	if entry.kind == nodePriorityStateRepresented {
		return queuePriorityLow, priorityReasonNodeHighPriorityQueued
	}

	s.nodes[item.NodeName] = nodePriorityStateEntry{
		kind:              nodePriorityStateRepresented,
		representativeKey: representativeKey(item),
	}

	return queuePriorityHigh, priorityReasonNodeNotYetDraining
}

// releaseRepresentative allows a node to receive another high-priority item
// after its current high-priority representative has left the ready queue.
func (s *nodePriorityState) releaseRepresentative(item NodeEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := s.nodes[item.NodeName]
	if entry.kind != nodePriorityStateRepresented || entry.representativeKey != representativeKey(item) {
		return
	}

	delete(s.nodes, item.NodeName)
}

// markNodeDraining moves future work for this node to low priority until the
// draining label is removed or replaced by a terminal state.
func (s *nodePriorityState) markNodeDraining(nodeName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.nodes[nodeName] = nodePriorityStateEntry{kind: nodePriorityStateDraining}
}

// clearNodeDraining lets the node receive high-priority work again after it
// leaves the draining state.
func (s *nodePriorityState) clearNodeDraining(nodeName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.nodes[nodeName].kind == nodePriorityStateDraining {
		delete(s.nodes, nodeName)
	}
}

func popNodeEvent(items *[]NodeEvent) NodeEvent {
	item := (*items)[0]
	(*items)[0] = NodeEvent{}
	*items = (*items)[1:]

	return item
}

func representativeKey(item NodeEvent) string {
	return item.NodeName + ":" + item.EventID
}
