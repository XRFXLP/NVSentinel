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

package pipeline

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
)

type mockTransformer struct {
	name   string
	called bool
	fail   bool
	mutate func(*pb.HealthEvent)
}

func (m *mockTransformer) Transform(ctx context.Context, event *pb.HealthEvent) error {
	m.called = true
	if m.fail {
		return fmt.Errorf("mock error")
	}
	if m.mutate != nil {
		m.mutate(event)
	}
	return nil
}

func (m *mockTransformer) Name() string {
	return m.name
}

type mockFilter struct {
	name   string
	called bool
	keep   bool
	fail   bool
}

func (m *mockFilter) Filter(ctx context.Context, event *pb.HealthEvent) (bool, error) {
	m.called = true
	if m.fail {
		return true, fmt.Errorf("mock filter error")
	}

	return m.keep, nil
}

func (m *mockFilter) Name() string {
	return m.name
}

func TestPipeline(t *testing.T) {
	tests := []struct {
		name         string
		transformers []Transformer
		expectCalls  int
	}{
		{
			name:         "empty-pipeline",
			transformers: []Transformer{},
			expectCalls:  0,
		},
		{
			name: "single-transformer",
			transformers: []Transformer{
				&mockTransformer{name: "t1"},
			},
			expectCalls: 1,
		},
		{
			name: "multiple-transformers",
			transformers: []Transformer{
				&mockTransformer{name: "t1"},
				&mockTransformer{name: "t2"},
				&mockTransformer{name: "t3"},
			},
			expectCalls: 3,
		},
		{
			name: "continues-on-error",
			transformers: []Transformer{
				&mockTransformer{name: "t1", fail: true},
				&mockTransformer{name: "t2"},
			},
			expectCalls: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline := New(tt.transformers...)
			event := &pb.HealthEvent{NodeName: "test-node"}

			pipeline.Process(context.Background(), event)

			callCount := 0
			for _, tr := range tt.transformers {
				if mock, ok := tr.(*mockTransformer); ok && mock.called {
					callCount++
				}
			}

			assert.Equal(t, tt.expectCalls, callCount)
		})
	}
}

func TestPipelineOrder(t *testing.T) {
	order := []string{}

	t1 := &mockTransformer{
		name: "first",
		mutate: func(e *pb.HealthEvent) {
			order = append(order, "first")
		},
	}

	t2 := &mockTransformer{
		name: "second",
		mutate: func(e *pb.HealthEvent) {
			order = append(order, "second")
		},
	}

	pipeline := New(t1, t2)
	event := &pb.HealthEvent{}

	pipeline.Process(context.Background(), event)

	assert.Equal(t, []string{"first", "second"}, order)
}

func TestPipelineFiltersRunAfterTransformers(t *testing.T) {
	order := []string{}

	t1 := &mockTransformer{
		name: "first",
		mutate: func(e *pb.HealthEvent) {
			order = append(order, "transformer")
		},
	}
	f1 := &mockFilter{name: "filter", keep: true}
	pipeline := NewWithFilters([]Transformer{t1}, []Filter{f1})

	keep := pipeline.Process(context.Background(), &pb.HealthEvent{})

	assert.True(t, keep)
	assert.True(t, f1.called)
	assert.Equal(t, []string{"transformer"}, order)
}

func TestPipelineDropsWhenFilterReturnsFalse(t *testing.T) {
	drop := &mockFilter{name: "drop", keep: false}
	notCalled := &mockFilter{name: "not-called", keep: true}
	pipeline := NewWithFilters(nil, []Filter{drop, notCalled})

	keep := pipeline.Process(context.Background(), &pb.HealthEvent{})

	assert.False(t, keep)
	assert.True(t, drop.called)
	assert.False(t, notCalled.called)
}

func TestPipelineKeepsWhenFilterErrors(t *testing.T) {
	failing := &mockFilter{name: "failing", keep: false, fail: true}
	next := &mockFilter{name: "next", keep: true}
	pipeline := NewWithFilters(nil, []Filter{failing, next})

	keep := pipeline.Process(context.Background(), &pb.HealthEvent{})

	assert.True(t, keep)
	assert.True(t, failing.called)
	assert.True(t, next.called)
}
