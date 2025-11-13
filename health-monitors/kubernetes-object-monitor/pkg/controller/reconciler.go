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
package controller

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/config"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/metrics"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/policy"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type HealthEventPublisher interface {
	PublishHealthEvent(policy *config.Policy, nodeName string, isHealthy bool) error
}

type ResourceReconciler struct {
	client.Client
	evaluator   *policy.Evaluator
	publisher   HealthEventPublisher
	policies    []config.Policy
	gvk         schema.GroupVersionKind
	matchStates map[string]bool
}

func NewResourceReconciler(
	c client.Client,
	evaluator *policy.Evaluator,
	pub HealthEventPublisher,
	policies []config.Policy,
	gvk schema.GroupVersionKind,
) *ResourceReconciler {
	return &ResourceReconciler{
		Client:      c,
		evaluator:   evaluator,
		publisher:   pub,
		policies:    policies,
		gvk:         gvk,
		matchStates: make(map[string]bool),
	}
}

func (r *ResourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.gvk)

	if err := r.Get(ctx, req.NamespacedName, obj); err != nil {
		return r.handleGetError(err, req)
	}

	for _, p := range r.policies {
		if !p.Enabled {
			continue
		}

		if err := r.reconcilePolicy(ctx, &p, obj); err != nil {
			slog.Error("Failed to reconcile policy", "policy", p.Name, "resource", req.NamespacedName, "error", err)
			metrics.ReconciliationErrors.WithLabelValues(r.gvk.Kind, "policy_reconcile_error").Inc()
		}
	}

	return ctrl.Result{}, nil
}

func (r *ResourceReconciler) handleGetError(err error, req ctrl.Request) (ctrl.Result, error) {
	if client.IgnoreNotFound(err) == nil {
		r.cleanupDeletedResource(req.Name)
		return ctrl.Result{}, nil
	}

	metrics.ReconciliationErrors.WithLabelValues(r.gvk.Kind, "get_error").Inc()

	return ctrl.Result{}, err
}

func (r *ResourceReconciler) cleanupDeletedResource(resourceName string) {
	for _, p := range r.policies {
		if !p.Enabled {
			continue
		}

		stateKey := r.getStateKey(&p, resourceName)
		if r.matchStates[stateKey] {
			delete(r.matchStates, stateKey)
		}
	}
}

func (r *ResourceReconciler) reconcilePolicy(
	ctx context.Context,
	p *config.Policy,
	obj *unstructured.Unstructured,
) error {
	matched, err := r.evaluator.EvaluatePredicate(ctx, p.Name, obj)
	if err != nil {
		metrics.PolicyEvaluationErrors.WithLabelValues(p.Name, "cel_error").Inc()
		return fmt.Errorf("predicate evaluation failed: %w", err)
	}

	nodeName, err := r.evaluator.EvaluateNodeAssociation(ctx, p.Name, obj)
	if err != nil {
		metrics.PolicyEvaluationErrors.WithLabelValues(p.Name, "node_association_error").Inc()
		return fmt.Errorf("node association evaluation failed: %w", err)
	}

	if nodeName == "" {
		nodeName = obj.GetName()
	}

	stateKey := r.getStateKey(p, obj.GetName())
	wasMatched := r.matchStates[stateKey]

	if matched && !wasMatched {
		return r.handleUnhealthyTransition(p, nodeName, stateKey)
	}

	if !matched && wasMatched {
		return r.handleHealthyTransition(p, nodeName, stateKey)
	}

	return nil
}

func (r *ResourceReconciler) handleUnhealthyTransition(
	p *config.Policy,
	nodeName string,
	stateKey string,
) error {
	if err := r.publisher.PublishHealthEvent(p, nodeName, false); err != nil {
		metrics.HealthEventsPublishErrors.WithLabelValues(p.Name, "grpc_error").Inc()
		return fmt.Errorf("failed to publish unhealthy event: %w", err)
	}

	r.matchStates[stateKey] = true
	metrics.PolicyMatches.WithLabelValues(p.Name, nodeName, r.gvk.Kind).Inc()

	return nil
}

func (r *ResourceReconciler) handleHealthyTransition(
	p *config.Policy,
	nodeName string,
	stateKey string,
) error {
	if err := r.publisher.PublishHealthEvent(p, nodeName, true); err != nil {
		metrics.HealthEventsPublishErrors.WithLabelValues(p.Name, "grpc_error").Inc()
		return fmt.Errorf("failed to publish healthy event: %w", err)
	}

	delete(r.matchStates, stateKey)

	return nil
}

func (r *ResourceReconciler) getStateKey(p *config.Policy, resourceName string) string {
	return fmt.Sprintf("%s/%s", p.Name, resourceName)
}
