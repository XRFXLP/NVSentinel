// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
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

package controller

import (
	"context"
	"fmt"
	"log/slog"

	preflightv1alpha1 "github.com/nvidia/nvsentinel/preflight/api/v1alpha1"
	"github.com/nvidia/nvsentinel/preflight/pkg/config"
	"github.com/nvidia/nvsentinel/preflight/pkg/gang"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PreflightConfigReconciler reconciles PreflightConfig objects into the shared
// DiscovererResolver. Each namespace may have at most one PreflightConfig; the
// reconciler recomputes the effective discoverer for the whole namespace on
// every event so that adds, updates, and deletes all converge, and reports the
// outcome on each object's status.
type PreflightConfigReconciler struct {
	client.Client
	restMapper apimeta.RESTMapper
	resolver   *gang.DiscovererResolver
}

// NewPreflightConfigReconciler creates a reconciler that keeps the resolver in
// sync with PreflightConfig objects.
func NewPreflightConfigReconciler(
	c client.Client,
	restMapper apimeta.RESTMapper,
	resolver *gang.DiscovererResolver,
) *PreflightConfigReconciler {
	return &PreflightConfigReconciler{
		Client:     c,
		restMapper: restMapper,
		resolver:   resolver,
	}
}

// SetupWithManager registers the reconciler with the manager.
func (r *PreflightConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&preflightv1alpha1.PreflightConfig{}).
		Complete(r)
}

// Reconcile recomputes the effective gang discoverer for the namespace of the
// reconciled object.
func (r *PreflightConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	namespace := req.Namespace

	var list preflightv1alpha1.PreflightConfigList
	if err := r.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list PreflightConfig in namespace %s: %w", namespace, err)
	}

	switch len(list.Items) {
	case 0:
		// All configs removed; the namespace falls back to the default.
		r.resolver.Remove(namespace)
		slog.Info("PreflightConfig removed; namespace falls back to default discoverer",
			"namespace", namespace)

		return ctrl.Result{}, nil

	case 1:
		return r.applyConfig(ctx, namespace, &list.Items[0])

	default:
		return r.markConflict(ctx, namespace, list.Items)
	}
}

// applyConfig builds the discoverer for a single config and registers it.
func (r *PreflightConfigReconciler) applyConfig(
	ctx context.Context,
	namespace string,
	pfc *preflightv1alpha1.PreflightConfig,
) (ctrl.Result, error) {
	discoverer, err := gang.NewDiscovererFromConfig(specToConfig(pfc.Spec.GangDiscovery), r.Client, r.restMapper)
	if err != nil {
		// Invalid or unavailable config: fall back to the default and report.
		r.resolver.Remove(namespace)
		slog.Error("Invalid PreflightConfig; namespace falls back to default discoverer",
			"namespace", namespace, "name", pfc.Name, "error", err)

		return ctrl.Result{}, r.updateStatus(ctx, pfc, false, "", fmt.Sprintf("invalid configuration: %v", err))
	}

	r.resolver.Set(namespace, discoverer)
	slog.Info("Registered namespace-scoped gang discoverer from PreflightConfig",
		"namespace", namespace, "name", pfc.Name, "discoverer", discoverer.Name())

	return ctrl.Result{}, r.updateStatus(ctx, pfc, true, discoverer.Name(), "discoverer active")
}

// markConflict handles the case where a namespace has more than one
// PreflightConfig: none take effect and every object is marked not ready.
func (r *PreflightConfigReconciler) markConflict(
	ctx context.Context,
	namespace string,
	items []preflightv1alpha1.PreflightConfig,
) (ctrl.Result, error) {
	r.resolver.Remove(namespace)
	slog.Error("Multiple PreflightConfig objects in namespace; none take effect",
		"namespace", namespace, "count", len(items))

	msg := fmt.Sprintf("namespace has %d PreflightConfig objects; at most one is allowed", len(items))

	var firstErr error

	for i := range items {
		if err := r.updateStatus(ctx, &items[i], false, "", msg); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return ctrl.Result{}, firstErr
}

// updateStatus writes the observed state back to the object's status subresource.
func (r *PreflightConfigReconciler) updateStatus(
	ctx context.Context,
	pfc *preflightv1alpha1.PreflightConfig,
	ready bool,
	discoverer, message string,
) error {
	status := metav1.ConditionFalse
	reason := "InvalidOrConflicting"

	if ready {
		status = metav1.ConditionTrue
		reason = "DiscovererActive"
	}

	// Skip the write when nothing observable changed, to avoid status-only
	// updates triggering further reconciles.
	if statusUpToDate(pfc, ready, discoverer, message, status, reason) {
		return nil
	}

	pfc.Status.ObservedGeneration = pfc.Generation
	pfc.Status.Ready = ready
	pfc.Status.Discoverer = discoverer
	pfc.Status.Message = message

	apimeta.SetStatusCondition(&pfc.Status.Conditions, metav1.Condition{
		Type:               preflightv1alpha1.ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: pfc.Generation,
	})

	if err := r.Status().Update(ctx, pfc); err != nil {
		return fmt.Errorf("failed to update PreflightConfig status: %w", err)
	}

	return nil
}

// statusUpToDate reports whether the object's current status already matches the
// desired values, so a redundant status write can be skipped.
func statusUpToDate(
	pfc *preflightv1alpha1.PreflightConfig,
	ready bool,
	discoverer, message string,
	condStatus metav1.ConditionStatus,
	reason string,
) bool {
	existing := apimeta.FindStatusCondition(pfc.Status.Conditions, preflightv1alpha1.ConditionReady)

	return existing != nil &&
		pfc.Status.ObservedGeneration == pfc.Generation &&
		pfc.Status.Ready == ready &&
		pfc.Status.Discoverer == discoverer &&
		pfc.Status.Message == message &&
		existing.Status == condStatus &&
		existing.Reason == reason &&
		existing.Message == message
}

// specToConfig converts the CRD gang discovery spec into the internal gang
// discovery config consumed by the discoverer factory.
func specToConfig(spec preflightv1alpha1.GangDiscoverySpec) config.GangDiscoveryConfig {
	return config.GangDiscoveryConfig{
		Name:           spec.Name,
		AnnotationKeys: spec.AnnotationKeys,
		LabelKeys:      spec.LabelKeys,
		PodGroupGVR: config.GVRConfig{
			Group:    spec.PodGroupGVR.Group,
			Version:  spec.PodGroupGVR.Version,
			Resource: spec.PodGroupGVR.Resource,
		},
		MinCountExpr: spec.MinCountExpr,
	}
}
