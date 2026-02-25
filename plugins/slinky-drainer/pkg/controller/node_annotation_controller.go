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

package controller

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// NodeAnnotationReconciler watches nodes and removes the slinky cordon
// annotation when the nvsentinel-state label indicates the node is healthy
// (label absent or remediation-succeeded).
type NodeAnnotationReconciler struct {
	client.Client
}

func (r *NodeAnnotationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	node := &corev1.Node{}
	if err := r.Get(ctx, req.NamespacedName, node); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !shouldRemoveAnnotation(node) {
		slog.Info("Node is not in draining state, skipping", "node", node.Name)

		return ctrl.Result{}, nil
	}

	val, ok := node.Annotations[annotationKey]
	if !ok || !strings.HasPrefix(val, annotationPrefix) {
		slog.Info("Node does not have annotation, skipping", "node", node.Name)
		return ctrl.Result{}, nil
	}

	slog.Info("Node healthy, removing cordon annotation", "node", node.Name)

	delete(node.Annotations, annotationKey)

	if err := r.Update(ctx, node); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}

		return ctrl.Result{}, fmt.Errorf("failed to remove annotation from node %s: %w", node.Name, err)
	}

	return ctrl.Result{}, nil
}

func shouldRemoveAnnotation(node *corev1.Node) bool {
	val, exists := node.Labels[nvsentinelStateLabelKey]

	return !exists || val == remediationSucceededValue
}

func (r *NodeAnnotationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Node{}).
		Complete(r)
}
