// Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	janitorv1alpha1 "github.com/nvidia/nvsentinel/janitor/api/v1alpha1"
	"github.com/nvidia/nvsentinel/janitor/pkg/distributedlock"
)

const (
	nodeAlreadyUnderMaintenanceReason = "NodeAlreadyUnderMaintenance"
	lockContentionRequeueMessage      = "checking node lock holder"
)

// activeSameKindHolder resolves the owner of a contended node lock. A holder
// is a duplicate only when it has the requested kind and has not completed.
// Cross-kind contention and the small window between completion and lease
// deletion are intentionally treated as normal waiting.
func activeSameKindHolder(
	ctx context.Context,
	k8sClient client.Client,
	nodeLock distributedlock.NodeLock,
	nodeName, expectedKind string,
) (client.Object, bool, error) {
	holder, err := nodeLock.GetHolder(ctx, nodeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("%s: %w", lockContentionRequeueMessage, err)
	}

	if holder.Kind != expectedKind {
		return nil, false, nil
	}

	var object client.Object

	switch expectedKind {
	case "RebootNode":
		object = &janitorv1alpha1.RebootNode{}
	case "TerminateNode":
		object = &janitorv1alpha1.TerminateNode{}
	case "GPUReset":
		object = &janitorv1alpha1.GPUReset{}
	default:
		return nil, false, fmt.Errorf("unsupported maintenance kind %q", expectedKind)
	}

	if err := k8sClient.Get(ctx, client.ObjectKey{Name: holder.Name}, object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("getting %s lock holder %q: %w", holder.Kind, holder.Name, err)
	}

	if object.GetUID() != holder.UID {
		return nil, false, nil
	}

	var completionTime *metav1.Time

	switch typed := object.(type) {
	case *janitorv1alpha1.RebootNode:
		completionTime = typed.Status.CompletionTime
	case *janitorv1alpha1.TerminateNode:
		completionTime = typed.Status.CompletionTime
	case *janitorv1alpha1.GPUReset:
		completionTime = typed.Status.CompletionTime
	}

	return object, completionTime == nil, nil
}

func gpuUUIDsOverlap(first, second *janitorv1alpha1.GPUSelector) bool {
	if first == nil || second == nil {
		return false
	}

	for _, firstUUID := range first.UUIDs {
		for _, secondUUID := range second.UUIDs {
			if firstUUID == secondUUID {
				return true
			}
		}
	}

	return false
}
