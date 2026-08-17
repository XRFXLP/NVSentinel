// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package labeler

import (
	"fmt"
	"reflect"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/nvidia/nvsentinel/labeler/pkg/devicecounts"
)

// nodeCacheProjection translates CEL paths into fields retained by the Node
// informer. It is compiled once at startup, so informer events only copy
// precomputed struct field indexes.
type nodeCacheProjection struct {
	fullMetadata   bool
	fullSpec       bool
	fullStatus     bool
	metadataFields map[int]struct{}
	specFields     map[int]struct{}
	statusFields   map[int]struct{}
	typeMetaFields map[int]struct{}
}

// newNodeCacheProjection validates CEL-derived Node paths and resolves their
// JSON field names to Go struct indexes before the informer starts.
func newNodeCacheProjection(requirements devicecounts.NodeFieldRequirements) (nodeCacheProjection, error) {
	projection := nodeCacheProjection{
		metadataFields: map[int]struct{}{},
		specFields:     map[int]struct{}{},
		statusFields:   map[int]struct{}{},
		typeMetaFields: map[int]struct{}{},
	}

	for _, path := range requirements.Paths {
		if err := projection.retainPath(path); err != nil {
			return nodeCacheProjection{}, err
		}
	}

	return projection, nil
}

// retainPath routes a Node JSON path to its TypeMeta, metadata, spec, or status
// projection and rejects paths that cannot be represented by a typed Node.
func (projection *nodeCacheProjection) retainPath(path []string) error {
	if len(path) == 0 {
		return fmt.Errorf("empty Node field path")
	}

	switch path[0] {
	case "apiVersion", "kind":
		if len(path) != 1 {
			return fmt.Errorf("invalid Node field path %q", strings.Join(path, "."))
		}

		fieldIndex, exists := jsonFieldIndex(reflect.TypeOf(metav1.TypeMeta{}), path[0])
		if !exists {
			return fmt.Errorf("unsupported Node field path %q", strings.Join(path, "."))
		}

		projection.typeMetaFields[fieldIndex] = struct{}{}
	case "metadata":
		return projection.retainSectionPath(
			path,
			reflect.TypeOf(metav1.ObjectMeta{}),
			&projection.fullMetadata,
			projection.metadataFields,
		)
	case "spec":
		return projection.retainSectionPath(
			path,
			reflect.TypeOf(v1.NodeSpec{}),
			&projection.fullSpec,
			projection.specFields,
		)
	case "status":
		return projection.retainSectionPath(
			path,
			reflect.TypeOf(v1.NodeStatus{}),
			&projection.fullStatus,
			projection.statusFields,
		)
	default:
		return fmt.Errorf("unsupported Node field path %q", strings.Join(path, "."))
	}

	return nil
}

// retainSectionPath records the direct child of metadata, spec, or status that
// must remain cached. Deeper path components do not need separate entries
// because Kubernetes fields such as status.allocatable are copied as one typed
// subtree.
func (projection *nodeCacheProjection) retainSectionPath(
	path []string,
	sectionType reflect.Type,
	fullSection *bool,
	fields map[int]struct{},
) error {
	if len(path) == 1 {
		// An expression that references node.status as a value needs the whole
		// section because no narrower dependency can be inferred.
		*fullSection = true
		return nil
	}

	// Retain the first field below metadata, spec, or status as one typed
	// subtree. For node.status.allocatable["gpu"], this copies Allocatable but
	// leaves Conditions, Addresses, Images, and the rest of Status empty.
	fieldIndex, exists := jsonFieldIndex(sectionType, path[1])
	if !exists {
		return fmt.Errorf("unsupported Node field path %q", strings.Join(path, "."))
	}

	fields[fieldIndex] = struct{}{}

	return nil
}

// transform removes every Node field outside the fixed Labeler requirements
// and the CEL-derived projection before the object enters the informer cache.
func (projection nodeCacheProjection) transform(obj any) (any, error) {
	node, ok := obj.(*v1.Node)
	if !ok {
		return nil, fmt.Errorf("node transform: expected Node object, got %T", obj)
	}

	// TransformFunc owns this object before it enters the informer cache, so it
	// is safe to trim in place. Snapshot each section before clearing it.
	typeMeta := node.TypeMeta
	objectMeta := node.ObjectMeta
	spec := node.Spec
	status := node.Status

	node.TypeMeta = metav1.TypeMeta{}
	copyProjectedFields(&node.TypeMeta, typeMeta, projection.typeMetaFields)

	var annotations map[string]string
	if value, exists := objectMeta.Annotations[DCGMBootstrapCompletedAnnotation]; exists {
		annotations = map[string]string{DCGMBootstrapCompletedAnnotation: value}
	}

	// These fields are always required by Labeler itself, independently of CEL:
	// identity/cache bookkeeping, label decisions, and DCGM bootstrap state.
	node.ObjectMeta = metav1.ObjectMeta{
		Name:            objectMeta.Name,
		UID:             objectMeta.UID,
		ResourceVersion: objectMeta.ResourceVersion,
		Labels:          objectMeta.Labels,
		Annotations:     annotations,
	}
	if projection.fullMetadata {
		node.ObjectMeta = objectMeta
	} else {
		copyProjectedFields(&node.ObjectMeta, objectMeta, projection.metadataFields)
	}

	node.Spec = v1.NodeSpec{}
	if projection.fullSpec {
		node.Spec = spec
	} else {
		copyProjectedFields(&node.Spec, spec, projection.specFields)
	}

	node.Status = v1.NodeStatus{}
	if projection.fullStatus {
		node.Status = status
	} else {
		copyProjectedFields(&node.Status, status, projection.statusFields)
	}

	return node, nil
}

// jsonFieldIndex resolves a Kubernetes JSON field name to its Go struct index.
func jsonFieldIndex(structType reflect.Type, fieldName string) (int, bool) {
	// CEL paths use Kubernetes JSON names (for example "allocatable"), while
	// reflection addresses Go fields (for example NodeStatus.Allocatable).
	for i := 0; i < structType.NumField(); i++ {
		name, _, _ := strings.Cut(structType.Field(i).Tag.Get("json"), ",")
		if name == fieldName {
			return i, true
		}
	}

	return 0, false
}

// copyProjectedFields copies fields whose indexes were resolved at startup.
func copyProjectedFields(destination any, source any, fieldIndexes map[int]struct{}) {
	// The indexes were validated when the projection was built, keeping the hot
	// informer path small and avoiding repeated JSON-tag scans.
	destinationValue := reflect.ValueOf(destination).Elem()
	sourceValue := reflect.ValueOf(source)

	for fieldIndex := range fieldIndexes {
		destinationValue.Field(fieldIndex).Set(sourceValue.Field(fieldIndex))
	}
}
