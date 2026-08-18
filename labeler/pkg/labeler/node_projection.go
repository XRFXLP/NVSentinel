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
// informer. The data flows through these stages:
//
//	CEL expression -> NodeFieldRequirements -> nodeCacheProjection -> cached Node
//
// The projection is compiled once at startup. JSON names are resolved to the
// corresponding positions in Kubernetes Go structs, so informer events do not
// repeatedly parse field names.
type nodeCacheProjection struct {
	metadata nodeSectionProjection
	spec     nodeSectionProjection
	status   nodeSectionProjection

	typeMetaFieldIndexes structFieldIndexSet
}

// nodeSectionProjection describes one typed Node section: ObjectMeta, Spec, or
// Status. allFields is used when CEL references the section itself; otherwise,
// fieldIndexes identifies only the direct child fields that must be copied.
type nodeSectionProjection struct {
	structType   reflect.Type
	allFields    bool
	fieldIndexes structFieldIndexSet
}

// structFieldIndexSet stores positions returned by reflect.Type.Field. These
// positions select fields in a Go struct; they are not indexes into Kubernetes
// lists or arrays.
type structFieldIndexSet map[int]struct{}

// newNodeCacheProjection validates paths returned by
// devicecounts.Manager.RequiredNodeFields and prepares their typed copies.
// For example, {"status", "allocatable"} resolves to
// v1.NodeStatus.Allocatable. The resulting transform retains Allocatable while
// leaving unrelated fields such as v1.NodeStatus.Conditions empty.
func newNodeCacheProjection(requirements devicecounts.NodeFieldRequirements) (nodeCacheProjection, error) {
	projection := nodeCacheProjection{
		metadata:             newNodeSectionProjection(reflect.TypeOf(metav1.ObjectMeta{})),
		spec:                 newNodeSectionProjection(reflect.TypeOf(v1.NodeSpec{})),
		status:               newNodeSectionProjection(reflect.TypeOf(v1.NodeStatus{})),
		typeMetaFieldIndexes: structFieldIndexSet{},
	}

	for _, path := range requirements.Paths {
		if err := projection.addRequiredPath(path); err != nil {
			return nodeCacheProjection{}, err
		}
	}

	return projection, nil
}

// newNodeSectionProjection initializes an empty projection for one Kubernetes
// struct section.
func newNodeSectionProjection(structType reflect.Type) nodeSectionProjection {
	return nodeSectionProjection{
		structType:   structType,
		fieldIndexes: structFieldIndexSet{},
	}
}

// addRequiredPath routes a required Node JSON path to its TypeMeta, metadata,
// spec, or status projection and rejects paths that cannot be represented by a
// typed Node.
func (projection *nodeCacheProjection) addRequiredPath(path devicecounts.NodeFieldPath) error {
	if len(path) == 0 {
		return fmt.Errorf("empty Node field path")
	}

	switch path[0] {
	case "apiVersion", "kind":
		if len(path) != 1 {
			return fmt.Errorf("invalid Node field path %q", strings.Join(path, "."))
		}

		fieldIndex, exists := structFieldIndexForJSONName(reflect.TypeOf(metav1.TypeMeta{}), path[0])
		if !exists {
			return fmt.Errorf("unsupported Node field path %q", strings.Join(path, "."))
		}

		projection.typeMetaFieldIndexes[fieldIndex] = struct{}{}
	case "metadata":
		return projection.metadata.addRequiredField(path)
	case "spec":
		return projection.spec.addRequiredField(path)
	case "status":
		return projection.status.addRequiredField(path)
	default:
		return fmt.Errorf("unsupported Node field path %q", strings.Join(path, "."))
	}

	return nil
}

// addRequiredField records the direct child of metadata, spec, or status that
// must remain cached. Deeper path components do not need separate entries
// because Kubernetes fields such as status.allocatable are copied as one typed
// subtree.
func (section *nodeSectionProjection) addRequiredField(path devicecounts.NodeFieldPath) error {
	if len(path) == 1 {
		// An expression that references node.status as a value needs the whole
		// section because no narrower dependency can be inferred.
		section.allFields = true
		return nil
	}

	// Retain the first field below metadata, spec, or status as one typed
	// subtree. For node.status.allocatable["gpu"], this copies Allocatable but
	// leaves Conditions, Addresses, Images, and the rest of Status empty.
	fieldIndex, exists := structFieldIndexForJSONName(section.structType, path[1])
	if !exists {
		return fmt.Errorf("unsupported Node field path %q", strings.Join(path, "."))
	}

	section.fieldIndexes[fieldIndex] = struct{}{}

	return nil
}

// transformNodeForCache removes every Node field outside the fixed Labeler
// requirements and the CEL-derived projection before the object enters the
// informer cache.
func (projection nodeCacheProjection) transformNodeForCache(obj any) (any, error) {
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
	copyFieldsByIndex(&node.TypeMeta, typeMeta, projection.typeMetaFieldIndexes)

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
	if projection.metadata.allFields {
		node.ObjectMeta = objectMeta
	} else {
		copyFieldsByIndex(&node.ObjectMeta, objectMeta, projection.metadata.fieldIndexes)
	}

	node.Spec = v1.NodeSpec{}
	if projection.spec.allFields {
		node.Spec = spec
	} else {
		copyFieldsByIndex(&node.Spec, spec, projection.spec.fieldIndexes)
	}

	node.Status = v1.NodeStatus{}
	if projection.status.allFields {
		node.Status = status
	} else {
		copyFieldsByIndex(&node.Status, status, projection.status.fieldIndexes)
	}

	return node, nil
}

// structFieldIndexForJSONName resolves a Kubernetes JSON field name to its
// position in the corresponding Go struct.
func structFieldIndexForJSONName(structType reflect.Type, fieldName string) (int, bool) {
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

// copyFieldsByIndex copies fields whose indexes were resolved at startup.
func copyFieldsByIndex(destination any, source any, fieldIndexes structFieldIndexSet) {
	// The indexes were validated when the projection was built, keeping the hot
	// informer path small and avoiding repeated JSON-tag scans.
	destinationValue := reflect.ValueOf(destination).Elem()
	sourceValue := reflect.ValueOf(source)

	for fieldIndex := range fieldIndexes {
		destinationValue.Field(fieldIndex).Set(sourceValue.Field(fieldIndex))
	}
}
