// Copyright (c) 2026, NVIDIA CORPORATION.  All rights reserved.
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

package initializer

import (
	"log/slog"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	toolscache "k8s.io/client-go/tools/cache"

	celenv "github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/cel"
	"github.com/nvidia/nvsentinel/health-monitors/kubernetes-object-monitor/pkg/config"
)

const metadataKey = "metadata"

// informerFieldPaths are retained for every watched GVK regardless of what the
// policies read. The informer keys objects by name and namespace, delete and
// resync handling need uid, resourceVersion and deletionTimestamp, and the
// cache resolves an unstructured object's type from apiVersion and kind.
// Outside CEL the only reads of a watched object are GetName() and
// GetNamespace() in the reconciler and GetKind() in the policy evaluator.
var informerFieldPaths = [][]string{
	{"apiVersion"},
	{"kind"},
	{metadataKey, "name"},
	{metadataKey, "namespace"},
	{metadataKey, "uid"},
	{metadataKey, "resourceVersion"},
	{metadataKey, "deletionTimestamp"},
}

// buildCacheTransforms derives a cache transform for each GVK from the CEL its
// enabled policies compile to. A GVK is absent from the result when its objects
// must be cached in full, either because a policy uses the object as a whole or
// because an expression failed to compile.
func buildCacheTransforms(
	compiler *celenv.Environment,
	policies []config.Policy,
) map[schema.GroupVersionKind]toolscache.TransformFunc {
	policiesByGVK := make(map[schema.GroupVersionKind][]config.Policy)

	for _, policy := range policies {
		if policy.Enabled {
			gvk := policyGVK(policy)
			policiesByGVK[gvk] = append(policiesByGVK[gvk], policy)
		}
	}

	transforms := make(map[schema.GroupVersionKind]toolscache.TransformFunc, len(policiesByGVK))

	for gvk, gvkPolicies := range policiesByGVK {
		tree, opaquePolicy := deriveFieldTree(compiler, gvkPolicies)
		if tree == nil {
			slog.Info("Caching full objects: policy fields could not be derived from CEL",
				"gvk", gvk.String(), "policy", opaquePolicy)

			continue
		}

		transforms[gvk] = newFieldPruningTransform(tree)

		slog.Info("Cache transform derived from policy CEL",
			"gvk", gvk.String(), "retainedFields", strings.Join(tree.describe(), " "))
	}

	return transforms
}

// deriveFieldTree returns the fields the policies watching one GVK read. The
// tree is nil when extraction failed, along with the name of the policy it
// failed on, and that GVK's objects must then be cached in full.
func deriveFieldTree(compiler *celenv.Environment, policies []config.Policy) (*fieldTree, string) {
	tree := newFieldTree(informerFieldPaths)

	for _, policy := range policies {
		for _, expression := range policyExpressions(policy) {
			paths, ok := policyFieldPaths(compiler, expression)
			if !ok {
				return nil, policy.Name
			}

			for _, path := range paths {
				tree.insert(path)
			}
		}
	}

	return tree, ""
}

func policyExpressions(p config.Policy) []string {
	if p.NodeAssociation == nil {
		return []string{p.Predicate.Expression}
	}

	return []string{p.Predicate.Expression, p.NodeAssociation.Expression}
}

// policyFieldPaths compiles expression and returns the resource field paths it
// reads. A compile failure is reported as an incomplete extraction rather than
// an error: policy.NewEvaluator compiles the same expression with the policy
// name for context and fails startup there.
func policyFieldPaths(compiler *celenv.Environment, expression string) ([][]string, bool) {
	compiled, err := compiler.Compile(expression)
	if err != nil {
		return nil, false
	}

	return celenv.ResourceFieldPaths(compiled)
}

// newFieldPruningTransform returns a cache.TransformFunc that strips every
// field the tree does not retain.
func newFieldPruningTransform(tree *fieldTree) toolscache.TransformFunc {
	return func(in any) (any, error) {
		obj, ok := in.(*unstructured.Unstructured)
		if !ok || obj == nil || obj.Object == nil {
			// Tombstones and structured objects are passed through: a
			// partially understood object is worse than a whole one.
			return in, nil
		}

		obj.Object = prune(obj.Object, tree)

		return obj, nil
	}
}

// fieldTree is a prefix tree of the field paths a cached object must retain.
// keepSubtree marks a node whose value is retained whole, which is what makes
// inserting status.conditions.type beneath status.conditions a no-op.
type fieldTree struct {
	keepSubtree bool
	children    map[string]*fieldTree
}

func newFieldTree(paths [][]string) *fieldTree {
	tree := &fieldTree{}
	for _, path := range paths {
		tree.insert(path)
	}

	return tree
}

// insert records a path as a sequence of segments, descending a level per
// segment and creating the nodes it passes through. Segments are never split or
// joined, so a map key containing dots stays a single level of the tree.
func (t *fieldTree) insert(segments []string) {
	node := t

	for _, segment := range segments {
		if node.keepSubtree {
			return
		}

		if node.children == nil {
			node.children = make(map[string]*fieldTree)
		}

		child := node.children[segment]
		if child == nil {
			child = &fieldTree{}
			node.children[segment] = child
		}

		node = child
	}

	node.keepSubtree = true
	node.children = nil
}

// describe renders the retained paths for logging, in sorted order and
// collapsed so that a retained subtree is reported once. The dotted form is for
// operators to read and is not used to prune, which is why it can be lossy for
// a map key that contains dots.
func (t *fieldTree) describe() []string {
	var out []string

	for name, child := range t.children {
		out = child.appendPaths(out, name)
	}

	slices.Sort(out)

	return out
}

// appendPaths appends the dotted paths retained at or beneath t, each carrying
// prefix, the path by which t was reached.
func (t *fieldTree) appendPaths(out []string, prefix string) []string {
	if t.keepSubtree || len(t.children) == 0 {
		return append(out, prefix)
	}

	for name, child := range t.children {
		out = child.appendPaths(out, prefix+"."+name)
	}

	return out
}

// prune returns a copy of obj holding only the fields the tree retains. Values
// under a retained path are carried over by reference; the maps that are dropped
// are the bulk of what an unstructured object costs, because every field of
// every object is a map entry with a boxed value.
func prune(obj map[string]any, tree *fieldTree) map[string]any {
	if tree == nil || tree.keepSubtree {
		return obj
	}

	pruned := make(map[string]any, len(tree.children))

	for name, child := range tree.children {
		if value, present := obj[name]; present {
			pruned[name] = pruneValue(value, child)
		}
	}

	return pruned
}

// pruneValue returns value with the fields tree does not retain removed.
func pruneValue(value any, tree *fieldTree) any {
	if tree.keepSubtree {
		return value
	}

	nested, ok := value.(map[string]any)
	if !ok {
		// A path continues past a value that is not an object, so keep the
		// value whole rather than guess at its shape.
		return value
	}

	return prune(nested, tree)
}
