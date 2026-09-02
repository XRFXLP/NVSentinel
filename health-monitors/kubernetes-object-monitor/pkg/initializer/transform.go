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

// gvkCache is how one GVK is cached, derived from the CEL of the enabled
// policies.
type gvkCache struct {
	// transform prunes the cache entry to the fields the policies read, and is
	// nil when the objects must be cached in full.
	transform toolscache.TransformFunc
	// servesLookups reports whether a lookup() returning this GVK may read the
	// entry rather than the API. It is false unless the entry retains
	// everything the policies read off a looked-up object of this GVK, because
	// a read that silently misses pruned fields changes how a policy evaluates.
	servesLookups bool
}

// buildCacheEntries derives how to cache each GVK the enabled policies reach,
// whether by watching it or by naming it in a lookup(). A GVK a lookup() names
// but no policy watches is included, so that the lookup can be served from an
// entry pruned to what it reads instead of from the API.
func buildCacheEntries(
	compiler *celenv.Environment,
	policies []config.Policy,
) map[schema.GroupVersionKind]gvkCache {
	reads := collectPolicyReads(compiler, policies)
	entries := make(map[schema.GroupVersionKind]gvkCache, len(reads))

	for gvk, read := range reads {
		if !read.watched && read.wholeLookup != "" {
			// Nothing to cache: no policy watches this GVK, and the lookup that
			// names it reads the returned object as a whole.
			slog.Info("Reading lookup() through the API: it uses the whole object",
				"gvk", gvk.String(), "policy", read.wholeLookup)

			continue
		}

		entries[gvk] = read.gvkCache(gvk)
	}

	return entries
}

// gvkReads is what the enabled policies read off one GVK, in each of the two
// roles it can play. wholeWatch and wholeLookup name a policy that reads the
// object as a whole in that role, which no set of paths describes.
type gvkReads struct {
	watched     bool
	watchPaths  [][]string
	wholeWatch  string
	lookedUp    bool
	lookupPaths [][]string
	wholeLookup string
}

// policyReads is what the policies read, per GVK.
type policyReads map[schema.GroupVersionKind]*gvkReads

func (r policyReads) at(gvk schema.GroupVersionKind) *gvkReads {
	reads := r[gvk]
	if reads == nil {
		reads = &gvkReads{}
		r[gvk] = reads
	}

	return reads
}

// collectPolicyReads compiles every expression of every enabled policy and
// records what it reads, off the object the policy watches and off the objects
// its lookup() calls return.
//
// A compile failure counts as reading the watched object whole rather than
// being reported: policy.NewEvaluator compiles the same expression with the
// policy name for context and fails startup there.
func collectPolicyReads(compiler *celenv.Environment, policies []config.Policy) policyReads {
	reads := make(policyReads)

	for _, policy := range policies {
		if !policy.Enabled {
			continue
		}

		watched := reads.at(policyGVK(policy))
		watched.watched = true

		for _, expression := range policyExpressions(policy) {
			compiled, err := compiler.Compile(expression)
			if err != nil {
				watched.wholeWatch = policy.Name

				continue
			}

			if paths, ok := celenv.ResourceFieldPaths(compiled); ok {
				watched.watchPaths = append(watched.watchPaths, paths...)
			} else {
				watched.wholeWatch = policy.Name
			}

			reads.addLookups(policy.Name, celenv.LookupTargets(compiled))
		}
	}

	return reads
}

func (r policyReads) addLookups(policyName string, targets []celenv.LookupTarget) {
	for _, target := range targets {
		gvk := schema.FromAPIVersionAndKind(target.APIVersion, target.Kind)

		lookedUp := r.at(gvk)
		lookedUp.lookedUp = true

		if !target.Derivable {
			lookedUp.wholeLookup = policyName

			continue
		}

		lookedUp.lookupPaths = append(lookedUp.lookupPaths, target.Paths...)
	}
}

// gvkCache decides how the GVK is cached from what the policies read off it.
func (r *gvkReads) gvkCache(gvk schema.GroupVersionKind) gvkCache {
	if r.wholeWatch != "" {
		slog.Info("Caching full objects: policy fields could not be derived from CEL",
			"gvk", gvk.String(), "policy", r.wholeWatch, "servesLookups", r.lookedUp)

		// Every field is there, so a lookup finds whatever it reads.
		return gvkCache{servesLookups: r.lookedUp}
	}

	servesLookups := r.lookedUp && r.wholeLookup == ""

	tree := newFieldTree(informerFieldPaths)

	for _, path := range r.watchPaths {
		tree.insert(path)
	}

	if servesLookups {
		for _, path := range r.lookupPaths {
			tree.insert(path)
		}
	}

	if r.wholeLookup != "" {
		slog.Info("Reading lookup() through the API: it uses the whole object",
			"gvk", gvk.String(), "policy", r.wholeLookup)
	}

	slog.Info("Cache transform derived from policy CEL",
		"gvk", gvk.String(),
		"retainedFields", strings.Join(tree.describe(), " "),
		"servesLookups", servesLookups)

	return gvkCache{transform: newFieldPruningTransform(tree), servesLookups: servesLookups}
}

func policyExpressions(p config.Policy) []string {
	if p.NodeAssociation == nil {
		return []string{p.Predicate.Expression}
	}

	return []string{p.Predicate.Expression, p.NodeAssociation.Expression}
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
