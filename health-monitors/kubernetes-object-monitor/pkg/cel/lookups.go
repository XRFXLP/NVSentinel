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

package cel

import (
	"github.com/google/cel-go/cel"
)

// lookupArgs is the arity of lookup(apiVersion, kind, namespace, name).
const lookupArgs = 4

// LookupTarget is a GVK an expression reads through lookup(), with the field
// paths it reads off the returned object.
type LookupTarget struct {
	APIVersion string
	Kind       string
	// Paths carry the meaning they do in ResourceFieldPaths: each is a slice of
	// segments standing for the whole subtree beneath it. They are nil when
	// Derivable is false.
	Paths [][]string
	// Derivable is false when the expression uses a returned object as a whole,
	// as in size(lookup(...)), because no set of paths describes what such an
	// expression reads. The GVK must then be read through the API, since a
	// pruned cache entry would silently answer with fields it dropped.
	Derivable bool
}

// LookupTargets returns the GVKs compiled reads through lookup(), one entry per
// GVK however many calls name it, for the calls that give their apiVersion and
// kind as string literals. A call that computes either is absent: nothing can
// be cached for a GVK that is not known until the expression runs.
//
// Extraction stops at an expression that uses the watched object as a whole, so
// a lookup() beyond that point goes unreported. What makes that safe is the
// contract callers owe an absent GVK, which is the same one they owe a computed
// GVK: read it through the API.
func LookupTargets(compiled *cel.Ast) []LookupTarget {
	if compiled == nil || compiled.NativeRep() == nil {
		return nil
	}

	return mergeLookups(walkExpression(compiled).lookups)
}

// lookupRead is one path an expression reads off the object a lookup() call
// returns. An empty path means it read the object as a whole.
type lookupRead struct {
	apiVersion string
	kind       string
	path       []string
}

// mergeLookups gathers the reads of each GVK into a single target, in the order
// the GVKs were first read.
func mergeLookups(reads []lookupRead) []LookupTarget {
	if len(reads) == 0 {
		return nil
	}

	type gvk struct {
		apiVersion string
		kind       string
	}

	order := make([]gvk, 0, len(reads))
	targets := make(map[gvk]*LookupTarget, len(reads))

	for _, read := range reads {
		key := gvk{apiVersion: read.apiVersion, kind: read.kind}

		target := targets[key]
		if target == nil {
			target = &LookupTarget{APIVersion: read.apiVersion, Kind: read.kind, Derivable: true}
			targets[key] = target
			order = append(order, key)
		}

		if len(read.path) == 0 {
			target.Derivable = false
			continue
		}

		target.Paths = append(target.Paths, read.path)
	}

	merged := make([]LookupTarget, 0, len(order))

	for _, key := range order {
		target := *targets[key]

		if target.Derivable {
			target.Paths = sortPaths(target.Paths)
		} else {
			target.Paths = nil
		}

		merged = append(merged, target)
	}

	return merged
}
