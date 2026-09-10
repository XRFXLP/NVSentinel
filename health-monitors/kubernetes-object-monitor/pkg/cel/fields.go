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

package cel

import (
	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
)

// resourceVar is the CEL variable bound to the watched object.
const resourceVar = "resource"

// ReferencedPaths reports which fields of `resource` an expression reads, as
// dotted paths relative to the object root, for example "status.conditions".
//
// The second return value is false when the expression touches `resource` in a
// way that cannot be resolved statically -- a computed index, or the whole
// object passed somewhere opaque. Callers must treat that as "keep everything",
// because pruning on an incomplete path set would silently change evaluation
// results.
//
// A recorded path means the entire subtree beneath it is required. That is what
// makes comprehensions safe: `resource.status.conditions.exists(c, c.type == x)`
// records "status.conditions" and the per-element accesses through the
// iteration variable need no separate handling.
func ReferencedPaths(ast *cel.Ast) ([][]string, bool) {
	c := &pathCollector{scopes: map[string]int{}, complete: true}
	c.walk(ast.NativeRep().Expr())

	if !c.complete {
		return nil, false
	}

	return c.paths, true
}

type pathCollector struct {
	paths [][]string
	// scopes counts shadowing bindings by name, so a comprehension iteration
	// variable named "resource" does not get mistaken for the object itself.
	scopes   map[string]int
	complete bool
}

func (c *pathCollector) record(path []string) {
	if len(path) == 0 {
		// A bare reference to `resource` needs the whole object.
		c.complete = false
		return
	}

	for _, existing := range c.paths {
		if isPrefix(existing, path) {
			return
		}
	}

	kept := c.paths[:0]

	for _, existing := range c.paths {
		if !isPrefix(path, existing) {
			kept = append(kept, existing)
		}
	}

	c.paths = append(kept, path)
}

// isPrefix reports whether a is a prefix of b, i.e. keeping a already keeps b.
func isPrefix(a, b []string) bool {
	if len(a) > len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// resolve walks a select/index chain down to its root identifier and returns
// the field path. ok is false when the chain is not rooted at a plain
// identifier, or uses a non-literal key.
func (c *pathCollector) resolve(e celast.Expr) (root string, path []string, ok bool) {
	switch e.Kind() {
	case celast.IdentKind:
		return e.AsIdent(), nil, true

	case celast.SelectKind:
		sel := e.AsSelect()

		root, path, ok = c.resolve(sel.Operand())
		if !ok {
			return "", nil, false
		}

		return root, append(path, sel.FieldName()), true

	case celast.CallKind:
		call := e.AsCall()
		// a["b"] parses as the _[_] operator rather than a select.
		if call.FunctionName() == operators.Index && len(call.Args()) == 2 {
			root, path, ok = c.resolve(call.Args()[0])
			if !ok {
				return "", nil, false
			}

			key := call.Args()[1]
			if key.Kind() != celast.LiteralKind {
				return "", nil, false
			}

			s, isStr := key.AsLiteral().Value().(string)
			if !isStr {
				return "", nil, false
			}

			return root, append(path, s), true
		}
	}

	return "", nil, false
}

// walk visits every node, recording resource paths and flagging anything that
// reaches `resource` without a resolvable path.
func (c *pathCollector) walk(e celast.Expr) {
	if e == nil {
		return
	}

	switch e.Kind() {
	case celast.IdentKind:
		if e.AsIdent() == resourceVar && c.scopes[resourceVar] == 0 {
			// `resource` used whole, not through a field access.
			c.complete = false
		}

	case celast.SelectKind, celast.CallKind:
		if root, path, ok := c.resolve(e); ok {
			if root == resourceVar && c.scopes[resourceVar] == 0 {
				c.record(path)
				return
			}
			// Rooted at some other identifier: nothing to record, and the
			// operand chain contains no further resource references.
			return
		}

		if e.Kind() == celast.CallKind {
			call := e.AsCall()
			c.walk(call.Target())

			for _, a := range call.Args() {
				c.walk(a)
			}
		} else {
			c.walk(e.AsSelect().Operand())
		}

	case celast.ComprehensionKind:
		comp := e.AsComprehension()
		c.walk(comp.IterRange())

		c.scopes[comp.IterVar()]++
		c.scopes[comp.AccuVar()]++

		c.walk(comp.AccuInit())
		c.walk(comp.LoopCondition())
		c.walk(comp.LoopStep())
		c.walk(comp.Result())

		c.scopes[comp.IterVar()]--
		c.scopes[comp.AccuVar()]--

	case celast.ListKind:
		for _, el := range e.AsList().Elements() {
			c.walk(el)
		}

	case celast.MapKind:
		for _, en := range e.AsMap().Entries() {
			entry := en.AsMapEntry()
			c.walk(entry.Key())
			c.walk(entry.Value())
		}

	case celast.StructKind:
		for _, f := range e.AsStruct().Fields() {
			c.walk(f.AsStructField().Value())
		}
	}
}
