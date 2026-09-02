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
	"maps"
	"slices"
	"strings"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
)

// ResourceVar is the name of the CEL variable bound to the watched object.
const ResourceVar = "resource"

// ResourceFieldPaths returns the dot-separated field paths of the resource
// variable that compiled reads. `resource.status.conditions.exists(c, c.type ==
// "Ready")` yields ["status.conditions"].
//
// A returned path stands for the entire subtree beneath it. That is what makes
// comprehensions safe to derive fields from: recording status.conditions covers
// every field of every element, so the per-element bindings need no handling of
// their own. The same holds for a computed index such as
// metadata.labels[resource.spec.nodeName], which records metadata.labels and
// therefore retains whichever key is present at runtime.
//
// The second return value reports whether extraction was complete. It is false
// when the expression uses the object as a whole rather than through a field
// access, as in size(resource), because no set of paths describes what such an
// expression reads. Callers must cache the object in full in that case: pruning
// against an incomplete field set silently changes evaluation results.
func ResourceFieldPaths(compiled *cel.Ast) ([]string, bool) {
	if compiled == nil || compiled.NativeRep() == nil {
		return nil, false
	}

	w := &fieldWalker{
		paths: make(map[string]struct{}),
		ok:    true,
	}
	w.walk(compiled.NativeRep().Expr())

	if !w.ok {
		return nil, false
	}

	return slices.Sorted(maps.Keys(w.paths)), true
}

// fieldWalker collects resource field paths from an expression graph. shadowed
// counts the enclosing comprehension bindings named after the resource
// variable, so an iteration or accumulator variable that shadows it is not
// mistaken for the object itself.
type fieldWalker struct {
	paths    map[string]struct{}
	shadowed int
	ok       bool
}

func (w *fieldWalker) walk(e ast.Expr) {
	if e == nil || !w.ok {
		return
	}

	if w.recordChain(e) {
		return
	}

	w.walkChildren(e)
}

// recordChain records e if it is a chain of field accesses rooted at the
// resource variable, reporting whether it was one.
func (w *fieldWalker) recordChain(e ast.Expr) bool {
	path, rooted, _ := w.resolve(e)
	if !rooted {
		return false
	}

	if len(path) == 0 {
		w.ok = false
		return true
	}

	w.paths[strings.Join(path, ".")] = struct{}{}
	w.walkIndexKeys(e)

	return true
}

func (w *fieldWalker) walkChildren(e ast.Expr) {
	switch e.Kind() {
	case ast.SelectKind:
		w.walk(e.AsSelect().Operand())
	case ast.CallKind:
		w.walkCall(e.AsCall())
	case ast.ListKind:
		for _, element := range e.AsList().Elements() {
			w.walk(element)
		}
	case ast.MapKind:
		w.walkMapEntries(e.AsMap().Entries())
	case ast.StructKind:
		w.walkStructFields(e.AsStruct().Fields())
	case ast.ComprehensionKind:
		w.walkComprehension(e.AsComprehension())
	case ast.IdentKind, ast.LiteralKind, ast.UnspecifiedExprKind:
	}
}

func (w *fieldWalker) walkCall(call ast.CallExpr) {
	if call.IsMemberFunction() {
		w.walk(call.Target())
	}

	for _, arg := range call.Args() {
		w.walk(arg)
	}
}

func (w *fieldWalker) walkMapEntries(entries []ast.EntryExpr) {
	for _, entry := range entries {
		mapEntry := entry.AsMapEntry()
		w.walk(mapEntry.Key())
		w.walk(mapEntry.Value())
	}
}

func (w *fieldWalker) walkStructFields(fields []ast.EntryExpr) {
	for _, field := range fields {
		w.walk(field.AsStructField().Value())
	}
}

// walkComprehension walks a comprehension with its bindings scoped. The
// iteration range and the accumulator initialiser are evaluated in the
// enclosing scope; the loop body sees the iteration and accumulator variables,
// and the result expression sees only the accumulator.
func (w *fieldWalker) walkComprehension(c ast.ComprehensionExpr) {
	w.walk(c.IterRange())
	w.walk(c.AccuInit())

	w.push(c.AccuVar())
	w.push(c.IterVar())

	if c.HasIterVar2() {
		w.push(c.IterVar2())
	}

	w.walk(c.LoopCondition())
	w.walk(c.LoopStep())

	if c.HasIterVar2() {
		w.pop(c.IterVar2())
	}

	w.pop(c.IterVar())

	w.walk(c.Result())

	w.pop(c.AccuVar())
}

// walkIndexKeys walks the key expressions of the index operations inside an
// already recorded resource-rooted chain. The chain covers the values, but a
// computed key such as metadata.labels[resource.spec.nodeName] reads the
// resource in its own right.
func (w *fieldWalker) walkIndexKeys(e ast.Expr) {
	for w.ok {
		switch e.Kind() {
		case ast.SelectKind:
			e = e.AsSelect().Operand()
		case ast.CallKind:
			call := e.AsCall()
			if !isIndex(call) {
				return
			}

			w.walk(call.Args()[1])

			e = call.Args()[0]
		case ast.ComprehensionKind, ast.IdentKind, ast.ListKind,
			ast.LiteralKind, ast.MapKind, ast.StructKind, ast.UnspecifiedExprKind:
			return
		}
	}
}

// resolve interprets e as a chain of field accesses rooted at the resource
// variable. rooted reports whether the chain is anchored at the resource, and
// path is the field path it reaches. An inexact path is one truncated by a
// computed index: the subtree at path is retained whole, so accesses beneath it
// are already covered and need not extend the path.
func (w *fieldWalker) resolve(e ast.Expr) (path []string, rooted, exact bool) {
	switch e.Kind() {
	case ast.IdentKind:
		return nil, e.AsIdent() == ResourceVar && w.shadowed == 0, true
	case ast.SelectKind:
		return w.resolveSelect(e.AsSelect())
	case ast.CallKind:
		return w.resolveIndex(e.AsCall())
	case ast.ComprehensionKind, ast.ListKind, ast.LiteralKind,
		ast.MapKind, ast.StructKind, ast.UnspecifiedExprKind:
		return nil, false, false
	}

	return nil, false, false
}

func (w *fieldWalker) resolveSelect(selectExpr ast.SelectExpr) (path []string, rooted, exact bool) {
	path, rooted, exact = w.resolve(selectExpr.Operand())
	if !rooted || !exact {
		return path, rooted, exact
	}

	return append(path, selectExpr.FieldName()), true, true
}

func (w *fieldWalker) resolveIndex(call ast.CallExpr) (path []string, rooted, exact bool) {
	if !isIndex(call) {
		return nil, false, false
	}

	path, rooted, exact = w.resolve(call.Args()[0])
	if !rooted || !exact {
		return path, rooted, exact
	}

	if key, ok := stringLiteral(call.Args()[1]); ok {
		return append(path, key), true, true
	}

	// A computed key can select any entry, so the whole subtree is retained.
	return path, true, false
}

func (w *fieldWalker) push(name string) {
	if name == ResourceVar {
		w.shadowed++
	}
}

func (w *fieldWalker) pop(name string) {
	if name == ResourceVar {
		w.shadowed--
	}
}

func isIndex(call ast.CallExpr) bool {
	return call.FunctionName() == operators.Index && !call.IsMemberFunction() && len(call.Args()) == 2
}

func stringLiteral(e ast.Expr) (string, bool) {
	if e.Kind() != ast.LiteralKind {
		return "", false
	}

	value, ok := e.AsLiteral().(types.String)
	if !ok {
		return "", false
	}

	return string(value), true
}
