package cel

import (
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
)

func compile(t *testing.T, expr string) *cel.Ast {
	t.Helper()

	env, err := cel.NewEnv(cel.Variable("resource", cel.DynType), cel.Variable("now", cel.TimestampType))
	if err != nil {
		t.Fatal(err)
	}

	a, iss := env.Compile(expr)
	if iss != nil && iss.Err() != nil {
		t.Fatalf("compile %q: %v", expr, iss.Err())
	}

	return a
}

func join(paths [][]string) string {
	out := make([]string, 0, len(paths))
	for _, p := range paths {
		out = append(out, strings.Join(p, "."))
	}

	return strings.Join(out, ",")
}

func TestReferencedPaths(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want string
		ok   bool
	}{
		// the two policies this cluster actually runs
		{`has(resource.status.conditions) && resource.status.conditions.exists(c, c.type == "Ready" && c.status == "False")`,
			"status.conditions", true},
		{`resource.status.phase == "Failed"`, "status.phase", true},
		{`resource.spec.nodeName`, "spec.nodeName", true},

		{`resource.metadata.labels["foo"] == "bar"`, "metadata.labels.foo", true},
		{`resource.status.conditions.exists(c, c.type == resource.spec.x)`, "status.conditions,spec.x", true},
		// a shorter path must absorb the longer one
		{`resource.status.phase == "x" && resource.status == {}`, "status", true},

		// A computed key is still safe: the whole parent subtree is kept, so
		// any key the expression selects at runtime is present.
		{`resource.metadata.labels[resource.spec.key] == "x"`, "metadata.labels,spec.key", true},

		// Unresolvable: the object is used whole rather than through a field.
		{`resource == null`, "", false},
		{`size(resource) > 0`, "", false},
	} {
		got, ok := ReferencedPaths(compile(t, tc.expr))
		if ok != tc.ok {
			t.Errorf("%s: ok=%v want %v", tc.expr, ok, tc.ok)
			continue
		}

		if ok && join(got) != tc.want {
			t.Errorf("%s:\n  got  %s\n  want %s", tc.expr, join(got), tc.want)
		}
	}
}
