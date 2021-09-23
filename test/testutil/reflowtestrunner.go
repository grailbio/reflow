package testutil

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// RunReflowTests executes .rf tests from disk
func RunReflowTests(t *testing.T, testFiles []string) {
	t.Helper()
	sess := syntax.NewSession(nil)
Prog:
	for _, prog := range testFiles {
		var err error
		m, err := sess.Open(prog)
		if err != nil {
			t.Errorf("%s: %v", prog, err)
			continue
		}
		tests := make(map[string]bool)
		for _, f := range m.Type(nil).Fields {
			if strings.HasPrefix(f.Name, "Test") {
				if _, ok := tests[f.Name]; ok {
					panic(fmt.Sprintf("Test name '%s' is used more than once, this can lead to test failures being masked. Ensure all test names in '%s' are unique.", f.Name, prog))
				}
				tests[f.Name] = true
				if f.T.Kind != types.BoolKind {
					t.Errorf("%s.%s: tests must be boolean, not %s", prog, f.Name, f.T)
					continue Prog
				}
			}
		}
		if len(tests) == 0 {
			t.Errorf("%s: no tests", prog)
			continue
		}

		v, err := m.Make(sess, sess.Values)
		if err != nil {
			t.Errorf("make %s: %s", prog, err)
			continue
		}
	tests:
		for test, _ := range tests {
			switch v := v.(values.Module)[test].(type) {
			case *flow.Flow:
				// We have to evaluate the flow (isn't expected to contain external flow nodes,
				// so we don't need an executor or scheduler).
				// We do provide an in-memory repository so that local interns work.
				eval := flow.NewEval(v, flow.EvalConfig{Repository: NewInmemoryRepository("")})
				if err := eval.Do(context.Background()); err != nil {
					t.Errorf("%s.%s: %v", prog, test, err)
					continue tests
				}
				if err := eval.Err(); err != nil {
					t.Errorf("%s.%s: evaluation error: %v", prog, test, err)
					continue tests
				}
				if !eval.Value().(bool) {
					t.Errorf("%s.%s failed", prog, test)
				}
			case bool:
				if !v {
					t.Errorf("%s.%s failed", prog, test)
				}
			}

		}
	}
}
