package testutil

import (
	"io/ioutil"
	"strings"
	"testing"
)

func TestRunReflowTests(t *testing.T) {
	rf, err := ioutil.TempFile("", "reflowtestrunner_*.rf")
	if err != nil {
		t.Fatal(err)
	}
	duplicateTestNames := `val TestFoo = false
val TestFoo = true`
	_, err = rf.WriteString(duplicateTestNames)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("RunReflowTests should have panicked due to reuse of TestFoo")
		}
		errstr := r.(string)
		if !strings.Contains(errstr, "Test name 'TestFoo' is used more than once") {
			t.Errorf("RunReflowTests panicked, but not because of the reused test name: %s", errstr)
		}
	}()
	RunReflowTests(t, []string{rf.Name()})
}
