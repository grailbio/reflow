package expect_test

import (
	"testing"

	"github.com/grailbio/testutil/expect"
)

type tester struct {
	msg string
}

func (t *tester) Error(args ...interface{}) {
	if t.msg != "" || len(args) != 1 {
		panic(t)
	}
	t.msg = args[0].(string)
}

func TestExpectFailure(t *testing.T) {
	tt := tester{}
	expect.EQ(&tt, 1, 2, "test message")
	expect.Regexp(t, tt.msg, `Actual:.*1\nExpected:.*2\n.*test message$`)

	tt = tester{}
	expect.EQ(&tt, 1, 2, "test message %d", 10)
	expect.Regexp(t, tt.msg, `Actual:.*1\nExpected:.*2\n.*test message 10$`)
}
