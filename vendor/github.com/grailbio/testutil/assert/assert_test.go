package assert_test

import (
	"testing"

	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/h"
)

func TestAssert(t *testing.T) {
	assert.EQ(t, 42, 42)
	assert.LE(t, 42, 42)
	assert.LT(t, 41, 42)
	assert.GE(t, 42, 42)
	assert.GT(t, 42, 41)
	assert.NEQ(t, 42, 43)

	assert.EQ(t, "abc", "abc")
	assert.LE(t, "abc", "abc")
	assert.LT(t, "ab", "abc")
	assert.GE(t, "abc", "abc")
	assert.GT(t, "abc", "ab")
	assert.NEQ(t, "abc", "abcd")

	assert.Regexp(t, "abc", "a.*c")
	assert.Regexp(t, "xabcy", "a.*c")
	assert.That(t, "abc", h.Not(h.Regexp("a..c")))
	assert.That(t, "xabcy", h.Not(h.Regexp("^a.c")))

	assert.EQ(t, []int{1, 2}, []int{1, 2})
	assert.That(t, 42, h.EQ(42)) // verbose equivalent of assert.EQ(42, 42)
	assert.That(t, []int{42, 43}, h.Contains(42))
	assert.That(t, []int{42, 43}, h.Each(h.LT(44)))
	assert.That(t, []int{42, 43}, h.ElementsAre(42, 43))
	assert.That(t, []int{42, 43}, h.UnorderedElementsAre(43, 42))
	assert.That(t, []int{43, 42}, h.WhenSorted(h.ElementsAre(42, 43)))

	assert.EQ(t, map[int]int{1: 2, 3: 4}, map[int]int{3: 4, 1: 2})
	assert.NEQ(t, map[int]int{1: 2}, map[int]int{1: 3})
	assert.NEQ(t, map[int]int{1: 2}, map[int]int{2: 2})
	assert.NEQ(t, map[int]int{1: 2}, map[int]int{2: 2, 3: 2})
}

type tester struct {
	msg string
}

func (t *tester) Fatal(args ...interface{}) {
	if t.msg != "" || len(args) != 1 {
		panic(t)
	}
	t.msg = args[0].(string)
}

func TestAssertFailure(t *testing.T) {
	tt := tester{}
	assert.EQ(&tt, 1, 2, "test message")
	assert.Regexp(t, tt.msg, `Actual:.*1\nExpected:.*2\n.*test message$`)

	tt = tester{}
	assert.EQ(&tt, 1, 2, "test message %d", 10)
	assert.Regexp(t, tt.msg, `Actual:.*1\nExpected:.*2\n.*test message 10$`)
}
