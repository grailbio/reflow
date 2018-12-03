// Package expect provides helper functions for unittests, in a style of
// hamcrest, gtest and gmock. It is a thin wrapper around the "h" package (
// https://godoc.org/github.com/grailbio/testutil/h).
//
// Features:
//
// - Succinctly check if a value is ==, <, <=, another value.
//
// - Hamcrest-style assertions.
//
// Example:
//   package foo_test
//
//   import (
//   	"testing"
//
//   	"github.com/grailbio/testutil/assert"
//   	"github.com/grailbio/testutil/h"
//   	"github.com/grailbio/foo"
//   )
//
//   func TestFoo(t* testing.T) {
//   	 expect.EQ(t, foo.DoFoo(), 10)
//   	 expect.LT(t, foo.DoBar(), 15)
//   	 expect.EQ(t, foo.DoBar(), []int{1, 2})
//   }
//
// The only difference between packages expect and assert is that expect.XXX
// will call testing.T.Error on error, whereas assert.XXX will call
// testing.T.Fatal on error. So you should use expect if the test code should
// continue after a failure, but assert if the testcase should abort immediately
// on failure. They can be mixed within one test.
//
// Matchers:
//
// Most of the actual matching features are implemented in the
// github.com/grailbio/testutil/h package. Package expect is just a thin wrapper
// around the matchers defined in h.
//
// expect.That supports fine-grain expectation matching, in the style of
// gmock(https://github.com/google/googletest/blob/master/googlemock/docs/CheatSheet.md)
// and hamcrest(https://en.wikipedia.org/wiki/Hamcrest).  For example:
//
//   	 // Check that every element in the slice is < 10.
//   	 expect.That(t, []int{15, 16}, h.Each(h.LT(10)))
//
// Package testutil/h defines many common matchers.
//
// - h.LT, h.LE, h.GT, h.GE for arithmetic comparisons.
//
// - h.EQ and h.NEQ to compare equality of two values.  They can be used not
//   just for scalar values, but for structs, slices, and maps too.
//
// - h.Contains checks if a slice or an array contains a given value or matcher.
//
//   	 expect.That(t, []int{15, 16}, h.Contains(15))
//   	 expect.That(t, []int{15, 16}, h.Contains(h.LE(15)))
//
// - h.Each checks if every value of a slice or an array matches a given value or matcher.
//
// - h.Not(m) negates the match result of m.
//
//   	 // Check that every element in the slice doesn't start with "b"
//   	 expect.That(t, []int{"abc", "abd"}, h.Each(h.Not(h.HasPrefix("b"))))
//
//
// - h.Regexp, h.HasPrefix, h.HasSubstr, h.HasSuffix checks properties of a string.
package expect

// Generated from utils.go.tpl. DO NOT EDIT.

import (
	"github.com/grailbio/testutil/h"
)

// EQ checks if the two values are equal. It is a shorthand for That(got,
// h.EQ(want), ...).
//
// If msgs... is not empty, msgs[0] must be a format string, and they are
// printed using fmt.Printf on error.
func EQ(t TB, got, want interface{}, msgs ...interface{}) {
	That(t, got, h.EQ(want), msgs...)
}

// NEQ checks if want != got. If msgs... is not empty, msgs[0] must
// be a format string, and they are printed using fmt.Printf on error.
func NEQ(t TB, got, want interface{}, msgs ...interface{}) {
	That(t, got, h.NEQ(want), msgs...)
}

// LE checks if x <= y. x and y must be of the same type and operator '<=' must
// be defined for the type in Go. If msgs... is not empty, msgs[0] must be a
// format string, and they are printed using fmt.Printf on error.
func LE(t TB, x, y interface{}, msgs ...interface{}) {
	That(t, x, h.LE(y), msgs...)
}

// LT checks if x < y. x and y must be of the same type and operator '<' must be
// defined for the type in Go. If msgs... is not empty, msgs[0] must be a format
// string, and they are printed using fmt.Printf on error.
func LT(t TB, x, y interface{}, msgs ...interface{}) {
	That(t, x, h.LT(y), msgs...)
}

// GE checks if x >= y. x and y must be of the same type and operator '>=' must
// be defined for the type in Go. If msgs... is not empty, msgs[0] must be a
// format string, and they are printed using fmt.Printf on error.
func GE(t TB, x, y interface{}, msgs ...interface{}) {
	That(t, x, h.GE(y), msgs...)
}

// GT checks if x > y. x and y must be of the same type and operator '>' must be
// defined for the type in Go. If msgs... is not empty, msgs[0] must be a format
// string, and they are printed using fmt.Printf on error.
func GT(t TB, x, y interface{}, msgs ...interface{}) {
	That(t, x, h.GT(y), msgs...)
}

// Nil checks if the value is nil. If msgs... is not empty, msgs[0] must
// be a format string, and they are printed using fmt.Printf on error.
func Nil(t TB, x interface{}, msgs ...interface{}) {
	That(t, x, h.Nil(), msgs...)
}

// NotNil checks if the value is not nil. If msgs... is not empty, msgs[0] must
// be a format string, and they are printed using fmt.Printf on error.
func NotNil(t TB, x interface{}, msgs ...interface{}) {
	That(t, x, h.Not(h.Nil()), msgs...)
}

// NoError is an alias of Nil. If msgs... is not empty, msgs[0] must
// be a format string, and they are printed using fmt.Printf on error.
func NoError(t TB, got error, msgs ...interface{}) {
	That(t, got, h.NoError(), msgs...)
}

// Regexp checks if the value "got" matches a regexp. "re" can be either a
// string or an object of type *regexp.Regexp. If "got" is not a string, it is
// converted to string using fmt.Sprintf("%v"). The value is matched using
// regexp.Find. If msgs... is not empty, msgs[0] must be a format string, and
// they are printed using fmt.Printf on error.
func Regexp(t TB, got interface{}, re interface{}, msgs ...interface{}) {
	That(t, got, h.Regexp(re), msgs...)
}

// HasSubstr checks if the value "got" contains "sub". If "got" is not a string,
// it is converted to string using fmt.Sprintf("%v"). If msgs... is not empty,
// msgs[0] must be a format string, and they are printed using fmt.Printf on
// error.
func HasSubstr(t TB, got interface{}, sub string, msgs ...interface{}) {
	That(t, got, h.HasSubstr(sub), msgs...)
}

// HasPrefix checks if the value "got" starts with "prefix". If "got" is not a
// string, it is converted to string using fmt.Sprintf("%v"). If msgs... is not
// empty, msgs[0] must be a format string, and they are printed using fmt.Printf
// on error.
func HasPrefix(t TB, got interface{}, prefix string, msgs ...interface{}) {
	That(t, got, h.HasPrefix(prefix), msgs...)
}

// True checks if got==true. If msgs... is not empty, msgs[0] must
// be a format string, and they are printed using fmt.Printf on error.
func True(t TB, got bool, msgs ...interface{}) {
	EQ(t, got, true, msgs...)
}

// False checks if got==false. If msgs... is not empty, msgs[0] must be a format
// string, and they are printed using fmt.Printf on error.
func False(t TB, got bool, msgs ...interface{}) {
	EQ(t, got, false, msgs...)
}
