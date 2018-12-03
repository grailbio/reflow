// Generated from utils_test.go.tpl. DO NOT EDIT.
package PACKAGE_test

import (
	"fmt"
        "math"

	"github.com/grailbio/testutil/PACKAGE"
)

type T struct{}

func (t *T) Error(args ...interface{}) {
	fmt.Println(args...)
}

func (t *T) Fatal(args ...interface{}) {
	fmt.Println(args...)
	panic("fatal")
}

func ExampleEQ() {
	t := &T{}
	PACKAGE.EQ(t, 42, 42)
	PACKAGE.EQ(t, uint(42), uint(42))
	PACKAGE.EQ(t, uint8(42), uint8(42))
	PACKAGE.EQ(t, uint16(42), uint16(42))
	PACKAGE.EQ(t, uint32(42), uint32(42))
	PACKAGE.EQ(t, uint64(42), uint64(42))

	PACKAGE.EQ(t, int8(42), int8(42))
	PACKAGE.EQ(t, int16(42), int16(42))
	PACKAGE.EQ(t, int32(42), int32(42))
	PACKAGE.EQ(t, int64(42), int64(42))
	PACKAGE.EQ(t, "42", "42")
	PACKAGE.EQ(t, 42.0, 42.0)

	PACKAGE.EQ(t, []int{42, 43}, []int{42, 43})
	PACKAGE.EQ(t, [...]int{42, 43}, [...]int{42, 43})
	PACKAGE.EQ(t, map[int]int{1: 2, 3: 4}, map[int]int{3: 4, 1: 2})
	// Output:
}

func ExampleNEQ() {
	t := &T{}
	PACKAGE.NEQ(t, 42, 43)
	PACKAGE.NEQ(t, "42", "43")
	PACKAGE.NEQ(t, 42.0, 43.0)
	PACKAGE.NEQ(t, []int{42, 43}, []int{43, 42})
	PACKAGE.NEQ(t, [...]int{42, 43}, [...]int{43, 42})
	nan := math.NaN()
	PACKAGE.NEQ(t, nan, nan)
	PACKAGE.NEQ(t, nan, 0.0)
	// Output:
}

func ExampleEQ_structs() {
	t := &T{}
	type tt struct {
		x int
		y string
	}
	PACKAGE.EQ(t, tt{10, "x"}, tt{10, "x"})
	PACKAGE.EQ(t, tt{x: 10, y: "x"}, tt{y: "x", x: 10})
}

func ExampleEQ_interfaceAndPointers() {
	t := &T{}
	type tt struct {
		x int
		y string
	}
	var xi, yi interface{}
	xi = tt{10, "x"}
	yi = tt{10, "x"}
	PACKAGE.EQ(t, xi, yi)

	xp := &tt{10, "x"}
	yp := &tt{10, "x"}
	PACKAGE.EQ(t, xp, yp)
	// Output:
}

func ExampleLE() {
	t := &T{}
	PACKAGE.LE(t, 42, 42)
	PACKAGE.LE(t, 42, 43)
	PACKAGE.LE(t, "42", "43")
	PACKAGE.LE(t, "123", "43")
	// Output:
}

func ExampleGE() {
	t := &T{}
	PACKAGE.GE(t, 42, 42)
	PACKAGE.GE(t, 43, 42)
	PACKAGE.GE(t, "43", "42")
	PACKAGE.GE(t, "43", "123")
	// Output:
}

func ExampleLT() {
	t := &T{}
	PACKAGE.LT(t, 42, 43)
	PACKAGE.LT(t, "42", "43")
	PACKAGE.LT(t, "123", "43")
	// Output:
}

func ExampleGT() {
	t := &T{}
	PACKAGE.GT(t, 43, 42)
	PACKAGE.GT(t, "43", "42")
	PACKAGE.GT(t, "43", "123")
	// Output:
}

func ExampleHasSubstr() {
	t := &T{}
	PACKAGE.HasSubstr(t, "12345", "234")
	PACKAGE.HasSubstr(t, 12345, "234")
	// Output:
}

func ExampleHasPrefix() {
	t := &T{}
	PACKAGE.HasPrefix(t, "12345", "123")
	PACKAGE.HasPrefix(t, 12345, "123")
	// Output:
}

func ExampleTrue() {
	t := &T{}
	PACKAGE.True(t, true)
	PACKAGE.True(t, 1 == 1)
	// Output:
}

func ExampleFalse() {
	t := &T{}
	PACKAGE.False(t, false)
	PACKAGE.False(t, 1 == 2)
	// Output:
}

func ExampleNil() {
	t := &T{}
	PACKAGE.Nil(t, nil)

	var m map[int]int
	PACKAGE.Nil(t, m)
	// Output:
}

func ExampleNoError() {
	t := &T{}
	PACKAGE.NoError(t, nil)
	var e error
	PACKAGE.NoError(t, e)
}

func ExampleNotNil() {
	t := &T{}
	PACKAGE.NotNil(t, false)
	PACKAGE.NotNil(t, 1)
	PACKAGE.NotNil(t, map[int]int{})
	// Output:
}
