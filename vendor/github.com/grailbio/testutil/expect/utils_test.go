package expect_test

import (
	"fmt"
	"math"

	"github.com/grailbio/testutil/expect"
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
	expect.EQ(t, 42, 42)
	expect.EQ(t, uint(42), uint(42))
	expect.EQ(t, uint8(42), uint8(42))
	expect.EQ(t, uint16(42), uint16(42))
	expect.EQ(t, uint32(42), uint32(42))
	expect.EQ(t, uint64(42), uint64(42))

	expect.EQ(t, int8(42), int8(42))
	expect.EQ(t, int16(42), int16(42))
	expect.EQ(t, int32(42), int32(42))
	expect.EQ(t, int64(42), int64(42))
	expect.EQ(t, "42", "42")
	expect.EQ(t, 42.0, 42.0)

	expect.EQ(t, []int{42, 43}, []int{42, 43})
	expect.EQ(t, [...]int{42, 43}, [...]int{42, 43})
	expect.EQ(t, map[int]int{1: 2, 3: 4}, map[int]int{3: 4, 1: 2})
	// Output:
}

func ExampleNEQ() {
	t := &T{}
	expect.NEQ(t, 42, 43)
	expect.NEQ(t, "42", "43")
	expect.NEQ(t, 42.0, 43.0)
	expect.NEQ(t, []int{42, 43}, []int{43, 42})
	expect.NEQ(t, [...]int{42, 43}, [...]int{43, 42})
	nan := math.NaN()
	expect.NEQ(t, nan, nan)
	expect.NEQ(t, nan, 0.0)
	// Output:
}

func ExampleEQ_structs() {
	t := &T{}
	type tt struct {
		x int
		y string
	}
	expect.EQ(t, tt{10, "x"}, tt{10, "x"})
	expect.EQ(t, tt{x: 10, y: "x"}, tt{y: "x", x: 10})
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
	expect.EQ(t, xi, yi)

	xp := &tt{10, "x"}
	yp := &tt{10, "x"}
	expect.EQ(t, xp, yp)
	// Output:
}

func ExampleLE() {
	t := &T{}
	expect.LE(t, 42, 42)
	expect.LE(t, 42, 43)
	expect.LE(t, "42", "43")
	expect.LE(t, "123", "43")
	// Output:
}

func ExampleGE() {
	t := &T{}
	expect.GE(t, 42, 42)
	expect.GE(t, 43, 42)
	expect.GE(t, "43", "42")
	expect.GE(t, "43", "123")
	// Output:
}

func ExampleLT() {
	t := &T{}
	expect.LT(t, 42, 43)
	expect.LT(t, "42", "43")
	expect.LT(t, "123", "43")
	// Output:
}

func ExampleGT() {
	t := &T{}
	expect.GT(t, 43, 42)
	expect.GT(t, "43", "42")
	expect.GT(t, "43", "123")
	// Output:
}

func ExampleHasSubstr() {
	t := &T{}
	expect.HasSubstr(t, "12345", "234")
	expect.HasSubstr(t, 12345, "234")
	// Output:
}

func ExampleHasPrefix() {
	t := &T{}
	expect.HasPrefix(t, "12345", "123")
	expect.HasPrefix(t, 12345, "123")
	// Output:
}

func ExampleTrue() {
	t := &T{}
	expect.True(t, true)
	expect.True(t, 1 == 1)
	// Output:
}

func ExampleFalse() {
	t := &T{}
	expect.False(t, false)
	expect.False(t, 1 == 2)
	// Output:
}

func ExampleNil() {
	t := &T{}
	expect.Nil(t, nil)

	var m map[int]int
	expect.Nil(t, m)
	// Output:
}

func ExampleNoError() {
	t := &T{}
	expect.NoError(t, nil)
	var e error
	expect.NoError(t, e)
}

func ExampleNotNil() {
	t := &T{}
	expect.NotNil(t, false)
	expect.NotNil(t, 1)
	expect.NotNil(t, map[int]int{})
	// Output:
}
