package assert_test

import (
	"fmt"
	"math"

	"github.com/grailbio/testutil/assert"
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
	assert.EQ(t, 42, 42)
	assert.EQ(t, uint(42), uint(42))
	assert.EQ(t, uint8(42), uint8(42))
	assert.EQ(t, uint16(42), uint16(42))
	assert.EQ(t, uint32(42), uint32(42))
	assert.EQ(t, uint64(42), uint64(42))

	assert.EQ(t, int8(42), int8(42))
	assert.EQ(t, int16(42), int16(42))
	assert.EQ(t, int32(42), int32(42))
	assert.EQ(t, int64(42), int64(42))
	assert.EQ(t, "42", "42")
	assert.EQ(t, 42.0, 42.0)

	assert.EQ(t, []int{42, 43}, []int{42, 43})
	assert.EQ(t, [...]int{42, 43}, [...]int{42, 43})
	assert.EQ(t, map[int]int{1: 2, 3: 4}, map[int]int{3: 4, 1: 2})
	// Output:
}

func ExampleNEQ() {
	t := &T{}
	assert.NEQ(t, 42, 43)
	assert.NEQ(t, "42", "43")
	assert.NEQ(t, 42.0, 43.0)
	assert.NEQ(t, []int{42, 43}, []int{43, 42})
	assert.NEQ(t, [...]int{42, 43}, [...]int{43, 42})
	nan := math.NaN()
	assert.NEQ(t, nan, nan)
	assert.NEQ(t, nan, 0.0)
	// Output:
}

func ExampleEQ_structs() {
	t := &T{}
	type tt struct {
		x int
		y string
	}
	assert.EQ(t, tt{10, "x"}, tt{10, "x"})
	assert.EQ(t, tt{x: 10, y: "x"}, tt{y: "x", x: 10})
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
	assert.EQ(t, xi, yi)

	xp := &tt{10, "x"}
	yp := &tt{10, "x"}
	assert.EQ(t, xp, yp)
	// Output:
}

func ExampleLE() {
	t := &T{}
	assert.LE(t, 42, 42)
	assert.LE(t, 42, 43)
	assert.LE(t, "42", "43")
	assert.LE(t, "123", "43")
	// Output:
}

func ExampleGE() {
	t := &T{}
	assert.GE(t, 42, 42)
	assert.GE(t, 43, 42)
	assert.GE(t, "43", "42")
	assert.GE(t, "43", "123")
	// Output:
}

func ExampleLT() {
	t := &T{}
	assert.LT(t, 42, 43)
	assert.LT(t, "42", "43")
	assert.LT(t, "123", "43")
	// Output:
}

func ExampleGT() {
	t := &T{}
	assert.GT(t, 43, 42)
	assert.GT(t, "43", "42")
	assert.GT(t, "43", "123")
	// Output:
}

func ExampleHasSubstr() {
	t := &T{}
	assert.HasSubstr(t, "12345", "234")
	assert.HasSubstr(t, 12345, "234")
	// Output:
}

func ExampleHasPrefix() {
	t := &T{}
	assert.HasPrefix(t, "12345", "123")
	assert.HasPrefix(t, 12345, "123")
	// Output:
}

func ExampleTrue() {
	t := &T{}
	assert.True(t, true)
	assert.True(t, 1 == 1)
	// Output:
}

func ExampleFalse() {
	t := &T{}
	assert.False(t, false)
	assert.False(t, 1 == 2)
	// Output:
}

func ExampleNil() {
	t := &T{}
	assert.Nil(t, nil)

	var m map[int]int
	assert.Nil(t, m)
	// Output:
}

func ExampleNoError() {
	t := &T{}
	assert.NoError(t, nil)
	var e error
	assert.NoError(t, e)
}

func ExampleNotNil() {
	t := &T{}
	assert.NotNil(t, false)
	assert.NotNil(t, 1)
	assert.NotNil(t, map[int]int{})
	// Output:
}
