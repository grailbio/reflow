package h_test

import (
	"fmt"
	"regexp"
	"testing"
	"unsafe"

	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/testutil/h"
)

func resultIs(status h.Status, reStr string) *h.Matcher {
	re := regexp.MustCompile(reStr)
	m := &h.Matcher{
		Msg:    fmt.Sprintf("result status is %v and message contains regexp '%s'", status, re),
		NotMsg: fmt.Sprintf("result status is not %v or message does not contain regexp '%s'", status, re),
	}
	m.Match = func(got interface{}) h.Result {
		r := got.(h.Result)
		return h.NewResult(r.Status() == status && re.FindString(r.String()) != "", got, m)
	}
	return m
}

func TestEQStruct(t *testing.T) {
	type tt struct {
		x int
		y string
		z *tt
	}
	expect.NEQ(t, tt{10, "x", nil}, tt{11, "x", nil})
	type tt2 struct {
		x int
		y string
		z *tt
	}
	expect.Regexp(t, h.EQ(tt2{10, "x", nil}).Match(tt{10, "x", nil}), "Error.*are not comparable")

	// Cyclic struct
	v0 := tt{x: 10}
	v1 := tt{x: 10}
	v2 := tt{x: 11, z: &v0}
	v0.z = &v2
	v1.z = &v2
	expect.EQ(t, v0, v1)
}

func TestExpect(t *testing.T) {
	expect.Regexp(t, h.EQ(42).Match(43).String(), "(?s)Actual: *43.*Expected: 42")
	expect.Regexp(t, h.EQ(42).Match("aoeu"), "Error.*are not comparable")

	expect.EQ(t, h.Contains(10).Match([]int{42, 10}).Status(), h.Match)
	expect.Regexp(t, h.Contains("x").Match([]int{42, 10}), "Error.*not comparable")

	expect.EQ(t, h.Regexp("a.*d").Match("abcd").Status(), h.Match)
	expect.Regexp(t, h.Regexp("a.*e").Match("abcd"), "Expected: matches regexp")
	expect.EQ(t, h.Not(h.Regexp("a.*e")).Match("abcd").Status(), h.Match)

	expect.EQ(t, h.HasPrefix("abc").Match("abcd").Status(), h.Match)
	expect.EQ(t, h.Not(h.HasPrefix("abcde")).Match("abcd").Status(), h.Match)

	expect.EQ(t, h.LE(11).Match(10).Status(), h.Match)
	expect.EQ(t, h.LE(10).Match(10).Status(), h.Match)

	expect.EQ(t, h.Match, h.LT(11).Match(10).Status(), h.Match)
	expect.Regexp(t, h.LT(10).Match(10).String(), "Expected: is < 10")
	expect.EQ(t, h.NoError().Match(nil).Status(), h.Match)

	expect.EQ(t, h.Contains(h.HasSubstr("a")).Match([]string{"ab", "cd"}).Status(), h.Match)
	expect.EQ(t, h.Contains(h.LE(10)).Match([]int{10, 11}).Status(), h.Match)
	expect.Regexp(t, h.Contains(h.HasSubstr("e")).Match([]string{"ab", "cd"}),
		"(?s)contains element that has substr")
	expect.Regexp(t, h.Contains(h.HasSubstr("e")).Match([]string{"ab", "cd"}),
		"(?s)contains element that has substr")
}

func TestEach(t *testing.T) {
	expect.Regexp(t, h.Each(h.HasSubstr("a")).Match([]string{"ab", "cd"}).String(),
		"(?s)whose element #1 doesn't match.*every element in sequence has substr")
	expect.EQ(t, h.Each(h.HasSubstr("a")).Match([]string{"ab", "ac"}).Status(), h.Match)
	expect.Regexp(t, h.Each(h.LT(5.0)).Match([]int{12, 34}),
		`Error:.*are not comparable`)
}

func TestNot(t *testing.T) {
	expect.Regexp(t, h.Not(h.EQ(42)).Match(42), `(?s)Actual: *42.*Expected: is != 42`)
	expect.EQ(t, h.Not(h.EQ(42)).Match(43).Status(), h.Match)
	expect.Regexp(t, h.Not(h.EQ(42)).Match("str"),
		`(?s)Actual: *str.*Error:.*are not comparable`)
}

func TestDomainErrors(t *testing.T) {
	type tt struct{ x int }
	expect.Regexp(t, h.LT(tt{11}).Match(tt{10}), `Error:.*are not comparable`)
	expect.Regexp(t, h.LT([]int{11}).Match([]int{12}), `Error:.*are not comparable`)
	expect.Regexp(t, h.LT([...]int{11}).Match([...]int{12}), `Error:.*are not comparable`)

	expect.Regexp(t, h.LT(42).Match("x"), "Error:.*are not comparable")

	v0 := [...]int{1, 2}
	v1 := [...]int{2, 3}
	expect.Regexp(t, h.LT(v0).Match(v1), "Error:.*are not comparable")
}

func TestElementsAre(t *testing.T) {
	expect.Regexp(t, h.ElementsAre(10, 11).Match([]int{9, 11}),
		`(?s)Actual:.*\[9 11\] whose element #0 doesn't match.*Expected: elements are \[10, 11\]`)
	expect.Regexp(t, h.ElementsAre(10, 11).Match([]int{10, 12}),
		`(?s)Actual:.*\[10 12\] whose element #1 doesn't match.*Expected: elements are \[10, 11\]`)
	expect.Regexp(t, h.ElementsAre(10, 11).Match(map[int]int{10: 110, 12: 112}),
		`Error:.*must be a slice, array, or string`)
}

func TestUnorderedElementsAre(t *testing.T) {
	expect.Regexp(t, h.UnorderedElementsAre(10, 11, 12).Match([]int{12, 10, 9}),
		"Expected: match some permutation of")
	expect.Regexp(t, h.UnorderedElementsAre(10, 12).Match(map[int]int{10: 110, 12: 112}),
		`Error:.*must be a slice, array, or string`)
}

func TestWhenSorted(t *testing.T) {
	expect.That(t, h.WhenSorted(h.ElementsAre(10, 11)).Match([]int{11, 10, 9}),
		resultIs(h.DomainError, "length mismatch"))
	expect.Regexp(t, h.WhenSorted(h.ElementsAre(10, 11)).Match([]int{11, 9}),
		`(?s)Actual: *\[9 11\] whose element #0.*when sorted, elements are \[10, 11\].*`)
	expect.EQ(t, h.WhenSorted(h.ElementsAre(10, 11)).Match([]int{11, 10}).Status(), h.Match)

	expect.Regexp(t, h.WhenSorted(h.ElementsAre(10, 11)).Match([...]int{11, 9}),
		`(?s)Actual: *\[9 11\] whose element #0.*when sorted, elements are \[10, 11\].*`)
	expect.Regexp(t, h.WhenSorted(h.ElementsAre(10, 11)).Match(map[int]int{11: 110, 9: 99}),
		`Error:.*must be a slice, array, or string`)

	// Unsortable type
	type tt struct{ x, y int }
	expect.Regexp(t, h.WhenSorted(h.ElementsAre(tt{1, 2}, tt{3, 4})).Match([]tt{tt{3, 4}, tt{1, 2}}),
		`Error:.*is not sortable`)
}

func TestContains(t *testing.T) {
	expect.That(t, h.Contains(h.LE(9)).Match([]int{10, 11}),
		resultIs(h.Mismatch, "contains element that is <= 9"))
	expect.That(t, h.Contains(10).Match([]int{42}),
		resultIs(h.Mismatch, "contains element that is 10"))
	expect.Regexp(t, h.Contains(10).Match(map[int]int{10: 110, 12: 112}),
		`Error:.*must be a slice, array, or string`)
}

func TestMapContains(t *testing.T) {
	m := map[int]string{
		10: "s10",
		12: "s12",
		13: "s13",
	}
	expect.EQ(t, h.MapContains(10, h.Any()).Match(m).Status(), h.Match)
	expect.EQ(t, h.MapContains(10, "s10").Match(m).Status(), h.Match)
	expect.Regexp(t, h.MapContains(10, "s12").Match(m), "contains entry whose key is 10 and value is s12")
	expect.Regexp(t, h.MapContains(10, "s12").Match([]int{1, 2}), "Error:.*must be a map")
	expect.Regexp(t, h.MapContains("s12", 10).Match(m), "Error:.*are not comparable")
	expect.EQ(t, h.MapContains(h.Any(), "s12").Match(m).Status(), h.Match)
}

func TestNestedMatcher(t *testing.T) {
	data := [][]int{[]int{10, 11, 12, 13}}
	expect.Regexp(t, h.Contains(h.LE(9)).Match(data[0]), `(?s)Actual: *\[10 11 12 13\].*Expected:.*contains element that is <= 9`)

	expect.Regexp(t, h.Contains(h.Contains(h.LE(9))).Match(data),
		`(?s)Actual: *\[\[10 11 12 13\]\].*Expected:.*contains element that contains element that is <= 9`)

	expect.Regexp(t, h.Each(h.LE(12)).Match(data[0]), `(?s)Actual: *\[10 11 12 13\] whose element #3 doesn't match.*Expected:.*every element in sequence is <= 12`)
	expect.Regexp(t, h.Each(h.Each(h.LE(12))).Match(data), `(?s)Actual: *\[\[10 11 12 13\]\] whose element #0 doesn't match, whose element #3 doesn't match.*Expected:.*every element in sequence is <= 12`)

}

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
	expect.EQ(t, true, true)
	expect.EQ(t, false, false)
	expect.NEQ(t, false, true)
	expect.EQ(t, nil, nil)
	expect.EQ(t, uintptr(1234), uintptr(1234))
	expect.NEQ(t, uintptr(1234), uintptr(1235))

	expect.EQ(t, complex(1.0, 3.0), complex(1.0, 3.0))
	expect.EQ(t, complex(float64(2.0), float64(3.0)), complex(float64(2.0), float64(3.0)))
	expect.EQ(t, complex(float32(2.0), float32(3.0)), complex(float32(2.0), float32(3.0)))
	expect.NEQ(t, complex(2.0, 3.0), complex(1.0, 3.0))

	expect.EQ(t, []int{42, 43}, []int{42, 43})
	expect.EQ(t, [...]int{42, 43}, [...]int{42, 43})
	expect.EQ(t, map[int]int{1: 2, 3: 4}, map[int]int{3: 4, 1: 2})

	type tt struct {
		x int
		y *tt
	}
	expect.EQ(t, tt{10, nil}, tt{10, nil})
	expect.EQ(t, &tt{10, nil}, &tt{10, nil})
	expect.EQ(t, []tt{{10, nil}, {11, nil}}, []tt{{10, nil}, {11, nil}})
	expect.EQ(t, tt{10, &tt{11, nil}}, tt{10, &tt{11, nil}})

	xt0 := &tt{10, nil}
	xt0.y = xt0
	xt1 := &tt{10, nil}
	xt1.y = xt1
	expect.EQ(t, xt0, xt1)

	var it0 interface{} = tt{10, nil}
	var it1 interface{} = tt{10, nil}
	expect.EQ(t, it0, it1)

	ch0 := make(chan uint8)
	ch1 := make(chan uint8)
	expect.EQ(t, ch0, ch0)
	expect.NEQ(t, ch0, ch1)
	// Output:
}

func ExampleRegisterComparator() {
	t := &T{}
	type tt struct{ val int }
	h.RegisterComparator(func(x, y tt) (int, error) {
		if x.val > 1000 {
			return 0, fmt.Errorf("test failure")
		}
		return x.val - y.val, nil
	})
	expect.LT(t, tt{val: 10}, tt{val: 11})
	expect.EQ(t, tt{val: 10}, tt{val: 10})
	expect.GT(t, tt{val: 11}, tt{val: 10})
	expect.Regexp(t, h.EQ(tt{1001}).Match(tt{1001}), "test failure")
	// Output:
}

func ExampleZero() {
	t := &T{}
	expect.That(t, 0, h.Zero())
	expect.That(t, 0.0, h.Zero())
	expect.That(t, "", h.Zero())
	expect.That(t, 1, h.Not(h.Zero()))
	expect.That(t, 0.1, h.Not(h.Zero()))
	expect.That(t, "x", h.Not(h.Zero()))
	type tt struct{ x int }
	expect.That(t, tt{x: 0}, h.Zero())
	expect.That(t, tt{x: 1}, h.Not(h.Zero()))
	expect.That(t, nil, h.Zero())
	expect.That(t, []int{}, h.Not(h.Zero()))
	expect.That(t, []int{0}, h.Not(h.Zero()))
	// Output:
}

func ExampleLT() {
	t := &T{}
	expect.That(t, 10, h.LT(11))
	expect.That(t, 10, h.Not(h.LT(10)))
	expect.That(t, 10, h.Not(h.LT(9)))

	var z0 [2]int
	x := &z0[0]
	y := &z0[1]
	expect.That(t, x, h.LT(y))
}

func ExampleLE() {
	t := &T{}
	expect.That(t, 10, h.LE(11))
	expect.That(t, 10, h.LE(10))
	expect.That(t, 10, h.Not(h.LE(9)))

	var z0 [2]int
	x := &z0[0]
	y := &z0[1]
	expect.That(t, x, h.LE(y))
	expect.That(t, x, h.LE(x))
}

func ExampleElementsAre() {
	t := &T{}
	expect.That(t, []int{10, 11}, h.ElementsAre([]interface{}{10, 11}...))
	expect.That(t, []int{10, 16}, h.ElementsAre(10, h.GT(15)))
	expect.That(t, []int{}, h.ElementsAre())
	// Output:
}

func ExampleElementsAreArray() {
	t := &T{}
	expect.That(t, []int{10, 16}, h.ElementsAreArray([]int{10, 16}))
	expect.That(t, []int{10, 16}, h.ElementsAreArray([]interface{}{10, h.GT(15)}))
	// Output:
}

func ExampleHasSubstr() {
	t := &T{}
	expect.That(t, "abc def", h.HasSubstr("c d"))
	expect.That(t, 12345, h.HasSubstr("234"))
	// Output:
}

func ExampleRegexp() {
	t := &T{}
	expect.Regexp(t, 12345, "2.*4")
	expect.Regexp(t, "12345", "2.*4")
	// Output:
}

func ExampleAllOf() {
	t := &T{}
	expect.That(t, 10, h.AllOf())
	expect.That(t, 10, h.AllOf(h.LT(11), h.GT(9)))
	expect.That(t, 10, h.Not(h.AllOf(h.LT(11), h.GT(10))))
	// Output:
}

func ExampleAnyOf() {
	t := &T{}
	expect.That(t, 10, h.AnyOf(9, 10, 11))
	expect.That(t, 10, h.Not(h.AnyOf(h.GE(11), h.LE(9))))
	expect.That(t, 10, h.AnyOf(h.LT(11), h.GT(10)))
	expect.That(t, 10, h.Not(h.AnyOf()))
	// Output:
}

func ExampleUnorderedElementsAre() {
	t := &T{}
	expect.That(t, []int{12, 10, 11}, h.UnorderedElementsAre(10, 11, 12))
	expect.That(t, []int{12, 11, 10}, h.UnorderedElementsAre(10, 11, 12))
	expect.That(t, []int{11, 10, 12}, h.UnorderedElementsAre(10, 11, 12))
	expect.That(t, []int{10}, h.UnorderedElementsAre(10))
	expect.That(t, []int{}, h.UnorderedElementsAre())
	// Output:
}

func ExampleUnorderedElementsAreArray() {
	t := &T{}
	expect.That(t, []int{12, 10, 11}, h.UnorderedElementsAreArray([]int{10, 11, 12}))
	expect.That(t, []int{12, 11, 10}, h.UnorderedElementsAreArray([]int{10, 11, 12}))
	expect.That(t, []int{11, 10, 12}, h.UnorderedElementsAreArray([]int{10, 11, 12}))
	expect.That(t, []int{10}, h.UnorderedElementsAreArray([]int{10}))
	expect.That(t, []int{}, h.UnorderedElementsAreArray([]int{}))
	// Output:
}

func ExampleUnorderedElementsAre_struct() {
	t := &T{}
	type T struct {
		Name string
		Type string
	}
	val := []T{
		T{Name: "n0", Type: "t0"},
		T{Name: "n1", Type: "t1"},
	}
	expect.That(t, val, h.UnorderedElementsAre(
		T{Name: "n1", Type: "t1"},
		T{Name: "n0", Type: "t0"}))
	// Output:
}

func ExampleUnorderedElementsAreArray_struct() {
	t := &T{}
	type T struct {
		Name string
		Type string
	}
	val := []T{
		T{Name: "n0", Type: "t0"},
		T{Name: "n1", Type: "t1"},
	}
	expect.That(t, val, h.UnorderedElementsAreArray([]T{
		{Name: "n1", Type: "t1"},
		{Name: "n0", Type: "t0"}}))
	// Output:
}

func ExampleWhenSorted() {
	t := &T{}
	expect.That(t, []int{11, 10, 12}, h.WhenSorted(h.ElementsAre(10, 11, 12)))
	// Output:
}

func ExampleNot() {
	t := &T{}
	expect.That(t, 10, h.Not(11))
	expect.That(t, "abc", h.Not(h.HasPrefix("b")))
}

func ExampleContains() {
	t := &T{}
	expect.That(t, []int{42, 43}, h.Contains(43))
	expect.That(t, []int{42, 43}, h.Contains(h.LE(42)))
	expect.That(t, []int{42, 43}, h.Not(h.Contains(h.LT(42))))
	// Output:
}

func ExampleAny() {
	t := &T{}
	expect.That(t, nil, h.Any())
	// Output:
}

func ExampleNone() {
	t := &T{}
	expect.That(t, nil, h.Not(h.None()))
	// Output:
}

func ExampleNil() {
	t := &T{}
	expect.That(t, nil, h.Nil())
	var m map[int]int
	expect.That(t, m, h.Nil())
	expect.That(t, map[int]int{}, h.Not(h.Nil()))

	var c chan int
	expect.That(t, c, h.Nil())
	expect.That(t, make(chan int), h.Not(h.Nil()))
	var i interface{}
	expect.That(t, i, h.Nil())

	var p *int
	expect.That(t, p, h.Nil())
	expect.That(t, unsafe.Pointer(p), h.Nil())

	var q int
	p = &q
	expect.That(t, p, h.Not(h.Nil()))
	expect.That(t, unsafe.Pointer(p), h.Not(h.Nil()))
	// Output:
}

func ExamplePanics() {
	t := &T{}
	expect.That(t, func() { fmt.Println("hello") }, h.Panics(h.Nil()))
	expect.That(t, func() { panic("blue fox jumped over a red hen") }, h.Panics(h.HasSubstr("fox jumped")))
	// Output:
	// hello
}
