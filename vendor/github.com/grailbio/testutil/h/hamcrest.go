// Package h provides matchers used by assert and expect packages.  See the
// documentations in assert and expect for more details.
package h

//go:generate sh -c "sed -e s/PACKAGE/assert/ utils.go.tpl > ../assert/utils.go"
//go:generate sh -c "sed -e s/PACKAGE/assert/ utils_test.go.tpl > ../assert/utils_test.go"
//go:generate sh -c "sed -e s/PACKAGE/expect/ utils.go.tpl > ../expect/utils.go"
//go:generate sh -c "sed -e s/PACKAGE/expect/ utils_test.go.tpl > ../expect/utils_test.go"

import (
	"bytes"
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

// Status represents the match result.
type Status int

const (
	// DomainError is reported when the value has a wrong type or cardinality.
	// For example, assert.That(t, 10, h.LT("foo")) raises this error.
	//
	// DomainError is different from Mismatch in that Not() of Mismatch becomes a
	// Match, but Not() of DomainError remains a DomainError.
	DomainError Status = iota
	// Match is reported on a successful match.
	Match
	// Mismatch is reported when the matcher fails.
	Mismatch
)

// String returns a human-readable description.
func (s Status) String() string {
	msgs := []string{"DomainError", "Match", "Mismatch"}
	return msgs[s]
}

// Result is the result of a matcher.
type Result struct {
	// Status is the match result.
	status Status
	// Backtrace is the stack backtrace when the matcher ran.
	backtrace string

	msg              string
	value            interface{}
	valueAnnotations []string
}

// Status returns the status code of the match result.
func (r Result) Status() Status { return r.status }

func (r Result) String() string {
	if r.status == Match {
		return ""
	}
	buf := bytes.NewBuffer(nil)
	buf.WriteString(fmt.Sprintf("Failure:\n%s\n", r.backtrace))
	buf.WriteString(fmt.Sprintf("Actual:   %s", describe(r.value)))
	for i := len(r.valueAnnotations) - 1; i >= 0; i-- {
		if i < len(r.valueAnnotations)-1 {
			buf.WriteString(", ")
		} else {
			buf.WriteString(" ")
		}
		buf.WriteString(r.valueAnnotations[i])
	}
	buf.WriteByte('\n')
	if r.status == Mismatch {
		buf.WriteString(fmt.Sprintf("Expected: %s\n", r.msg))
	} else {
		buf.WriteString(fmt.Sprintf("Error:    %s\n", r.msg))
	}
	return buf.String()
}

// Matcher represents a node of a matcher expression tree.
type Matcher struct {
	// Desc describes this matcher.
	Msg string
	// NotDesc describes the negation of this matcher. It's shown when this
	// matcher is wrapped around Not().
	NotMsg string
	// Match is invoked to check if the given value satisfies the matcher
	// condition.
	Match func(val interface{}) Result
	// isEqeual is set only for EQ(). For prettypretting the results.
	isEqual bool
}

func describe(v interface{}) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprintf("%+v", v)
}

func describeVerbose(v interface{}) string {
	if v == nil {
		return "nil"
	}
	vType := reflect.TypeOf(v)
	return fmt.Sprintf("%s(type: %v)", describe(v), vType)
}

// Function backtrace generates the current stack backtrace w/o internal frames.
func backtrace() string {
	pcs := make([]uintptr, 20)
	n := runtime.Callers(0, pcs)
	frames := runtime.CallersFrames(pcs[1:n])
	buf := bytes.NewBuffer(nil)
	for {
		frame, ok := frames.Next()
		if !ok {
			break
		}
		buf.WriteString(fmt.Sprintf("%s:%d: %s\n", frame.File, frame.Line, frame.Function))
	}
	return buf.String()
}

// NewResult creates a new Result object. If matched (or !matched), the status
// will be Match (Mismatch), respectively. got is the value under test.
func NewResult(matched bool, got interface{}, m *Matcher) Result {
	r := Result{
		status: Match,
		msg:    m.Msg,
		value:  got,
	}
	if !matched {
		r.status = Mismatch
		r.backtrace = backtrace()
	}
	return r
}

func (r Result) wrap(got interface{}, m *Matcher, annot string) Result {
	n := r
	n.msg = m.Msg
	n.value = got
	n.valueAnnotations = append(n.valueAnnotations, annot)
	return n
}

// NewErrorf creates a new Result object of DomainError type.
func NewErrorf(got interface{}, f string, args ...interface{}) Result {
	str := fmt.Sprintf(f, args...)
	return Result{
		status:    DomainError,
		backtrace: backtrace(),
		msg:       str,
		value:     got,
	}
}

// func (m Matcher) Message(str string) Matcher {
// 	return Matcher{
// 		Desc:  str + ": " + m.Desc,
// 		Match: m.Match,
// 	}
// }

// HasSubstr checks if the value contains the given substring.  If the target
// value is not a string, it is converted to a string with fmt.Sprintf("%v").
func HasSubstr(want string) *Matcher {
	m := &Matcher{
		Msg:    fmt.Sprintf("has substring `%s`", want),
		NotMsg: fmt.Sprintf("doesn't have substring `%s`", want),
	}
	m.Match = func(got interface{}) Result {
		s := fmt.Sprintf("%v", got)
		return NewResult(strings.Contains(s, want), got, m)
	}
	return m
}

// HasPrefix checks if the value starts with the given substring.  If the target
// value is not a string, it is converted to a string with fmt.Sprintf("%v").
func HasPrefix(want string) *Matcher {
	m := &Matcher{
		Msg:    fmt.Sprintf("has prefix `%s`", want),
		NotMsg: fmt.Sprintf("doesn't have prefix `%s`", want),
	}
	m.Match = func(got interface{}) Result {
		s := fmt.Sprintf("%v", got)
		return NewResult(strings.HasPrefix(s, want), got, m)
	}
	return m
}

// Regexp checks that the value matches the given regex.  The regex can be given
// as a string, or *regexp.Regexp.  The regexp is matched using
// regexp.Regexp.Find. If the target value is not a string, it is converted to a
// string with fmt.Sprintf("%v").
//
// Example:
//   assert.That(t, "fox jumped over a dog", h.Regexp("j.*d"))
func Regexp(want interface{}) *Matcher {
	re, ok := want.(*regexp.Regexp)
	if !ok {
		re = regexp.MustCompile(want.(string))
	}
	m := &Matcher{
		Msg:    fmt.Sprintf("matches regexp '%s'", re.String()),
		NotMsg: fmt.Sprintf("does not match regexp '%s'", re.String()),
	}
	m.Match = func(got interface{}) Result {
		b, ok := got.([]byte)
		if !ok {
			b = []byte(fmt.Sprintf("%v", got))
		}
		return NewResult(re.Find(b) != nil, got, m)
	}
	return m
}

// NoError is an alias of Nil. It should be checked when the value is of type
// error.
//
//   assert.That(t, os.Close(fd), h.NoError())
var NoError = Nil

// Nil checks that the value is nil.
func Nil() *Matcher {
	m := &Matcher{
		Msg:    "nil",
		NotMsg: "not nil",
	}
	m.Match = func(got interface{}) Result {
		if got == nil {
			return NewResult(true, got, m)
		}
		v := reflect.ValueOf(got)
		switch v.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			if v.IsNil() {
				return NewResult(true, got, m)
			}
		case reflect.UnsafePointer:
			if v.Pointer() == 0 {
				return NewResult(true, got, m)
			}
		}
		return NewResult(false, got, m)
	}
	return m
}

// NotNil is a shorthand for Not(Nil))
func NotNil() *Matcher { return Not(Nil()) }

// Any matches anything.
func Any() *Matcher {
	m := &Matcher{
		Msg:    "any",
		NotMsg: "not any",
	}
	m.Match = func(got interface{}) Result {
		return NewResult(true, got, m)
	}
	return m
}

// None matches nothing.
func None() *Matcher {
	m := &Matcher{
		Msg:    "none",
		NotMsg: "not none",
	}
	m.Match = func(got interface{}) Result {
		return NewResult(false, got, m)
	}
	return m
}

// Not negates the result of the child matcher.  The argument can be a *Matcher,
// or any value.
//
// Example:
//  assert.That(t, "abc", h.Not(h.Regexp("e+")))
//  assert.That(t, 10, h.Not(11))
func Not(val interface{}) *Matcher {
	m := toMatcher(val)
	nm := &Matcher{
		Msg:    m.NotMsg,
		NotMsg: m.Msg,
	}
	nm.Match = func(got interface{}) Result {
		r := m.Match(got)
		switch r.status {
		case DomainError:
			return r
		case Mismatch:
			r.status = Match
		default:
			r.status = Mismatch
		}
		r.msg = nm.Msg
		return r

	}
	return nm
}

// EQ checks if the two values are equal.  Equality is defined as follows.
//
// EQ returns true only if the two values are of the same type. In addition:
//
// - Two slices/arrays are equal if they are of the same length and the element
// at every position are equal.
//
// - For other data types, the two values x and y are equal if
//   reflect.DeepEqual(x, y)
func EQ(want interface{}) *Matcher {
	m := &Matcher{
		isEqual: true,
		Msg:     fmt.Sprintf("%s", describe(want)),
		NotMsg:  fmt.Sprintf("is != %s", describe(want)),
	}
	m.Match = func(got interface{}) Result {
		c, err := compare(got, want)
		if err != nil {
			return NewErrorf(got, m.Msg+": "+err.Error())
		}
		return NewResult(c == cEQ, got, m)
	}
	return m
}

// NEQ is a shorthand for Not(EQ(want))
func NEQ(want interface{}) *Matcher { return Not(EQ(want)) }

func totalOrderPredicate(
	msg string,
	notMsg string,
	want interface{},
	cond func(c compareResult) bool) *Matcher {
	m := &Matcher{
		Msg:    msg,
		NotMsg: notMsg,
	}
	m.Match = func(got interface{}) Result {
		c, err := compare(got, want)
		if err != nil {
			return NewErrorf(got, msg+": "+err.Error())
		}
		if c == cNEQ {
			return NewErrorf(got, msg+": %s and %s are not comparable", describeVerbose(got), describeVerbose(want))
		}
		if cond(c) {
			return NewResult(true, got, m)
		}
		return NewResult(false, got, m)
	}
	return m
}

// LT checks if got < want. Got and want must be a numeric (int*, uint*, float*)
// or a string type.
func LT(want interface{}) *Matcher {
	return totalOrderPredicate(
		fmt.Sprintf("is < %s", describe(want)),
		fmt.Sprintf("is not < %s", describe(want)),
		want, func(c compareResult) bool { return c == cLT })
}

// LE checks if got <= want. Got and want must be a numeric (int*, uint*, float*)
// or a string type.
func LE(want interface{}) *Matcher {
	return totalOrderPredicate(
		fmt.Sprintf("is <= %v", want),
		fmt.Sprintf("is not <= %v", want),
		want, func(c compareResult) bool {
			return c == cLT || c == cEQ
		})
}

// GT checks if got > want. Got and want must be a numeric (int*, uint*, float*)
// or a string type.
func GT(want interface{}) *Matcher {
	return totalOrderPredicate(
		fmt.Sprintf("> %s", describe(want)),
		fmt.Sprintf("not > %s", describe(want)),
		want, func(c compareResult) bool { return c == cGT })
}

// GE checks if got >= want. Got and want must be a numeric (int*, uint*,
// float*) or a string type.
func GE(want interface{}) *Matcher {
	return totalOrderPredicate(
		fmt.Sprintf(">= %s", describe(want)),
		fmt.Sprintf("not >= %s", want),
		want, func(c compareResult) bool {
			return c == cGT || c == cEQ
		})
}

func toMatcher(val interface{}) *Matcher {
	if m, ok := val.(*Matcher); ok {
		return m
	}
	return EQ(val)
}

// MapContains checks if a map contains an entry with the given key and
// value. Key and value can be immediate values, or Matchers. The target value
// must be a map.
//
// Example:
//   assert.That(t, map[int]int{10:15, 11:20}, h.MapContains(10, 15))
//   assert.That(t, map[int]int{10:15, 11:20}, h.MapContains(h.Any, 15))
//   assert.That(t, map[int]int{10:15, 11:20}, h.MapContains(h.LT(11), h.GT(12)))
func MapContains(key, val interface{}) *Matcher {
	wantKey := toMatcher(key)
	wantVal := toMatcher(val)

	m := &Matcher{
		Msg:    fmt.Sprintf("map contains entry whose key %s and value %s", phrasify(wantKey), phrasify(wantVal)),
		NotMsg: fmt.Sprintf("map does not contain entry whose key %s and value %s", phrasify(wantKey), phrasify(wantVal)),
	}
	m.Match = func(got interface{}) Result {
		gotV := reflect.ValueOf(got)
		if gotV.Kind() != reflect.Map {
			return NewErrorf(got, "MapContains: %v must be a map", describeVerbose(got))
		}
		keys := gotV.MapKeys()
		i := 0
		for _, key := range keys {
			r := wantKey.Match(key.Interface())
			if r.status == DomainError {
				return r
			}
			if r.status == Match {
				val := gotV.MapIndex(key)
				r = wantVal.Match(val.Interface())
				if r.status == Match {
					return NewResult(true, got, m)
				}
			}
			i++
		}
		return NewResult(false, got, m)
	}
	return m
}

func indexable(v reflect.Value) error {
	if v.Kind() == reflect.Slice ||
		v.Kind() == reflect.Array ||
		v.Kind() == reflect.String {
		return nil
	}
	return fmt.Errorf("%v must be a slice, array, or string", describeVerbose(v.Interface()))
}

// Each checks if the every element in the target value has the given property.
// The target must be a slice, array, or a string.
//
// Example:
//   assert.That(t, []string{"abc", "abd"}, h.Each(h.HasPrefix("ab")))
//   assert.That(t, []int{10, 10}, h.Each(10))
func Each(w interface{}) *Matcher {
	want := toMatcher(w)
	m := &Matcher{
		Msg:    fmt.Sprintf("every element in sequence %s", want.Msg),
		NotMsg: fmt.Sprintf("at least one element in sequence %s", want.NotMsg),
	}
	m.Match = func(got interface{}) Result {
		gotV := reflect.ValueOf(got)
		if err := indexable(gotV); err != nil {
			return NewErrorf(got, m.Msg+": "+err.Error())
		}
		n := gotV.Len()
		for i := 0; i < n; i++ {
			elem := gotV.Index(i).Interface()
			r := want.Match(elem)
			if r.status == DomainError {
				return r
			}
			if r.status == Mismatch {
				return r.wrap(got, m, fmt.Sprintf("whose element #%d doesn't match", i))
			}
		}
		return NewResult(true, got, m)
	}
	return m
}

func phrasify(m *Matcher) string {
	if m.isEqual {
		return "is " + m.Msg
	}
	return m.Msg
}

// Contains checks if an array, slice, or string contains the given element.
// The element can be an immediate value or a Matcher.
//
// Example:
//   assert.That(t, []int{10, 12}, h.Contains(10))
//   assert.That(t, []int{10, 12}, h.Contains(h.LT(11)))
func Contains(w interface{}) *Matcher {
	want := toMatcher(w)
	m := &Matcher{
		Msg:    fmt.Sprintf("contains element that %s", phrasify(want)),
		NotMsg: fmt.Sprintf("does not contain element that %s", phrasify(want)),
	}
	m.Match = func(got interface{}) Result {
		gotV := reflect.ValueOf(got)
		if err := indexable(gotV); err != nil {
			return NewErrorf(got, m.Msg+": "+err.Error())
		}
		n := gotV.Len()
		for i := 0; i < n; i++ {
			elem := gotV.Index(i).Interface()
			r := want.Match(elem)
			if r.status == DomainError {
				return r
			}
			if r.status == Match {
				return NewResult(true, got, m)
			}
		}
		return NewResult(false, got, m)
	}
	return m
}

// ElementsAre checks if a sequence matches the given conditions, in order.
//
// Example:
//   // Note: the below is the same as assert.EQ(t, []int{12, 10}, []int{12, 10})
//   assert.That(t, []int{12, 10}, h.ElementsAre(12, 10))
//   assert.That(t, []int{12, 10}, h.ElementsAre(h.LT(20), 10))
func ElementsAre(w ...interface{}) *Matcher {
	wants := make([]*Matcher, len(w))
	for i := range w {
		wants[i] = toMatcher(w[i])
	}
	return elementsAreImpl("ElementsAreArray", wants)
}

// Construct a list of matchers. "w" must be an array-like object.
func toMatcherArray(label string, w interface{}) []*Matcher {
	wantV := reflect.ValueOf(w)
	if err := indexable(wantV); err != nil {
		panic(fmt.Sprintf("%s: arg must be array-like, but got %s", label, describe(w)))
	}
	l := wantV.Len()
	wants := make([]*Matcher, l)
	for i := range wants {
		wants[i] = toMatcher(wantV.Index(i).Interface())
	}
	return wants
}

// ElementsAreArray checks if a sequence matches the given array of values, in
// order.  The argument must be array-like (slice, array, string, ...).
//
// Example:
//   // Note: the below is the same as assert.EQ(t, []int{12, 10}, []int{12, 10})
//   assert.That(t, []int{12, 10}, h.ElementsAreArray([]int{12, 10}))
//   assert.That(t, []int{12, 10}, h.ElementsAreArray([]interface{}{h.LT(20), 10)))
func ElementsAreArray(w interface{}) *Matcher {
	const label = "ElementsAreArray"
	return elementsAreImpl(label, toMatcherArray(label, w))
}

// elementsAreImpl implements ElementsAre{,Array}.
func elementsAreImpl(label string, wants []*Matcher) *Matcher {
	msgs := make([]string, len(wants))
	for i := range wants {
		msgs[i] = wants[i].Msg
	}
	m := &Matcher{
		Msg:    fmt.Sprintf("elements are [%s]", strings.Join(msgs, ", ")),
		NotMsg: fmt.Sprintf("at least one element is not (%s)", strings.Join(msgs, ", ")),
	}
	m.Match = func(got interface{}) Result {
		gotV := reflect.ValueOf(got)
		if err := indexable(gotV); err != nil {
			return NewErrorf(got, m.Msg+": "+err.Error())
		}
		n := gotV.Len()
		if n != len(wants) {
			return NewErrorf(got, "%s: length mismatch (%d != %d), got %s, want %s",
				label, n, len(wants),
				describe(gotV), strings.Join(msgs, ", "))
		}
		for i := 0; i < n; i++ {
			elem := gotV.Index(i).Interface()
			r := wants[i].Match(elem)
			if r.status == DomainError {
				return r
			}
			if r.status == Mismatch {
				return r.wrap(got, m, fmt.Sprintf("whose element #%d doesn't match", i))
			}
		}
		return NewResult(true, got, m)
	}
	return m
}

// WhenSorted wraps another matcher that matches against a sequence, most often
// ElementsAre.  It sorts the target value using go's "<" operator, then passes
// the resulting sequence to the underlying matcher. The target value must be an
// array, slice, or string.
//
// Example:
//   assert.That(t, []int{12, 10}, h.WhenSorted(h.ElementsAre(10, 12)))
func WhenSorted(m *Matcher) *Matcher {
	ws := &Matcher{
		Msg:    "when sorted, " + m.Msg,
		NotMsg: "when sorted, " + m.NotMsg,
	}
	ws.Match = func(got interface{}) Result {
		gotV := reflect.ValueOf(got)
		if err := indexable(gotV); err != nil {
			return NewErrorf(got, m.Msg+": "+err.Error())
		}
		n := gotV.Len()
		sorted := make([]interface{}, n)
		for i := 0; i < n; i++ {
			sorted[i] = gotV.Index(i).Interface()
		}
		var sortErr error
		sort.Slice(sorted, func(i, j int) bool {
			var c compareResult
			c, err := compare(sorted[i], sorted[j])
			if err != nil {
				if sortErr == nil {
					sortErr = err
				}
				return false
			}
			if c == cNEQ {
				if sortErr == nil {
					sortErr = fmt.Errorf("WhenSorted: %s is not sortable", describe(got))
				}
				return false
			}
			return c == cLT
		})
		if sortErr != nil {
			return NewErrorf(got, m.Msg+": "+sortErr.Error())
		}
		r := m.Match(sorted)
		if r.status == Mismatch {
			r.msg = ws.Msg
		}
		return r
	}
	return ws
}

// permuter produces permutations of integers {0,1,2,...,n-1}
//
// p := newPermuter(3) // produces all permutations of {0,1,2}
// for p.scan() { // six permutations will be produced.
//   fmt.Print(p.value())
// }
type permuter struct {
	idxs          []int
	iter, maxIter int
}

func newPermuter(n int) *permuter {
	p := &permuter{idxs: make([]int, n), maxIter: 1}
	for i := 2; i <= n; i++ {
		p.maxIter *= i
	}
	return p
}

// nextRec produces the k'th permutation of the given slice.
//
// REQUIRES: 0 <= k < factorial(len(s))
func (p *permuter) permuteRec(s []int, k int) {
	if len(s) <= 1 {
		return
	}
	m := k % len(s)
	s[0], s[m] = s[m], s[0]
	p.permuteRec(s[1:], k/len(s))
}

// value returns the current permutation.
//
// REQUIRES: last scan returned true.
func (p *permuter) value() []int { return p.idxs }

// scan produces the next permutations. Returns false on EOD.
func (p *permuter) scan() bool {
	if p.iter >= p.maxIter {
		return false
	}
	for i := range p.idxs {
		p.idxs[i] = i
	}
	p.permuteRec(p.idxs, p.iter)
	p.iter++
	return true
}

// UnorderedElementsAre checks if the target sequence matches some permutation
// of the given values.
func UnorderedElementsAre(w ...interface{}) *Matcher {
	wants := make([]*Matcher, len(w))
	for i := range w {
		wants[i] = toMatcher(w[i])
	}
	return unorderedElementsAreImpl("UnorderedElementsAreArray", wants)
}

// UnorderedElementsAreArray checks if the target sequence matches some
// permutation of the given list of values. The argument must be array-like (slice,
// array, string, ...).
func UnorderedElementsAreArray(w interface{}) *Matcher {
	const label = "UnorderedElementsAreArray"
	return unorderedElementsAreImpl(label, toMatcherArray(label, w))
}

// UnorderedElementsAre checks if the target sequence matches some permutation
// of the given values. {
func unorderedElementsAreImpl(label string, wants []*Matcher) *Matcher {
	if len(wants) > 8 {
		panic(fmt.Sprintf("%s: too many args, %d (max: 8)", label, len(wants)))
	}
	msgs := make([]string, len(wants))
	for i := range wants {
		msgs[i] = wants[i].Msg
	}
	m := &Matcher{
		Msg:    fmt.Sprintf("match some permutation of [%s]", strings.Join(msgs, ", ")),
		NotMsg: fmt.Sprintf("do not match any permutation of [%s]", strings.Join(msgs, ", ")),
	}
	m.Match = func(got interface{}) Result {
		gotV := reflect.ValueOf(got)
		if err := indexable(gotV); err != nil {
			return NewErrorf(got, m.Msg+": "+err.Error())
		}
		n := gotV.Len()
		if n != len(wants) {
			return NewErrorf(got, "%s: length mismatch (%d != %d), got %s, want %s",
				label, n, len(wants),
				describe(gotV), strings.Join(msgs, ", "))
		}

		permuter := newPermuter(n)
		for permuter.scan() {
			perm := permuter.value()
			ok := true
			for i, j := range perm {
				r := wants[i].Match(gotV.Index(j).Interface())
				if r.status == DomainError {
					return r
				}
				if r.status == Mismatch {
					ok = false
					break
				}
			}
			if ok {
				return NewResult(true, got, m)
			}
		}
		return NewResult(false, got, m)
	}
	return m
}

// AllOf checks if the target value matches all the submatchers.
func AllOf(values ...interface{}) *Matcher {
	matchers := toMatcherArray("AllOf", values)
	m := &Matcher{
		Msg:    "allof",
		NotMsg: "not allof",
	}
	m.Match = func(got interface{}) Result {
		for _, sm := range matchers {
			switch r := sm.Match(got); r.status {
			case DomainError:
				return r
			case Mismatch:
				return NewResult(false, got, m)
			}
		}
		return NewResult(true, got, m)
	}
	return m
}

// AnyOf checks if the target value matches at least one of the submatchers.
func AnyOf(values ...interface{}) *Matcher {
	matchers := toMatcherArray("AnyOf", values)
	m := &Matcher{
		Msg:    "anyof",
		NotMsg: "not anyof",
	}
	m.Match = func(got interface{}) Result {
		for _, sm := range matchers {
			if r := sm.Match(got); r.status == Match {
				return NewResult(true, got, m)
			}
		}
		return NewResult(false, got, m)
	}
	return m
}

// Panics checks that the a function invocation panics with the given value.
// If you just want to test that the code panics, pass NotNil() as the matcher.
//
// Example:
//   assert.That(t, func() { panic("fox jumped over a dog") }, h.Panics(h.Regexp("j.*d"))
//   assert.That(t, func() { panic("fox jumped over a dog") }, h.Panics(h.NotNil()))
func Panics(want interface{}) *Matcher {
	m := toMatcher(want)
	pm := &Matcher{
		Msg:    "panic value " + m.Msg,
		NotMsg: "does not panic with " + m.Msg,
	}
	pm.Match = func(got interface{}) Result {
		gotV := reflect.ValueOf(got)
		if gotV.Kind() != reflect.Func {
			return NewErrorf(got, "Panics: value must be a function")
		}
		if t := gotV.Type(); t.NumIn() != 0 || t.NumOut() != 0 {
			return NewErrorf(got, "Panics: value must be a function without input args and no output")
		}

		var panicValue interface{}
		doCall := func() {
			defer func() {
				panicValue = recover()
			}()
			gotV.Call(nil)
		}
		doCall()
		return m.Match(panicValue)
	}
	return pm
}

// Zero checks if the value is the zero value of its type.
func Zero() *Matcher {
	m := &Matcher{
		Msg:    "is zero value for the type",
		NotMsg: "is not the zero value for the type",
	}
	m.Match = func(got interface{}) Result {
		if got == nil {
			return NewResult(true, got, m)
		}
		if reflect.DeepEqual(got, reflect.Zero(reflect.TypeOf(got)).Interface()) {
			return NewResult(true, got, m)
		}
		return NewResult(false, got, m)
	}
	return m
}

var (
	comparatorMu = sync.Mutex{}
	comparators  = map[reflect.Type]reflect.Value{}
)

// Given a function of form func(a,b T) (int, error) this function returns T.
func comparatorArgType(callback interface{}) reflect.Type {
	typ := reflect.TypeOf(callback)
	if typ.Kind() != reflect.Func {
		panic(fmt.Sprintf("h.RegisterComparator: %+v is not a function", callback))
	}
	errorInterface := reflect.TypeOf((*error)(nil)).Elem()
	ok := true
	if typ.NumIn() != 2 || typ.In(0) != typ.In(1) ||
		typ.NumOut() != 2 || typ.Out(0).Kind() != reflect.Int || !typ.Out(1).Implements(errorInterface) {
		ok = false
	}
	if !ok {
		panic(fmt.Sprintf("h.RegisterComparator: %+v must be a binary function that takes inputs of the same type, and returns (int, error)", callback))
	}
	return typ.In(0)
}

// RegisterComparator registers a comparator function for a user-defined type.
// The callback should have signature
//
//     func(a, b T) (int, error)
//
// where T is an arbitrary type.  The callback will be invoked by h.EQ, h.GT and
// similar matchers. It is also invoked if type T appears inside a struct,
// pointer, or an interface.
//
// The callback should return a negative value if a<b, zero if a==b, a positive
// value if a>b. It should return a non-nil error if a and b are not comparable,
// or on any other error. The callback may define its own meanings of ">", "==",
// and "<", but they must define a total ordering over T.
func RegisterComparator(callback interface{}) {
	argType := comparatorArgType(callback)
	comparatorMu.Lock()
	comparators[argType] = reflect.ValueOf(callback)
	comparatorMu.Unlock()
}

// UnregisterComparator unregisters the callback registered in
// RegisteredComparator.  It panics if the callback was not registered.
func UnregisterComparator(callback interface{}) {
	argType := comparatorArgType(callback)
	comparatorMu.Lock()
	if _, ok := comparators[argType]; !ok {
		panic(fmt.Sprintf("h.UnregisterComparator: function %+v not regiseterd", callback))
	}
	delete(comparators, argType)
	comparatorMu.Unlock()
}

func findComparator(typ reflect.Type) (reflect.Value, bool) {
	comparatorMu.Lock()
	v, ok := comparators[typ]
	comparatorMu.Unlock()
	return v, ok
}

type compareResult int

const (
	cLT compareResult = iota
	cEQ
	cGT
	cNEQ
)

// compare compares the given two values.
//
// - It returns an error if the values are not of the same type.
//
// - If operator '<' is defined for the values in Go, then this function returns
//   cLT, cEQ, cGT iff. x<y, x==y, x>y, respectively.
//
// - Otherwise, this function returns cEQ if x==y (using Go's '==' operator),
//   cNEQ else.

type visit struct {
	a1, a2 unsafe.Pointer
	typ    reflect.Type
}

func compare(x, y interface{}) (compareResult, error) {
	visited := map[visit]bool{}
	return compareRec(reflect.ValueOf(x), reflect.ValueOf(y), visited)
}

func compareRec(xv, yv reflect.Value, visited map[visit]bool) (compareResult, error) {
	if !xv.IsValid() {
		if yv.IsValid() {
			return cNEQ, nil
		}
		return cEQ, nil
	}
	xType := xv.Type()
	yType := yv.Type()
	if xType != yType {
		return cEQ, fmt.Errorf("%+v(type:%v) and %+v(type:%v) are not comparable", xv, xType, yv, yType)
	}

	comparator, ok := findComparator(xType)
	if ok {
		retval := comparator.Call([]reflect.Value{xv, yv})
		if !retval[1].IsNil() { // error?
			return 0, retval[1].Interface().(error)
		}
		v := retval[0].Int()
		switch {
		case v < 0:
			return cLT, nil
		case v == 0:
			return cEQ, nil
		default:
			return cGT, nil
		}
	}

	hard := func(k reflect.Kind) bool {
		return k == reflect.Map || k == reflect.Slice || k == reflect.Ptr || k == reflect.Interface
	}
	if xv.CanAddr() && yv.CanAddr() && hard(xv.Kind()) {
		xaddr := unsafe.Pointer(xv.UnsafeAddr())
		yaddr := unsafe.Pointer(yv.UnsafeAddr())
		if uintptr(xaddr) > uintptr(yaddr) {
			// Canonicalize order to reduce number of entries in visited.
			// Assumes non-moving garbage collector.
			xaddr, yaddr = yaddr, xaddr
		}
		// Short circuit if references are already seen.
		typ := xv.Type()
		v := visit{xaddr, yaddr, typ}
		if visited[v] {
			return cEQ, nil
		}
		visited[v] = true
	}

	switch xType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		xi, yi := xv.Int(), yv.Int()
		if xi < yi {
			return cLT, nil
		}
		if xi > yi {
			return cGT, nil
		}
		return cEQ, nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		xi, yi := xv.Uint(), yv.Uint()
		if xi < yi {
			return cLT, nil
		}
		if xi > yi {
			return cGT, nil
		}
		return cEQ, nil
	case reflect.Uintptr:
		xi, yi := xv.Uint(), yv.Uint()
		if xi == yi {
			return cEQ, nil
		}
		return cNEQ, nil
	case reflect.Float32, reflect.Float64:
		xi, yi := xv.Float(), yv.Float()
		if xi < yi {
			return cLT, nil
		}
		if xi > yi {
			return cGT, nil
		}
		if xi == yi {
			return cEQ, nil
		}
		return cNEQ, nil // xi or yi is NaN
	case reflect.String:
		xi, yi := xv.String(), yv.String()
		if xi < yi {
			return cLT, nil
		}
		if xi > yi {
			return cGT, nil
		}
		return cEQ, nil
	case reflect.Complex64, reflect.Complex128:
		xi, yi := xv.Complex(), yv.Complex()
		if xi == yi {
			return cEQ, nil
		}
		return cNEQ, nil
	case reflect.Bool:
		xi, yi := xv.Bool(), yv.Bool()
		if xi == yi {
			return cEQ, nil
		}
		return cNEQ, nil
	case reflect.Slice, reflect.Array:
		if xv.Len() != yv.Len() {
			return cNEQ, nil
		}
		for i := 0; i < xv.Len(); i++ {
			c, err := compareRec(xv.Index(i), yv.Index(i), visited)
			if err != nil {
				return c, err
			}
			if c != cEQ {
				return cNEQ, nil
			}
		}
		return cEQ, nil
	case reflect.Interface:
		if xv.IsNil() || yv.IsNil() {
			if xv.IsNil() == yv.IsNil() {
				return cEQ, nil
			}
			return cNEQ, nil
		}
		return compareRec(xv.Elem(), yv.Elem(), visited)
	case reflect.Ptr:
		if xv.Pointer() == yv.Pointer() {
			return cEQ, nil
		}
		return compareRec(xv.Elem(), yv.Elem(), visited)
	case reflect.Chan:
		if xv.Pointer() == yv.Pointer() {
			return cEQ, nil
		}
		return cNEQ, nil
	case reflect.Struct:
		for i, n := 0, xv.NumField(); i < n; i++ {
			r, err := compareRec(xv.Field(i), yv.Field(i), visited)
			if r != cEQ || err != nil {
				return cNEQ, err
			}
		}
		return cEQ, nil
	case reflect.Map:
		if xv.IsNil() != yv.IsNil() {
			return cNEQ, nil
		}
		if xv.Len() != yv.Len() {
			return cNEQ, nil
		}
		if xv.Pointer() == yv.Pointer() {
			return cEQ, nil
		}
		for _, k := range xv.MapKeys() {
			val1 := xv.MapIndex(k)
			val2 := yv.MapIndex(k)
			if !val1.IsValid() || !val2.IsValid() {
				return cNEQ, nil
			}
			r, err := compareRec(xv.MapIndex(k), yv.MapIndex(k), visited)
			if r != cEQ || err != nil {
				return cNEQ, err
			}
		}
		return cEQ, nil
	case reflect.Func:
		if xv.IsNil() && yv.IsNil() {
			return cEQ, nil
		}
		return cNEQ, nil
	default:
		panic(fmt.Sprintf("Unsupported data type for EQ: %+v, %v", xv, xType))
	}
}
