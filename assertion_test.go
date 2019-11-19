// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/test/testutil"

	"encoding/json"

	"strings"

	"github.com/grailbio/reflow"
)

var (
	k1     = reflow.AssertionKey{"s3://bucket/hello", "blob"}
	k2     = reflow.AssertionKey{"ubuntu", "docker"}
	k1v1   = map[string]string{"etag": "v1"}
	k2v2   = map[string]string{"version": "v2"}
	anil   *reflow.Assertions
	aempty = new(reflow.Assertions)
	a1     = reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2})
	a2     = reflow.AssertionsFromEntry(k1, k1v1)
)

const minLen = 10
const maxLen = 100
const chars = "abcdefghijklmnopqrstuvwxyxABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randString() string {
	l := minLen + rand.Intn(maxLen-minLen+1)
	b := make([]byte, l)
	for i := 0; i < l; i++ {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func legacyAssertionKeySlice(size int) []reflow.LegacyAssertionKey {
	var keys []reflow.LegacyAssertionKey
	ns, su, o := randString(), randString(), randString()
	for k := 0; k < size; k++ {
		if rand.Intn(10) < 2 {
			ns = randString()
		}
		if rand.Intn(10) < 8 {
			su = randString()
		}
		if rand.Intn(10) < 2 {
			o = randString()
		}
		ak := reflow.LegacyAssertionKey{ns, su, o}
		keys = append(keys, ak)
	}
	return keys
}

func TestLegacyAssertionKeyLess(t *testing.T) {
	for i := 0; i < 10; i++ {
		keys := legacyAssertionKeySlice(100)
		sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })
		if !sort.SliceIsSorted(keys, func(i, j int) bool {
			return keys[i].Namespace+keys[i].Subject+keys[i].Object <
				keys[j].Namespace+keys[j].Subject+keys[j].Object
		}) {
			t.Fatalf("not sorted: %v", keys)
		}
	}
}

func assertionKeySlice(size int) []reflow.AssertionKey {
	var keys []reflow.AssertionKey
	ns, su := randString(), randString()
	for k := 0; k < size; k++ {
		if rand.Intn(10) < 2 {
			ns = randString()
		}
		if rand.Intn(10) < 8 {
			su = randString()
		}
		gk := reflow.AssertionKey{su, ns}
		keys = append(keys, gk)
	}
	return keys
}

func TestAssertionKeyLess(t *testing.T) {
	for i := 0; i < 10; i++ {
		keys := assertionKeySlice(100)
		sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })
		if !sort.SliceIsSorted(keys, func(i, j int) bool {
			return keys[i].Namespace+keys[i].Subject < keys[j].Namespace+keys[j].Subject
		}) {
			t.Fatalf("not sorted: %v", keys)
		}
	}
}

func TestAssertionsMerge(t *testing.T) {
	tests := []struct {
		s, t, w *reflow.Assertions
		we      bool
	}{
		{
			reflow.AssertionsFromEntry(k1, k1v1),
			nil,
			reflow.AssertionsFromEntry(k1, k1v1),
			false,
		},

		{
			reflow.AssertionsFromEntry(k1, k1v1),
			reflow.AssertionsFromEntry(k1, map[string]string{"etag": "v2"}),
			nil,
			true,
		},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2}),
			false,
		},
		{
			reflow.AssertionsFromEntry(k1, k1v1),
			reflow.AssertionsFromEntry(k2, k2v2),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2}),
			false,
		},
	}
	for _, tt := range tests {
		got, gotE := reflow.MergeAssertions(tt.s, tt.t)
		if tt.we != (gotE != nil) {
			t.Errorf("got error %v, want error?=%v", gotE, tt.we)
		}
		if tt.we {
			continue
		}
		if want := tt.w; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestAssertionsDigest(t *testing.T) {
	// Hard-coded digest ensures that assertion digesting logic doesn't change.
	// Changing this will cause cached results to become invalidated.
	const assertionsDigest = "sha256:91ff89b8a50fc7e2d35311f6a1bb1e2d9be9d2aab4c62795588dc951b33321e8"
	if got, want := a1.Digest().String(), assertionsDigest; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	a1copy := reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2})
	tests := []struct {
		a, b *reflow.Assertions
		we   bool
	}{
		{anil, aempty, true},
		{a1, a1copy, true},
		{anil, a1, false},
		{aempty, a1copy, false},
		{a2, a1, false},
	}
	for _, tt := range tests {
		if gotD, wantD := tt.a.Digest(), tt.b.Digest(); tt.we != (gotD == wantD) {
			t.Errorf("%v == %v : got %v, want %v", tt.a, tt.b, gotD == wantD, tt.we)
		}
	}
}

func TestAssertionsEqual(t *testing.T) {
	a1copy := reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2})
	tests := []struct {
		a, b *reflow.Assertions
		w    bool
	}{
		{anil, nil, true},
		{nil, anil, true},
		{aempty, anil, true},
		{anil, aempty, true},
		{a1, a1copy, true},
		{a1copy, a1, true},
		{aempty, a1, false},
		{a1, anil, false},
		{aempty, a1copy, false},
		{a1copy, aempty, false},
		{a2, a1, false},
		{a1, a2, false},
	}
	for _, tt := range tests {
		if got, want := tt.a.Equal(tt.b), tt.w; got != want {
			t.Errorf("%v == %v : got %v, want %v", tt.a, tt.b, got, want)
		}
	}
}

func TestAssertionsFilter(t *testing.T) {
	tests := []struct {
		s, t, w *reflow.Assertions
		wk      []reflow.AssertionKey
	}{
		{
			reflow.AssertionsFromEntry(k1, k1v1),
			nil,
			nil,
			nil,
		},
		{
			reflow.AssertionsFromEntry(k1, k1v1),
			reflow.AssertionsFromEntry(k1, map[string]string{"etag": "v2"}),
			reflow.AssertionsFromEntry(k1, k1v1),
			nil,
		},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: {"etag": "v"}, k2: {"version": "v"}}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2}),
			nil,
		},
		{
			reflow.AssertionsFromEntry(k1, k1v1),
			reflow.AssertionsFromEntry(k2, map[string]string{"version": "v1"}),
			new(reflow.Assertions),
			[]reflow.AssertionKey{k2},
		},
	}
	for _, tt := range tests {
		gotA, gotK := tt.s.Filter(tt.t)
		if got, want := gotA, tt.w; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := gotK, tt.wk; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestAssertionsIsEmpty(t *testing.T) {
	tests := []struct {
		a *reflow.Assertions
		w bool
	}{
		{nil, true},
		{reflow.NewAssertions(), true},
		{reflow.AssertionsFromEntry(k1, k1v1), false},
	}
	for _, tt := range tests {
		if got, want := tt.a.IsEmpty(), tt.w; got != want {
			t.Errorf("IsEmpty(%v) got %v, want %v", tt.a, got, want)
		}
	}
}

func TestAssertionsMarshal(t *testing.T) {
	tests := []struct {
		a *reflow.Assertions
		w []byte
	}{
		{
			reflow.NewAssertions(),
			[]byte(`[]`),
		},
		{
			reflow.AssertionsFromEntry(k1, k1v1),
			[]byte(`[{"Key":{"Namespace":"blob","Subject":"s3://bucket/hello","Object":"etag"},"Value":"v1"}]`),
		},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{
				reflow.AssertionKey{"b", "b"}: {"b": "8", "a": "7"},
				reflow.AssertionKey{"a", "a"}: {"b": "2", "a": "1"},
				reflow.AssertionKey{"a", "b"}: {"a": "5", "b": "6"},
				reflow.AssertionKey{"b", "a"}: {"b": "4", "a": "3"},
			}),
			[]byte("[" + strings.Join([]string{
				`{"Key":{"Namespace":"a","Subject":"a","Object":"a"},"Value":"1"}`,
				`{"Key":{"Namespace":"a","Subject":"a","Object":"b"},"Value":"2"}`,
				`{"Key":{"Namespace":"a","Subject":"b","Object":"a"},"Value":"3"}`,
				`{"Key":{"Namespace":"a","Subject":"b","Object":"b"},"Value":"4"}`,
				`{"Key":{"Namespace":"b","Subject":"a","Object":"a"},"Value":"5"}`,
				`{"Key":{"Namespace":"b","Subject":"a","Object":"b"},"Value":"6"}`,
				`{"Key":{"Namespace":"b","Subject":"b","Object":"a"},"Value":"7"}`,
				`{"Key":{"Namespace":"b","Subject":"b","Object":"b"},"Value":"8"}`,
			}, ",") + "]"),
		},
	}
	for _, tt := range tests {
		b, err := json.Marshal(tt.a)
		if err != nil {
			t.Errorf("marshal(%s) %v", a1, err)
		}
		if got, want := string(b), string(tt.w); got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestAssertionsUnmarshal(t *testing.T) {
	tests := []struct {
		b    []byte
		want *reflow.Assertions
		we   bool
	}{
		{
			[]byte(`[]`),
			reflow.NewAssertions(),
			false,
		},
		{
			[]byte(`[{"Key":{"Namespace":"blob","Subject":"s3://bucket/hello","Object":"etag"},"Value":"v1"}]`),
			reflow.AssertionsFromEntry(k1, k1v1),
			false,
		},
		{
			[]byte(`[{"Key":{"Namespace":"blob","Subject":"s3://bucket/hello","Object":"etag"},"Value":"v1"},{"Key":{"Namespace":"blob","Subject":"s3://bucket/hello","Object":"etag"},"Value":"v2"}]`),
			nil,
			true,
		},
	}
	for _, tt := range tests {
		got := new(reflow.Assertions)
		if err := json.Unmarshal(tt.b, got); tt.we != (err != nil) {
			t.Errorf("unmarshal got %v, want error: %v ", err, tt.we)
		}
		if tt.we {
			continue
		}
		if !got.Equal(tt.want) {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
}

func TestAssertionsJson(t *testing.T) {
	for i := 0; i < 10; i++ {
		a := createAssertions(100)
		bytes, _ := a.MarshalJSON()
		got := new(reflow.Assertions)
		if err := json.Unmarshal(bytes, got); err != nil {
			t.Error(err)
		}
		if !got.Equal(a) {
			t.Error(fmt.Errorf("unmarshal(marshal(%v)) ", a))
		}
	}
}

func TestAssertionsPrettyDiff(t *testing.T) {
	a1 = reflow.AssertionsFromMap(map[reflow.AssertionKey]map[string]string{k1: k1v1, k2: k2v2})
	a2 := reflow.AssertionsFromEntry(k1, map[string]string{"etag": "v2"})
	a3 := reflow.AssertionsFromEntry(k2, map[string]string{"version": "v1"})
	tests := []struct {
		a, b *reflow.Assertions
		w    string
	}{
		{a1, nil, ""},
		{a1, aempty, ""},
		{nil, a2, fmt.Sprintf("extra: %s: etag=v2", k1)},
		{aempty, a2, fmt.Sprintf("extra: %s: etag=v2", k1)},
		{a1, a1, ""},
		{a1, a3, fmt.Sprintf("conflict %s: version (%s -> %s)", k2, "v2", "v1")},
		{a3, a1, strings.Join([]string{fmt.Sprintf("conflict %s: version (%s -> %s)", k2, "v1", "v2"), fmt.Sprintf("extra: %s: etag=%s", k1, "v1")}, "\n")},
		{a1, a2, fmt.Sprintf("conflict %s: etag (%s -> %s)", k1, "v1", "v2")},
		{a2, a3, fmt.Sprintf("extra: %s: version=%s", k2, "v1")},
	}
	for _, tt := range tests {
		if got, want := tt.a.PrettyDiff(tt.b), tt.w; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestAssertionsString(t *testing.T) {
	tests := []struct {
		as    *reflow.Assertions
		sh, f string
	}{
		{nil, "empty", "empty"}, {aempty, "empty", "empty"},
		{a1, "#2", "blob s3://bucket/hello etag=v1, docker ubuntu version=v2"},
	}
	for _, tt := range tests {
		if got, want := tt.as.Short(), tt.sh; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.as.String(), tt.f; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

// callMethods repeatedly calls various methods on the given assertions objects until the given context is cancelled.
func callMethods(ctx context.Context, all, a *reflow.Assertions) {
	done := false
	for !done {
		a.IsEmpty()
		a.Digest()
		_ = a.String()
		all.PrettyDiff(a)
		all.Equal(a)
		bytes, _ := a.MarshalJSON()
		_ = json.Unmarshal(bytes, new(reflow.Assertions))
		select {
		case <-ctx.Done():
			done = true
		default:
		}
	}
}

func TestAssertionsConcurrency(t *testing.T) {
	const N = 10
	list := make([]*reflow.Assertions, N)
	for i := 0; i < N; i++ {
		list[i] = createAssertions(100)
	}
	fuzz := testutil.NewFuzz(nil)
	err := traverse.Each(1000, func(i int) error {
		i = i % N
		a, b, c := list[i], list[N-i-1], list[fuzz.Intn(N)]
		all := reflow.NewAssertions()
		ctx, cancel := context.WithCancel(context.Background())
		go callMethods(ctx, all, a)
		go callMethods(ctx, all, b)
		go callMethods(ctx, all, c)
		var wg sync.WaitGroup
		wg.Add(3)
		for _, a := range []*reflow.Assertions{a, b, c} {
			a := a
			go func() {
				_ = all.AddFrom(a)
				wg.Done()
			}()
		}
		wg.Wait()
		cancel()
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

type testGenerator struct{}

func (t testGenerator) Generate(ctx context.Context, key reflow.AssertionKey) (*reflow.Assertions, error) {
	switch key.Namespace {
	case "a":
		return reflow.AssertionsFromEntry(key, map[string]string{"tag": "a"}), nil
	case "b":
		return reflow.AssertionsFromEntry(key, map[string]string{"tag": "b"}), nil
	case "e":
		return nil, fmt.Errorf("error")
	}
	return nil, fmt.Errorf("namespace error")
}

func TestAssertionGeneratorMux(t *testing.T) {
	tg := testGenerator{}
	am := reflow.AssertionGeneratorMux{"a": tg, "b": tg, "e": tg}
	tests := []struct {
		key reflow.AssertionKey
		w   *reflow.Assertions
		we  error
	}{
		{reflow.AssertionKey{"s", "unknown"}, nil, fmt.Errorf("namespace error")},
		{reflow.AssertionKey{"s", "a"}, reflow.AssertionsFromEntry(reflow.AssertionKey{"s", "a"}, map[string]string{"tag": "a"}), nil},
		{reflow.AssertionKey{"s", "b"}, reflow.AssertionsFromEntry(reflow.AssertionKey{"s", "b"}, map[string]string{"tag": "b"}), nil},
		{reflow.AssertionKey{"s", "e"}, nil, fmt.Errorf("error")},
	}
	for _, tt := range tests {
		got, gotE := am.Generate(context.Background(), tt.key)
		if (gotE == nil && tt.we != nil) || (gotE != nil && tt.we == nil) || got.Digest() != tt.w.Digest() {
			t.Errorf("got %v, want %v, gotE: %v, wantE: %v", got, tt.w, gotE, tt.we)
		}
	}
}

func TestAssertionsAssertExact(t *testing.T) {
	an := new(reflow.Assertions)
	a2 := reflow.AssertionsFromEntry(k1, map[string]string{"etag": "v2"})
	a3 := reflow.AssertionsFromEntry(k2, map[string]string{"version": "v1"})
	tests := []struct {
		src, tgt *reflow.Assertions
		w        bool
	}{
		{a1, nil, true},
		{a1, an, true},
		{nil, a2, false},
		{an, a2, false},
		{a1, a1, true},
		{a1, a3, false},
		{a3, a1, false},
		{a1, a2, false},
		{a2, a3, false},
		{a1, reflow.AssertionsFromEntry(k1, k1v1), true},
		{a1, reflow.AssertionsFromEntry(k2, k2v2), true},
	}
	for _, tt := range tests {
		if got, want := reflow.AssertExact(context.Background(), []*reflow.Assertions{tt.src}, []*reflow.Assertions{tt.tgt}), tt.w; got != want {
			t.Errorf("AssertExact(%v, %v): got %v, want %v", tt.src, tt.tgt, got, want)
		}
	}
}

func lessConcat(a, b reflow.LegacyAssertionKey) bool {
	as := strings.Join([]string{a.Namespace, a.Subject, a.Object}, " ")
	bs := strings.Join([]string{b.Namespace, b.Subject, b.Object}, " ")
	return as < bs
}

func lessCompare(a, b reflow.LegacyAssertionKey) bool {
	if a.Namespace == b.Namespace {
		if a.Subject == b.Subject {
			return a.Object < b.Object
		}
		return a.Subject < b.Subject
	}
	return a.Namespace < b.Namespace
}

func BenchmarkSort(b *testing.B) {
	for _, size := range []int{100, 1000, 10000} {
		for _, s := range []struct {
			name   string
			lessFn func(a, b reflow.LegacyAssertionKey) bool
		}{
			{fmt.Sprintf("Concat_N%d", size), lessConcat},
			{fmt.Sprintf("Compare_N%d", size), lessCompare},
		} {
			b.Run(s.name, func(b *testing.B) {
				allKeys := make([][]reflow.LegacyAssertionKey, b.N)
				for i := 0; i < b.N; i++ {
					allKeys[i] = legacyAssertionKeySlice(size)
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					keys := allKeys[i]
					sort.Slice(keys, func(i, j int) bool { return s.lessFn(keys[i], keys[j]) })
				}
			})
		}
	}
}
