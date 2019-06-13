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
	"testing"

	"encoding/json"

	"strings"

	"github.com/grailbio/reflow"
)

var (
	k1 = reflow.AssertionKey{"blob", "s3://bucket/hello", "etag"}
	k2 = reflow.AssertionKey{"docker", "ubuntu", "version"}
	v  = reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k2: "v2", k1: "v1"})
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

func assertionKeySlice(size int) []reflow.AssertionKey {
	var keys []reflow.AssertionKey
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
		ak := reflow.AssertionKey{ns, su, o}
		keys = append(keys, ak)
	}
	return keys
}

func TestAssertionKeyLess(t *testing.T) {
	for i := 0; i < 10; i++ {
		keys := assertionKeySlice(100)
		sort.Slice(keys, func(i, j int) bool { return keys[i].Less(keys[j]) })
		if !sort.SliceIsSorted(keys, func(i, j int) bool {
			return keys[i].Namespace+keys[i].Subject+keys[i].Object <
				keys[j].Namespace+keys[j].Subject+keys[j].Object
		}) {
			t.Fatalf("not sorted: %v", keys)
		}
	}
}

func TestAssertionsAddToNil(t *testing.T) {
	func() {
		var a *reflow.Assertions
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic")
			}
		}()
		a.AddFrom(reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}))
	}()
}

func TestAssertionsAdd(t *testing.T) {
	tests := []struct {
		s, t, w *reflow.Assertions
		we      bool
	}{
		// {nil, nil, nil, false},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}), nil,
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}), false},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v2"}), nil, true},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"}), false},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k2: "v2"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"}), false},
	}
	for _, tt := range tests {
		gotE := tt.s.AddFrom(tt.t)
		if tt.we != (gotE != nil) {
			t.Errorf("got error %v, want error: %v", gotE, tt.we)
		}
		if tt.we {
			continue
		}
		if got, want := tt.s, tt.w; !got.Equal(want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestAssertionsDigest(t *testing.T) {
	// Hard-coded digest ensures that assertion digesting logic doesn't change.
	// Changing this cause reflow cached results to become invalidated.
	const assertionsDigest = "sha256:91ff89b8a50fc7e2d35311f6a1bb1e2d9be9d2aab4c62795588dc951b33321e8"
	var an1 *reflow.Assertions
	an2 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{})
	a1 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"})
	a2 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"})
	a3 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"})
	if got, want := v.Digest().String(), assertionsDigest; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	tests := []struct {
		a, b *reflow.Assertions
		we   bool
	}{
		{an1, an2, true},
		{a1, a2, true},
		{an1, a1, false},
		{an2, a2, false},
		{a3, v, false},
	}
	for _, tt := range tests {
		if gotD, wantD := tt.a.Digest(), tt.b.Digest(); tt.we != (gotD == wantD) {
			t.Errorf("%v == %v : got %v, want %v", tt.a, tt.b, gotD == wantD, tt.we)
		}
	}
}

func TestAssertionsEqual(t *testing.T) {
	var an1 *reflow.Assertions
	an2 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{})
	a1 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"})
	a2 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"})
	a3 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"})
	tests := []struct {
		a, b *reflow.Assertions
		w    bool
	}{
		{an1, an2, true},
		{a1, a2, true},
		{an1, a1, false},
		{a1, an1, false},
		{an2, a2, false},
		{a2, an2, false},
		{a3, v, false},
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
		{nil, reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}),
			nil, []reflow.AssertionKey{k1}},
		{reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}),
			nil, nil, nil},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v2"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}),
			nil},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v", k2: "v"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"}),
			nil},
		{
			nil, reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v", k2: "v"}),
			nil, []reflow.AssertionKey{k1, k2}},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k2: "v1"}),
			new(reflow.Assertions), []reflow.AssertionKey{k2}},
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
		{reflow.AssertionsFromMap(map[reflow.AssertionKey]string{}), true},
		{reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}), false},
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
		{reflow.AssertionsFromMap(map[reflow.AssertionKey]string{}), []byte(`[]`)},
		{reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}), []byte(`[{"Key":{"Namespace":"blob","Subject":"s3://bucket/hello","Object":"etag"},"Value":"v1"}]`)},
		{
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{
				reflow.AssertionKey{"b", "b", "b"}: "8",
				reflow.AssertionKey{"a", "a", "b"}: "2",
				reflow.AssertionKey{"b", "a", "a"}: "5",
				reflow.AssertionKey{"b", "b", "a"}: "7",
				reflow.AssertionKey{"a", "b", "b"}: "4",
				reflow.AssertionKey{"a", "a", "a"}: "1",
				reflow.AssertionKey{"b", "a", "b"}: "6",
				reflow.AssertionKey{"a", "b", "a"}: "3",
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
			t.Errorf("marshal(%s) %v", v, err)
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
		{[]byte(`[]`), reflow.AssertionsFromMap(map[reflow.AssertionKey]string{}), false},
		{[]byte(`[{"Key":{"Namespace":"blob","Subject":"s3://bucket/hello","Object":"etag"},"Value":"v1"}]`),
			reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"}), false},
		{[]byte(`[{"Key":{"Namespace":"blob","Subject":"s3://bucket/hello","Object":"etag"},"Value":"v1"},{"Key":{"Namespace":"blob","Subject":"s3://bucket/hello","Object":"etag"},"Value":"v2"}]`),
			nil, true},
	}
	for _, tt := range tests {
		var got reflow.Assertions
		if err := json.Unmarshal(tt.b, &got); tt.we != (err != nil) {
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

func TestAssertionsPrettyDiff(t *testing.T) {
	an := new(reflow.Assertions)
	a1 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"})
	a2 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v2"})
	a3 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k2: "v1"})
	tests := []struct {
		a, b *reflow.Assertions
		w    string
	}{
		{a1, nil, ""},
		{a1, an, ""},
		{nil, a2, fmt.Sprintf("extra: %s=%s", k1, "v2")},
		{an, a2, fmt.Sprintf("extra: %s=%s", k1, "v2")},
		{a1, a1, ""},
		{a1, a3, fmt.Sprintf("conflict %s: %s -> %s", k2, "v2", "v1")},
		{a3, a1, strings.Join([]string{fmt.Sprintf("conflict %s: %s -> %s", k2, "v1", "v2"), fmt.Sprintf("extra: %s=%s", k1, "v1")}, "\n")},
		{a1, a2, fmt.Sprintf("conflict %s: %s -> %s", k1, "v1", "v2")},
		{a2, a3, fmt.Sprintf("extra: %s=%s", k2, "v1")},
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
		{nil, "empty", "empty"},
		{reflow.AssertionsFromMap(map[reflow.AssertionKey]string{}), "empty", "empty"},
		{v, "2 (91ff89b8)", "blob s3://bucket/hello etag=v1, docker ubuntu version=v2"},
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

type testGenerator struct{}

func (t testGenerator) Generate(ctx context.Context, key reflow.GeneratorKey) (*reflow.Assertions, error) {
	switch key.Namespace {
	case "a":
		return reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"a", key.Subject, "tag"}: "a"}), nil
	case "b":
		return reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"b", key.Subject, "tag"}: "b"}), nil
	case "e":
		return nil, fmt.Errorf("error")
	}
	return nil, fmt.Errorf("namespace error")
}

func TestAssertionGeneratorMux(t *testing.T) {
	tg := testGenerator{}
	am := reflow.AssertionGeneratorMux{"a": tg, "b": tg, "e": tg}
	tests := []struct {
		key reflow.GeneratorKey
		w   *reflow.Assertions
		we  error
	}{
		{reflow.GeneratorKey{"s", "unknown"}, nil, fmt.Errorf("namespace error")},
		{reflow.GeneratorKey{"s", "a"}, reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"a", "s", "tag"}: "a"}), nil},
		{reflow.GeneratorKey{"s", "b"}, reflow.AssertionsFromMap(map[reflow.AssertionKey]string{{"b", "s", "tag"}: "b"}), nil},
		{reflow.GeneratorKey{"s", "e"}, nil, fmt.Errorf("error")},
	}
	for _, tt := range tests {
		got, gotE := am.Generate(context.Background(), tt.key)
		if got.Digest() != tt.w.Digest() || (gotE == nil && tt.we != nil) || (gotE != nil && tt.we == nil) {
			t.Errorf("got %v, want %v, gotE: %v, wantE: %v", got, tt.w, gotE, tt.we)
		}
	}
}

func TestAssertionsAssertExact(t *testing.T) {
	an := new(reflow.Assertions)
	a1 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1", k2: "v2"})
	a2 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v2"})
	a3 := reflow.AssertionsFromMap(map[reflow.AssertionKey]string{k1: "v1"})
	tests := []struct {
		src, tgt *reflow.Assertions
		w        bool
	}{
		{a1, nil, true},
		{a1, an, true},
		{nil, a2, false},
		{an, a2, false},
		{a1, a1, true},
		{a1, a3, true},
		{a3, a1, false},
		{a1, a2, false},
		{a2, a3, false},
	}
	for _, tt := range tests {
		if got, want := reflow.AssertExact(context.Background(), tt.src, tt.tgt), tt.w; got != want {
			t.Errorf("AssertExact(%v, %v): got %v, want %v", tt.src, tt.tgt, got, want)
		}
	}
}

func lessConcat(a, b reflow.AssertionKey) bool {
	as := strings.Join([]string{a.Namespace, a.Subject, a.Object}, " ")
	bs := strings.Join([]string{b.Namespace, b.Subject, b.Object}, " ")
	return as < bs
}

func lessCompare(a, b reflow.AssertionKey) bool {
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
			lessFn func(a, b reflow.AssertionKey) bool
		}{
			{fmt.Sprintf("Concat_N%d", size), lessConcat},
			{fmt.Sprintf("Compare_N%d", size), lessCompare},
		} {
			b.Run(s.name, func(b *testing.B) {
				allKeys := make([][]reflow.AssertionKey, b.N)
				for i := 0; i < b.N; i++ {
					allKeys[i] = assertionKeySlice(size)
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
