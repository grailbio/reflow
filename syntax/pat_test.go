// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"

	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

type idents map[string]bool

func (ids idents) Contains(id string) bool { return ids[id] }

func TestRemove(t *testing.T) {
	pat := &Pat{
		Kind: PatTuple,
		List: []*Pat{
			{
				Kind:  PatIdent,
				Ident: "ok",
			},
			{
				Kind: PatList,
				List: []*Pat{
					{
						Kind:  PatIdent,
						Ident: "baz",
					},
				},
				Tail: &Pat{
					Kind:  PatIdent,
					Ident: "qux",
				},
			},
			{
				Kind: PatStruct,
				Fields: []PatField{
					{
						Name: "hello",
						Pat: &Pat{
							Kind:  PatIdent,
							Ident: "foo",
						},
					},
					{
						Name: "world",
						Pat: &Pat{
							Kind:  PatIdent,
							Ident: "bar",
						},
					},
				},
			},
			{
				Kind: PatVariant,
				Tag:  "A",
			},
			{
				Kind: PatVariant,
				Tag:  "B",
				Elem: &Pat{Kind: PatIdent, Ident: "quux"},
			},
		},
	}
	if got, want := pat.Idents(nil), []string{"ok", "baz", "qux", "foo", "bar", "quux"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	pat, removed := pat.Remove(idents{
		"ok":    true,
		"bar":   true,
		"baz":   true,
		"qux":   true,
		"quux":  true,
		"other": true,
	})
	if got, want := removed, []string{"ok", "baz", "qux", "bar", "quux"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := pat.Idents(nil), []string{"foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := fmt.Sprint(pat), "(_, [_, ..._], {hello:foo, world:_}, #A, #B(_))"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestBindValues(t *testing.T) {
	pat := &Pat{
		Kind: PatTuple,
		List: []*Pat{
			{
				Kind: PatList,
				List: []*Pat{
					{
						Kind:  PatIdent,
						Ident: "a",
					},
				},
				Tail: &Pat{
					Kind:  PatIdent,
					Ident: "bc",
				},
			},
			{
				Kind: PatStruct,
				Fields: []PatField{
					{
						Name: "f",
						Pat: &Pat{
							Kind:  PatIdent,
							Ident: "d",
						},
					},
				},
			},
			{
				Kind: PatVariant,
				Tag:  "A",
			},
			{
				Kind: PatVariant,
				Tag:  "B",
				Elem: &Pat{Kind: PatIdent, Ident: "e"},
			},
		},
	}
	v := values.Tuple{
		values.List{"va", "vb", "vc"},
		values.Struct{"f": "vd"},
		&values.Variant{Tag: "A"},
		&values.Variant{Tag: "B", Elem: "ve"},
	}
	env := values.NewEnv()
	if ok := pat.BindValues(env, v); !ok {
		t.Fatal("failed to bind valid value")
	}
	for _, c := range []struct {
		id string
		v  values.T
	}{
		{"a", "va"},
		{"bc", values.List{"vb", "vc"}},
		{"d", "vd"},
		{"e", "ve"},
	} {
		if got, want := env.Value(c.id), c.v; !values.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestMatchers(t *testing.T) {
	pat := &Pat{
		Kind: PatTuple,
		List: []*Pat{
			{
				Kind: PatList,
				List: []*Pat{
					{
						Kind:  PatIdent,
						Ident: "a",
					},
				},
				Tail: &Pat{
					Kind:  PatIdent,
					Ident: "bc",
				},
			},
			{
				Kind: PatStruct,
				Fields: []PatField{
					{
						Name: "f",
						Pat:  &Pat{Kind: PatIgnore},
					},
				},
			},
			{
				Kind: PatVariant,
				Tag:  "A",
			},
			{
				Kind: PatVariant,
				Tag:  "B",
				Elem: &Pat{Kind: PatIdent, Ident: "e"},
			},
		},
	}
	ms := pat.Matchers()
	// Expect a matcher for each PatIdent or PatIgnore.
	if got, want := len(ms), 5; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	var ids []string
	for _, m := range ms {
		if m.Ident != "" {
			ids = append(ids, m.Ident)
		}
	}
	sort.Strings(ids)
	if got, want := ids, []string{"a", "bc", "e"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	typ := types.Tuple(
		&types.Field{T: types.List(types.String)},
		&types.Field{
			T: types.Struct(&types.Field{Name: "f", T: types.String}),
		},
		&types.Field{
			T: types.Sum(
				&types.Variant{Tag: "A"},
				&types.Variant{Tag: "B", Elem: types.String},
			),
		},
		&types.Field{
			T: types.Sum(
				&types.Variant{Tag: "A"},
				&types.Variant{Tag: "B", Elem: types.String},
			),
		},
	)
	v := values.Tuple{
		values.List{"va", "vb", "vc"},
		values.Struct{"f": "vd"},
		&values.Variant{Tag: "A"},
		&values.Variant{Tag: "B", Elem: "ve"},
	}
	for _, m := range ms {
		var (
			v   values.T = v
			typ          = typ
			p            = m.Path()
			err error
		)
		for !p.Done() {
			v, typ, p, err = p.Match(v, typ)
			if err != nil {
				t.Fatalf("unexpected match failure %s", err)
			}
		}
		id := m.Ident
		switch {
		case id == "a":
			if got, want := v, "va"; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		case id == "b":
			if got, want := v, []string{"b", "c"}; !reflect.DeepEqual(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		case id == "e":
			if got, want := v, "ve"; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		case m.Kind == MatchValue && id == "": // Only one ignore pattern, so it is not ambiguous.
			if got, want := v, "vd"; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
		}
	}
}

// TestPatDigest verifies that digests can be computed for any pattern.
func TestPatDigest(t *testing.T) {
	const (
		N        = 1000
		MaxDepth = 4
	)
	var (
		pats = make(map[string]struct{})
		r    = rand.New(rand.NewSource(0))
	)
	for i := 0; i < N; i++ {
		var p *Pat
		for {
			// Generate random patterns until we get a new one.
			p = randPat(r, MaxDepth)
			pStr := p.String()
			if _, ok := pats[pStr]; !ok {
				pats[pStr] = struct{}{}
				break
			}
		}
		// Just make sure that a digest can be successfully computed.
		_ = p.Digest()
	}
}

// randPat returns a random *Pat with maxDepth of subpatterns.
func randPat(r *rand.Rand, maxDepth int) *Pat {
	if maxDepth == 1 {
		if r.Intn(2) == 0 {
			return &Pat{Kind: PatIgnore}
		}
		return &Pat{Kind: PatIdent, Ident: "x"}
	}
	k := PatKind(r.Intn(int(patMax)-1) + 1)
	switch k {
	case PatIdent:
		return &Pat{Kind: PatIdent, Ident: "x"}
	case PatTuple:
		var list []*Pat
		for i := 0; i < r.Intn(4)+1; i++ {
			list = append(list, randPat(r, maxDepth-1))
		}
		return &Pat{Kind: PatList, List: list}
	case PatList:
		var list []*Pat
		for i := 0; i < r.Intn(4)+1; i++ {
			list = append(list, randPat(r, maxDepth-1))
		}
		var tail *Pat
		if r.Intn(2) == 0 {
			tail = randPat(r, maxDepth-1)
		}
		return &Pat{Kind: PatTuple, List: list, Tail: tail}
	case PatStruct:
		var fields []PatField
		for i := 0; i < r.Intn(4)+1; i++ {
			fields = append(fields, PatField{
				Name: fmt.Sprintf("%c", 'a'+i),
				Pat:  randPat(r, maxDepth-1),
			})
		}
		return &Pat{Kind: PatStruct, Fields: fields}
	case PatVariant:
		tag := "A"
		var elem *Pat
		if r.Intn(2) == 0 {
			elem = randPat(r, maxDepth-1)
		}
		return &Pat{Kind: PatVariant, Tag: tag, Elem: elem}
	case PatIgnore:
		return &Pat{Kind: PatIgnore}
	}
	panic(fmt.Sprintf("unhandled PatKind: %v", k))
}
