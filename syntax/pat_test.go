// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"fmt"
	"reflect"
	"testing"

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
		},
	}
	pat, removed := pat.Remove(idents{
		"ok":    true,
		"bar":   true,
		"baz":   true,
		"qux":   true,
		"other": true,
	})
	if got, want := removed, []string{"ok", "baz", "qux", "bar"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := pat.Idents(nil), []string{"foo"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := fmt.Sprint(pat), "(_, [_, ..._], {hello:foo, world:_})"; got != want {
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
		},
	}
	v := values.Tuple{
		values.List{"va", "vb", "vc"},
		values.Struct{"f": "vd"},
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
	} {
		if got, want := env.Value(c.id), c.v; !values.Equal(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
