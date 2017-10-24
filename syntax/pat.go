// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"fmt"
	"io"
	"strings"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/internal/scanner"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// PatKind is the kind of pattern.
type PatKind int

const (
	// PatError is an erroneous pattern
	// (e.g., uninitialized or parse error).
	PatError PatKind = iota
	// PatIdent is an identifier pattern.
	PatIdent
	// PatTuple is a tuple pattern.
	PatTuple
	// PatList is a list pattern.
	PatList
	// PatStruct is a struct pattern.
	PatStruct
	// PatIgnore is an ignore pattern.
	PatIgnore
)

// A Pat stores a pattern tree used in destructuring operations.
// Patterns can bind type and value environments. They can also
// produce matchers that can be used to selectively match and bind
// identifiers.
type Pat struct {
	scanner.Position

	Kind PatKind

	Ident string
	List  []*Pat

	Map map[string]*Pat
}

// Equal tells whether pattern p is equal to pattern q.
func (p *Pat) Equal(q *Pat) bool {
	if p.Kind != q.Kind {
		return false
	}
	if p.Ident != q.Ident {
		return false
	}
	if len(p.List) != len(q.List) {
		return false
	}
	for i := range p.List {
		if !p.List[i].Equal(q.List[i]) {
			return false
		}
	}

	keys := map[string]bool{}
	for k := range p.Map {
		keys[k] = true
	}
	for k := range q.Map {
		delete(keys, k)
	}
	if len(keys) > 0 {
		return false
	}
	for k, v := range p.Map {
		w := q.Map[k]
		if w == nil {
			return false
		}
		if !v.Equal(w) {
			return false
		}
	}
	return true
}

// Debug prints the pattern's AST for debugging.
func (p *Pat) Debug() string {
	switch p.Kind {
	default:
		panic("bad pat")
	case PatIdent:
		return "ident(" + p.Ident + ")"
	case PatTuple:
		pats := make([]string, len(p.List))
		for i, q := range p.List {
			pats[i] = q.Debug()
		}
		return "tuple(" + strings.Join(pats, ", ") + ")"
	case PatList:
		pats := make([]string, len(p.List))
		for i, q := range p.List {
			pats[i] = q.Debug()
		}
		return "list(" + strings.Join(pats, ", ") + ")"
	case PatStruct:
		var pats []string
		for id, q := range p.Map {
			pats = append(pats, id+":"+q.Debug())
		}
		return "struct(" + strings.Join(pats, ", ") + ")"
	case PatIgnore:
		return "ignore"
	}
}

// String prints a parseable representation of the pattern.
func (p *Pat) String() string {
	switch p.Kind {
	default:
		panic("bad pat")
	case PatIdent:
		return p.Ident
	case PatTuple:
		pats := make([]string, len(p.List))
		for i, q := range p.List {
			pats[i] = q.String()
		}
		return "(" + strings.Join(pats, ", ") + ")"
	case PatList:
		pats := make([]string, len(p.List))
		for i, q := range p.List {
			pats[i] = q.String()
		}
		return "[" + strings.Join(pats, ", ") + "]"
	case PatStruct:
		var pats []string
		for id, q := range p.Map {
			pats = append(pats, id+":"+q.String())
		}
		return "{" + strings.Join(pats, ", ") + "}"
	case PatIgnore:
		return "_"
	}
}

// Idents appends the pattern's bound identifiers to the passed slice.
func (p *Pat) Idents(ids []string) []string {
	switch p.Kind {
	default:
		panic("bad pat")
	case PatIdent:
		return append(ids, p.Ident)
	case PatTuple, PatList:
		for _, p := range p.List {
			ids = p.Idents(ids)
		}
		return ids
	case PatStruct:
		for id := range p.Map {
			ids = append(ids, id)
		}
		return ids
	case PatIgnore:
		return ids
	}
}

// BindTypes binds the pattern's identifier's types in the passed environment,
// given the type of binding value t.
func (p *Pat) BindTypes(env *types.Env, t *types.T) error {
	switch p.Kind {
	default:
		panic("bad pat")
	case PatIdent:
		env.Bind(p.Ident, t)
		return nil
	case PatTuple:
		if t.Kind != types.TupleKind {
			return fmt.Errorf("expected tuple, got %s", t)
		}
		if got, want := len(p.List), len(t.Fields); got != want {
			return fmt.Errorf("expected tuple of size %d, got %d (%s)", want, got, t)
		}
		for i, q := range p.List {
			if err := q.BindTypes(env, t.Fields[i].T); err != nil {
				return err
			}
		}
		return nil
	case PatList:
		if t.Kind != types.ListKind {
			return fmt.Errorf("expected list, got %s", t)
		}
		for _, q := range p.List {
			if err := q.BindTypes(env, t.Elem); err != nil {
				return err
			}
		}
		return nil
	case PatStruct:
		if t.Kind != types.StructKind {
			return fmt.Errorf("expected struct, got %s", t)
		}
		fm := t.FieldMap()
		for id, q := range p.Map {
			u := fm[id]
			if u == nil {
				return fmt.Errorf("struct %s does not have field %s", t, id)
			}
			if err := q.BindTypes(env, u); err != nil {
				return err
			}
		}
		return nil
	case PatIgnore:
		return nil
	}
}

// BindValues binds this pattern's values in the given value environment.
func (p *Pat) BindValues(env *values.Env, v values.T) bool {
	switch p.Kind {
	default:
		panic("bad pat")
	case PatIdent:
		env.Bind(p.Ident, v)
		return true
	case PatTuple:
		tup := v.(values.Tuple)
		for i := range p.List {
			if !p.BindValues(env, tup[i]) {
				return false
			}
		}
		return true
	case PatList:
		list := v.(values.List)
		if len(list) != len(p.List) {
			return false
		}
		for i := range p.List {
			if !p.BindValues(env, list[i]) {
				return false
			}
		}
		return true
	case PatStruct:
		s := v.(values.Struct)
		for id, q := range p.Map {
			if !q.BindValues(env, s[id]) {
				return false
			}
		}
		return true
	case PatIgnore:
		return true
	}
}

// Matchers returns a map of matchers representing this pattern.
func (p *Pat) Matchers() map[string]*Matcher {
	m := map[string]*Matcher{}
	p.matchers(m, nil)
	return m
}

func (p *Pat) matchers(m map[string]*Matcher, parent *Matcher) {
	switch p.Kind {
	default:
		panic("bad pat")
	case PatIdent:
		m[p.Ident] = &Matcher{Kind: MatchValue, Parent: parent}
	case PatTuple:
		for i, q := range p.List {
			q.matchers(m, &Matcher{Kind: MatchTuple, Index: i, Parent: parent})
		}
	case PatList:
		for i, q := range p.List {
			q.matchers(m, &Matcher{Kind: MatchList, Index: i, Parent: parent})
		}
	case PatStruct:
		for id, q := range p.Map {
			q.matchers(m, &Matcher{Kind: MatchStruct, Field: id, Parent: parent})
		}
	case PatIgnore:
	}
}

// MatchKind is the kind of match performed by a Matcher.
type MatchKind int

const (
	// MatchError is an erroneous matcher.
	MatchError MatchKind = iota
	// MatchValue matches a value.
	MatchValue
	// MatchTuple indexes a tuple.
	MatchTuple
	// MatchList indexes a list.
	MatchList
	// MatchStruct indexes a struct
	MatchStruct
)

// A Matcher binds individual pattern components (identifiers)
// in a pattern.  Matchers form a tree; their interpretation
// (through method Match) performs value destructuring.
type Matcher struct {
	// Kind is the kind of matcher.
	Kind MatchKind
	// Index is the index of the match (MatchTuple, MatchList).
	Index int
	// Parent is this matcher's parent.
	Parent *Matcher
	// Field holds a struct field (MatchStruct).
	Field string
}

// Path constructs a path from this matcher. The path may be used
// to simultaneously deconstruct a value and type.
func (m *Matcher) Path() Path {
	var path Path
	for m != nil {
		path = append(path, m)
		m = m.Parent
	}
	for i := len(path)/2 - 1; i >= 0; i-- {
		j := len(path) - 1 - i
		path[i], path[j] = path[j], path[i]
	}
	return path
}

// Path represents a path to a value.
type Path []*Matcher

// Match performs single step deconstruction of a type and value.
// It returns the next level; terminating when len(Path) == 0.
func (p Path) Match(v values.T, t *types.T) (values.T, *types.T, bool, Path) {
	if len(p) == 0 {
		panic("bad path")
	}
	m, p := p[0], p[1:]
	switch m.Kind {
	default:
		panic("bad path")
	case MatchValue:
		return v, t, true, p
	case MatchTuple:
		return v.(values.Tuple)[m.Index], t.Fields[m.Index].T, true, p
	case MatchList:
		l := v.(values.List)
		if m.Index >= len(l) {
			return nil, t.Elem, false, p
		}
		return l[m.Index], t.Elem, true, p
	case MatchStruct:
		return v.(values.Struct)[m.Field], t.Field(m.Field), true, p
	}
}

// Digest returns a digest representing this path.
func (p Path) Digest() digest.Digest {
	w := reflow.Digester.NewWriter()
	io.WriteString(w, "grail.com/reflow/syntax.Path")
	for _, m := range p {
		writeN(w, int(m.Kind))
		switch m.Kind {
		default:
			panic("bad matcher")
		case MatchValue:
		case MatchTuple, MatchList:
			writeN(w, m.Index)
		case MatchStruct:
			io.WriteString(w, m.Field)
		}
	}
	return w.Digest()
}

// Done tells whether this path is complete.
func (p Path) Done() bool {
	return len(p) == 0
}
