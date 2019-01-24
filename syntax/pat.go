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
	"github.com/grailbio/reflow/errors"
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

// A PatField stores a field entry in a pattern.
type PatField struct {
	Name string
	*Pat
}

// A Pat stores a pattern tree used in destructuring operations.
// Patterns can bind type and value environments. They can also
// produce matchers that can be used to selectively match and bind
// identifiers.
type Pat struct {
	scanner.Position

	Kind PatKind

	Ident string

	List []*Pat

	// Tail is the pattern to which to bind the tail of the list (PatList).  If
	// nil, the pattern will only match lists of exactly the same length as the
	// pattern.
	Tail *Pat

	Fields []PatField
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
	if (p.Tail == nil) != (q.Tail == nil) {
		// Only exactly one of them has a tail pattern.
		return false
	}
	if p.Tail != nil && q.Tail != nil && !p.Tail.Equal(q.Tail) {
		return false
	}
	var (
		pfields = p.FieldMap()
		qfields = q.FieldMap()
		keys    = make(map[string]bool)
	)
	for k := range pfields {
		keys[k] = true
	}
	for k := range qfields {
		delete(keys, k)
	}
	if len(keys) > 0 {
		return false
	}
	for k, v := range pfields {
		w := qfields[k]
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
		if p.Tail != nil {
			pats = append(pats, fmt.Sprintf("...%s", p.Tail.Debug()))
		}
		return "list(" + strings.Join(pats, ", ") + ")"
	case PatStruct:
		var pats []string
		for _, f := range p.Fields {
			pats = append(pats, f.Name+":"+f.Pat.Debug())
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
		if p.Tail != nil {
			pats = append(pats, fmt.Sprintf("...%s", p.Tail.String()))
		}
		return "[" + strings.Join(pats, ", ") + "]"
	case PatStruct:
		var pats []string
		for _, f := range p.Fields {
			pats = append(pats, f.Name+":"+f.Pat.String())
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
		if p.Tail != nil {
			p.Tail.Idents(ids)
		}
		return ids
	case PatStruct:
		for _, f := range p.Fields {
			ids = f.Idents(ids)
		}
		return ids
	case PatIgnore:
		return ids
	}
}

// BindTypes binds the pattern's identifier's types in the passed environment,
// given the type of binding value t.
func (p *Pat) BindTypes(env *types.Env, t *types.T, use types.Use) error {
	switch p.Kind {
	default:
		panic("bad pat")
	case PatIdent:
		env.Bind(p.Ident, t, p.Position, use)
		return nil
	case PatTuple:
		if t.Kind != types.TupleKind {
			return errors.Errorf("expected tuple, got %s", t)
		}
		if got, want := len(p.List), len(t.Fields); got != want {
			return errors.Errorf("expected tuple of size %d, got %d (%s)", want, got, t)
		}
		for i, q := range p.List {
			if err := q.BindTypes(env, t.Fields[i].T, use); err != nil {
				return err
			}
		}
		return nil
	case PatList:
		if t.Kind != types.ListKind {
			return errors.Errorf("expected list, got %s", t)
		}
		for _, q := range p.List {
			if err := q.BindTypes(env, t.Elem, use); err != nil {
				return err
			}
		}
		if p.Tail != nil {
			if err := p.Tail.BindTypes(env, t, use); err != nil {
				return err
			}
		}
		return nil
	case PatStruct:
		if t.Kind != types.StructKind {
			return errors.Errorf("expected struct, got %s", t)
		}
		fm := t.FieldMap()
		for id, q := range p.FieldMap() {
			u := fm[id]
			if u == nil {
				return errors.Errorf("struct %s does not have field %s", t, id)
			}
			if err := q.BindTypes(env, u, use); err != nil {
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
		for i, q := range p.List {
			if !q.BindValues(env, tup[i]) {
				return false
			}
		}
		return true
	case PatList:
		list := v.(values.List)
		if len(list) < len(p.List) {
			return false
		}
		if p.Tail == nil && len(p.List) < len(list) {
			return false
		}
		for i, q := range p.List {
			if !q.BindValues(env, list[i]) {
				return false
			}
		}
		if p.Tail != nil {
			p.Tail.BindValues(env, list[len(p.List):])
		}
		return true
	case PatStruct:
		s := v.(values.Struct)
		for _, f := range p.Fields {
			if !f.Pat.BindValues(env, s[f.Name]) {
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

// Remove removes any identifiers in the set provided by idents and
// returns a new pattern where these identifiers are replaced by
// ignore patterns. The set of removed identifiers are returned
// alongside the new pattern.
func (p *Pat) Remove(idents interface {
	Contains(string) bool
}) (*Pat, []string) {
	switch p.Kind {
	default:
		panic("bad pat")
	case PatIdent:
		if idents.Contains(p.Ident) {
			return &Pat{Kind: PatIgnore}, []string{p.Ident}
		}
		return p, nil
	case PatTuple, PatList:
		var (
			removed []string
			list    = make([]*Pat, len(p.List))
		)
		for i, q := range p.List {
			var ids []string
			list[i], ids = q.Remove(idents)
			if ids != nil {
				removed = append(removed, ids...)
			}
		}
		if len(removed) == 0 {
			return p, nil
		}
		q := new(Pat)
		*q = *p
		q.List = list
		if q.Tail != nil {
			tailPat, ids := q.Tail.Remove(idents)
			q.Tail = tailPat
			removed = append(removed, ids...)
		}
		return q, removed
	case PatStruct:
		var (
			removed []string
			fields  = make([]PatField, len(p.Fields))
		)
		for i, f := range p.Fields {
			var ids []string
			fields[i].Name = f.Name
			fields[i].Pat, ids = f.Remove(idents)
			if ids != nil {
				removed = append(removed, ids...)
			}
		}
		if len(removed) == 0 {
			return p, nil
		}
		q := new(Pat)
		*q = *p
		q.Fields = fields
		return q, removed
	case PatIgnore:
		return p, nil
	}
}

// FieldMap returns the set of fields in pattern p as a map.
func (p *Pat) FieldMap() map[string]*Pat {
	m := make(map[string]*Pat, len(p.Fields))
	for _, f := range p.Fields {
		m[f.Name] = f.Pat
	}
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
			q.matchers(m, &Matcher{Kind: MatchTuple, Index: i, Length: len(p.List), Parent: parent})
		}
	case PatList:
		allowTail := p.Tail != nil
		for i, q := range p.List {
			q.matchers(m, &Matcher{Kind: MatchList, Index: i, Length: len(p.List), AllowTail: allowTail, Parent: parent})
		}
		if p.Tail != nil {
			p.Tail.matchers(m, &Matcher{Kind: MatchListTail, Length: len(p.List), AllowTail: allowTail, Parent: parent})
		}
	case PatStruct:
		for _, f := range p.Fields {
			f.matchers(m, &Matcher{Kind: MatchStruct, Field: f.Name, Parent: parent})
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
	// MatchListTail indexes the tail of a list.
	MatchListTail
	// MatchStruct indexes a struct.
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
	// Length is the required length of the containing tuple or list
	// (MatchTuple, MatchList).  For MatchTuple, this is already validated by
	// the type-checker and only included here for convenience.
	Length int
	// AllowTail specifies whether the matcher allows the containing list to
	// have a tail (i.e. be longer than the pattern) (MatchList).
	AllowTail bool
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
func (p Path) Match(v values.T, t *types.T) (values.T, *types.T, bool, Path, error) {
	if len(p) == 0 {
		panic("bad path")
	}
	m, p := p[0], p[1:]
	switch m.Kind {
	default:
		panic("bad path")
	case MatchValue:
		return v, t, true, p, nil
	case MatchTuple:
		return v.(values.Tuple)[m.Index], t.Fields[m.Index].T, true, p, nil
	case MatchList:
		l := v.(values.List)
		if len(l) < m.Length {
			return nil, t.Elem, false, p, errors.Errorf("cannot match list pattern of size %d with a list of size %d", m.Length, len(l))
		}
		if !m.AllowTail && m.Length < len(l) {
			return nil, t.Elem, false, p, errors.Errorf("cannot match list pattern of size %d with a list of size %d", m.Length, len(l))
		}
		if len(l) <= m.Index {
			panic("matcher index exceeds list length; should be caught by length check")
		}
		return l[m.Index], t.Elem, true, p, nil
	case MatchListTail:
		l := v.(values.List)
		if len(l) < m.Length {
			return nil, t.Elem, false, p, errors.Errorf("cannot match pattern of size %d with a list of size %d", m.Length, len(l))
		}
		if !m.AllowTail {
			panic("must allow tails to match tail")
		}
		return l[m.Length:], t, true, p, nil
	case MatchStruct:
		return v.(values.Struct)[m.Field], t.Field(m.Field), true, p, nil
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
		case MatchTuple:
			writeN(w, m.Index)
			writeN(w, m.Length)
		case MatchList:
			writeN(w, m.Index)
			writeN(w, m.Length)
			if m.AllowTail {
				w.Write([]byte{1})
			} else {
				w.Write([]byte{0})
			}
		case MatchListTail:
			writeN(w, m.Length)
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
