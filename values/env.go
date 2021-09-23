// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package values

import (
	"fmt"
	"strings"
	"sync"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/types"
)

// symtab is a symbol table of values.
type Symtab map[string]T

func (s Symtab) PrettyString() string {
	b := strings.Builder{}
	b.WriteString("--------------Symtab--------------\n")
	for k, v := range s {
		b.WriteString(fmt.Sprintf("%s = %v\n", k, v))
	}
	b.WriteString("----------------------------------\n")
	return b.String()
}

// Env binds identifiers to evaluation.
type Env struct {
	// symtab is the symbol table for this level.
	symtab Symtab
	debug  bool
	next   *Env

	// digests maps ids to computed/cached digest values.
	mu      sync.Mutex
	digests map[string]digest.Digest
}

// NewEnv constructs and initializes a new Env.
func NewEnv() *Env {
	var e *Env
	return e.Push()
}

// Bind binds the identifier id to value v.
func (e *Env) Bind(id string, v T) {
	if id == "" {
		panic("empty identifier")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if w, ok := e.symtab[id]; ok && !Equal(v, w) {
		delete(e.digests, id)
	}
	e.symtab[id] = v
}

// String returns a string describing all the bindings in this
// environment.
func (e *Env) String() string {
	tab := make(Symtab)
	for ; e != nil; e = e.next {
		for id, v := range e.symtab {
			_, ok := tab[id]
			if !ok {
				tab[id] = v
			}
		}
	}
	return fmt.Sprint(tab)
}

type digester interface {
	Digest() digest.Digest
}

// Digest returns the digest for the value with identifier id.
// The supplied type is used to compute the digest. If the value
// is a digest.Digest, it is returned directly; if implements the
// interface
//
//	interface{
//		Digest() digest.Digest
//	}
//
// it returns the result of calling the Digest Method.
//
// Digest also caches the computed digests for an id, and will return the cached value
// upon subsequent calls.
func (e *Env) Digest(id string, t *types.T) (d digest.Digest) {
	v, ve := e.valueAndEnv(id)
	e.mu.Lock()
	defer e.mu.Unlock()
	if ve != nil {
		var cached bool
		if d, cached = ve.digests[id]; cached {
			return
		}
	}
	switch vv := v.(type) {
	case digest.Digest:
		d = vv
	case digester:
		d = vv.Digest()
	default:
		d = Digest(vv, t)
	}
	if ve != nil {
		ve.digests[id] = d
	}
	return d
}

// Level returns the level of identifier id. Level can thus be used
// as a de-Bruijn index (in conjunction with the identifier).
func (e *Env) Level(id string) int {
	for l := 0; e != nil; e, l = e.next, l+1 {
		if e.symtab[id] != nil {
			return l
		}
	}
	return -1
}

// Contains tells whether environment e binds identifier id.
func (e *Env) Contains(id string) bool {
	for ; e != nil; e = e.next {
		if _, ok := e.symtab[id]; ok {
			return true
		}
	}
	return false
}

// Value returns the value bound to identifier id, or else nil.
func (e *Env) Value(id string) T {
	v, _ := e.valueAndEnv(id)
	return v
}

// Value returns the value bound to identifier id, or else nil.
func (e *Env) valueAndEnv(id string) (T, *Env) {
	if e == nil {
		return nil, nil
	}
	for ; e != nil; e = e.next {
		if v := e.symtab[id]; v != nil {
			return v, e
		}
	}
	return nil, nil
}

// Push returns returns a new environment level, linked
// to the previous.
func (e *Env) Push() *Env {
	return &Env{
		symtab:  make(Symtab),
		digests: make(map[string]digest.Digest),
		next:    e,
	}
}

func (e *Env) copy() *Env {
	f := new(Env)
	*f = *e
	return f
}

// Concat returns a new concatenated environment.
func (e *Env) Concat(f *Env) *Env {
	e = e.copy()
	g := e
	for e.next != nil {
		e.next = e.next.copy()
		e = e.next
	}
	e.next = f
	return g
}

// Debug sets the debug flag on this environment.
// This causes IsDebug to return true.
func (e *Env) Debug() {
	e.debug = true
}

// IsDebug returns true if the debug flag is set in this environment.
func (e *Env) IsDebug() bool {
	if e == nil {
		return false
	}
	for ; e != nil; e = e.next {
		if e.debug {
			return true
		}
	}
	return false
}
