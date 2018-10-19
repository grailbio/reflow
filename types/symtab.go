// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package types

// Symtab is a symbol table of *Ts.
type Symtab map[string]*T

// Env represents a type environment that binds
// value and type identifiers to *Ts.
type Env struct {
	Values, Types Symtab

	next *Env
}

// NewEnv creates and initializes a new Env.
func NewEnv() *Env {
	var e *Env
	return e.Push()
}

// Bind binds the identifier id to type t.
func (e *Env) Bind(id string, t *T) {
	e.Values[id] = t
}

// BindAlias binds the type t to the type alias with the provided identifier.
func (e *Env) BindAlias(id string, t *T) {
	e.Types[id] = t
}

// Type returns the type bound to identifier id, if any.
func (e *Env) Type(id string) *T {
	for ; e != nil; e = e.next {
		if t := e.Values[id]; t != nil {
			return t
		}
	}
	return nil
}

// Alias returns the type alias bound to identifier id, if any.
func (e *Env) Alias(id string) *T {
	for ; e != nil; e = e.next {
		if t := e.Types[id]; t != nil {
			return t
		}
	}
	return nil
}

// Push returns a new environment level linked to e.
func (e *Env) Push() *Env {
	return &Env{
		Values: make(Symtab),
		Types:  make(Symtab),
		next:   e,
	}
}

// Pop returns the topmost frame in env. The returned
// frame is unlinked.
func (e *Env) Pop() *Env {
	top := &Env{
		Values: make(Symtab),
		Types:  make(Symtab),
	}
	for id, t := range e.Values {
		top.Values[id] = t
	}
	for id, t := range e.Types {
		top.Types[id] = t
	}
	return top
}

// Symbols returns the full value environment as a map.
func (e *Env) Symbols() map[string]*T {
	sym := map[string]*T{}
	for ; e != nil; e = e.next {
		for s, t := range e.Values {
			if sym[s] == nil {
				sym[s] = t
			}
		}
	}
	return sym
}
