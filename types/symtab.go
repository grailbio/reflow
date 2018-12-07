// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package types

import (
	"unicode"
	"unicode/utf8"

	"github.com/grailbio/reflow/internal/scanner"
)

// Use denotes how a binding is consider used.
type Use int

const (
	// Always indicates that a binding is always unused unless
	// explicitly marked used.
	Always Use = iota
	// Never indicates that a binding is never considered unused.
	Never
	// Unexported denotes that a binding is considered unused
	// only if it is unexported.
	Unexported
)

// Symbol stores symbol information for a binding maintained by
// an environment.
type Symbol struct {
	// Position is the scanner position of the symbol; used
	// for error reporting.
	scanner.Position
	// Name is the identifier of the symbol.
	Name string
	// Value is the type of the symbol.
	Value *T
	// Use is the use rule of the symbol.
	Use Use
	// Used is true if the binding has been marked used.
	Used bool
}

// Symtab is a symbol table of symbols.
type Symtab map[string]*Symbol

// Env represents a type environment that binds
// value and type identifiers to *Ts.
type Env struct {
	Values, Types Symtab
	next          *Env
}

// NewEnv creates and initializes a new Env.
func NewEnv() *Env {
	var e *Env
	return e.Push()
}

// Bind binds the identifier id to type t.
func (e *Env) Bind(id string, t *T, pos scanner.Position, use Use) {
	e.Values[id] = &Symbol{
		Name:     id,
		Position: pos,
		Value:    t,
		Use:      use,
	}
}

// BindAlias binds the type t to the type alias with the provided identifier.
func (e *Env) BindAlias(id string, t *T) {
	e.Types[id] = &Symbol{Value: t}
}

// Type returns the type bound to identifier id, if any.
func (e *Env) Type(id string) *T {
	sym := e.sym(id)
	if sym == nil {
		return nil
	}
	return sym.Value
}

// Use marks the provided identifier as used.
func (e *Env) Use(id string) {
	sym := e.sym(id)
	if sym != nil {
		sym.Used = true
	}
}

func (e *Env) sym(id string) *Symbol {
	for ; e != nil; e = e.next {
		if sym := e.Values[id]; sym != nil {
			return sym
		}
	}
	return nil
}

// Alias returns the type alias bound to identifier id, if any.
func (e *Env) Alias(id string) *T {
	for ; e != nil; e = e.next {
		if sym := e.Types[id]; sym != nil {
			return sym.Value
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

func (e *Env) Unused() []*Symbol {
	var syms []*Symbol
	for id, sym := range e.Values {
		if sym.Used || sym.Use == Never {
			continue
		}
		if sym.Use == Always || !IsExported(id) {
			syms = append(syms, sym)
		}
	}
	return syms
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
	tab := map[string]*T{}
	for ; e != nil; e = e.next {
		for id, sym := range e.Values {
			if tab[id] == nil {
				tab[id] = sym.Value
			}
		}
	}
	return tab
}

// IsExported returns whether the provided identifier is considered
// exported: that is, it begins in an uppercase character as defined
// by unicode.
func IsExported(ident string) bool {
	first, _ := utf8.DecodeRuneInString(ident)
	return first != utf8.RuneError && unicode.IsUpper(first)
}
