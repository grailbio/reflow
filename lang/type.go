// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package lang

import "fmt"

// Type is the type of types in the reflow language.
type Type int

const (
	// typeError is returned when an erroneous type was encountered
	typeError Type = iota + 1
	// typeVoid is the "unit" type--a type with a singular value
	typeVoid
	// typeNum is the type of numerics
	typeNum
	// typeString is the type of strings
	typeString
	// typeTemplate is the type of command templates
	typeTemplate
	// typeFlow is the type of flows
	typeFlow
	// typeFlowList is the type a list of flows
	typeFlowList
	// typeImage is the type of a Docker image
	typeImage
	// typeStringList is the type of lists of strings
	typeStringList

	// typeFuncBase is the base type of funcs: funcs are parameterized
	// by their arity and return type. We encode his directly into the
	// enum.
	// TODO(marius): fix this ugly hack.
	typeFuncBase = 100
)

// String returns a human-readable string for type t.
func (t Type) String() string {
	switch t {
	case typeError:
		return "error"
	case typeString:
		return "string"
	case typeTemplate:
		return "template"
	case typeVoid:
		return "void"
	case typeNum:
		return "num"
	case typeFlow:
		return "flow"
	case typeFlowList:
		return "list<flow>"
	case typeImage:
		return "image"
	case typeStringList:
		return "list<string>"
	}
	n, r := funcType(t)
	return fmt.Sprintf("func(%d, %s)", n, r)
}

func typeFunc(arity int, ret Type) Type {
	return Type(typeFuncBase + Type(arity*10) + ret)
}

func funcType(t Type) (int, Type) {
	tt := t - typeFuncBase
	if tt < 0 {
		panic(fmt.Sprintf("bad type %d", t))
	}
	n := tt / 10
	return int(n), Type(tt - 10*n)
}

// TypeEnv is a type environment used during typechecking and
// type inference. It is an error reporter.
type TypeEnv struct {
	*Error
	// The set of toplevel defs
	Def   map[string]*Expr
	tenvs []*typeEnv
}

// Bind binds an identifier to a type.
func (t *TypeEnv) Bind(ident string, typ Type) {
	t.tenvs[0] = t.tenvs[0].Bind(ident, typ)
}

// Type returns the type the type of an identifier in this
// environment. The second return value indicates whether ident was
// defined.
func (t *TypeEnv) Type(ident string) (Type, bool) {
	return t.tenvs[0].Type(ident)
}

// Push pushes the current type environment onto the environment
// stack.
func (t *TypeEnv) Push() {
	var tenv *typeEnv
	if len(t.tenvs) > 0 {
		tenv = t.tenvs[0]
	}
	t.tenvs = append([]*typeEnv{tenv}, t.tenvs...)
}

// Pop pops the type environment stack.
func (t *TypeEnv) Pop() {
	t.tenvs = t.tenvs[1:]
}

// typeEnv implements a type environment as a linked list.
type typeEnv struct {
	ident string
	typ   Type
	next  *typeEnv
}

// Bind binds a type to an idenfitier.
func (t *typeEnv) Bind(ident string, typ Type) *typeEnv {
	return &typeEnv{ident, typ, t}
}

// Type returns the type bound to an identifier. The second return
// argument indicates whether the identifier was defined in the
// environment.
func (t *typeEnv) Type(ident string) (Type, bool) {
	tt := t
	for tt != nil {
		if tt.ident == ident {
			return tt.typ, true
		}
		tt = tt.next
	}
	return typeError, false
}
