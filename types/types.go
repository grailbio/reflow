// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package types contains data structures and algorithms for dealing
// with value types in Reflow. In particular, it defines type-trees,
// constructors for type trees, together with unification and
// subtyping algorithms.
//
// A Reflow type is one of:
//
//	bottom                                    the type of no values
//	int                                       the type of arbitrary precision integers
//	float                                     the type of arbitrary precision float point numbers
//	string                                    the type of (utf-8 encoded) strings
//	bool                                      the type of booleans
//	file                                      the type of files
//	dir                                       the type of directories
//	unit                                      the type of single-valued unit/void
//	fileset                                   the type of filesets (legacy)
//	<ident>                                   a type reference to ident
//	[t]                                       the type of lists of element type t
//	[t1:t2]                                   the type of maps of index type t1, element type t2
//	(t1, t2, ..., tn)                         the type of tuples of type t1, ..., tn
//	func(arg1 t1, arg2 t2, ..., argn tn) t    the type of function with arguments t1, ..., tn, and return type t
//	{k1: t1, k2: t2, ..., kn: tn}             the type of struct with fields k1, ..., kn, of types t1, ..., tn
//	module{I1: t1, I2: t2, ..., In: tn,
//	       type a1 at1, type a2 at2}          the type of module with values I1, ... In, of types t1, ..., tn
//	                                          and type aliases a1, a2 of at1, at2.
//
//
// See package grail.com/reflow/syntax for parsing concrete syntax into
// type trees.
//
// Two types are unified by recursively computing the common subtype
// of the two arguments. Subtyping is limited to structs and modules;
// type equality is required for all other types.
package types

import (
	"errors"
	"fmt"
	"strings"
)

// Kind represents a type's kind.
type Kind int

const (
	// ErrorKind is an illegal type.
	ErrorKind Kind = iota

	// BottomKind is a value-less type.
	BottomKind

	// Kind 0 types.

	// IntKind is the type of arbitrary precision integers.
	IntKind
	// StringKind is the type of UTF-8 encoded strings.
	StringKind
	// BoolKind is the type of booleans.
	BoolKind
	// FileKind is the type of files.
	FileKind
	// DirKind is the type of directories.
	DirKind

	// UnitKind is a 1-valued type.
	UnitKind

	// Kind >0 types.

	// ListKind is the type of lists.
	ListKind
	// MapKind is the type of maps.
	MapKind

	// TupleKind is the kind of n-tuples of values (containing positional fields).
	TupleKind
	// FuncKind is the type of funcs (arguments and return types).
	FuncKind
	// StructKind is the type of structs (containing named fields).
	StructKind

	// ModuleKind is the type of modules. They are like structs,
	// but: (1) will eventually also contain types; (2) can never
	// be subject to delayed evaluation: their values are known
	// at compile time.
	ModuleKind

	// RefKind is a pseudo-kind to carry type alias references.
	RefKind

	// FloatKind is the type of arbitrary precision floats.
	FloatKind

	// FilesetKind is the type of filesets.
	FilesetKind

	typeMax
)

var kindStrings = [typeMax]string{
	ErrorKind:   "error",
	BottomKind:  "bottom",
	IntKind:     "int",
	FloatKind:   "float",
	StringKind:  "string",
	BoolKind:    "bool",
	FileKind:    "file",
	DirKind:     "dir",
	FilesetKind: "fileset",
	UnitKind:    "unit",
	RefKind:     "ident",
	ListKind:    "list",
	MapKind:     "map",
	TupleKind:   "tuple",
	FuncKind:    "func",
	StructKind:  "struct",
	ModuleKind:  "module",
}

func (k Kind) String() string {
	return kindStrings[k]
}

var typeError = Error(errors.New("type error"))

func typeErrorf(format string, args ...interface{}) *T {
	return &T{
		Kind:  ErrorKind,
		Error: fmt.Errorf(format, args...),
	}
}

// A Field is a labelled type. It is used in records,
// function arguments, and tuples.
type Field struct {
	Name string
	*T
}

// Equal checks whether Field f is equivalent to Field e.
func (f *Field) Equal(e *Field) bool {
	return f.Name == e.Name && f.T.StructurallyEqual(e.T)
}

// FieldsString returns a parseable string representation of the
// given fields.
func FieldsString(fields []*Field) string {
	args := make([]string, len(fields))
	for i, f := range fields {
		// We print a type if we have a name and the next type
		// is different.
		switch {
		case f.Name == "":
			args[i] = f.T.String()
		case i < len(fields)-1 && fields[i+1].T.StructurallyEqual(f.T):
			args[i] = f.Name
		default:
			args[i] = f.Name + " " + f.T.String()
		}
	}
	return strings.Join(args, ", ")
}

// A T is a Reflow type. The zero of a T is a TypeError.
type T struct {
	// Kind is the kind of the type. See above.
	Kind Kind
	// Index is the type of the type's index; used in maps.
	Index *T
	// Elem is the type of the type's elem; used in lists, maps, and funcs.
	Elem *T
	// Fields stores struct and tuple fields, as well as func arguments.
	Fields []*Field
	// Aliases stores a module's (exported) type aliases.
	Aliases []*Field
	// Error holds the type's error.
	Error error

	// Path is a type reference path, used by RefKind.
	// It is also used after alias expansion to retain identifiers
	// for pretty printing.
	Path []string

	// Label is an optional label for this type.
	// It does not affect the type's semantics.
	Label string

	// Flow is a flag indicating that types of this value are derived
	// from *flow.Flow evaluations. This is used in the type checker
	// to check for "immediate" evaluation.
	Flow bool
}

// Convenience vars for common types.
var (
	Bottom  = &T{Kind: BottomKind}
	Int     = &T{Kind: IntKind}
	Float   = &T{Kind: FloatKind}
	String  = &T{Kind: StringKind}
	Bool    = &T{Kind: BoolKind}
	File    = &T{Kind: FileKind}
	Dir     = &T{Kind: DirKind}
	Unit    = &T{Kind: UnitKind}
	Fileset = &T{Kind: FilesetKind}
)

// Promote promotes errors and flow flags in the type t.
func Promote(t *T) *T {
	if t.Index != nil {
		index := Promote(t.Index)
		if index.Error != nil {
			return index
		}
		if index.Flow {
			t.Flow = true
		}
	}
	if t.Elem != nil {
		elem := Promote(t.Elem)
		if elem.Error != nil {
			return elem
		}
		if elem.Flow {
			t.Flow = true
		}
	}
	for _, f := range t.Fields {
		field := Promote(f.T)
		if field.Error != nil {
			return field
		}
		if field.Flow {
			t.Flow = true
		}
	}
	return t
}

// Error constructs a new error type.
func Error(err error) *T {
	return &T{Kind: ErrorKind, Error: err}
}

// Errorf formats a new error type
func Errorf(format string, args ...interface{}) *T {
	return Error(fmt.Errorf(format, args...))
}

// List returns a new list type with the given element type.
func List(elem *T) *T {
	return Promote(&T{Kind: ListKind, Elem: elem})
}

// Map returns a new map type with the given index and element types.
func Map(index, elem *T) *T {
	switch index.Kind {
	case StringKind, IntKind, FloatKind, BoolKind, FileKind:
	default:
		return Errorf("%v is not a valid map key type", index)
	}
	return Promote(&T{Kind: MapKind, Index: index, Elem: elem})
}

// Tuple returns a new tuple type with the given fields.
func Tuple(fields ...*Field) *T {
	return Promote(&T{Kind: TupleKind, Fields: fields})
}

// Func returns a new func type with the given element
// (return type) and argument fields.
func Func(elem *T, fields ...*Field) *T {
	return Promote(&T{Kind: FuncKind, Fields: fields, Elem: elem})
}

// Struct returns a new struct type with the given fields.
func Struct(fields ...*Field) *T {
	return Promote(&T{Kind: StructKind, Fields: fields})
}

// Module returns a new module type with the given fields.
func Module(fields []*Field, aliases []*Field) *T {
	return Promote(&T{Kind: ModuleKind, Fields: fields, Aliases: aliases})
}

// Ref returns a new pseudo-type reference to the given path.
func Ref(path ...string) *T {
	return Promote(&T{Kind: RefKind, Path: path})
}

// Labeled returns a labeled version of type t.
func Labeled(label string, t *T) *T {
	t = t.Copy()
	t.Label = label
	return t
}

// Flow returns type t as a flow type.
func Flow(t *T) *T {
	t = t.Copy()
	t.Flow = true
	return t
}

// Copy returns a shallow copy of type t.
func (t *T) Copy() *T {
	u := new(T)
	*u = *t
	return u
}

// String renders a parseable version of Type t.
func (t *T) String() string {
	var s string
	switch t.Kind {
	default:
		if t.Error != nil {
			return "error: " + t.Error.Error()
		}
		s = "error"
	case IntKind:
		s = "int"
	case FloatKind:
		s = "float"
	case StringKind:
		s = "string"
	case BoolKind:
		s = "bool"
	case FileKind:
		s = "file"
	case DirKind:
		s = "dir"
	case UnitKind:
		s = "unit"
	case RefKind:
		s = t.Ident()
	case ListKind:
		s = "[" + t.Elem.String() + "]"
	case MapKind:
		s = "[" + t.Index.String() + ":" + t.Elem.String() + "]"
	case TupleKind:
		s = "(" + FieldsString(t.Fields) + ")"
	case FuncKind:
		s = "func(" + FieldsString(t.Fields) + ") " + t.Elem.String()
	case StructKind:
		s = "{" + FieldsString(t.Fields) + "}"
	case ModuleKind:
		s = "module{" + FieldsString(t.Fields)
		var aliases []string
		if len(t.Aliases) > 0 {
			aliases = make([]string, len(t.Aliases))
			for i, field := range t.Aliases {
				aliases[i] = fmt.Sprintf("type %s %s", field.Name, field.T)
			}
			s += ", " + strings.Join(aliases, ", ")
		}
		s += "}"
	case BottomKind:
		s = "bottom"
	case FilesetKind:
		s = "fileset"
	}
	if t.Label != "" {
		return "(" + t.Label + " " + s + ")"
	}
	return s
}

// FieldMap returns the Type's fields as a map.
func (t *T) FieldMap() map[string]*T {
	if len(t.Fields) == 0 {
		return nil
	}
	m := make(map[string]*T)
	for _, f := range t.Fields {
		m[f.Name] = f.T
	}
	return m
}

// Field indexes the type's fields.
func (t *T) Field(n string) *T {
	for _, f := range t.Fields {
		if f.Name == n {
			return f.T
		}
	}
	return Errorf("field %q not found", n)
}

// Tupled returns a tuple version of this type's fields.
func (t *T) Tupled() *T {
	if t.Kind == TupleKind {
		return t
	}
	return Tuple(&Field{T: t, Name: t.Label})
}

// Ident returns the identifier of this type (the expanded path).
func (t *T) Ident() string {
	return strings.Join(t.Path, ".")
}

// Alias returns the alias id in a module type, if any.
func (t *T) Alias(id string) *T {
	for _, f := range t.Aliases {
		if f.Name == id {
			return f.T
		}
	}
	return nil
}

// Equal tests whether type t is equivalent to type u.
func (t *T) Equal(u *T) bool {
	return t.equal(u, false)
}

// StructurallyEqual tells whether type t is structurally equal to type u.
// Structural equality permits type aliases and considers two aliases
// equal if their paths are equal.
func (t *T) StructurallyEqual(u *T) bool {
	return t.equal(u, true)
}

func (t *T) equal(u *T, refok bool) bool {
	if t == nil || u == nil {
		return t == nil && u == nil
	}
	if t.Kind == ErrorKind {
		return false
	}
	if !refok && (t.Kind == RefKind || u.Kind == RefKind) {
		panic("reference types cannot be tested for equality")
	}
	if t.Kind != u.Kind {
		return false
	}
	if t.Index != nil && !t.Index.equal(u.Index, refok) {
		return false
	}
	if t.Elem != nil && !t.Elem.equal(u.Elem, refok) {
		return false
	}
	if len(t.Fields) != len(u.Fields) {
		return false
	}
	switch t.Kind {
	case TupleKind, FuncKind:
		for i := range t.Fields {
			if !t.Fields[i].T.equal(u.Fields[i].T, refok) {
				return false
			}
		}
	case StructKind, ModuleKind:
		tf, uf := t.FieldMap(), u.FieldMap()
		for k := range tf {
			if !tf[k].equal(uf[k], refok) {
				return false
			}
		}
	case RefKind:
		if len(t.Path) != len(u.Path) {
			return false
		}
		for i := range t.Path {
			if t.Path[i] != u.Path[i] {
				return false
			}
		}
		return true
	}
	return true
}

// Assign assigns type t from type u: if either t or u are error
// types, it returns a new error; if u is a Flow type, this flag is
// set on the returned type. Otherwise, t is returned unadulterated.
func (t *T) Assign(u *T) *T {
	if t.Kind == ErrorKind {
		return t
	}
	if u != nil && u.Kind == ErrorKind {
		return u
	}
	if u != nil && u.Flow {
		t = t.Copy()
		t.Flow = true
	}
	return t
}

// Unify unifies type t with type u. It returns an error type if
// unification is not possible. Unificiation is conservative: structs
// and modules may be subtyped (by narrowing available fields), but
// all other compound types require equality.
func (t *T) Unify(u *T) *T {
	if t.Kind == RefKind || u.Kind == RefKind {
		panic("reference types cannot be unified")
	}
	if t.Kind == BottomKind {
		return u
	}
	if u.Kind == BottomKind {
		return t
	}
	if t.Kind != u.Kind {
		return Errorf("kind mismatch: %v != %v", t.Kind, u.Kind)
	}
	switch t.Kind {
	default:
		return Errorf("unknown kind %v", t.Kind)
	case IntKind, FloatKind, StringKind, BoolKind, FileKind, DirKind:
		if u.Flow {
			return u
		}
		return t
	case ErrorKind:
		return typeError
	case ListKind:
		return List(t.Elem.Unify(u.Elem))
	case MapKind:
		if !t.Index.Equal(u.Index) {
			return Errorf("map key mismatch: %v != %v", t.Index, u.Index)
		}
		return Map(t.Index, t.Elem.Unify(u.Elem))
	case TupleKind:
		if nt, nu := len(t.Fields), len(u.Fields); nt != nu {
			return Errorf("mismatched tuple length: %v != %v", nt, nu)
		}
		fields := make([]*Field, len(t.Fields))
		for i := range t.Fields {
			fields[i] = &Field{T: t.Fields[i].T.Unify(u.Fields[i].T)}
		}
		return Tuple(fields...)
	case FuncKind:
		if nt, nu := len(t.Fields), len(u.Fields); nt != nu {
			return Errorf("mismatched argument length: %v != %v", nt, nu)
		}
		for i := range t.Fields {
			if tt, ut := t.Fields[i], u.Fields[i]; !tt.Equal(ut) {
				return Errorf("argument %v does not match: %v != %v", i, tt, ut)
			}
		}
		return Func(t.Elem.Unify(u.Elem), t.Fields...)
	case StructKind:
		tfields := t.FieldMap()
		var fields []*Field
		for _, uf := range u.Fields {
			tty := tfields[uf.Name]
			if tty == nil {
				continue
			}
			fields = append(fields, &Field{Name: uf.Name, T: tty.Unify(uf.T)})
		}
		if len(fields) == 0 {
			return Errorf("%s %v and %v have no fields in common", t.Kind, t, u)
		}
		return Struct(fields...)
	case ModuleKind:
		tfields := t.FieldMap()
		var fields []*Field
		for _, uf := range u.Fields {
			tty := tfields[uf.Name]
			if tty == nil {
				continue
			}
			fields = append(fields, &Field{Name: uf.Name, T: tty.Unify(uf.T)})
		}
		if len(fields) == 0 {
			return Errorf("%s %v and %v have no fields in common", t.Kind, t, u)
		}
		// TODO(marius): unify type aliases too. Now we just drop them and
		// this could lead to perplexing type errors.
		return Module(fields, nil)
	}
}

// Sub tells whether t is a subtype of u. Subtyping is only
// applied to structs and modules, as in (*T).Unify.
func (t *T) Sub(u *T) bool {
	if t.Kind == RefKind || u.Kind == RefKind {
		panic("reference types cannot have subtype relationships")
	}
	if t.Kind == BottomKind {
		return true
	}
	if t.Kind != u.Kind {
		return false
	}
	switch t.Kind {
	default:
		return false
	case IntKind, FloatKind, StringKind, BoolKind, FileKind, DirKind, BottomKind:
		return true
	case ListKind:
		return t.Elem.Sub(u.Elem)
	case MapKind:
		return t.Index.Equal(u.Index) && t.Elem.Sub(u.Elem)
	case TupleKind:
		if len(t.Fields) != len(u.Fields) {
			return false
		}
		for i := range t.Fields {
			if !t.Fields[i].T.Sub(u.Fields[i].T) {
				return false
			}
		}
		return true
	case FuncKind:
		if len(t.Fields) != len(u.Fields) {
			return false
		}
		for i := range t.Fields {
			if !t.Fields[i].Equal(u.Fields[i]) {
				return false
			}
		}
		return t.Elem.Sub(u.Elem)
	case StructKind, ModuleKind:
		tfields := t.FieldMap()
		for _, uf := range u.Fields {
			tty := tfields[uf.Name]
			if tty == nil {
				return false
			}
			if !tty.Sub(uf.T) {
				return false
			}
		}
		return true
	}
}

// Symtab is a symbol table of *Ts.
type Symtab map[string]*T

func (s Symtab) equal(t Symtab) bool {
	if len(s) != len(t) {
		return false
	}
	for key, sv := range s {
		if tv, ok := t[key]; !ok || !sv.Equal(tv) {
			return false
		}
	}
	return true
}

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

// Equal tells whether environments e and f are equivalent.
func (e *Env) Equal(f *Env) bool {
	if e == nil || f == nil {
		return e == nil && f == nil
	}
	return e.Values.equal(f.Values) && e.Types.equal(f.Types) && e.next.Equal(f.next)
}
