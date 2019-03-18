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
//	top                                       the type of any value
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
// See package grail.com/reflow/syntax for parsing concrete syntax into
// type trees.
//
// Each type carries a const level: NotConst, CanConst, or Const.
// Level Const indicates that values of this type do not depend on
// dynamic program input, and can be computed constantly within a
// simple module-level lexical scope. CanConst types may be upgraded
// to Const by static analysis, whereas NotConst types may never be
// Const. Unification imposes a maximum const level, and a type's
// constedness is affirmed by T.Const. This API requires the static
// analyzer to explicitly propagate const levels.
//
// Types of level Const may also carry a set of predicates,
// indicating that their constedness is conditional. The static
// analyzer can mint fresh predicates (for example indicating that a
// module's parameter is satisfied by a constant type), and then
// later derive types based on satisfied predicates. This is used to
// implement cross-module constant propagation within Reflow.
//
// Two types are unified by recursively computing the common subtype
// of the two arguments. Subtyping is limited to structs and modules;
// type equality is required for all other types.
package types

//go:generate stringer -type=ConstLevel

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
)

// Kind represents a type's kind.
type Kind int

const (
	// ErrorKind is an illegal type.
	ErrorKind Kind = iota

	// BottomKind is a value-less type.
	BottomKind

	// TopKind is the type of any value.
	TopKind

	// Kind 0 types.

	// IntKind is the type of arbitrary precision integers.
	IntKind
	// FloatKind is the type of arbitrary precision floats.
	FloatKind
	// StringKind is the type of UTF-8 encoded strings.
	StringKind
	// BoolKind is the type of booleans.
	BoolKind
	// FileKind is the type of files.
	FileKind
	// DirKind is the type of directories.
	DirKind
	// FilesetKind is the type of filesets.
	FilesetKind
	// UnitKind is a 1-valued type.
	UnitKind

	kind0

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
	TopKind:     "top",
}

func (k Kind) String() string {
	return kindStrings[k]
}

// KindsInOrder stores the order in which kinds were added.
var kindsInOrder = [...]Kind{
	ErrorKind,
	BottomKind,
	IntKind,
	StringKind,
	BoolKind,
	FileKind,
	DirKind,
	UnitKind,
	ListKind,
	MapKind,
	TupleKind,
	FuncKind,
	StructKind,
	ModuleKind,
	RefKind,
	FloatKind,
	FilesetKind,
	TopKind,
}

var kindID [typeMax]byte

func init() {
	for i, k := range kindsInOrder {
		kindID[k] = byte(i)
	}
}

// ID returns the kind's stable identifier, which may be used
// for serialization.
func (k Kind) ID() byte {
	return kindID[k]
}

// ConstLevel determines the "constedness" of a type.
// See definitions below.
type ConstLevel int

const (
	// NotConst indicates that the value represented by this type
	// cannot be computed statically by the compiler.
	NotConst ConstLevel = -1
	// CanConst indicates that the value represented by this type is
	// derived from IsConst values, and thus may be promoted to
	// IsConst if the underlying operation allows for it.
	CanConst ConstLevel = 0
	// Const indicates that the value represented by this type can
	// be computed statically by the compiler.
	Const ConstLevel = 1
)

// Min returns the smaller (more conservative) of m and l.
func (l ConstLevel) Min(m ConstLevel) ConstLevel {
	if m < l {
		return m
	}
	return l
}

var nextPredicate int64

// A Predicate represents a condition upon which a type's constedness
// is predictated.
type Predicate int

// FreshPredicate returns a fresh predicate.
func FreshPredicate() Predicate {
	return Predicate(atomic.AddInt64(&nextPredicate, 1))
}

// Predicates is a predicate set.
type Predicates map[Predicate]bool

// Satisfies determines whether the predicate set p meets the predicate
// set q: that is, whether all predicates in q are present in p.
func (p Predicates) Satisfies(q Predicates) bool {
	for pr := range q {
		if !p[pr] {
			return false
		}
	}
	return true
}

// Clear removes all predicates from the set.
func (p *Predicates) Clear() {
	*p = nil
}

// Copy returns a copy of the the predicate set p.
func (p Predicates) Copy() Predicates {
	if p == nil {
		return nil
	}
	q := make(Predicates)
	for k := range p {
		q[k] = true
	}
	return q
}

// Add adds predicate pred to the predicate set.
func (p *Predicates) Add(pred Predicate) {
	if *p == nil {
		*p = make(map[Predicate]bool)
	}
	(*p)[pred] = true
}

// AddAll adds all predicates in q to p.
func (p *Predicates) AddAll(q Predicates) {
	if len(q) > 0 && *p == nil {
		*p = make(map[Predicate]bool)
	}
	for k := range q {
		(*p)[k] = true
	}
}

// RemoveAll removes from set p all predicates in q.
func (p Predicates) RemoveAll(q Predicates) {
	if p == nil {
		return
	}
	for k := range q {
		delete(p, k)
	}
}

// NIntersect returns the number of predicates shared by predicate
// sets p and q.
func (p Predicates) NIntersect(q Predicates) int {
	var n int
	for k := range p {
		if q[k] {
			n++
		}
	}
	return n
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

func (f *Field) String() string {
	return fmt.Sprintf("%s %s", f.Name, f.T)
}

// Equal checks whether Field f is equivalent to Field e.
func (f *Field) Equal(e *Field) bool {
	return f.Name == e.Name && f.T.Equal(e.T)
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

// A T is a Reflow type. The zero T is a TypeError.
type T struct {
	// Kind is the kind of the type. See above.
	Kind Kind
	// Index is the type of the type's index; used in maps.
	Index *T
	// Elem is the type of the type's elem; used in lists, maps, and funcs.
	Elem *T
	// Fields stores module, struct, and tuple fields, as well as func
	// arguments.
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

	// Level is this type's const level.
	Level ConstLevel
	// Predicates is the set of predicates upon which the types
	// constedness is predicated.
	Predicates Predicates
}

// Convenience vars for common types.
var (
	Bottom  = &T{Kind: BottomKind}
	Top     = &T{Kind: TopKind}
	Int     = &T{Kind: IntKind}
	Float   = &T{Kind: FloatKind}
	String  = &T{Kind: StringKind}
	Bool    = &T{Kind: BoolKind}
	File    = &T{Kind: FileKind}
	Dir     = &T{Kind: DirKind}
	Unit    = &T{Kind: UnitKind}
	Fileset = &T{Kind: FilesetKind}
)

// Make initializes type t and returns it, propagating errors
// of any child type.
func Make(t *T) *T {
	return t.Map(func(t *T) *T {
		if t.Index != nil && t.Index.Error != nil {
			return t.Index
		}
		if t.Elem != nil && t.Elem.Error != nil {
			return t.Elem
		}
		for _, f := range t.Fields {
			if f.T.Error != nil {
				return f.T
			}
		}
		return t
	})
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
	return Make(&T{Kind: ListKind, Elem: elem})
}

// Map returns a new map type with the given index and element types.
func Map(index, elem *T) *T {
	switch index.Kind {
	case StringKind, IntKind, FloatKind, BoolKind, FileKind, TopKind:
	default:
		return Errorf("%v is not a valid map key type", index)
	}
	return Make(&T{Kind: MapKind, Index: index, Elem: elem})
}

// Tuple returns a new tuple type with the given fields.
func Tuple(fields ...*Field) *T {
	return Make(&T{Kind: TupleKind, Fields: fields})
}

// Func returns a new func type with the given element
// (return type) and argument fields.
func Func(elem *T, fields ...*Field) *T {
	return Make(&T{Kind: FuncKind, Fields: fields, Elem: elem})
}

// Struct returns a new struct type with the given fields.
func Struct(fields ...*Field) *T {
	return Make(&T{Kind: StructKind, Fields: fields})
}

// Module returns a new module type with the given fields.
func Module(fields []*Field, aliases []*Field) *T {
	return Make(&T{Kind: ModuleKind, Fields: fields, Aliases: aliases})
}

// Ref returns a new pseudo-type reference to the given path.
func Ref(path ...string) *T {
	return Make(&T{Kind: RefKind, Path: path})
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
	t.Level = NotConst
	t.Predicates = Predicates{}
	return t
}

// Copy returns a shallow copy of type t.
func (t *T) Copy() *T {
	u := new(T)
	*u = *t
	u.Predicates = t.Predicates.Copy()
	if u.Fields != nil {
		u.Fields = make([]*Field, len(t.Fields))
		for i := range t.Fields {
			u.Fields[i] = new(Field)
			*u.Fields[i] = *t.Fields[i]
		}
	}
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
	case TopKind:
		s = "top"
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
	if u == nil {
		return false
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

// Sub tells whether t is a subtype of u. Subtyping is only
// applied to structs and modules, as in (*T).Unify.
func (t *T) Sub(u *T) bool {
	if t.Kind == RefKind || u.Kind == RefKind {
		panic("reference types cannot have subtype relationships")
	}
	if t.Kind == BottomKind {
		return true
	}
	if u.Kind == TopKind {
		return true
	}
	if t.Kind != u.Kind {
		return false
	}
	switch t.Kind {
	default:
		return false
	case IntKind, FloatKind, StringKind, BoolKind, FileKind, DirKind, BottomKind, FilesetKind:
		return true
	case ListKind:
		return t.Elem.Sub(u.Elem)
	case MapKind:
		return u.Index.Sub(t.Index) && t.Elem.Sub(u.Elem)
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
			if !u.Fields[i].T.Sub(t.Fields[i].T) {
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

// Map applies fn structurally in a depth-first fashion. If fn
// performs modifications to type t, it should return a copy.
func (t *T) Map(fn func(*T) *T) *T {
	if t == nil {
		return t
	}
	u := t
	if index := t.Index.Map(fn); index != t.Index {
		set(&u, t).Index = index
	}
	if elem := t.Elem.Map(fn); elem != t.Elem {
		set(&u, t).Elem = elem
	}
	for i, f := range t.Fields {
		if ftyp := f.T.Map(fn); ftyp != f.T {
			set(&u, t).Fields[i].T = ftyp
		}
	}
	return fn(u)
}

// Const affirms type t's constedness with an additional set of
// predicates. The returned type is const only if all child types are
// also const.
func (t *T) Const(predicates ...Predicate) *T {
	if t.Level == Const {
		if len(predicates) > 0 {
			if len(t.Predicates) > 0 {
				t = t.Copy()
			}
			for _, p := range predicates {
				t.Predicates.Add(p)
			}
		}
		return t
	}
	t = t.Map(func(t *T) *T {
		if t.Level != CanConst ||
			(t.Index != nil && t.Index.Level < Const) ||
			(t.Elem != nil && t.Elem.Level < Const) {
			return t
		}
		for _, f := range t.Fields {
			if f.T.Level < Const {
				return t
			}
		}
		t = t.Copy()
		t.Level = Const
		return t
	})
	if len(predicates) > 0 {
		t = t.Copy()
		for _, p := range predicates {
			t.Predicates.Add(p)
		}
	}
	return t
}

// NotConst denies constedness to type t. NotConst is tainting: all
// child types are marked NotConst as well. Thus NotConst can be used
// in static analysis where non-const dataflow is introduced.
func (t *T) NotConst() *T {
	if t.Level == NotConst {
		return t
	}
	return t.Map(func(t *T) *T {
		if t.Level == NotConst {
			return t
		}
		t = t.Copy()
		t.Level = NotConst
		return t
	})
}

// IsConst tells whether type t is const given the provided predicates.
func (t *T) IsConst(predicates Predicates) bool {
	return t.Level == Const && predicates.Satisfies(t.Predicates)
}

// Satisfied returns type t with the given predicates satisfied.
func (t *T) Satisfied(predicates Predicates) *T {
	return t.Map(func(t *T) *T {
		if t.Level != Const || predicates.NIntersect(t.Predicates) == 0 {
			return t
		}
		t = t.Copy()
		t.Predicates.RemoveAll(predicates)
		return t
	})
}

// Swizzle returns a version of t with modified const and flow flags:
//	- if any of the type ts are NotConst, the returned type is also NotConst
//	- const predicates from type ts are added to t's predicate set
//	- t's flow flag is set if any of type ts are
//	- the returned type's level is at most maxlevel
func Swizzle(t *T, maxlevel ConstLevel, ts ...*T) *T {
	if t.Kind == ErrorKind {
		return t
	}
	u := t
	for _, w := range ts {
		if w.Kind == ErrorKind {
			return w
		}
		if !u.Flow && w.Flow {
			set(&u, t).Flow = true
		}
		if w.Level == NotConst && u.Level > NotConst {
			set(&u, t).Level = NotConst
		}
		if len(w.Predicates) > 0 {
			set(&u, t).Predicates.AddAll(w.Predicates)
		}
	}
	return u.Map(func(t *T) *T {
		if t.Level <= maxlevel {
			return t
		}
		t = t.Copy()
		t.Level = maxlevel
		return t
	})
}

// Unify returns the minimal common supertype of ts.  That is, where
// u := Unify(ts), then forall t in ts. t <: u, and there exists no v, v != u
// and v <: u, such that forall t in ts. t <: v.  Unify returns an error type if
// unification is impossible.  The returned type's const level is limited by the
// provided maxlevel.
func Unify(maxlevel ConstLevel, ts ...*T) *T {
	if len(ts) == 0 {
		return Bottom
	}
	var (
		t          = ts[0]
		level      = t.Level
		predicates = t.Predicates.Copy()
	)
	for _, u := range ts[1:] {
		if t.Kind == RefKind || u.Kind == RefKind {
			panic("reference types cannot be unified")
		}
		if t.Kind == BottomKind {
			t = Swizzle(u, maxlevel, t)
			continue
		}
		if u.Kind == BottomKind {
			t = Swizzle(t, maxlevel, u)
			continue
		}
		if t.Kind != u.Kind {
			return Errorf("kind mismatch: %v != %v", t.Kind, u.Kind)
		}
		predicates.AddAll(u.Predicates)
		level = level.Min(u.Level)
		switch t.Kind {
		default:
			return Errorf("unknown kind %v", t.Kind)
		case IntKind, FloatKind, StringKind, BoolKind, FileKind, DirKind:
			t = Swizzle(t, maxlevel, u)
		case ErrorKind:
			return typeError
		case ListKind:
			t = List(Unify(level, t.Elem, u.Elem))
		case MapKind:
			// Manually check subtypes here and Swizzle instead since
			// map keys are contravariant.
			var kt *T
			switch {
			case u.Index.Sub(t.Index):
				kt = Swizzle(u.Index, maxlevel, t.Index)
			case t.Index.Sub(u.Index):
				kt = Swizzle(t.Index, maxlevel, u.Index)
			default:
				return Errorf("%v and %v are incompatible map key types", t.Index, u.Index)
			}
			t = Map(kt, Unify(maxlevel, t.Elem, u.Elem))
		case TupleKind:
			if nt, nu := len(t.Fields), len(u.Fields); nt != nu {
				return Errorf("mismatched tuple length: %v != %v", nt, nu)
			}
			fields := make([]*Field, len(t.Fields))
			for i := range t.Fields {
				fields[i] = &Field{T: Unify(maxlevel, t.Fields[i].T, u.Fields[i].T)}
			}
			t = Tuple(fields...)
		case FuncKind:
			if nt, nu := len(t.Fields), len(u.Fields); nt != nu {
				return Errorf("mismatched argument length: %v != %v", nt, nu)
			}
			for i := range t.Fields {
				if tt, ut := t.Fields[i].T, u.Fields[i].T; !tt.Equal(ut) {
					return Errorf("argument %v does not match: %v != %v", i, tt, ut)
				}
			}
			t = Func(Unify(maxlevel, t.Elem, u.Elem), t.Fields...)
		case StructKind:
			tfields := t.FieldMap()
			var fields []*Field
			for _, uf := range u.Fields {
				tty := tfields[uf.Name]
				if tty == nil {
					continue
				}
				fields = append(fields, &Field{Name: uf.Name, T: Unify(maxlevel, tty, uf.T)})
			}
			if len(fields) == 0 {
				return Errorf("%s %v and %v have no fields in common", t.Kind, t, u)
			}
			t = Struct(fields...)
		case ModuleKind:
			tfields := t.FieldMap()
			var fields []*Field
			for _, uf := range u.Fields {
				tty := tfields[uf.Name]
				if tty == nil {
					continue
				}
				fields = append(fields, &Field{Name: uf.Name, T: Unify(maxlevel, tty, uf.T)})
			}
			if len(fields) == 0 {
				return Errorf("%s %v and %v have no fields in common", t.Kind, t, u)
			}
			// TODO(marius): unify type aliases too. Now we just drop them and
			// this could lead to perplexing type errors.
			t = Module(fields, nil)
		}
	}
	if t == ts[0] {
		t = t.Copy()
	}
	t.Level = level
	if t.Level > maxlevel {
		t = t.Copy()
		t.Level = maxlevel
		t.Predicates = predicates
	} else if len(predicates) > 0 {
		t = t.Copy()
		t.Predicates = predicates
	}
	return t
}

func set(u **T, t *T) *T {
	if *u == t {
		*u = t.Copy()
	}
	return *u
}
