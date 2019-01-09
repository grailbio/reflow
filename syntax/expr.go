// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/internal/scanner"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// ExprKind is the kind of an expression.
type ExprKind int

const (
	// ExprError indicates an erroneous expression (e.g., through a parse error)
	ExprError ExprKind = iota
	// ExprIdent is an identifier reference.
	ExprIdent
	// ExprBinop is a binary operation.
	ExprBinop
	// ExprUnop is a unary operation.
	ExprUnop
	// ExprApply is function application.
	ExprApply
	// ExprLit is a literal value.
	ExprLit
	// ExprAscribe is an ascription (static type assertion).
	ExprAscribe
	// ExprBlock is a declaration-block.
	ExprBlock
	// ExprFunc is a function definition.
	ExprFunc
	// ExprTuple is a tuple literal.
	ExprTuple
	// ExprStruct is a struct literal.
	ExprStruct
	// ExprList is a list literal.
	ExprList
	// ExprMap is a map literal.
	ExprMap
	// ExprExec is an exec expression.
	ExprExec
	// ExprCond is a conditional expression.
	ExprCond
	// ExprDeref is a struct derefence expression.
	ExprDeref
	// ExprIndex is an indexing (map or list) expression.
	ExprIndex
	// ExprCompr is a comprehension expression.
	ExprCompr
	// ExprMake is a module instantiation expression.
	ExprMake
	// ExprBuiltin is a builtin expression (e.g., len, zip, unzip).
	ExprBuiltin
	// ExprRequires assigns resources to the underlying expression.
	// It also necessarily forces the value.
	ExprRequires
	// ExprThunk is a delayed evaluation (expression + environment).
	// These are never produced from parsing--they are used internally
	// by the evaluator. (But see note there.)
	ExprThunk

	maxExpr
)

// FieldExpr stores a field name and expression.
type FieldExpr struct {
	Name string
	*Expr
}

// Equal tests whether f is equivalent to e.
func (f *FieldExpr) Equal(e *FieldExpr) bool {
	return f.Name == e.Name && f.Expr.Equal(e.Expr)
}

// ComprKind is the type of the kind of a comprehension clause.
type ComprKind int

const (
	// ComprEnum is the kind of an enumeration clause.
	ComprEnum ComprKind = iota
	// ComprFilter is the kind of a filter clause.
	ComprFilter
)

// A ComprClause is a single clause in a comprehension expression.
type ComprClause struct {
	// Kind is the clause's kind.
	Kind ComprKind
	// Pat is the clause's pattern (ComprEnum).
	Pat *Pat
	// Expr is the clause's expression (ComprEnum, ComprFilter).
	Expr *Expr
}

// Template is an exec template and its interpolation arguments.
// The template is stored as a number of fragments interspersed
// by argument expressions to be rendered.
//
// The following are guaranteed invariant:
//	len(Frags) > 0
//	len(Frags) == len(Args)+1
type Template struct {
	Text  string
	Frags []string
	Args  []*Expr
}

// String returns t.Text.
func (t *Template) String() string {
	return t.Text
}

// FormatString returns a format string that can be used to render the
// final template. It's provided for backwards compatibility only.
func (t *Template) FormatString() string {
	var b bytes.Buffer
	for i, s := range t.Frags {
		s = strings.Replace(s, "%", "%%", -1)
		b.WriteString(s)
		if i > 0 {
			b.WriteString("%s")
		}
	}
	return b.String()
}

// An Expr is a node in Reflow's expression AST.
type Expr struct {
	// Position contains the source position of the node.
	// It is set by the parser.
	scanner.Position

	// Comment is the commentary text that precedes this expression,
	// if any.
	Comment string

	// Kind is the expression's op; see above.
	Kind ExprKind

	// Cond is the condition expression in ExprCond.
	Cond *Expr

	// Left is the "left" operand for expressions.
	Left *Expr
	// Right is the "right" operand for expressions.
	Right *Expr

	// Op is the binary operation in ExprBinop, unary operation in
	// ExprUnop, and builtin in ExprBuiltin.
	Op string

	// Args holds function arguments in an ExprFunc.
	Args []*types.Field

	// List holds expressions for list literals.
	List []*Expr

	// Map holds expressions for map literals.
	Map map[*Expr]*Expr

	// Decls holds declarations for ExprBlock and ExprExec.
	Decls []*Decl

	// Fields holds field definitions (identifiers and expressions)
	// in ExprStruct, ExprTuple
	Fields []*FieldExpr

	// Ident stores the identifier for ExprIdent
	Ident string

	// Val stores constant values in ExprConst.
	Val values.T

	// Type holds the Type in ExprAscribe, ExprExec, and ExprConst.
	Type *types.T

	// Template is the exec template in ExprExec.
	Template *Template

	// Image is the (evaluated) image string used in the exec.
	// It is populated during evaluation.
	Image string

	ComprExpr    *Expr
	ComprClauses []*ComprClause

	// Env stores a value environmetn for ExprThunk.
	Env *values.Env

	// Pat stores the bind pattern in a comprehension.
	Pat *Pat

	// Module stores the module as opened during type checking.
	Module Module
}

// ExprValue stores the evaluated values associated with the dependencies
// of an Expr node. It is used while evaluating an expression tree.
type ExprValue struct {
	Left, Right values.T
	Cond        values.T
	List        []values.T
	Map         []struct{ K, V *values.T }
	Fields      []values.T
	Extras      []values.T
}

// Subexpr returns a slice of this expression's dependencies.
func (e *Expr) Subexpr() []*Expr {
	var x []*Expr
	if e.Cond != nil {
		x = append(x, e.Cond)
	}
	if e.Left != nil {
		x = append(x, e.Left)
	}
	if e.Right != nil {
		x = append(x, e.Right)
	}
	x = append(x, e.List...)
	for k, v := range e.Map {
		x = append(x, k)
		x = append(x, v)
	}
	for _, f := range e.Fields {
		x = append(x, f.Expr)
	}
	return x
}

// Init performs type checking and synthesis on this expression tree;
// sets each node's Type field, and then returns any type error.
func (e *Expr) Init(sess *Session, env *types.Env) error {
	e.init(sess, env)
	return e.err()
}

func (e *Expr) err() error {
	var el errlist
	for _, d := range e.Decls {
		if d.Type == nil {
			continue
		}
		if err := d.Type.Error; err != nil {
			el = el.Error(d.Position, d.Type.Error)
		}
		el = el.Append(d.Expr.err())
	}
	if len(el) > 0 {
		return el.Make()
	}
	for _, sub := range e.Subexpr() {
		el = el.Append(sub.err())
	}
	for _, clause := range e.ComprClauses {
		if clause.Expr != nil {
			el = el.Append(clause.Expr.err())
		}
	}
	if e.Kind == ExprError {
		el = el.Errorf(e.Position, "erroneous expression")
	}
	// Suppress consequent errors.
	if len(el) == 0 && e.Type != nil && e.Type.Error != nil {
		el = el.Error(e.Position, e.Type.Error)
	}
	return el.Make()
}

func comparable(t *types.T) bool {
	switch t.Kind {
	case types.StringKind, types.IntKind, types.FloatKind, types.FileKind, types.DirKind, types.BoolKind, types.BottomKind:
		return true
	case types.ListKind:
		return comparable(t.Elem)
	case types.MapKind:
		return comparable(t.Index) && comparable(t.Elem)
	case types.TupleKind, types.StructKind:
		for _, f := range t.Fields {
			if !comparable(f.T) {
				return false
			}
		}
		return true
	}
	return false
}

func (e *Expr) init(sess *Session, env *types.Env) {
	switch e.Kind {
	case ExprBlock:
		env = env.Push()
		defer reportUnused(sess, env)
		for _, d := range e.Decls {
			d.Init(sess, env)
			if d.Type.Kind == types.ErrorKind {
				e.Type = d.Type
			} else if err := d.Pat.BindTypes(env, types.Swizzle(d.Type, types.NotConst), types.Always); err != nil {
				d.Type = types.Error(err)
			}
		}
	case ExprFunc:
		env = env.Push()
		defer reportUnused(sess, env)
		for i := range e.Args {
			e.Args[i].T = expand(e.Args[i].T, env)
		}
		for _, a := range e.Args {
			env.Bind(a.Name, a.T, e.Position, types.Never)
		}
	}
	for _, sub := range e.Subexpr() {
		sub.init(sess, env)
	}
	// Expand out all aliases in this expr's type. Type inference cannot
	// introduce aliases, so we're fine (and it's simpler) to do it
	// before then.
	if e.Type != nil {
		e.Type = expand(e.Type, env)
	}
	if e.Type != nil && e.Type.Kind == types.ErrorKind {
		return
	}
	switch e.Kind {
	default:
		panic("invalid expression " + e.String())
	case ExprIdent:
		e.Type = env.Type(e.Ident)
		if e.Type == nil {
			e.Type = types.Errorf("identifier %q not defined", e.Ident)
		}
		env.Use(e.Ident)
	case ExprBinop:
		if e.Op == "~>" {
			e.Type = types.Swizzle(e.Right.Type, types.NotConst, e.Left.Type)
			return
		}
		if e.Op == "+" && (e.Left.Type.Kind == types.ListKind || e.Left.Type.Kind == types.MapKind || e.Left.Type.Kind == types.DirKind) {
			e.Type = types.Unify(types.Const, e.Left.Type, e.Right.Type)
			return
		}
		if !e.Left.Type.Equal(e.Right.Type) {
			e.Type = types.Errorf(
				"cannot apply binary operator %q to type %v and %v",
				e.Op, e.Left.Type, e.Right.Type)
			return
		}
		unified := types.Unify(types.Const, e.Left.Type, e.Right.Type)
		switch e.Op {
		case "+":
			switch e.Left.Type.Kind {
			case types.StringKind, types.IntKind, types.FloatKind:
				e.Type = unified
			default:
				e.Type = types.Errorf("binary operator %s not allowed for type %v", e.Op, e.Left.Type)
			}
		case "%", "<<", ">>":
			switch e.Left.Type.Kind {
			case types.IntKind:
				e.Type = unified
			default:
				e.Type = types.Errorf("binary operator \"%s\" not allowed for type %v", e.Op, e.Left.Type)
			}
		case "*", "-", "/":
			switch e.Left.Type.Kind {
			case types.IntKind, types.FloatKind:
				e.Type = unified
			default:
				e.Type = types.Errorf("binary operator \"%s\" not allowed for type %v", e.Op, e.Left.Type)
			}
		case "&&", "||":
			if e.Left.Type.Kind == types.BoolKind {
				e.Type = types.Swizzle(types.Bool.Const(), types.Const, unified)
			} else {
				e.Type = types.Errorf("binary operator %q not allowed for type %v", e.Op, e.Left.Type)
			}
		case "==", "!=":
			if comparable(e.Left.Type) {
				e.Type = types.Swizzle(types.Bool.Const(), types.Const, unified)
			} else {
				e.Type = types.Errorf("cannot compare values of type %v", e.Left.Type)
			}
		case ">", "<", "<=", ">=":
			switch e.Left.Type.Kind {
			case types.StringKind, types.IntKind, types.FloatKind:
				e.Type = types.Swizzle(types.Bool.Const(), types.Const, unified)
			default:
				e.Type = types.Errorf("cannot compare values of type %v", e.Left.Type)
			}
		default:
			e.Type = types.Errorf("binary operator %q not allowed for type %v", e.Op, e.Left.Type)
		}
	case ExprUnop:
		switch e.Op {
		default:
			panic("unknown unary operator " + e.Op)
		case "!":
			if e.Left.Type.Kind != types.BoolKind {
				e.Type = types.Errorf("unary operator \"!\" is only valid for bools, not %s", e.Left.Type)
			} else {
				e.Type = types.Swizzle(types.Bool.Const(), types.Const, e.Left.Type)
			}
		case "-":
			switch e.Left.Type.Kind {
			case types.IntKind, types.FloatKind:
				e.Type = e.Left.Type
			default:
				e.Type = types.Errorf("unary operator \"-\" is only valid for integers and floats, not %s", e.Left.Type)
			}
		}
	case ExprApply:
		if e.Left.Type.Kind != types.FuncKind {
			e.Type = types.Errorf("cannot call non-function %s (type %v)", e.Left.identOr(""), e.Left.Type)
			return
		}
		if len(e.Fields) < len(e.Left.Type.Fields) {
			have := make([]*types.Field, len(e.Fields))
			for i := range e.Fields {
				have[i] = &types.Field{T: e.Fields[i].Type}
			}
			e.Type = types.Errorf(
				"too few arguments in call to %s\n\thave (%v)\n\twant (%v)",
				e.Left.identOr("function"), types.FieldsString(have), types.FieldsString(e.Left.Type.Fields))
			return
		}
		if len(e.Fields) > len(e.Left.Type.Fields) {
			have := make([]*types.Field, len(e.Fields))
			for i := range e.Fields {
				have[i] = &types.Field{T: e.Fields[i].Type}
			}
			e.Type = types.Errorf(
				"too many arguments in call to %v\n\thave (%v)\n\twant (%v)",
				e.Left.identOr("function"), types.FieldsString(have), types.FieldsString(e.Left.Type.Fields))
			return
		}
		typs := make([]*types.T, 1+len(e.Fields))
		typs[0] = e.Left.Type
		for i, f := range e.Fields {
			if !f.Type.Sub(e.Left.Type.Fields[i].T) {
				e.Type = types.Errorf(
					"cannot use type %v as type %v in argument to %s (type %s)",
					f.Type, e.Left.Type.Fields[i].T, e.Left.identOr("function"), e.Left.Type)
				return
			}
			typs[i+1] = f.Type
		}
		e.Type = types.Swizzle(e.Left.Type.Elem, types.NotConst, typs...)
		return
	case ExprLit:
		e.Type = e.Type.Const()
	case ExprAscribe:
		if !e.Left.Type.Sub(e.Type) {
			e.Type = types.Errorf("cannot use %s (type %v) as type %v", e.Left.identOr("value"), e.Left.Type, e.Type)
		}
		e.Type = types.Swizzle(e.Type, types.Const, e.Left.Type)
	case ExprBlock:
		e.Type = e.Left.Type
	case ExprFunc:
		if len(e.Args) > 128 {
			e.Type = types.Errorf("functions can have at most 128 arguments")
		} else {
			e.Type = types.Func(e.Left.Type, e.Args...).Const()
		}
	case ExprTuple:
		fields := make([]*types.Field, len(e.Fields))
		for i := range e.Fields {
			fields[i] = &types.Field{T: e.Fields[i].Type}
		}
		e.Type = types.Tuple(fields...).Const()
	case ExprStruct:
		fields := make([]*types.Field, len(e.Fields))
		for i, f := range e.Fields {
			fields[i] = &types.Field{Name: f.Name, T: f.Expr.Type}
		}
		e.Type = types.Struct(fields...).Const()
	case ExprList:
		ts := make([]*types.T, len(e.List))
		for i, ee := range e.List {
			ts[i] = ee.Type
		}
		e.Type = types.List(types.Unify(types.CanConst, ts...)).Const()
	case ExprMap:
		var kts, vts []*types.T
		for k, v := range e.Map {
			kts = append(kts, k.Type)
			vts = append(vts, v.Type)
		}
		var kt *types.T
		if len(kts) == 0 {
			kt = types.Top
		} else {
			kt = types.Unify(types.Const, kts...)
		}
		e.Type = types.Map(kt, types.Unify(types.Const, vts...)).Const()
	case ExprExec:
		params := map[string]bool{}
		for _, d := range e.Decls {
			if d.Pat.Kind != PatIdent {
				e.Type = types.Errorf("execs do not support pattern matching declarations")
				return
			}
			if err := d.Init(sess, env); err != nil {
				e.Type = types.Errorf("type error in parameter: %s", err)
				return
			}
			if d.Pat.Ident == "image" && d.Expr.Type.Flow {
				e.Type = types.Errorf("exec parameter %s is not immediate", d.Pat.Ident)
				return
			}
			ident := d.Pat.Ident
			params[ident] = true
			switch ident {
			case "image":
				// Check is disabled temporarily until it is more useful.
				if false && !d.Type.IsConst(nil) {
					sess.Warn(e.Position, "image is not a const value")
				}
				if d.Type.Kind != types.StringKind {
					e.Type = types.Errorf("image must be a string")
					return
				}
			case "cpu":
				switch d.Type.Kind {
				case types.IntKind, types.FloatKind:
				default:
					e.Type = types.Errorf("%s must be integer or floating point", ident)
					return
				}
			case "mem", "disk":
				if d.Type.Kind != types.IntKind {
					e.Type = types.Errorf("%s must be an integer", ident)
					return
				}
			case "cpufeatures":
				if d.Type.Kind != types.ListKind || d.Type.Elem.Kind != types.StringKind {
					e.Type = types.Errorf("%s must be a list of strings", ident)
					return
				}
			default:
				e.Type = types.Errorf("unrecognized exec parameter %s", ident)
				return
			}
		}
		if !params["image"] {
			e.Type = types.Errorf("exec image parameter is required")
			return
		}
		fields := map[string]*types.T{}
		for i, f := range e.Type.Tupled().Fields {
			if f.Name == "" {
				e.Type = types.Errorf("output %d (type %s) must be labelled", i, f.T)
				return
			}
			switch f.T.Kind {
			case types.FileKind, types.DirKind:
			default:
				e.Type = types.Errorf("execs can only return files and dirs, not %s", f.T)
				return
			}
			fields[f.Name] = f.T
		}
		for _, ae := range e.Template.Args {
			if t, ok := fields[ae.Ident]; ok && ae.Kind == ExprIdent {
				ae.Type = t
				continue
			}
			ae.init(sess, env)
			// Promote interpolation errors here since they are not part of the regular
			// syntax tree.
			if err := ae.Type.Error; err != nil {
				e.Type = types.Errorf("interpolation expression error: %s", err)
				return
			}
			switch ae.Type.Kind {
			case types.FileKind, types.DirKind, types.StringKind, types.IntKind, types.FloatKind:
			case types.ListKind:
				switch ae.Type.Elem.Kind {
				case types.FileKind, types.DirKind:
				default:
					e.Type = types.Errorf("values of type %s cannot be interpolated", ae.Type)
					return
				}
			default:
				e.Type = types.Errorf("values of type %s cannot be interpolated", ae.Type)
				return
			}
		}
		e.Type = e.Type.Copy()
		e.Type.Flow = true
		// TODO(marius): technically we can compute the flow as a const
		// but that's not necessarily a useful definition.
		e.Type.Level = types.NotConst
		e.Type.Predicates.Clear()
	case ExprCond:
		if e.Cond.Type.Kind != types.BoolKind {
			e.Type = types.Errorf("expected boolean expression, got %v", e.Cond.Type)
			return
		}
		e.Type = types.Unify(types.CanConst, e.Left.Type, e.Right.Type)
		// Don't allow conditional constant evaluation (yet).
		e.Type = types.Swizzle(e.Type, types.NotConst, e.Cond.Type)
	case ExprDeref:
		if e.Left.Type.Kind != types.StructKind && e.Left.Type.Kind != types.ModuleKind {
			e.Type = types.Errorf("expected struct or module, got %v", e.Left.Type)
			return
		}
		e.Type = types.Swizzle(e.Left.Type.Field(e.Ident), types.Const, e.Left.Type)
	case ExprIndex:
		switch e.Left.Type.Kind {
		case types.ListKind:
			if !e.Right.Type.Equal(types.Int) {
				e.Type = types.Errorf("expected %v, got %v", types.Int, e.Right.Type)
				return
			}
			e.Type = types.Swizzle(e.Left.Type.Elem, types.Const, e.Right.Type)
		case types.MapKind:
			if !e.Left.Type.Index.Equal(e.Right.Type) {
				e.Type = types.Errorf("expected %v, got %v", e.Right.Type, e.Left.Type)
				return
			}
			e.Type = types.Swizzle(e.Left.Type.Elem, types.Const, e.Right.Type)
		default:
			e.Type = types.Errorf("expected a map or list, got %v", e.Left.Type)
			return
		}
	case ExprCompr:
		env = env.Push()
		defer reportUnused(sess, env)
		clauseTypes := make([]*types.T, len(e.ComprClauses))
		for i, clause := range e.ComprClauses {
			clause.Expr.init(sess, env)
			if clause.Expr.Type.Kind == types.ErrorKind {
				e.Type = clause.Expr.Type
				return
			}
			clauseTypes[i] = clause.Expr.Type
			switch clause.Kind {
			case ComprEnum:
				switch clause.Expr.Type.Kind {
				case types.ListKind:
					if err := clause.Pat.BindTypes(env, clause.Expr.Type.Elem, types.Always); err != nil {
						e.Type = types.Error(err)
						return
					}
				case types.MapKind:
					if err := clause.Pat.BindTypes(env, types.Tuple(&types.Field{T: clause.Expr.Type.Index}, &types.Field{T: clause.Expr.Type.Elem}), types.Always); err != nil {
						e.Type = types.Error(err)
						return
					}
				default:
					e.Type = types.Errorf("expected list or map, got %v", clause.Expr.Type)
					return
				}
			case ComprFilter:
				if i == 0 {
					e.Type = types.Errorf("the first clause of a comprehension must be an enumeration")
					return
				}
				if clause.Expr.Type.Kind != types.BoolKind {
					e.Type = types.Errorf("expected boolean expression, got %v", clause.Expr.Type)
					return
				}
			}
		}
		e.ComprExpr.init(sess, env)
		e.Type = types.List(e.ComprExpr.Type)
		// Don't (yet) allow constant comptuation of comprehensions.
		e.Type = types.Swizzle(e.Type, types.NotConst, clauseTypes...)
	case ExprThunk:
		// ExprThunks are synthetic expressions and are always typed.
		if e.Type == nil {
			panic("untyped thunk")
		}
	case ExprMake:
		if e.Left.Kind != ExprLit {
			panic("invalid make expression")
		}
		if e.Left.Type.Kind != types.StringKind {
			e.Type = types.Errorf("expected string, got %v", e.Left.Type)
			return
		}
		name := e.Left.Val.(string)
		var err error
		e.Module, err = sess.Open(name)
		if err != nil {
			e.Type = types.Errorf("failed to open module %s: %v", name, err)
			return
		}
		penv := types.NewEnv()
		for _, d := range e.Decls {
			d.Init(sess, env)
			if d.Type.Kind == types.ErrorKind {
				e.Type = types.Errorf("type error")
				return
			} else if err := d.Pat.BindTypes(penv, d.Type, types.Never); err != nil {
				e.Type = types.Error(err)
				return
			}
		}
		if err := e.Module.ParamErr(penv); err != nil {
			e.Type = types.Error(err)
			return
		}
		// Modules are never flow values because their evaluation never depends
		// on parameters fully evaluating.
		e.Type = e.Module.Type(penv)
	case ExprBuiltin:
		switch e.Op {
		default:
			panic("invalid builtin " + e.Op)
		case "len":
			switch e.Left.Type.Kind {
			case types.FileKind, types.DirKind, types.ListKind, types.MapKind:
				e.Type = types.Swizzle(types.Int, types.Const, e.Left.Type)
			default:
				e.Type = types.Errorf("cannot apply len operator to value of type %s", e.Left.Type)
			}
		case "int":
			switch e.Left.Type.Kind {
			case types.FloatKind:
				e.Type = types.Swizzle(types.Int, types.Const, e.Left.Type)
			default:
				e.Type = types.Errorf("cannot convert type %s to int", e.Left.Type)
			}

		case "float":
			switch e.Left.Type.Kind {
			case types.IntKind:
				e.Type = types.Swizzle(types.Float, types.Const, e.Left.Type)
			default:
				e.Type = types.Errorf("cannot convert type %s to float", e.Left.Type)
			}
		case "zip":
			if e.Left.Type.Kind != types.ListKind {
				e.Type = types.Errorf("zip expects a list, not %s", e.Left.Type)
			} else if e.Right.Type.Kind != types.ListKind {
				e.Type = types.Errorf("zip expects a list, not %s", e.Right.Type)
			} else {
				e.Type = types.List(types.Tuple(
					&types.Field{T: e.Left.Type.Elem},
					&types.Field{T: e.Right.Type.Elem}))
			}
		case "unzip":
			if e.Left.Type.Kind != types.ListKind {
				e.Type = types.Errorf("unzip expects a list, not %s", e.Left.Type)
			} else if e.Left.Type.Elem.Kind != types.TupleKind {
				e.Type = types.Errorf("unzip expects a list of tuples, not %s", e.Left.Type.Elem)
			} else if len(e.Left.Type.Elem.Fields) != 2 {
				e.Type = types.Errorf("unzip expects a list of 2-tuples, not %s", e.Left.Type.Elem)
			} else {
				e.Type = types.Tuple(
					&types.Field{T: types.List(e.Left.Type.Elem.Fields[0].T)},
					&types.Field{T: types.List(e.Left.Type.Elem.Fields[1].T)})
			}
		case "flatten":
			if e.Left.Type.Kind != types.ListKind || e.Left.Type.Elem.Kind != types.ListKind {
				e.Type = types.Errorf("flatten expects a list of lists, got %s", e.Left.Type)
			} else {
				e.Type = types.Swizzle(e.Left.Type.Elem, types.Const, e.Left.Type)
			}
		case "map":
			switch e.Left.Type.Kind {
			default:
				e.Type = types.Errorf("cannot convert type %s to map", e.Left.Type)
			case types.ListKind:
				if e.Left.Type.Elem.Kind != types.TupleKind {
					e.Type = types.Errorf("map expects a list of tuples, not %s", e.Left.Type.Elem)
				} else if len(e.Left.Type.Elem.Fields) != 2 {
					e.Type = types.Errorf("map expects a list of 2-tuples, not %s", e.Left.Type.Elem)
				} else if e.Left.Type.Elem.Fields[0].Kind != types.StringKind && e.Left.Type.Elem.Fields[0].Kind != types.IntKind {
					e.Type = types.Errorf("type %s is not a valid map index", e.Left.Type.Elem.Fields[0].T)
				} else {
					e.Type = types.Map(e.Left.Type.Elem.Fields[0].T, e.Left.Type.Elem.Fields[1].T)
				}
			case types.DirKind:
				e.Type = types.Map(types.String, types.File)
			}
			e.Type = types.Swizzle(e.Type, types.Const, e.Left.Type)
		case "list":
			switch e.Left.Type.Kind {
			default:
				e.Type = types.Errorf("cannot convert type %s to list", e.Left.Type)
			case types.MapKind:
				e.Type = types.List(types.Tuple(
					&types.Field{T: e.Left.Type.Index},
					&types.Field{T: e.Left.Type.Elem})).Const()
			case types.DirKind:
				e.Type = types.List(types.Tuple(
					&types.Field{T: types.String},
					&types.Field{T: types.File})).Const()
			}
			e.Type = types.Swizzle(e.Type, types.Const, e.Left.Type)
		case "panic":
			if e.Left.Type.Kind != types.StringKind {
				e.Type = types.Errorf("panic expects a string, not %s", e.Left.Type)
			} else {
				e.Type = types.Bottom
			}
		case "delay":
			e.Type = e.Left.Type.Copy()
			e.Type.Level = types.NotConst
			e.Type.Flow = true
		case "trace":
			e.Type = e.Left.Type
		case "range":
			if e.Left.Type.Kind != types.IntKind {
				e.Type = types.Errorf("range expects an integer, not %s", e.Left.Type)
			} else if e.Right.Type.Kind != types.IntKind {
				e.Type = types.Errorf("range expects an integer, not %s", e.Right.Type)
			} else {
				e.Type = types.List(types.Int)
				e.Type.Level = types.CanConst
				e.Type = types.Swizzle(e.Type, types.Const, e.Left.Type, e.Right.Type)
			}
		}
	case ExprRequires:
		if err := e.initResources(sess, env); err != nil {
			e.Type = types.Error(err)
		} else {
			e.Type = e.Left.Type
		}
	}
}

func (e *Expr) initResources(sess *Session, env *types.Env) error {
	for _, d := range e.Decls {
		if d.Pat.Kind != PatIdent {
			return fmt.Errorf("pattern matching declarations are not supported")
		}
		if err := d.Init(sess, env); err != nil {
			return fmt.Errorf("type error in parameter: %s", err)
		}
		if d.Expr.Type.Flow {
			return fmt.Errorf("parameter %s is not immediate", d.Pat.Ident)
		}
		ident := d.Pat.Ident
		switch d.Pat.Ident {
		case "cpu":
			switch d.Type.Kind {
			case types.IntKind, types.FloatKind:
			default:
				return fmt.Errorf("%s must be integer or floating point", ident)
			}
		case "mem", "disk":
			if d.Type.Kind != types.IntKind {
				return fmt.Errorf("%s must be an integer", ident)
			}
		case "cpufeatures":
			if d.Type.Kind != types.ListKind || d.Type.Elem.Kind != types.StringKind {
				return fmt.Errorf("%s must be a list of strings", ident)
			}
		case "wide":
			if d.Type.Kind != types.BoolKind {
				return fmt.Errorf("%s must be a boolean", ident)
			}
		default:
			return fmt.Errorf("unrecognized parameter %s", ident)
		}
	}
	return nil
}

// closure stores an expression and an environment, so that it
// can later be invoked in lexical scope.
type closure struct {
	expr  *Expr
	sess  *Session
	env   *values.Env
	ident string
}

// Apply applies the closure with the given arguments.
func (c closure) Apply(loc values.Location, args []values.T) (values.T, error) {
	env := c.env.Push()
	for i := range c.expr.Args {
		env.Bind(c.expr.Args[i].Name, args[i])
	}
	return c.expr.Left.eval(c.sess, env, c.ident)
}

// Digest returns the digest for this closure. The digest is computed
// from the expression and stored environment.
func (c closure) Digest() digest.Digest {
	return c.expr.Digest(c.env)
}

// Equal tests whether expression e is equivalent to expression f.
func (e *Expr) Equal(f *Expr) bool {
	if e.Kind == ExprError {
		return false
	}
	if e.Kind != f.Kind {
		return false
	}
	switch e.Kind {
	default:
		panic("error")
	case ExprIdent:
		return e.Ident == f.Ident
	case ExprBinop:
		return e.Left.Equal(f.Left) && e.Right.Equal(f.Right)
	case ExprUnop:
		return e.Left.Equal(f.Left)
	case ExprLit:
		return e.Type.Equal(f.Type) && values.Equal(e.Val, f.Val)
	case ExprAscribe:
		return e.Left.Equal(f.Left) && e.Type.Equal(f.Type)
	case ExprBlock:
		if len(e.Decls) != len(f.Decls) {
			return false
		}
		for i := range e.Decls {
			if !e.Decls[i].Equal(f.Decls[i]) {
				return false
			}
		}
		return e.Left.Equal(f.Left)
	case ExprFunc:
		if len(e.Args) != len(f.Args) {
			return false
		}
		for i := range e.Args {
			if !e.Args[i].Equal(f.Args[i]) {
				return false
			}
		}
		return e.Left.Equal(f.Left)
	case ExprList:
		if len(e.List) != len(f.List) {
			return false
		}
		for i := range e.List {
			if !e.List[i].Equal(f.List[i]) {
				return false
			}
		}
		return true
	case ExprTuple:
		if len(e.Fields) != len(f.Fields) {
			return false
		}
		for i := range e.Fields {
			if !e.Fields[i].Expr.Equal(f.Fields[i].Expr) {
				return false
			}
		}
		return true
	case ExprStruct:
		if len(e.Fields) != len(f.Fields) {
			return false
		}
		for i := range e.Fields {
			if !e.Fields[i].Equal(f.Fields[i]) {
				return false
			}
		}
		return true
	case ExprMap:
		if len(e.Map) != len(f.Map) {
			return false
		}
		// TODO(marius: This is really ugly (and quadratic!);
		// it suggests we should store map literals differently.
		for ek, ev := range e.Map {
			var fk, fv *Expr
			for k, v := range f.Map {
				if ek.Equal(k) {
					fk, fv = k, v
					break
				}
			}
			if fk == nil {
				return false
			}
			if !ev.Equal(fv) {
				return false
			}
		}
		return true
	case ExprExec:
		if len(e.Decls) != len(f.Decls) {
			return false
		}
		for i := range e.Decls {
			if !e.Decls[i].Equal(f.Decls[i]) {
				return false
			}
		}
		if !e.Type.Equal(f.Type) {
			return false
		}
		return e.Template == f.Template
	case ExprCond:
		return e.Cond.Equal(f.Cond) && e.Left.Equal(f.Left) && e.Right.Equal(f.Right)
	}
}

// String renders a tree-formatted version of e.
func (e *Expr) String() string {
	if e == nil {
		return "<nil>"
	}
	b := new(bytes.Buffer)
	if e.Type != nil {
		b.WriteString("<" + e.Type.String() + ">")
	}
	switch e.Kind {
	default:
		panic("unknown expression type " + fmt.Sprint(e.Kind))
	case ExprError:
		b.WriteString("error")
	case ExprIdent:
		fmt.Fprintf(b, "ident(%q)", e.Ident)
	case ExprBinop:
		fmt.Fprintf(b, "binop(%v, %q, %v)", e.Left, e.Op, e.Right)
	case ExprUnop:
		fmt.Fprintf(b, "unop(%q, %v", e.Op, e.Left)
	case ExprApply:
		fields := make([]string, len(e.Fields))
		for i, f := range e.Fields {
			fields[i] = f.Expr.String()
		}
		fmt.Fprintf(b, "apply(%v(%v))", e.Left, strings.Join(fields, ", "))
	case ExprLit:
		fmt.Fprintf(b, "const(%v)", values.Sprint(e.Val, e.Type))
	case ExprAscribe:
		fmt.Fprintf(b, "ascribe(%v)", e.Left)
	case ExprBlock:
		decls := make([]string, len(e.Decls))
		for i := range e.Decls {
			decls[i] = e.Decls[i].String()
		}
		fmt.Fprintf(b, "block(%v in %v)", strings.Join(decls, ", "), e.Left)
	case ExprFunc:
		fmt.Fprintf(b, "func((%v) => %v)", types.FieldsString(e.Args), e.Left)
	case ExprTuple:
		fields := make([]string, len(e.Fields))
		for i, f := range e.Fields {
			fields[i] = f.Expr.String()
		}
		fmt.Fprintf(b, "tuple(%v)", strings.Join(fields, ", "))
	case ExprStruct:
		list := make([]string, len(e.Fields))
		for i, f := range e.Fields {
			list[i] = f.Name + ":" + f.Expr.String()
		}
		fmt.Fprintf(b, "struct(%v)", strings.Join(list, ", "))
	case ExprList:
		list := make([]string, len(e.List))
		for i, ee := range e.List {
			list[i] = ee.String()
		}
		fmt.Fprintf(b, "list(%v)", strings.Join(list, ", "))
	case ExprMap:
		var (
			m    = map[string]string{}
			keys []string
		)
		for ke, ve := range e.Map {
			key := ke.String()
			m[key] = ve.String()
			keys = append(keys, key)
		}
		sort.Strings(keys)
		list := make([]string, len(keys))
		for i, key := range keys {
			list[i] = key + ":" + m[key]
		}
		fmt.Fprintf(b, "map(%v)", strings.Join(list, ", "))
	case ExprExec:
		decls := make([]string, len(e.Decls))
		for i := range e.Decls {
			decls[i] = e.Decls[i].String()
		}
		fmt.Fprintf(b, "exec(decls(%v), %v, %q)",
			strings.Join(decls, ", "), e.Type, e.Template)
	case ExprCond:
		fmt.Fprintf(b, "cond(%v, %v, %v)", e.Cond, e.Left, e.Right)
	case ExprDeref:
		fmt.Fprintf(b, "deref(%v, %v)", e.Left, e.Ident)
	case ExprCompr:
		clauses := make([]string, len(e.ComprClauses))
		for i, clause := range e.ComprClauses {
			switch clause.Kind {
			case ComprEnum:
				clauses[i] = fmt.Sprintf("enum(%v, %v)", clause.Expr, clause.Pat)
			case ComprFilter:
				clauses[i] = fmt.Sprintf("filter(%v)", clause.Expr)
			}
		}
		fmt.Fprintf(b, "compr(%v, %s)", e.ComprExpr, strings.Join(clauses, ", "))
	case ExprThunk:
		fmt.Fprintf(b, "thunk(%v, %v)", e.Left, e.Env)
	case ExprBuiltin:
		fmt.Fprintf(b, "builtin(%v, %v)", e.Op, e.Left)
	case ExprRequires:
		decls := make([]string, len(e.Decls))
		for i := range e.Decls {
			decls[i] = e.Decls[i].String()
		}
		fmt.Fprintf(b, "resources(%s, %s)",
			e.Left, strings.Join(decls, ", "))
	case ExprMake:
		decls := make([]string, len(e.Decls))
		for i := range e.Decls {
			decls[i] = e.Decls[i].String()
		}
		fmt.Fprintf(b, "make(%s, %v)", e.Left, strings.Join(decls, ", "))
	case ExprIndex:
		fmt.Fprintf(b, "index(%s, %s)", e.Left, e.Right)
	}
	return b.String()
}

// BinopPrec stores the precedence of binary operators
// as specified in the grammar. These are used to render
// expressions with proper parenthesization.
var binopPrec = map[string]int{
	"||": 2,
	"&&": 3,
	"<":  5, ">": 5, "<=": 5, ">=": 5, "!=": 5, "==": 5,
	"+": 6, "-": 6, "|": 6, "^": 6,
	"*": 7, "/": 7, "%": 7, "&": 7, "<<": 7, ">>": 7,
}

// Prec returns the precedence of expression e. If it is not a binary
// op, its precedence is 0.
func (e *Expr) prec() int {
	if e.Kind != ExprBinop {
		return 0
	}
	prec, ok := binopPrec[e.Op]
	if !ok {
		panic("undefined precedence for binop " + e.Op)
	}
	return prec
}

// Abbrev shows an "abbreviated" pretty-printed version of expression e.
// These are useful when showing expression values in documentary output;
// but they do not necessarily parse. Abbrev strips unnecessary parentheses
// from arithmetic expressions.
func (e *Expr) Abbrev() string {
	switch e.Kind {
	case ExprError, ExprThunk:
		return "<error>"
	case ExprIdent:
		return e.Ident
	case ExprBinop:
		right := e.Right.Abbrev()
		if l, r := e.prec(), e.Right.prec(); r <= l && l != 0 && r != 0 {
			right = "(" + right + ")"
		}
		return e.Left.Abbrev() + " " + e.Op + " " + right
	case ExprUnop:
		return e.Op + e.Left.Abbrev()
	case ExprApply:
		fields := make([]string, len(e.Fields))
		for i := range fields {
			fields[i] = e.Fields[i].Abbrev()
		}
		return fmt.Sprintf("%s(%s)", e.Left.Abbrev(), strings.Join(fields, ", "))
	case ExprLit:
		// Constant expressions are always constructed with a type.
		return values.Sprint(e.Val, e.Type)
	case ExprAscribe:
		// TODO(marius): this doesn't have concrete syntax
		return e.Left.Abbrev()
	case ExprBlock:
		// TODO(marius):
		return fmt.Sprintf("{...; %s}", e.Left.Abbrev())
	case ExprFunc:
		return "<func>"
	case ExprTuple:
		fields := make([]string, len(e.Fields))
		for i := range fields {
			fields[i] = e.Fields[i].Abbrev()
		}
		return "(" + strings.Join(fields, ", ") + ")"
	case ExprStruct:
		fields := make([]string, len(e.Fields))
		for i := range fields {
			fields[i] = e.Fields[i].Name + ":" + e.Fields[i].Abbrev()
		}
		return "{" + strings.Join(fields, ", ") + ")"
	case ExprList:
		elems := make([]string, len(e.List))
		for i := range elems {
			elems[i] = e.List[i].Abbrev()
		}
		return "[" + strings.Join(elems, ", ") + "]"
	case ExprMap:
		var elems []string
		for ke, ve := range e.Map {
			elems = append(elems, fmt.Sprintf("%s: %s", ke.Abbrev(), ve.Abbrev()))
		}
		return "[" + strings.Join(elems, ", ") + "]"
	case ExprExec:
		return "<exec>"
	case ExprCond:
		return fmt.Sprintf("if %s { %s } else { %s }", e.Cond.Abbrev(), e.Left.Abbrev(), e.Right.Abbrev())
	case ExprDeref:
		return e.Left.Abbrev() + "." + e.Ident
	case ExprIndex:
		return fmt.Sprintf("%s[%s]", e.Left.Abbrev(), e.Right.Abbrev())
	case ExprCompr:
		return "<compr>"
	case ExprMake:
		return "<make>"
	case ExprBuiltin:
		var b bytes.Buffer
		b.WriteString(e.Op)
		b.WriteString("(")
		if e.Left != nil {
			b.WriteString(e.Left.Abbrev())
		}
		if e.Right != nil {
			b.WriteString(", ")
			b.WriteString(e.Right.Abbrev())
		}
		b.WriteString(")")
		return b.String()
	case ExprRequires:
		return "<requires>"
	default:
		panic("unhandled expression " + e.String())
	}
}

func (e *Expr) identOr(alt string) string {
	switch e.Kind {
	case ExprIdent:
		return e.Ident
	default:
		return alt
	}
}

// sortedMapKeys returns e.Map's keys, sorted by expression digest.
func (e *Expr) sortedMapKeys(env *values.Env) []*Expr {
	var keys []*Expr
	for k := range e.Map {
		keys = append(keys, k)
	}
	sortExprs(env, keys)
	return keys
}

type sortedExpr struct {
	exprs []*Expr
	env   *values.Env
}

func sortExprs(env *values.Env, exprs []*Expr) {
	sort.Sort(sortedExpr{exprs: exprs, env: env})
}

func (s sortedExpr) Len() int { return len(s.exprs) }
func (s sortedExpr) Less(i, j int) bool {
	var (
		di = s.exprs[i].Digest(s.env)
		dj = s.exprs[j].Digest(s.env)
	)
	return di.Less(dj)
}
func (s sortedExpr) Swap(i, j int) {
	s.exprs[i], s.exprs[j] = s.exprs[j], s.exprs[i]
}

func reportUnused(sess *Session, env *types.Env) {
	syms := env.Unused()
	// Sort them so that we get consistent (testable) warning ordering.
	sort.Slice(syms, func(i, j int) bool {
		x, y := syms[i], syms[j]
		if x.Filename != y.Filename {
			return x.Filename < y.Filename
		}
		if x.Offset != y.Offset {
			return x.Offset < y.Offset
		}
		if x.Line != y.Line {
			return x.Line < y.Line
		}
		return x.Column < y.Column
	})
	for _, sym := range syms {
		sess.Warnf(sym.Position, "%s declared and not used", sym.Name)
	}
}
