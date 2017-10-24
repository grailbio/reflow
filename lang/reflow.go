// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package lang

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strings"
	"text/scanner"

	units "github.com/docker/go-units"
	"github.com/grailbio/reflow"
)

//go:generate goyacc reflow.y
//go:generate stringer -type=Op

func init() {
	// These are usually helpful, even for end-users.
	yyErrorVerbose = true
}

// Op is the operation that an expression or statement implements.
type Op int

const (
	opIdent Op = 1 + iota
	opApply
	opConst
	opIntern
	opExtern
	opDef
	opImage
	opCollect
	opGroupby
	opMap
	opFunc
	opVarfunc
	opExec
	opParam
	opConcat
	opPullup
	opLet
)

// Stmt implements a statement in reflow. It contains
// its operation and arguments (left, right, list).
type Stmt struct {
	scanner.Position
	left  *Expr
	op    Op
	right *Expr
	list  []*Expr
	val   *Val
}

// Pos tells the position of the statement.
func (s *Stmt) Pos() scanner.Position { return s.Position }

// Type computes the type of s in the type environment env.
func (s *Stmt) Type(env TypeEnv) Type {
	switch s.op {
	case opDef:
		if s.right.Type(env) == typeError {
			return typeError
		}
		return typeVoid
	case opExtern:
		if got, want := s.list[0].Type(env), typeFlow; got != want {
			env.Errorf(s.list[0].Pos(), "expected %s got %s", want, got)
			return typeError
		}
		if got, want := s.list[1].Type(env), typeString; got != want {
			env.Errorf(s.list[1].Pos(), "expected %s got %s", want, got)
			return typeError
		}
		return typeVoid
	default:
		env.Errorf(s.Pos(), "illegal statement type %s", s.op)
		return typeError
	}
	return typeError
}

// Expr implements expressions in reflow. They contain the
// expression's op and arguments (left, right, list) as well as any
// literal values (ident, val).
type Expr struct {
	scanner.Position
	op    Op
	left  *Expr
	right *Expr
	list  []*Expr
	ident string
	val   *Val
}

// Pos tells the position of the expression.
func (e *Expr) Pos() scanner.Position { return e.Position }

// Type computes the type of e in the type environment env.
func (e *Expr) Type(env TypeEnv) Type {
	env.Push()
	defer env.Pop()
	switch e.op {
	case opIdent:
		// Local environment overrides toplevel environment.
		if t, ok := env.Type(e.ident); ok {
			return t
		}
		// TODO(marius): cut off recursive definitions.
		ee, ok := env.Def[e.ident]
		if !ok {
			env.Errorf(e.Pos(), "identifier %q not defined", e.ident)
			return typeError
		}
		return ee.Type(env)
	case opFunc:
		// All arguments are products (for now)
		for _, ee := range e.list {
			env.Bind(ee.ident, typeFlow)
		}
		r := e.right.Type(env)
		if r == typeError {
			return typeError
		}
		return typeFunc(len(e.list), r)
	case opVarfunc:
		env.Bind(e.list[0].ident, typeFlowList)
		r := e.right.Type(env)
		if r == typeError {
			return typeError
		}
		return typeFunc(0, r)
	case opExec:
		if t := e.left.Type(env); t != typeImage {
			env.Errorf(e.left.Pos(), "expected typeImage, got %s", t)
			return typeError
		}
		if t := e.right.Type(env); t != typeTemplate {
			env.Errorf(e.right.Pos(), "expected typeTemplate, got %s", t)
			return typeError
		}
		for _, ee := range e.list {
			if t := ee.Type(env); t != typeString {
				env.Errorf(ee.Pos(), "expected typeString, got %s", t)
				return typeError
			}
		}
		return typeFlow
	case opApply:
		if t := e.left.Type(env); t >= typeFuncBase {
			n, r := funcType(t)
			if n == 0 {
				if got, wanted := len(e.list), 1; got != wanted {
					env.Errorf(e.Pos(), "wrong number of arguments; expected %d got %d", wanted, got)
					return typeError
				}
				if e := e.list[0]; e.Type(env) != typeFlowList {
					env.Errorf(e.Pos(), "only productsets are allowed as var function arguments")
					return typeError
				}
			} else {
				if got, wanted := len(e.list), n; got != wanted {
					env.Errorf(e.Pos(), "wrong number of arguments; expected %d got %d", wanted, got)
					return typeError
				}
				for _, ee := range e.list {
					if ee.Type(env) != typeFlow {
						env.Errorf(ee.Pos(), "only products are allowed as function arguments")
						return typeError
					}
				}
			}
			return r
		} else if t == typeError {
			return t
		} else {
			env.Errorf(e.left.Pos(), "type %s cannot be applied", t)
			return typeError
		}
	case opConst:
		if e.val.typ == typeTemplate {
			// typecheck interpolation, or else error
			template, err := newTemplate(e.val.str)
			if err != nil {
				env.Errorf(e.Pos(), "bad template: %s", err)
				return typeError
			}
			for _, id := range template.Idents {
				t, ok := env.Type(id)
				if !ok {
					ee, ok := env.Def[id]
					if !ok {
						env.Errorf(e.Pos(), "identifier %q not defined", id)
						return typeError
					}
					env.Push()
					t = ee.Type(env)
					env.Pop()
				}
				if t == typeError {
					return typeError
				} else if t != typeFlow && t != typeFlowList && t != typeString {
					env.Errorf(e.Pos(), "invalid type %s for interpolated string", t)
					return typeError
				}
			}
		}
		return e.val.typ
	case opIntern:
		t := e.left.Type(env)
		switch t {
		case typeString:
			return typeFlow
		case typeStringList:
			return typeFlowList
		default:
			env.Errorf(e.Pos(), "expected string expression, got %v", t)
			return typeError
		}
	case opImage:
		if e.left.Type(env) != typeString {
			env.Errorf(e.left.Pos(), "expected string expression")
			return typeError
		}
		return typeImage
	case opGroupby:
		product, sel := e.list[0], e.list[1]
		if t := product.Type(env); t != typeFlow {
			env.Errorf(product.Pos(), "expected product, not %s", t)
			return typeError
		}
		if sel.Type(env) != typeString {
			env.Errorf(sel.Pos(), "expected string")
			return typeError
		}
		return typeFlowList
	case opMap:
		productset, fun := e.list[0], e.list[1]
		if productset.Type(env) != typeFlowList {
			env.Errorf(productset.Pos(), "expected productset")
			return typeError
		}
		if t := fun.Type(env); t != typeFunc(1, typeFlow) {
			env.Errorf(fun.Pos(), "expected flow->flow, got %s", t)
			return typeError
		}
		return typeFlowList
	case opCollect:
		product, re := e.list[0], e.list[1]
		if product.Type(env) != typeFlow {
			env.Errorf(product.Pos(), "expected product")
			return typeError
		}
		if re.Type(env) != typeString {
			env.Errorf(re.Pos(), "expected regular expression")
			return typeError
		}
		if len(e.list) == 3 {
			repl := e.list[2]
			if repl.Type(env) != typeString {
				env.Errorf(repl.Pos(), "expected replacement string")
				return typeError
			}
		}
		return typeFlow
	case opParam:
		for _, e := range e.list {
			if got, want := e.Type(env), typeString; got != want {
				env.Errorf(e.Pos(), "expected %s got %s", want, got)
				return typeError
			}
		}
		return typeString
	case opConcat:
		// Once functions are real we can just put this in the environment.
		for _, e := range e.list {
			if got, want := e.Type(env), typeString; got != want {
				env.Errorf(e.Pos(), "expected %s got %s", want, got)
				return typeError
			}
		}
		return typeString
	case opPullup:
		if len(e.list) == 0 {
			env.Errorf(e.Pos(), "empty pullup")
			return typeError
		}
		for _, ee := range e.list {
			t := ee.Type(env)
			switch t {
			case typeFlow, typeFlowList:
			default:
				env.Errorf(ee.Pos(), "expected flow or list<flow>, got %v", t)
				return typeError
			}
		}
		return typeFlow
	case opLet:
		t := e.list[0].Type(env)
		if t == typeError {
			return typeError
		}
		env.Bind(e.left.ident, t)
		return e.right.Type(env)
	default:
		env.Errorf(e.Pos(), "illegal expression type %s", e.op)
	}
	return typeError
}

// Eval evaluates the expression e in the evaluation environment env.
// Eval assumes the expression has been typechecked; thus the
// expression is well-formed.
func (e *Expr) Eval(env EvalEnv) Val {
	env.Push()
	defer env.Pop()
	switch e.op {
	case opIdent:
		// Local environment overrides toplevel environment.
		if v, ok := env.Val(e.ident); ok {
			return v
		}
		ee, ok := env.Def[e.ident]
		if !ok {
			panic("bug")
		}
		env.SetIdent(e.ident)
		return ee.Eval(env)
	case opFunc, opVarfunc:
		fn := func(args ...Val) Val {
			if e.op == opFunc && len(args) != len(e.list) {
				panic("bug")
			}
			if e.op == opVarfunc && len(args) != 1 {
				panic("bug")
			}
			for i := range args {
				env.Bind(e.list[i].ident, args[i])
			}
			return e.right.Eval(env)
		}
		return Val{typ: typeFuncBase /*this is ugly*/, fn: fn}
	case opGroupby:
		// TODO(marius): parse regexp in type checking phase (and perhaps demand a literal).
		product := e.list[0].Eval(env)
		arg := e.list[1].Eval(env)
		re, err := regexp.Compile(arg.str)
		if err != nil {
			panic(fmt.Sprintf("regexp %s", err))
		}
		if re.NumSubexp() != 1 {
			panic("wrong number of subexpressions")
		}
		f := &reflow.Flow{Deps: []*reflow.Flow{product.flow}, Op: reflow.OpGroupby, Re: re, Ident: env.Ident(e.Position)}
		return Val{typ: typeFlowList, flow: f}
	case opMap:
		arg := e.list[0].Eval(env).flow
		fn := e.list[1].Eval(env).fn
		mapFunc := func(f *reflow.Flow) *reflow.Flow {
			return fn(Val{typ: typeFlow, flow: f}).flow
		}
		f := &reflow.Flow{Deps: []*reflow.Flow{arg}, Op: reflow.OpMap, MapFunc: mapFunc, Ident: env.Ident(e.Position)}
		f.MapInit()
		return Val{typ: typeFlowList, flow: f}
	case opExec:
		image := e.left.Eval(env)
		cmd, err := newTemplate(e.right.Eval(env).str)
		if err != nil {
			panic("bug")
		}
		var (
			deps    []*reflow.Flow
			filled  = make([]string, len(cmd.Idents))
			argstrs []string
		)
		for i, id := range cmd.Idents {
			v, ok := env.Val(id)
			if !ok {
				e, ok := env.Def[id]
				if !ok {
					panic("bug")
				}
				env.Push()
				env.SetIdent(id)
				v = e.Eval(env)
				env.Pop()
			}
			// TODO(marius): reflect on the type to determine how to render this.
			if v.flow != nil {
				deps = append(deps, v.flow)
				argstrs = append(argstrs, id)
				filled[i] = "%s"
			} else {
				filled[i] = v.str
			}
		}
		fmtstr := cmd.Format(filled...)
		f := &reflow.Flow{
			Deps:    deps,
			Op:      reflow.OpExec,
			Argstrs: argstrs,
			Image:   image.str,
			Cmd:     fmtstr,
			Ident:   env.Ident(e.Position),
		}
		for i, arg := range f.Argstrs {
			f.Argstrs[i] = "{{" + arg + "}}"
		}
		for _, ee := range e.list {
			// TODO(marius): move to type checking.
			s := ee.Eval(env).str
			limit, err := units.ParseUlimit(s)
			if err != nil {
				panic(fmt.Sprintf("%s: (%s %v) invalid limit %q: %s from %#v", ee.Position, e.op, e.op == opExec, s, err, e))
			}
			switch limit.Name {
			case "rss":
				f.Resources.Memory = uint64(limit.Hard)
			case "cpu":
				f.Resources.CPU = uint16(limit.Hard)
			case "data":
				f.Resources.Disk = uint64(limit.Hard)
			}
		}
		return Val{typ: typeFlow, flow: f}
	case opApply:
		fn := e.left.Eval(env).fn
		args := make([]Val, len(e.list))
		for i := range e.list {
			args[i] = e.list[i].Eval(env)
		}
		return fn(args...)
	case opConst:
		return *e.val
	case opIntern:
		v := e.left.Eval(env)
		switch v.typ {
		case typeString:
			flow, err := internFlow(v.str, env.Ident(e.Position))
			if err != nil {
				// TODO(marius): emit error flow.
				panic(err)
			}
			return Val{typ: typeFlow, flow: flow}
		case typeStringList:
			flow := &reflow.Flow{
				Op:   reflow.OpMerge,
				Deps: make([]*reflow.Flow, len(v.list)),
			}
			for i, v := range v.list {
				var err error
				flow.Deps[i], err = internFlow(v.str, env.Ident(e.Position))
				if err != nil {
					// TODO(marius): emit error flow.
					panic(err)
				}
			}
			return Val{typ: typeFlowList, flow: flow}
		}
	case opImage:
		return Val{typ: typeImage, str: e.left.Eval(env).str}
	case opCollect:
		dep := e.list[0].Eval(env).flow
		re := regexp.MustCompile(e.list[1].Eval(env).str)
		repl := "$0"
		if len(e.list) == 3 {
			repl = e.list[2].Eval(env).str
		}
		f := &reflow.Flow{Op: reflow.OpCollect, Deps: []*reflow.Flow{dep}, Re: re, Repl: repl, Ident: env.Ident(e.Position)}
		return Val{typ: typeFlow, flow: f}
	case opParam:
		ident, help := e.list[0].Eval(env).str, e.list[1].Eval(env).str
		p, ok := env.Param(ident, help)
		if !ok {
			panic(fmt.Sprintf("param %q undefined", ident))
		}
		return Val{typ: typeString, str: p}
	case opConcat:
		strs := make([]string, len(e.list))
		for i := range e.list {
			strs[i] = e.list[i].Eval(env).str
		}
		return Val{typ: typeString, str: strings.Join(strs, "")}
	case opPullup:
		flows := make([]*reflow.Flow, len(e.list))
		for i := range e.list {
			flows[i] = e.list[i].Eval(env).flow
		}
		return Val{typ: typeFlow, flow: &reflow.Flow{Op: reflow.OpPullup, Deps: flows, Ident: env.Ident(e.Position)}}
	case opLet:
		env.Bind(e.left.ident, e.list[0].Eval(env))
		return e.right.Eval(env)
	}
	panic(fmt.Sprintf("unexpected op %s", e.op))
}

// String returns a human-readable representation of an expression.
func (e *Expr) String() string {
	s := fmt.Sprintf("expr(%s", e.op)
	if e.left != nil {
		s += fmt.Sprintf(" left(%s)", e.left)
	}
	if e.right != nil {
		s += fmt.Sprintf(" right(%s)", e.right)
	}
	if len(e.list) > 0 {
		es := make([]string, len(e.list))
		for i := range e.list {
			es[i] = e.list[i].String()
		}
		s += fmt.Sprintf(" list(%s)", strings.Join(es, ", "))
	}
	if e.ident != "" {
		s += fmt.Sprintf(" ident(%q)", e.ident)
	}
	return s + ")"
}

// Walk walks the visitor v through the expression. Walk recursively
// visits the current expression, then its left, right, and list
// arguments. Finally, it visits nil.
func (e *Expr) Walk(v Visitor) {
	v = v.Visit(e)
	if v == nil {
		return
	}
	if e.left != nil {
		e.left.Walk(v)
	}
	if e.right != nil {
		e.right.Walk(v)
	}
	for _, ee := range e.list {
		ee.Walk(v)
	}
	v.Visit(nil)
}

// Visitor is implemented by Expr's visitors.
type Visitor interface {
	Visit(e *Expr) Visitor
}

// Val is a container type for values in the reflow language.
type Val struct {
	typ  Type
	num  float64
	str  string
	flow *reflow.Flow
	list []Val
	fn   func(args ...Val) Val
}

type evalEnvStack struct {
	venv  *valEnv
	ident string
}

// EvalEnv contains the evaluation used by reflow. It contains a set
// of defs, params, and a value environment. It is an error reporter.
type EvalEnv struct {
	*Error

	// Def contains toplevel defs available in the environment.
	Def map[string]*Expr
	// Param returns the value of parameter id.
	// The second argument returned indicates whether the
	// parameter was defined.
	Param func(id, help string) (string, bool)

	stack []evalEnvStack
}

// Bind binds a val to an ident.
func (e *EvalEnv) Bind(ident string, val Val) {
	e.stack[0].venv = e.stack[0].venv.Bind(ident, val)
}

// Val returns the val bound to ident.
func (e *EvalEnv) Val(ident string) (Val, bool) {
	return e.stack[0].venv.Val(ident)
}

// SetIdent sets the current ident.
func (e *EvalEnv) SetIdent(ident string) {
	e.stack[0].ident = ident
}

// Ident returns the current ident.
func (e *EvalEnv) Ident(pos scanner.Position) string {
	id := e.stack[0].ident
	if id == "" {
		return pos.String()
	}
	return id
}

// Push pushes the current evaluation environment onto the stack.
func (e *EvalEnv) Push() {
	var s evalEnvStack
	if len(e.stack) > 0 {
		s = e.stack[0]
	}
	e.stack = append([]evalEnvStack{s}, e.stack...)
}

// Pop pops the evaluation environment stack.
func (e *EvalEnv) Pop() {
	e.stack = e.stack[1:]
}

// valEnv implements value environment as a linked list.
type valEnv struct {
	ident string
	val   Val
	next  *valEnv
}

// Bind binds a value to an identifier.
func (e *valEnv) Bind(ident string, val Val) *valEnv {
	return &valEnv{ident, val, e}
}

// Val returns the value bound to ident. The second return value
// indicates whether ident was defined.
func (e *valEnv) Val(ident string) (Val, bool) {
	for e != nil {
		if e.ident == ident {
			return e.val, true
		}
		e = e.next
	}
	return Val{}, false
}

// internFlow constructs a flow from an intern spec.
func internFlow(intern, ident string) (*reflow.Flow, error) {
	urls := strings.Split(intern, ",")
	switch len(urls) {
	case 0:
		return nil, errors.New("empty interns")
	case 1:
		intern, name, err := internURLFlow(urls[0], ident)
		if err != nil {
			return nil, err
		}
		if name == "" {
			return intern, nil
		}
		collect := &reflow.Flow{
			Op:    reflow.OpCollect,
			Deps:  []*reflow.Flow{intern},
			Ident: ident,
		}
		_, file := path.Split(intern.URL.Path)
		if file == "" {
			collect.Re = regexp.MustCompile(`^`)
			collect.Repl = name + "/"
		} else {
			collect.Re = regexp.MustCompile(`\.`)
			collect.Repl = name
		}
		return collect, nil
	default:
		// In this case, we construct a "virtual" directory, using the
		// base names for each entry.
		pullup := &reflow.Flow{Op: reflow.OpPullup, Ident: ident}
		for _, u := range urls {
			if u == "" {
				continue
			}
			intern, name, err := internURLFlow(u, ident)
			if err != nil {
				return nil, err
			}
			collect := &reflow.Flow{Op: reflow.OpCollect, Deps: []*reflow.Flow{intern}, Ident: ident}
			dir, file := path.Split(intern.URL.Path)
			if file == "" {
				collect.Re = regexp.MustCompile(`^`)
				if name == "" {
					name = path.Base(dir)
				}
				collect.Repl = name + "/"
			} else {
				collect.Re = regexp.MustCompile(`\.`)
				if name == "" {
					name = file
				}
				collect.Repl = name
			}
			pullup.Deps = append(pullup.Deps, collect)
		}
		return pullup, nil
	}
}

func internURLFlow(u, ident string) (*reflow.Flow, string, error) {
	i := strings.Index(u, "://")
	if i < 0 {
		return nil, "", fmt.Errorf("URL %q has no scheme", u)
	}
	i = strings.Index(u[:i], "=")
	if i < 0 {
		flow := &reflow.Flow{Op: reflow.OpIntern, Ident: ident}
		var err error
		flow.URL, err = url.Parse(u)
		return flow, "", err
	}
	name, u := u[:i], u[i+1:]
	flow := &reflow.Flow{Op: reflow.OpIntern, Ident: ident}
	var err error
	flow.URL, err = url.Parse(u)
	return flow, name, err
}
