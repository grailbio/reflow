// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package lang

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"unicode"
	"unicode/utf8"

	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// Program represents a reflow program. It parses, typechecks, and
// evaluates reflow programs, managing parameters via Go's flags
// package.
type Program struct {
	// Errors is the writer to which errors are reported.
	Errors io.Writer
	// File is the name of the file containing the reflow program.
	File string
	// Args contains the command-line arguments (but not flags)
	// used for this program invocation. Args must be set before
	// calling Eval.
	Args []string

	config  flow.Config
	params  map[string]*string
	def     map[string]*Expr
	types   map[string]Type
	externs []*Stmt
	flags   *flag.FlagSet
}

// Visit implements the expression visitor used for converting
// program parameters to flags.
func (p *Program) Visit(e *Expr) Visitor {
	if e == nil || e.op != opParam {
		return p
	}
	flag, help := e.list[0].val.str, e.list[1].val.str
	if p.params[flag] == nil {
		p.params[flag] = p.flags.String(flag, "", help)
	}
	return nil
}

// ParseAndTypecheck parses the program presented by the io.Reader r.
// It returns any error.
func (p *Program) ParseAndTypecheck(r io.Reader) error {
	error := Error{W: p.Errors}
	lx := &Lexer{error: &error, File: p.File, Body: r, Mode: LexerTop}
	lx.Init()
	yyParse(lx)
	if error.N > 0 {
		return errors.New("syntax error")
	}
	switch lx.HashVersion {
	case "v2":
		p.config.HashV1 = false
	case "", "v1":
		p.config.HashV1 = true
	default:
		return fmt.Errorf("unrecognized hash version %q", lx.HashVersion)
	}
	p.def = map[string]*Expr{}
	for _, s := range lx.Stmts {
		if s.op == opDef {
			p.def[s.left.ident] = s.right
		}
	}
	env := TypeEnv{Error: &error, Def: p.def}
	env.Push()
	env.Bind("args", typeStringList)
	for _, s := range lx.Stmts {
		if t := s.Type(env); t == typeError {
			error.Errorf(s.Pos(), "bad type")
			return errors.New("type error")
		}
		if s.op == opExtern {
			p.externs = append(p.externs, s)
		}
	}
	// Compute the types for each toplevel def.
	p.types = map[string]Type{}
	for id, e := range p.def {
		p.types[id] = e.Type(env)
	}
	p.flags = &flag.FlagSet{}
	p.params = map[string]*string{}
	for _, e := range p.def {
		e.Walk(p)
	}
	return nil
}

// Flags returns the set of flags that are defined by the program.
// It is defined only after ParseAndTypecheck has been called.
// Flags may be set to parameterize the program.
func (p *Program) Flags() *flag.FlagSet {
	return p.flags
}

// Eval evaluates the program and returns a flow. All toplevel extern
// statements are merged into a single flow.Merge node.
func (p *Program) Eval() *flow.Flow {
	if len(p.externs) == 0 {
		// Nothing to do. Return empty value.
		return &flow.Flow{Op: flow.Val}
	}
	env := EvalEnv{
		Error: &Error{W: p.Errors},
		Def:   p.def,
		Param: func(id, help string) (string, bool) {
			vp, ok := p.params[id]
			v := ""
			if vp != nil {
				v = *vp
			}
			return v, ok
		},
	}
	var args Val
	args.typ = typeStringList
	args.list = make([]Val, len(p.Args))
	for i, arg := range p.Args {
		args.list[i] = Val{typ: typeString, str: arg}
	}
	env.Push()
	env.Bind("args", args)
	if env.Error.N > 0 {
		// TODO(marius): can we get rid of errors in the eval environment?
		panic("unexpected errors during evaluation")
	}

	flows := make([]*flow.Flow, len(p.externs))
	for i, s := range p.externs {
		dep := s.list[0].Eval(env).flow
		u, err := url.Parse(s.list[1].Eval(env).str)
		if err != nil {
			// typechecking?
			panic(err)
		}
		flows[i] = &flow.Flow{Op: flow.Extern, URL: u, Deps: []*flow.Flow{dep}, Ident: s.Position.String()}
	}
	root := &flow.Flow{Op: flow.Merge, Deps: flows, Ident: "main"}
	if p.config.IsZero() {
		return root
	}
	return root.Canonicalize(p.config)
}

// ModuleType computes and returns the Reflow module type for this
// program. This is used for bridging "v0" scripts into "v1" modules.
// This should be called only after type checking has completed.
//
// For simplicity we only export non-function values, since they
// always evaluate to either immediate values.T or else to Flows,
// both of which have defined digests. We don't let functions escape.
func (p *Program) ModuleType() *types.T {
	var fields []*types.Field
	for id, t := range p.types {
		first, _ := utf8.DecodeRuneInString(id)
		if first == utf8.RuneError || !unicode.IsUpper(first) {
			continue
		}
		typ := t.ReflowType()
		if typ != nil {
			fields = append(fields, &types.Field{Name: id, T: typ})
		}
	}
	fields = append(fields, &types.Field{Name: "Main", T: types.Fileset})
	return types.Module(fields, nil)
}

// ModuleValue computes the Reflow module value given the set of
// defined parameters.
func (p *Program) ModuleValue() (values.T, error) {
	env := EvalEnv{
		Error: &Error{W: p.Errors},
		Def:   p.def,
		Param: func(id, help string) (string, bool) {
			vp, ok := p.params[id]
			v := ""
			if vp != nil {
				v = *vp
			}
			return v, ok
		},
	}
	var args Val
	args.typ = typeStringList
	args.list = make([]Val, len(p.Args))
	for i, arg := range p.Args {
		args.list[i] = Val{typ: typeString, str: arg}
	}
	env.Push()
	env.Bind("args", args)

	mval := make(values.Module)
	for _, f := range p.ModuleType().Fields {
		if f.Name == "Main" {
			continue
		}
		expr := p.def[f.Name]
		mval[f.Name] = v02v1(expr.Eval(env), f.T)
	}
	if !p.config.IsZero() {
		for id, v := range mval {
			f, ok := v.(*flow.Flow)
			if !ok {
				continue
			}
			mval[id] = f.Canonicalize(p.config)
		}
	}
	return mval, nil
}

func v02v1(v0 Val, t *types.T) (v1 values.T) {
	switch t.Kind {
	case types.StringKind:
		v1 = v0.str
	case types.IntKind:
		v1 = values.NewInt(int64(v0.num))
	case types.UnitKind:
		v1 = values.Unit
	case types.ListKind:
		l := make(values.List, len(v0.list))
		switch t.Elem.Kind {
		default:
			panic("invalid type")
		case types.StringKind:
			for i := range l {
				l[i] = v0.list[i].str
			}
		case types.FilesetKind:
			for i := range l {
				l[i] = v0.list[i].flow
			}
		}
		v1 = l
	case types.FilesetKind:
		v1 = v0.flow
	default:
		panic("bad type")
	}
	return v1
}
