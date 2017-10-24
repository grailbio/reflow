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

	"github.com/grailbio/reflow"
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

	config  reflow.Config
	params  map[string]*string
	def     map[string]*Expr
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
	if len(p.externs) == 0 {
		return errors.New("no externs")
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
// statements are merged into a single reflow.OpMerge node.
func (p *Program) Eval() *reflow.Flow {
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
	flows := make([]*reflow.Flow, len(p.externs))
	for i, s := range p.externs {
		dep := s.list[0].Eval(env).flow
		u, err := url.Parse(s.list[1].Eval(env).str)
		if err != nil {
			// typechecking?
			panic(err)
		}
		flows[i] = &reflow.Flow{Op: reflow.OpExtern, URL: u, Deps: []*reflow.Flow{dep}, Ident: s.Position.String()}
	}
	root := &reflow.Flow{Op: reflow.OpMerge, Deps: flows, Ident: "main"}
	if p.config.IsZero() {
		return root
	}
	return root.Canonicalize(p.config)
}
