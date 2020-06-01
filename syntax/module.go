// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"errors"
	"flag"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

// Param holds module parameter metadata.
type Param struct {
	Ident    string
	Type     *types.T
	Doc      string
	Expr     *Expr
	Required bool
}

// Module abstracts a Reflow module, having the ability to type check
// parameters, inspect its type, and mint new instances.
type Module interface {
	// Make creates a new module instance in the provided session
	// with the provided parameters.
	Make(sess *Session, params *values.Env) (values.T, error)
	// ParamErr type-checks parameter types, returning an error on failure.
	ParamErr(env *types.Env) error
	// Flags returns the set of flags provided by this module.
	// Note that not all modules may have parameters that are supported
	// by the regular flag types. These return an error.
	Flags(sess *Session, env *values.Env) (*flag.FlagSet, error)
	// FlagEnv adds flags from the FlagSet to value environment env.
	// The FlagSet should be produced by Module.Flags.
	FlagEnv(flags *flag.FlagSet, env *values.Env, tenv *types.Env) error
	// Params returns the parameter descriptors for this module.
	Params() []Param
	// Doc returns the docstring for a toplevel identifier.
	Doc(string) string
	// Type returns the type of the module.
	Type() *types.T
	// Eager tells whether the module requires eager parameters.
	// When it does, all parameters are forced and fully evaluated
	// before instantiating a new module instance.
	Eager() bool
	// Source of this module. This can be used to archive the program
	// and rerun the program on a different machine.
	Source() []byte
	// InjectArgs injects a set of command line flags to override module
	// parameters.
	InjectArgs(sess *Session, args []string) error
	// InjectedArgs returns the arguments that were injected into this module.
	InjectedArgs() []string
}

// ModuleImpl defines a Reflow module comprising: a keyspace, a set of
// parameters, and a set of declarations.
type ModuleImpl struct {
	// Keyspace is the (optional) key space of this module.
	Keyspace *Expr
	// Reservation is the set of reservation declarations.
	Reservation []*Decl
	// ParamDecls is the set of declared parameters for this module.
	ParamDecls []*Decl
	// Decls is the set of declarations in this module.
	Decls []*Decl

	Docs map[string]string

	// source bytes of this module.
	source []byte
	typ    *types.T
	tenv   *types.Env

	injectedArgs []string
	fenv         *values.Env
	ftenv        *types.Env
}

// Source gets the source code of this module.
func (m *ModuleImpl) Source() []byte {
	return m.source
}

// Eager returns false.
func (m *ModuleImpl) Eager() bool { return false }

// Init type checks this module and returns any type checking errors.
func (m *ModuleImpl) Init(sess *Session, env *types.Env) error {
	var el errlist
	env = env.Push()
	for _, p := range m.ParamDecls {
		// TODO(marius): enforce no flow types here.
		if err := p.Init(sess, env); err != nil {
			el = el.Append(err)
			continue
		}
		switch p.Kind {
		default:
			// Type declarations aren't permitted in parameters yet.
			panic("bug")
		case DeclError:
			el = el.Errorf(p.Position, "declaration error")
		case DeclDeclare:
			env.Bind(p.Ident, p.Type)
		case DeclAssign:
			if err := p.Pat.BindTypes(env, p.Type); err != nil && p.Type.Kind != types.ErrorKind {
				p.Type = types.Error(err)
			}
		}
	}
	for _, d := range m.Decls {
		if err := d.Init(sess, env); err != nil {
			el = el.Append(err)
			continue
		}
		if d.Type.Kind == types.ErrorKind {
			el = el.Error(d.Position, d.Type.Error)
			continue
		}
		switch d.Kind {
		case DeclType:
			env.BindAlias(d.Ident, d.Type)
		case DeclAssign, DeclDeclare:
			if err := d.Pat.BindTypes(env, d.Type); err != nil {
				d.Type = types.Error(err)
				el = el.Error(d.Position, err)
			}
		}
	}

	m.Docs = make(map[string]string)

	var (
		fields  []*types.Field
		aliases []*types.Field
	)
	for _, d := range m.Decls {
		switch d.Kind {
		case DeclType:
			first, _ := utf8.DecodeRuneInString(d.Ident)
			if first == utf8.RuneError || !unicode.IsUpper(first) {
				continue
			}
			m.Docs[d.Ident] = d.Comment
			aliases = append(aliases, &types.Field{Name: d.Ident, T: d.Type})
		case DeclAssign, DeclDeclare:
			env := types.NewEnv()
			d.Pat.BindTypes(env, d.Type)
			for id, t := range env.Symbols() {
				m.Docs[id] = d.Comment
				first, _ := utf8.DecodeRuneInString(id)
				if first == utf8.RuneError || !unicode.IsUpper(first) {
					continue
				}
				fields = append(fields, &types.Field{Name: id, T: t})
			}
		}
	}
	m.typ = types.Module(fields, aliases)
	return el.Make()
}

// Param returns the type  of the module parameter with identifier id,
// and whether it is mandatory.
func (m *ModuleImpl) Param(id string) (*types.T, bool) {
	env := types.NewEnv()
	for _, p := range m.ParamDecls {
		switch p.Kind {
		case DeclError:
		case DeclDeclare:
			if p.Ident == id {
				return p.Type, true
			}
		case DeclAssign:
			p.Pat.BindTypes(env, p.Type)
		}
	}
	return env.Type(id), false
}

// ParamErr type checks the type environment env against the
// parameters of this module. It returns any type checking errors
// (e.g., badly typed parameters, or missing ones).
func (m *ModuleImpl) ParamErr(env *types.Env) error {
	required := make(map[string]bool)
	for _, p := range m.ParamDecls {
		if p.Kind != DeclDeclare {
			continue
		}
		required[p.Ident] = true
	}
	for id, pt := range env.Symbols() {
		t, _ := m.Param(id)
		if t == nil {
			return fmt.Errorf("module has no parameter %s", id)
		}
		if !t.Sub(pt) {
			return fmt.Errorf("invalid type %s for parameter %s (type %s)", pt, id, t)
		}
		delete(required, id)
	}
	var missing []string
	for id := range required {
		// If we have a injected parameter, then don't consider
		// it missing.
		if !m.fenv.Contains(id) {
			missing = append(missing, id)
		}
	}
	if len(missing) == 1 {
		return fmt.Errorf("missing required module parameter %s", missing[0])
	} else if len(missing) > 1 {
		return fmt.Errorf("missing required module parameters %s", strings.Join(missing, ", "))
	}
	return nil
}

// Flags returns a FlagSet that captures the parameters of this
// module. This can be used to parameterize a module from the command
// line. The returned FlagSet uses parameter documentation as the
// help text.
func (m *ModuleImpl) Flags(sess *Session, env *values.Env) (*flag.FlagSet, error) {
	env = env.Push()
	flags := new(flag.FlagSet)
	for _, p := range m.ParamDecls {
		switch p.Kind {
		case DeclError:
			panic("bad param")
		case DeclDeclare:
			help := strings.Replace(strings.TrimSpace(p.Comment), "\n", " ", -1)
			if help != "" {
				help += " "
			}
			if m.fenv.Contains(p.Ident) {
				v := m.fenv.Value(p.Ident)
				switch p.Type.Kind {
				case types.StringKind:
					flags.String(p.Ident, v.(string), help)
				case types.IntKind:
					flags.Uint64(p.Ident, v.(*big.Int).Uint64(), help)
				case types.FloatKind:
					fl, _ := v.(*big.Float).Float64()
					flags.Float64(p.Ident, fl, help)
				case types.BoolKind:
					flags.Bool(p.Ident, v.(bool), help)
				default:
					return nil, fmt.Errorf("-%s: flags of type %s not supported (valid types are: string, int, bool, and float)", p.Ident, p.Type)
				}
			} else {
				if p.Type.Kind != types.BoolKind {
					help += "(required)"
				}
				switch p.Type.Kind {
				case types.StringKind:
					flags.String(p.Ident, "", help)
				case types.IntKind:
					flags.Int(p.Ident, 0, help)
				case types.FloatKind:
					flags.Float64(p.Ident, 0.0, help)
				case types.BoolKind:
					flags.Bool(p.Ident, false, help)
				default:
					return nil, fmt.Errorf("-%s: flags of type %s not supported (valid types are: string, int, bool)", p.Ident, p.Type)
				}
			}
			// Assign error values here, so that we get a type error.
			env.Bind(p.Ident, fmt.Errorf("%s is undefined; flag parameters may not depend on other flag parameters", p.Ident))
		case DeclAssign:
			// In this case, we have a default value in the flag's environment.
			tenv := types.NewEnv()
			p.Pat.BindTypes(tenv, p.Type)
			v, err := p.Expr.eval(sess, env, p.ID(""))
			if err != nil {
				return nil, err
			}
			for id, match := range p.Pat.Matchers() {
				w, err := coerceMatch(v, p.Type, p.Pat.Position, match.Path())
				if err != nil {
					return nil, err
				}
				if m.fenv.Contains(id) {
					w = m.fenv.Value(id)
				}
				// Bind id so we can have parameters depend on each other.
				env.Bind(id, w)
				switch tenv.Type(id).Kind {
				case types.StringKind:
					flags.String(id, w.(string), p.Comment)
				case types.IntKind:
					flags.Uint64(id, w.(*big.Int).Uint64(), p.Comment)
				case types.FloatKind:
					f, _ := w.(*big.Float).Float64()
					flags.Float64(id, f, p.Comment)
				case types.BoolKind:
					flags.Bool(id, w.(bool), p.Comment)
				}
			}
		}
	}
	return flags, nil
}

// FlagEnv adds flags from the FlagSet to value environment venv and
// type environment tenv. The FlagSet should be produced by
// (*Module).Flags.
func (m *ModuleImpl) FlagEnv(flags *flag.FlagSet, venv *values.Env, tenv *types.Env) error {
	return m.flagEnv(true, flags, venv, tenv)
}

func (m *ModuleImpl) flagEnv(needMandatory bool, flags *flag.FlagSet, venv *values.Env, tenv *types.Env) error {
	var errs []string
	flags.VisitAll(func(f *flag.Flag) {
		t, mandatory := m.Param(f.Name)
		if t == nil {
			return
		}
		if f.Value.String() == "" && mandatory {
			if needMandatory {
				errs = append(errs,
					fmt.Sprintf("missing mandatory flag -%s", f.Name))
			}
			return
		}
		switch t.Kind {
		case types.StringKind:
			venv.Bind(f.Name, f.Value.String())
		case types.IntKind:
			v := new(big.Int)
			if _, ok := v.SetString(f.Value.String(), 10); !ok {
				errs = append(errs,
					fmt.Sprintf("-%s: invalid integer %q", f.Name, f.Value.String()))
				return
			}
			venv.Bind(f.Name, v)
		case types.FloatKind:
			v := new(big.Float)
			if _, ok := v.SetString(f.Value.String()); !ok {
				errs = append(errs,
					fmt.Sprintf("-%s: invalid float %q", f.Name, f.Value.String()))
				return
			}
			venv.Bind(f.Name, v)
		case types.BoolKind:
			venv.Bind(f.Name, f.Value.String() == "true")
		default:
			return
		}
		tenv.Bind(f.Name, t)
	})
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errors.New(errs[0])
	default:
		return errors.New("flag errors:\n\t" + strings.Join(errs, "\n\t"))
	}
}

// Make creates a new instance of this module. ParamDecls contains
// the value environment storing parameter values.
func (m *ModuleImpl) Make(sess *Session, params *values.Env) (values.T, error) {
	if m.fenv != nil {
		// Fall back to values provided by flags, if any.
		params = params.Concat(m.fenv)
	}
	env := params.Push()
	for _, p := range m.ParamDecls {
		switch p.Kind {
		case DeclError:
			panic("invalid decl")
		case DeclDeclare:
			// value is already bound in params.
		case DeclAssign:
			v, err := p.Expr.eval(sess, env, "")
			if err != nil {
				return nil, err
			}
			env = env.Push()
			for id, m := range p.Pat.Matchers() {
				// Passed parameters override definitions.
				if !params.Contains(id) {
					w, err := coerceMatch(v, p.Type, p.Pat.Position, m.Path())
					if err != nil {
						return nil, err
					}
					env.Bind(id, w)
				}
			}
		}
	}
	for _, d := range m.Decls {
		if d.Kind == DeclType {
			continue
		}
		v, err := d.Expr.eval(sess, env, d.ID(""))
		if err != nil {
			return nil, err
		}
		env = env.Push()
		for id, m := range d.Pat.Matchers() {
			w, err := coerceMatch(v, d.Type, d.Pat.Position, m.Path())
			if err != nil {
				return nil, err
			}
			env.Bind(id, w)
		}
	}
	v := make(values.Module)
	for _, f := range m.typ.Fields {
		v[f.Name] = env.Value(f.Name)
	}
	return v, nil
}

// String renders a tree-formatted version of m.
func (m *ModuleImpl) String() string {
	params := make([]string, len(m.ParamDecls))
	for i, d := range m.ParamDecls {
		params[i] = d.String()
	}
	decls := make([]string, len(m.Decls))
	for i, d := range m.Decls {
		decls[i] = d.String()
	}
	return fmt.Sprintf("module(keyspace(%v), params(%v), decls(%v))",
		m.Keyspace, strings.Join(params, ", "), strings.Join(decls, ", "))
}

// Params returns the parameter metadata for this module.
func (m *ModuleImpl) Params() []Param {
	params := make([]Param, 0, len(m.ParamDecls))
	for _, p := range m.ParamDecls {
		var param Param
		param.Type = p.Type
		param.Doc = p.Comment
		// We include the whole expression here, and leave it up
		// the caller to make sense of which identifiers go where.
		// (We can't do better, since that would require evaluation.)
		param.Expr = p.Expr
		switch p.Kind {
		case DeclDeclare:
			param.Ident = p.Ident
			param.Required = true
			// Synthesize an expression from a flag-injected value.
			if m.fenv.Contains(p.Ident) {
				param.Required = false
				param.Expr = &Expr{
					Kind: ExprConst,
					Val:  m.fenv.Value(p.Ident),
					Type: p.Type,
				}
			}
			params = append(params, param)
		case DeclAssign:
			pat := p.Pat
			var removed []string
			pat, removed = pat.Remove(m.fenv)
			for _, id := range removed {
				params = append(params, Param{
					Type:  m.ftenv.Type(id),
					Ident: id,
					Doc:   p.Comment,
					Expr: &Expr{
						Kind: ExprConst,
						Val:  m.fenv.Value(id),
						Type: m.ftenv.Type(id),
					},
				})
			}
			if len(pat.Matchers()) > 0 {
				param.Ident = fmt.Sprint(pat)
				params = append(params, param)
			}
		}
	}
	return params
}

// Doc returns the documentation for the provided identifier.
func (m *ModuleImpl) Doc(ident string) string {
	return m.Docs[ident]
}

// Type returns the module type of m.
func (m *ModuleImpl) Type() *types.T {
	return m.typ
}

// InjectArgs parameterizes the module with the provided flags.
// This is equivalent to either providing parameters or overriding
// their default values.
//
// TODO(marius): this is really a hack; we should be serializing the
// environments directly instead.
func (m *ModuleImpl) InjectArgs(sess *Session, args []string) error {
	flags, err := m.Flags(sess, sess.Values)
	if err != nil {
		return err
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	m.fenv = m.fenv.Push()
	m.ftenv = m.ftenv.Push()
	m.injectedArgs = args
	return m.flagEnv(false, flags, m.fenv, m.ftenv)
}

func (m *ModuleImpl) InjectedArgs() []string {
	return m.injectedArgs
}

// Equal tells whether modules m and n are equivalent.
func (m *ModuleImpl) Equal(n *ModuleImpl) bool {
	if m == nil || n == nil {
		return m == nil && n == nil
	}
	if (m.Keyspace == nil) != (n.Keyspace == nil) {
		return false
	}
	if m.Keyspace != nil && !m.Keyspace.Equal(n.Keyspace) {
		return false
	}

	if len(m.Reservation) != len(n.Reservation) {
		return false
	}
	for i := range m.Reservation {
		if !m.Reservation[i].Equal(n.Reservation[i]) {
			return false
		}
	}

	if len(m.ParamDecls) != len(n.ParamDecls) {
		return false
	}
	for i := range m.ParamDecls {
		if !m.ParamDecls[i].Equal(n.ParamDecls[i]) {
			return false
		}
	}

	if len(m.Decls) != len(n.Decls) {
		return false
	}
	for i := range m.Decls {
		if !m.Decls[i].Equal(n.Decls[i]) {
			return false
		}
	}

	if !reflect.DeepEqual(m.Docs, n.Docs) {
		return false
	}

	if !m.typ.Equal(n.typ) {
		return false
	}

	if !m.tenv.Equal(n.tenv) {
		return false
	}

	return true
}
