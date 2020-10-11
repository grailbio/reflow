// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"net/url"
	"os"
	"runtime/debug"
	"strings"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/log"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

var (
	coerceExecOutputDigest = reflow.Digester.FromString("grail.com/reflow/syntax.Eval.coerceExecOutput")
	sequenceDigest         = reflow.Digester.FromString("grail.com/reflow/syntax.Eval.~>")
	notDigest              = reflow.Digester.FromString("grail.com/reflow/syntax.Eval.not")
	errMatch               = errors.New("match error")
	compareDigest          = reflow.Digester.FromString("grail.com/reflow/syntax.evalEq")
	one                    = big.NewInt(1)
	errParam               = errors.New("flag parameters may not depend on other flag parameters")
)

// Eval evaluates the expression e and returns its value (or error).
// Evaluation is lazy with respect to *flow.Flow, and thus values
// may be delayed. Delayed values are returned as *flow.Flow
// values. Note that this relationship holds recursively: a composite
// value may contain other delayed values--evaluation is fully lazy
// with respect to *flow.Flow.
//
// All non-flow computation is strict: thus any immediately
// computable values are; evaluation that requires flow execution is
// delayed. Lazy evaluation is also witnessed by the type system:
// types with the flag "Flow" set may result in delayed evaluation;
// types without the flag set are guaranteed to be directly
// computable.
//
// Users can use syntax.Force to transform a value to a flow that
// will return a fully evaluated value.
//
// While lazy evaluation complicates matters here, they greatly
// improve and simplify the rest of the system. Lazy evaluation
// guarantees that we perform the smallest amount of computation
// required to produce the desired value. In particular, flows that
// in turn are evaluated by the flow evaluator:
//
// 1. have precise dependencies, and avoid false dependencies
//    (e.g., those that may be introduced in an expression block,
//    or may be delayable, e.g., because they are pushed down as
//    function arguments, or are part of compound data values that
//    that are never accessed or may be accessed only later).
// 2. have digests that are much more amenable to top-down cache
//    lookups; e.g., because dependencies can be pushed down,
//    and thus do not need to be computed before computing a node's
//    digest.
// 3. Tools can explore partially evaluated values; e.g., a map
//    need only its keys evaluated (maps are always strict in their
//    keys) and thus we perform the minimal amount of computation.
func (e *Expr) eval(sess *Session, env *values.Env, ident string) (val values.T, err error) {
	defer func() {
		if f, ok := val.(*flow.Flow); ok {
			f.Label(ident)
		}
	}()
	defer func() {
		if err := recover(); err != nil {
			log.Panicf("panic while evaluating %s: %s\n%s", e, err, string(debug.Stack()))
		}
	}()

	switch e.Kind {
	case ExprIdent:
		v := env.Value(e.Ident)
		if err, ok := v.(error); ok {
			return nil, err
		}
		return v, nil
	case ExprBinop:
		switch e.Op {
		case "||", "&&":
			// We implement short-circuiting for || and &&. This can potentially
			// limit concurrency, but it can also reduce the amount of
			// work we have to do.
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				left := vs[0]
				switch e.Op {
				case "&&":
					if !left.(bool) {
						return false, nil
					}
					return e.Right.eval(sess, env, ident)
				case "||":
					if left.(bool) {
						return true, nil
					}
					return e.Right.eval(sess, env, ident)
				default:
					panic("eval bug")
				}
			}, e.Left)
		case "~>":
			left, err := e.Left.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
			left = Force(left, e.Left.Type)
			right, err := e.Right.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
			leftFlow, ok := left.(*flow.Flow)
			if !ok {
				return right, nil
			}
			flowDigest := sequenceDigest
			if rightFlow, ok := right.(*flow.Flow); ok {
				flowDigest.Mix(rightFlow.Digest())
			}
			return &flow.Flow{
				Deps:       []*flow.Flow{leftFlow},
				Op:         flow.K,
				FlowDigest: flowDigest,
				K: func(vs []values.T) *flow.Flow {
					if f, ok := right.(*flow.Flow); ok {
						return f
					}
					return &flow.Flow{
						Op:         flow.Val,
						FlowDigest: values.Digest(right, e.Right.Type),
						Value:      right,
					}
				},
			}, nil
		case "==", "!=":
			eq := e.Op == "=="
			switch e.Left.Type.Kind {
			case types.ListKind, types.MapKind, types.TupleKind, types.StructKind, types.DirKind,
				types.SumKind:
				return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
					v, err := e.evalEq(sess, env, ident, vs[0], vs[1], e.Left.Type)
					if err != nil {
						return nil, err
					}
					if !eq {
						v, err = not(v)
					}
					return v, err
				}, e.Left, e.Right)
			case types.FileKind:
				comp := func(vs []values.T) (values.T, error) {
					v, err := e.evalEq(sess, env, ident, vs[0], vs[1], e.Left.Type)
					if err != nil {
						return nil, err
					}
					if !eq {
						v, err = not(v)
					}
					return v, err
				}
				forceIntern := func(src string) (values.T, error) {
					url, err := url.Parse(src)
					if err != nil {
						return nil, err
					}
					return &flow.Flow{
						Op:         flow.Coerce,
						FlowDigest: coerceFilesetToFileDigest,
						Coerce:     coerceFilesetToFile,
						Deps: []*flow.Flow{
							{
								Op:         flow.Intern,
								MustIntern: true,
								URL:        url,
								Position:   e.Position.String(),
								Ident:      ident,
							},
						},
					}, nil
				}
				return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
					l, r := vs[0].(reflow.File), vs[1].(reflow.File)
					if l.IsRef() != r.IsRef() {
						left, right := vs[0], vs[1]
						if l.IsRef() {
							left, err = forceIntern(l.Source)
							if err != nil {
								return nil, err
							}
						} else {
							right, err = forceIntern(r.Source)
							if err != nil {
								return nil, err
							}
						}
						return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
							return comp(vs)
						}, tval{e.Left.Type, left}, tval{e.Right.Type, right})
					}
					return comp(vs)
				}, e.Left, e.Right)
			default:
				return e.k(sess, env, ident, e.evalBinop, e.Left, e.Right)
			}
		default:
			return e.k(sess, env, ident, e.evalBinop, e.Left, e.Right)
		}
	case ExprUnop:
		return e.k(sess, env, ident, e.evalUnop, e.Left)
	case ExprApply:
		return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
			fn := vs[0].(values.Func)
			fields := make([]values.T, len(e.Fields))
			for i := range e.Fields {
				var err error
				fields[i], err = e.Fields[i].eval(sess, env, ident)
				if err != nil {
					return nil, err
				}
			}
			return fn.Apply(values.Location{Position: e.Position.String(), Ident: ident}, fields)
		}, e.Left)
	case ExprLit:
		return e.Val, nil
	case ExprAscribe:
		return e.Left.eval(sess, env, ident)
	case ExprBlock:
		for _, d := range e.Decls {
			env = env.Push()
			v, err := d.Eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
			env = env.Push()
			for _, m := range d.Pat.Matchers() {
				w, err := coerceMatch(v, d.Type, d.Pat.Position, m.Path())
				if err != nil {
					return nil, err
				}
				if m.Ident != "" {
					env.Bind(m.Ident, w)
				}
			}
		}
		return e.Left.eval(sess, env, ident)
	case ExprFunc:
		return closure{e, sess, env, ident}, nil
	case ExprTuple:
		v := make(values.Tuple, len(e.Fields))
		for i, f := range e.Fields {
			var err error
			v[i], err = f.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
		}
		return v, nil
	case ExprStruct:
		v := make(values.Struct)
		for _, f := range e.Fields {
			var err error
			v[f.Name], err = f.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
		}
		return v, nil
	case ExprList:
		v := make(values.List, len(e.List))
		for i, el := range e.List {
			var err error
			v[i], err = el.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
		}
		return v, nil
	case ExprMap:
		// We're forced to fully evaluate the maps keys before proceeding.
		// This is an inherent limitation of using Go's map.
		// TODO(marius): use a datastructure more amenable to laziness.
		sortedKeys := e.sortedMapKeys(env)
		keys := make([]interface{}, len(sortedKeys))
		for i := range sortedKeys {
			keys[i] = sortedKeys[i]
		}
		return e.k(sess, env, ident,
			func(vs []values.T) (values.T, error) {
				m := new(values.Map)
				for i, k := range vs {
					v, err := e.Map[sortedKeys[i]].eval(sess, env, ident)
					if err != nil {
						return nil, err
					}
					m.Insert(values.Digest(k, e.Type.Index), k, v)
				}
				return m, nil
			},
			keys...)
	case ExprVariant:
		if e.Left == nil {
			return &values.Variant{Tag: e.Ident}, nil
		}
		v, err := e.Left.eval(sess, env, ident)
		if err != nil {
			return nil, err
		}
		return &values.Variant{Tag: e.Ident, Elem: v}, nil
	case ExprExec:
		// Before we can emit an exec node, we have to fully evaluate exec
		// parameters as well as delayed template arguments that are not
		// file or directory typed. File and template dependencies are pushed
		// down to the exec node directly.
		var (
			tvals                   = make([]interface{}, len(e.Decls))
			argIndex                = make(map[int]int)
			hasNonFileDirDelayedDep bool
			hasFileDirDelayedDep    bool
			image                   string
		)
		for i, d := range e.Decls {
			v, err := d.Expr.eval(sess, env, d.ID(ident))
			if err != nil {
				return nil, err
			}
			if d.Pat.Ident == "image" {
				image = v.(string)
				e.image = v.(string)
			}
			if d.Pat.Ident == "nondeterministic" {
				e.NonDeterministic = v.(bool)
			}
			tvals[i] = tval{d.Type, v}
		}
		// TODO(marius): abstract into a utility (IsOutput(...))
		outputs := make(map[string]*types.T)
		for _, f := range e.Type.Tupled().Fields {
			outputs[f.Name] = f.T
		}
		for i, arg := range e.Template.Args {
			if arg.Kind == ExprIdent && outputs[arg.Ident] != nil {
				continue
			}
			argIndex[len(tvals)] = i
			v, err := arg.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}

			if _, ok := v.(*flow.Flow); ok && arg.Type.Kind != types.FileKind && arg.Type.Kind != types.DirKind {
				hasNonFileDirDelayedDep = true
			}
			if arg.Type.Kind == types.FileKind || arg.Type.Kind == types.DirKind {
				hasFileDirDelayedDep = true
			}

			// We need the full argument to render.
			v = Force(v, arg.Type)
			tvals = append(tvals, tval{arg.Type, v})
		}
		k, err := e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
			penv := values.NewEnv()
			for i, d := range e.Decls {
				v := vs[i]
				if !d.Pat.BindValues(penv, v) {
					return nil, errors.E(fmt.Sprintf("%s:", d.Pat.Position), errMatch)
				}
			}
			args := make(map[int]values.T)
			for i := len(e.Decls); i < len(vs); i++ {
				args[argIndex[i]] = vs[i]
			}
			return e.exec(sess, env, image, ident, args, makeResources(penv))
		}, tvals...)
		kf := k.(*flow.Flow)

		// if this exec has a delayed non-file/non-dir argument AND delayed file/dir argument, it could be susceptible to T41260
		kf.ExecDepIncorrectCacheKeyBug = hasNonFileDirDelayedDep && hasFileDirDelayedDep
		return kf, err
	case ExprCond:
		return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
			if vs[0].(bool) {
				return e.Left.eval(sess, env, ident)
			}
			return e.Right.eval(sess, env, ident)
		}, e.Cond)
	case ExprSwitch:
		return e.evalSwitch(sess, env, ident)
	case ExprDeref:
		return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
			switch e.Left.Type.Kind {
			case types.StructKind:
				return vs[0].(values.Struct)[e.Ident], nil
			case types.ModuleKind:
				return vs[0].(values.Module)[e.Ident], nil
			default:
				panic("bug")
			}
		}, e.Left)
	case ExprIndex:
		switch e.Left.Type.Kind {
		case types.MapKind:
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				m, k := vs[0].(*values.Map), vs[1]
				v := m.Lookup(values.Digest(k, e.Left.Type.Index), k)
				if v == nil {
					return nil, fmt.Errorf("%v: key %s not found", e.Position, values.Sprint(vs[1], e.Right.Type))
				}
				return v, nil
			}, e.Left, e.Right)
		case types.ListKind:
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				l, k := vs[0].(values.List), int(vs[1].(*big.Int).Int64())
				if k < 0 || k >= len(l) {
					return nil, fmt.Errorf("%v: index %d out of bounds for list of size %v", e.Position, k, len(l))
				}
				return l[k], nil
			}, e.Left, e.Right)
		}
	case ExprCompr:
		return e.evalCompr(sess, env, ident, 0)
	case ExprMake:
		var (
			params = make(map[string]Param)
			args   []interface{}
			argIds []string
		)
		for _, p := range e.Module.Params() {
			params[p.Ident] = p
		}
		for _, d := range e.Decls {
			v, err := d.Eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
			for _, m := range d.Pat.Matchers() {
				w, err := coerceMatch(v, d.Type, d.Pat.Position, m.Path())
				if err != nil {
					return nil, err
				}
				if m.Ident == "" {
					continue
				}
				if e.Module.Eager() {
					w = Force(w, params[m.Ident].Type)
				}
				args = append(args, tval{d.Type, w})
				argIds = append(argIds, m.Ident)
			}
		}
		if e.Module.Eager() {
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				penv := sess.Values.Push()
				for i, id := range argIds {
					penv.Bind(id, vs[i])
				}
				return e.Module.Make(sess, penv)
			}, args...)
		} else {
			penv := sess.Values.Push()
			for i, id := range argIds {
				penv.Bind(id, args[i].(tval).V)
			}
			return e.Module.Make(sess, penv)
		}
	case ExprBuiltin:
		switch e.Op {
		default:
			panic("invalid builtin " + e.Op)
		case "len":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				switch e.Fields[0].Expr.Type.Kind {
				case types.FileKind:
					file := vs[0].(reflow.File)
					return values.NewInt(file.Size), nil
				case types.DirKind:
					dir := vs[0].(values.Dir)
					return values.NewInt(int64(dir.Len())), nil
				case types.ListKind:
					list := vs[0].(values.List)
					return values.NewInt(int64(len(list))), nil
				case types.MapKind:
					m := vs[0].(*values.Map)
					return values.NewInt(int64(m.Len())), nil
				default:
					panic("bug")
				}
			}, e.Fields[0].Expr)
		case "int":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				f := vs[0].(*big.Float)
				i, _ := f.Int64()
				return values.NewInt(i), nil
			}, e.Fields[0].Expr)
		case "float":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				i := vs[0].(*big.Int)
				f := float64(i.Int64())
				return values.NewFloat(f), nil
			}, e.Fields[0].Expr)
		case "zip":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				var (
					left  = vs[0].(values.List)
					right = vs[1].(values.List)
					zip   = make(values.List, len(left))
				)
				if len(left) != len(right) {
					return nil, fmt.Errorf("%v: list sizes do not match: %d vs %d", e.Position, len(left), len(right))
				}
				for i := range left {
					zip[i] = values.Tuple{left[i], right[i]}
				}
				return zip, nil
			}, e.Fields[0].Expr, e.Fields[1].Expr)
		case "unzip":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				list := vs[0].(values.List)
				tuples := make([]interface{}, len(list))
				for i, v := range list {
					tuples[i] = tval{e.Fields[0].Expr.Type.Elem, v}
				}
				return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
					var (
						left  = make(values.List, len(vs))
						right = make(values.List, len(vs))
					)
					for i := range vs {
						tup := vs[i].(values.Tuple)
						left[i], right[i] = tup[0], tup[1]
					}
					return values.Tuple{left, right}, nil
				}, tuples...)
			}, e.Fields[0].Expr)
		case "flatten":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				return e.flatten(sess, env, ident, vs[0].(values.List), types.List(e.Type.Elem), stdEvalK)
			}, e.Fields[0].Expr)
		case "map":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				switch e.Fields[0].Expr.Type.Kind {
				case types.ListKind:
					// We have to unpack both the tuples and the key of the tuples
					// here.
					list := vs[0].(values.List)
					tuples := make([]interface{}, len(list))
					for i, v := range list {
						tuples[i] = tval{e.Fields[0].Expr.Type.Elem, v}
					}
					return e.k(sess, env, ident, func(tuples []values.T) (values.T, error) {
						keys := make([]interface{}, len(tuples))
						for i, v := range tuples {
							k := tval{e.Fields[0].Expr.Type.Elem.Fields[0].T, v.(values.Tuple)[0]}
							keys[i] = k
						}
						return e.k(sess, env, ident, func(keys []values.T) (values.T, error) {
							m := new(values.Map)
							for i, v := range tuples {
								m.Insert(values.Digest(keys[i], e.Fields[0].Expr.Type.Elem.Fields[0].T), keys[i], v.(values.Tuple)[1])
							}
							return m, nil
						}, keys...)
					}, tuples...)
				case types.DirKind:
					m := new(values.Map)
					d := vs[0].(values.Dir)
					for scan := d.Scan(); scan.Scan(); {
						m.Insert(values.Digest(scan.Path(), types.String), scan.Path(), scan.File())
					}
					return m, nil
				default:
					panic("bug")
				}
			}, e.Fields[0].Expr)
		case "reduce":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				fn := vs[0].(values.Func)
				l := vs[1].(values.List)
				var v values.T
				if len(l) == 0 {
					return nil, fmt.Errorf("%v: cannot reduce empty list", e.Position)
				}
				args := make([]values.T, 2)
				args[0] = l[0]
				for i := 1; i < len(l); i++ {
					args[1] = l[i]
					v, err = fn.Apply(values.Location{Position: e.Position.String()}, args)
					if err != nil {
						return nil, err
					}
					args[0] = v
				}
				return args[0], nil
			}, e.Fields[0].Expr, e.Fields[1].Expr)
		case "fold":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				fn := vs[0].(values.Func)
				l := vs[1].(values.List)
				var v values.T
				if len(l) == 0 {
					return vs[2], nil
				}
				args := make([]values.T, 2)
				args[0] = vs[2]
				for _, li := range l {
					args[1] = li
					v, err = fn.Apply(values.Location{Position: e.Position.String()}, args)
					if err != nil {
						return nil, err
					}
					args[0] = v
				}
				return args[0], nil
			}, e.Fields[0].Expr, e.Fields[1].Expr, e.Fields[2].Expr)
		case "list":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				var list values.List
				switch e.Fields[0].Expr.Type.Kind {
				case types.MapKind:
					m := vs[0].(*values.Map)
					list = make(values.List, 0, m.Len())
					m.Each(func(k, v values.T) {
						list = append(list, values.Tuple{k, v})
					})
				case types.DirKind:
					d := vs[0].(values.Dir)
					list = make(values.List, 0, d.Len())
					for scan := d.Scan(); scan.Scan(); {
						list = append(list, values.Tuple{scan.Path(), scan.File()})
					}
				default:
					panic("bug")
				}
				return list, nil
			}, e.Fields[0].Expr)
		case "panic":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				return nil, fmt.Errorf("%v: panic: %s", e.Position, vs[0].(string))
			}, e.Fields[0].Expr)
		case "delay":
			// Delay deliberately introduces delayed evaluation, which is
			// useful for testing and debugging. It is handled specially in
			// (*Expr).k so that it does not return immediately if the value
			// is already resolved.
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				return vs[0], nil
			}, e.Fields[0].Expr)
		case "trace":
			left, err := e.Fields[0].Expr.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
			left = Force(left, e.Fields[0].Expr.Type)
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				if ident != "" {
					ident = "(" + ident + ")"
				}
				stderr := sess.Stderr
				if stderr == nil {
					stderr = os.Stderr
				}
				fmt.Fprintf(stderr, "%s%s: %s\n", e.Position, ident, values.Sprint(vs[0], e.Fields[0].Expr.Type))
				return vs[0], nil
			}, tval{e.Fields[0].Expr.Type, left})
		case "range":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				left, right := vs[0].(*big.Int), vs[1].(*big.Int)
				if left.Cmp(right) > 0 {
					return nil, errors.New("invalid range")
				}
				var list values.List
				for i := new(big.Int).Set(left); i.Cmp(right) < 0; i.Add(i, one) {
					list = append(list, new(big.Int).Set(i))
				}
				return list, nil
			}, e.Fields[0].Expr, e.Fields[1].Expr)
		}
	case ExprRequires:
		req, err := e.evalRequirements(sess, env, ident)
		if err != nil {
			return nil, err
		}
		v, err := e.Left.eval(sess, env, ident)
		if err != nil {
			return nil, err
		}
		v = Force(v, e.Type)
		if f, ok := v.(*flow.Flow); ok {
			f = &flow.Flow{
				Deps: []*flow.Flow{f},
				Op:   flow.Requirements,
			}
			f.FlowRequirements = req
			return f, nil
		}
		return v, nil
	}
	panic("eval bug " + e.String())
}

// Exec returns a Flow value for an exec expression. The resolved
// image and resources are passed by the caller.
func (e *Expr) exec(sess *Session, env *values.Env, image string, ident string, args map[int]values.T, resources reflow.Resources) (values.T, error) {
	// Execs are special. The interpolation environment also has the
	// output ids.
	narg := len(e.Template.Args)
	outputs := make(map[string]*types.T)
	for _, f := range e.Type.Tupled().Fields {
		outputs[f.Name] = f.T
	}
	varg := make([]values.T, narg)
	for i, ae := range e.Template.Args {
		if ae.Kind == ExprIdent && outputs[ae.Ident] != nil {
			continue
		}
		var ok bool
		varg[i], ok = args[i]
		if ok {
			continue
		}
		var err error
		varg[i], err = ae.eval(sess, env, ident)
		if err != nil {
			return nil, err
		}
		// This isn't technically required here, but we retain it to keep
		// digests stable.
		varg[i] = Force(varg[i], ae.Type)
	}

	// Now for each argument that must be evaluated through the flow
	// evaluator, we attach as a dependency. Other arguments are inlined.
	var (
		deps    []*flow.Flow
		earg    []flow.ExecArg
		indexer = newIndexer()
		argstrs []string
		b       bytes.Buffer
	)
	b.WriteString(quotequote(e.Template.Frags[0]))
	for i, ae := range e.Template.Args {
		if ae.Kind == ExprIdent && outputs[ae.Ident] != nil {
			// An output argument: we replace it with an output exec argument,
			// indexed by its name.
			b.WriteString("%s")
			argstrs = append(argstrs, fmt.Sprintf("{{%s}}", ae.Ident))
			earg = append(earg, flow.ExecArg{Out: true, Index: indexer.Index(ae.Ident)})
		} else if f, ok := varg[i].(*flow.Flow); ok {
			// Runtime dependency: we attach this to our exec nodes, and let
			// the runtime perform argument substitution. Only files and dirs
			// are allowed as dynamic dependencies. These are both
			// represented by reflow.Fileset, and can be substituted by the
			// runtime. Input arguments are indexed by dependency.
			//
			// Because OpExec expects filesets, we have to coerce the input by
			// type.
			//
			// TODO(marius): collapse Vals here
			f = coerceFlowToFileset(ae.Type, f)
			b.WriteString("%s")
			deps = append(deps, f)
			earg = append(earg, flow.ExecArg{Index: len(deps) - 1})
			if ae.Kind == ExprIdent {
				argstrs = append(argstrs, fmt.Sprintf("{{%s}}", ae.Ident))
			} else {
				argstrs = append(argstrs, "{{flow}}")
			}
		} else {
			// Immediate argument: we render it and inline it. The typechecker guarantees
			// that only files, dirs, strings, and ints are allowed here.
			v := varg[i]
			switch e.Template.Args[i].Type.Kind {
			case types.StringKind:
				b.WriteString(strings.Replace(v.(string), "%", "%%", -1))
			case types.IntKind:
				vint := v.(*big.Int)
				b.WriteString(vint.String())
			case types.FloatKind:
				vfloat := v.(*big.Float)
				b.WriteString(vfloat.String())
			case types.FileKind, types.DirKind, types.ListKind:
				// Files and directories must be wrapped back into flows since
				// this is the only way they can be inlined by reflow's executor
				// (since it controls paths). Also, input arguments must be
				// coerced into reflow filesets.
				b.WriteString("%s")
				deps = append(deps, &flow.Flow{
					Op:    flow.Val,
					Value: coerceToFileset(e.Template.Args[i].Type, v),
				})
				earg = append(earg, flow.ExecArg{Index: len(deps) - 1})
				if ae.Kind == ExprIdent {
					argstrs = append(argstrs, fmt.Sprintf("{{%s}}", ae.Ident))
				} else {
					argstrs = append(argstrs, fmt.Sprintf("{{%s}}", v))
				}
			default:
				panic("illegal expression " + e.Template.Args[i].Type.String() + " ... " + fmt.Sprint(v))
			}
		}
		b.WriteString(quotequote(e.Template.Frags[i+1]))
	}
	dirs := make([]bool, indexer.N())
	for name, typ := range outputs {
		i, ok := indexer.Lookup(name)
		if !ok {
			continue
		}
		dirs[i] = typ.Kind == types.DirKind
	}

	sess.SeeImage(image)

	// The output from an exec is a fileset, so we must coerce it back into a
	// tuple indexed by the our indexer. We must also coerce filesets into
	// files and dirs.
	return &flow.Flow{
		Ident: ident,

		Deps: []*flow.Flow{{
			Op:        flow.Exec,
			Ident:     ident,
			Position:  e.Position.String(), // XXX TODO full path
			Image:     image,
			Resources: resources,
			// TODO(marius): use a better interpolation scheme that doesn't
			// require us to do these gymnastics wrt string interpolation.
			Cmd:              b.String(),
			Deps:             deps,
			Argmap:           earg,
			Argstrs:          argstrs,
			OutputIsDir:      dirs,
			NonDeterministic: e.NonDeterministic,
		}},

		Op:         flow.Coerce,
		FlowDigest: coerceExecOutputDigest,
		Coerce: func(v values.T) (values.T, error) {
			list := v.(reflow.Fileset).List
			if got, want := len(list), indexer.N(); got != want {
				return nil, fmt.Errorf("%v: bad exec result: expected size %d, got %d (deps %v, argmap %v, outputisdir %v)", e.Position, want, got, deps, earg, dirs)
			}
			tup := make(values.Tuple, len(outputs))
			for i, f := range e.Type.Tupled().Fields {
				idx, ok := indexer.Lookup(f.Name)
				if ok {
					fs := list[idx]
					var v values.T
					switch outputs[f.Name].Kind {
					case types.FileKind:
						file, ok := fs.Map["."]
						if !ok {
							return nil, errors.Errorf("output file not created in %s", ident)
						}
						v = file
					case types.DirKind:
						var dir values.Dir
						for k, file := range fs.Map {
							dir.Set(k, file)
						}
						v = dir
					default:
						panic("bad result type")
					}
					tup[i] = v
				} else {
					switch outputs[f.Name].Kind {
					case types.FileKind:
						tup[i] = reflow.File{}
					case types.DirKind:
						tup[i] = values.Dir{}
					default:
						panic("bad result type")
					}
				}
			}
			if len(tup) == 1 {
				return tup[0], nil
			}
			return tup, nil
		},
	}, nil
}

func not(v values.T) (values.T, error) {
	if _, ok := v.(*flow.Flow); !ok {
		return !v.(bool), nil
	}
	return &flow.Flow{
		Op:         flow.Coerce,
		FlowDigest: notDigest,
		Deps:       []*flow.Flow{v.(*flow.Flow)},
		Coerce: func(v values.T) (values.T, error) {
			return !(v.(bool)), nil
		},
	}, nil
}

func (e *Expr) evalRequirements(sess *Session, env *values.Env, ident string) (req reflow.Requirements, err error) {
	env2 := values.NewEnv()
	for _, d := range e.Decls {
		v, err := d.Expr.eval(sess, env, d.ID(ident))
		if err != nil {
			return req, err
		}
		if !d.Pat.BindValues(env2, v) {
			return req, errors.E(fmt.Sprintf("%s:", d.Pat.Position), errMatch)
		}
	}
	req.Min = makeResources(env2)
	if v := env2.Value("wide"); v != nil && v.(bool) {
		log.Printf("WARNING: setting `wide := true` has no real effect: %s %s", e.Position, e.Ident)
		req.Width = 1
	}
	return req, nil
}

var intOps = map[string]func(*big.Int, *big.Int, *big.Int) *big.Int{
	"+":  (*big.Int).Add,
	"*":  (*big.Int).Mul,
	"-":  (*big.Int).Sub,
	"/":  (*big.Int).Div,
	"%":  (*big.Int).Mod,
	"<<": func(z, x, y *big.Int) *big.Int { return z.Lsh(x, uint(y.Uint64())) },
	">>": func(z, x, y *big.Int) *big.Int { return z.Rsh(x, uint(y.Uint64())) },
}

var floatOps = map[string]func(*big.Float, *big.Float, *big.Float) *big.Float{
	"+": (*big.Float).Add,
	"*": (*big.Float).Mul,
	"-": (*big.Float).Sub,
	"/": (*big.Float).Quo,
}

func (e *Expr) evalMapKeysCompare(sess *Session, env *values.Env, ident string, keys []values.T, t *types.T, h map[digest.Digest]int, index map[int]int, j int) (values.T, error) {
	if len(keys) == 0 {
		return true, nil
	}
	return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
		var (
			v  int
			ok bool
		)
		if v, ok = h[values.Digest(vs[0], t)]; !ok {
			return false, nil
		}
		index[v] = j
		j++
		return e.evalMapKeysCompare(sess, env, ident, keys[1:], t, h, index, j)
	}, tval{t, keys[0]})
}

// EvalMapKeys evaluates all the lhs map keys and then evaluates and compares
// the rhs keys one at a time with the corresponding lhs key. It also builds
// an index that store the lhs-rhs mapping that can be used while comparing
// values.
func (e *Expr) evalMapKeys(sess *Session, env *values.Env, ident string, left, right []values.T, t *types.T, index map[int]int) (values.T, error) {
	h := make(map[digest.Digest]int)
	n := len(left)
	k := make([]interface{}, n)
	for i := 0; i < n; i++ {
		k[i] = tval{t, left[i]}
	}
	return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
		for i, v := range vs {
			h[values.Digest(v, t)] = i
		}
		return e.evalMapKeysCompare(sess, env, ident, right, t, h, index, n)
	}, k...)
}

// EvalValueVector evaluates and compares two value vectors.
func (e *Expr) evalValueVector(sess *Session, env *values.Env, ident string, left, right []values.T, t []*types.T, i int) (values.T, error) {
	if i >= len(left) {
		return true, nil
	}
	return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
		f, err := e.evalEq(sess, env, ident, vs[0], vs[1], t[i])
		if err != nil {
			return nil, err
		}
		return e.andK(sess, env, ident, f, func(vs []values.T) (values.T, error) {
			return e.evalValueVector(sess, env, ident, left, right, t, i+1)
		})
	}, tval{t[i], left[i]}, tval{t[i], right[i]})
}

// andK is only valid for boolean expressions. Continues kfn(subs...) only when cond is true.
func (e *Expr) andK(sess *Session, env *values.Env, ident string, cond values.T, kfn func([]values.T) (values.T, error), subs ...interface{}) (values.T, error) {
	return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
		if !vs[0].(bool) {
			return false, nil
		}
		return e.k(sess, env, ident, kfn, subs...)
	}, tval{types.Bool, cond})
}

func (e *Expr) evalEqList(sess *Session, env *values.Env, ident string, l, r values.List, t *types.T) (values.T, error) {
	return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
		v, err := e.evalEq(sess, env, ident, vs[0], vs[1], t.Elem)
		if err != nil {
			return nil, err
		}
		return e.andK(sess, env, ident, v, func(vs []values.T) (values.T, error) {
			return e.evalEq(sess, env, ident, vs[0], vs[1], t)
		}, tval{t, l[1:]}, tval{t, r[1:]})
	}, tval{t.Elem, l[0]}, tval{t.Elem, r[0]})
}

func (e *Expr) evalEqTuple(sess *Session, env *values.Env, ident string, l, r values.Tuple, t *types.T) (values.T, error) {
	return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
		v, err := e.evalEq(sess, env, ident, vs[0], vs[1], t.Fields[0].T)
		if err != nil {
			return nil, err
		}
		tt := types.Tuple(t.Fields[1:]...)
		return e.andK(sess, env, ident, v, func(vs []values.T) (values.T, error) {
			return e.evalEq(sess, env, ident, vs[0], vs[1], tt)
		}, tval{tt, l[1:]}, tval{tt, r[1:]})
	}, tval{t.Fields[0].T, l[0]}, tval{t.Fields[0].T, r[0]})
}

func (e *Expr) evalEq(sess *Session, env *values.Env, ident string, left, right values.T, t *types.T) (values.T, error) {
	switch t.Kind {
	case types.FileKind:
		l := left.(reflow.File)
		r := right.(reflow.File)
		return l.Equal(r), nil
	case types.ListKind:
		l := left.(values.List)
		r := right.(values.List)
		if len(l) != len(r) {
			return false, nil
		}
		if len(l) == 0 {
			return true, nil
		}
		return e.evalEqList(sess, env, ident, l, r, t)
	case types.TupleKind:
		l := left.(values.Tuple)
		r := right.(values.Tuple)
		if len(t.Fields) == 0 {
			return true, nil
		}
		return e.evalEqTuple(sess, env, ident, l, r, t)
	case types.MapKind:
		l := left.(*values.Map)
		r := right.(*values.Map)
		if l.Len() != r.Len() {
			return false, nil
		}
		n := l.Len()
		keys := make([]values.T, n*2)
		vals := make([]values.T, n*2)
		i := 0
		l.Each(func(k, v values.T) {
			keys[i], vals[i] = k, v
			i++
		})
		r.Each(func(k, v values.T) {
			keys[i], vals[i] = k, v
			i++
		})
		index := make(map[int]int, n)
		f, err := e.evalMapKeys(sess, env, ident, keys[:n], keys[n:], t.Index, index)
		if err != nil {
			return nil, err
		}
		return e.andK(sess, env, ident, f, func(vs []values.T) (values.T, error) {
			tv := make([]*types.T, 0, len(vs))
			for i := 0; i < n; i++ {
				tv = append(tv, t.Elem)
			}
			right := make([]values.T, n)
			for i := range index {
				right[i] = vals[index[i]]
			}
			return e.evalValueVector(sess, env, ident, vals[0:n], right, tv, 0)
		})
	case types.StructKind:
		var (
			leftStruct  = left.(values.Struct)
			rightStruct = right.(values.Struct)
			n           = len(t.Fields)
			leftTup     = make(values.Tuple, n)
			rightTup    = make(values.Tuple, n)
			tupType     = types.Tuple(t.Fields...)
		)
		for i, f := range t.Fields {
			leftTup[i] = leftStruct[f.Name]
			rightTup[i] = rightStruct[f.Name]
		}
		return e.evalEqTuple(sess, env, ident, leftTup, rightTup, tupType)
	case types.SumKind:
		leftVariant := left.(*values.Variant)
		rightVariant := right.(*values.Variant)
		if leftVariant.Tag != rightVariant.Tag {
			return false, nil
		}
		elemTyp := t.VariantMap()[leftVariant.Tag]
		if elemTyp == nil {
			return true, nil
		}
		return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
			return e.evalEq(sess, env, ident, vs[0], vs[1], elemTyp)
		}, tval{elemTyp, leftVariant.Elem}, tval{elemTyp, rightVariant.Elem})
	case types.DirKind:
		l := left.(values.Dir)
		r := right.(values.Dir)
		return l.Equal(r), nil
	default:
		return values.Equal(left, right), nil
	}
}

func (e *Expr) evalBinop(vs []values.T) (values.T, error) {
	left, right := vs[0], vs[1]
	switch e.Left.Type.Kind {
	case types.IntKind:
		if op := intOps[e.Op]; op != nil {
			v := new(big.Int)
			op(v, left.(*big.Int), right.(*big.Int))
			return values.T(v), nil
		}
	case types.FloatKind:
		if op := floatOps[e.Op]; op != nil {
			v := new(big.Float)
			op(v, left.(*big.Float), right.(*big.Float))
			return values.T(v), nil
		}
	}

	switch e.Op {
	case "+":
		switch e.Left.Type.Kind {
		case types.StringKind:
			return left.(string) + right.(string), nil
		case types.ListKind:
			var (
				left  = left.(values.List)
				right = right.(values.List)
			)
			return values.List(append(append(make(values.List, 0, len(left)+len(right)), left...), right...)), nil
		case types.MapKind:
			m := new(values.Map)
			left.(*values.Map).Each(func(k, v values.T) {
				m.Insert(values.Digest(k, e.Left.Type.Index), k, v)
			})
			right.(*values.Map).Each(func(k, v values.T) {
				m.Insert(values.Digest(k, e.Right.Type.Index), k, v)
			})
			return m, nil
		case types.DirKind:
			var dir values.Dir
			for scan := left.(values.Dir).Scan(); scan.Scan(); {
				dir.Set(scan.Path(), scan.File())
			}
			for scan := right.(values.Dir).Scan(); scan.Scan(); {
				dir.Set(scan.Path(), scan.File())
			}
			return dir, nil
		default:
			panic("bug")
		}
	case ">", "<", "<=", ">=", "!=", "==":
		switch e.Left.Type.Kind {
		case types.StringKind:
			left, right := left.(string), right.(string)
			switch e.Op {
			case "==":
				return left == right, nil
			case ">":
				return left > right, nil
			case "<":
				return left < right, nil
			case "<=":
				return left <= right, nil
			case ">=":
				return left >= right, nil
			case "!=":
				return left != right, nil
			}
		case types.IntKind:
			left, right := left.(*big.Int), right.(*big.Int)
			cmp := left.Cmp(right)
			switch e.Op {
			case "==":
				return cmp == 0, nil
			case ">":
				return cmp > 0, nil
			case "<":
				return cmp < 0, nil
			case "<=":
				return cmp <= 0, nil
			case ">=":
				return cmp >= 0, nil
			case "!=":
				return cmp != 0, nil
			}
		case types.FloatKind:
			left, right := left.(*big.Float), right.(*big.Float)
			cmp := left.Cmp(right)
			switch e.Op {
			case "==":
				return cmp == 0, nil
			case ">":
				return cmp > 0, nil
			case "<":
				return cmp < 0, nil
			case "<=":
				return cmp <= 0, nil
			case ">=":
				return cmp >= 0, nil
			case "!=":
				return cmp != 0, nil
			}
		default:
			switch e.Op {
			case "==":
				return values.Equal(left, right), nil
			case "!=":
				return !values.Equal(left, right), nil
			default:
				panic("bug")
			}
		}
	}
	panic("invalid binop")
}

func (e *Expr) evalUnop(vs []values.T) (values.T, error) {
	oper := vs[0]
	switch e.Op {
	case "!":
		return !oper.(bool), nil
	case "-":
		switch e.Left.Type.Kind {
		case types.IntKind:
			neg := new(big.Int)
			return neg.Neg(oper.(*big.Int)), nil
		case types.FloatKind:
			neg := new(big.Float)
			return neg.Neg(oper.(*big.Float)), nil
		default:
			panic("invalid unop")
		}
	default:
		panic("invalid unop")
	}
}

func (e *Expr) evalComprK(sess *Session, env *values.Env, ident string, begin int) evalK {
	return func(e *Expr, env *values.Env, dw io.Writer) {
		e.digest1(dw)
		// TODO: make sure we compute the same digest for the single-clause case.
		//	(and test this)
		env2 := env.Push()
		for i, j := begin, 0; i < len(e.ComprClauses); i++ {
			clause := e.ComprClauses[i]
			switch clause.Kind {
			case ComprEnum:
				for _, id := range clause.Pat.Idents(nil) {
					env2.Bind(id, digestN(j))
					j++
				}
			case ComprFilter:
			}
			if i > begin {
				clause.Expr.digest(dw, env2)
			}
		}
		e.ComprExpr.digest(dw, env2)
	}
}

func (e *Expr) evalCompr(sess *Session, env *values.Env, ident string, begin int) (values.T, error) {
	// The clause expression is captured directly.
	k := e.evalComprK(sess, env, ident, begin)
	clause := e.ComprClauses[begin]
	return k.Continue(e, sess, env, ident, func(vs []values.T) (values.T, error) {
		var (
			list values.List
			// elem *types.T
			last = begin == len(e.ComprClauses)-1
		)
		switch clause.Kind {
		case ComprEnum:
			switch clause.Expr.Type.Kind {
			default:
				panic("invalid expr")
			case types.ListKind:
				left := vs[0].(values.List)
				list = make(values.List, len(left))
				for i, v := range left {
					env2 := env.Push()
					for _, m := range clause.Pat.Matchers() {
						w, err := coerceMatch(v, clause.Expr.Type.Elem, clause.Pat.Position, m.Path())
						if err != nil {
							return nil, err
						}
						if m.Ident != "" {
							env2.Bind(m.Ident, w)
						}
					}
					var err error
					if last {
						list[i], err = e.ComprExpr.eval(sess, env2, ident)
					} else {
						list[i], err = e.evalCompr(sess, env2, ident, begin+1)
					}
					if err != nil {
						return nil, err
					}
				}
			case types.MapKind:
				left := vs[0].(*values.Map)
				var err error
				left.Each(func(k, v values.T) {
					env2 := env.Push()
					for _, matcher := range clause.Pat.Matchers() {
						tup := values.Tuple{k, v}
						var w values.T
						w, err = coerceMatch(
							tup,
							types.Tuple(
								&types.Field{T: clause.Expr.Type.Index},
								&types.Field{T: clause.Expr.Type.Elem},
							),
							clause.Pat.Position,
							matcher.Path(),
						)
						if err != nil {
							return
						}
						if matcher.Ident != "" {
							env2.Bind(matcher.Ident, w)
						}
					}
					var ev values.T
					if last {
						ev, err = e.ComprExpr.eval(sess, env2, ident)
					} else {
						ev, err = e.evalCompr(sess, env2, ident, begin+1)
					}
					if err != nil {
						return
					}
					list = append(list, ev)
				})
				if err != nil {
					return nil, err
				}
			}
		case ComprFilter:
			if !vs[0].(bool) {
				return values.List{}, nil
			}
			if last {
				v, err := e.ComprExpr.eval(sess, env, ident)
				if err != nil {
					return nil, err
				}
				return values.List{v}, nil
			}
			return e.evalCompr(sess, env, ident, begin+1)
		}
		if last {
			return list, nil
		}
		return e.flatten(sess, env, ident, list, e.Type, k)
	}, clause.Expr)
}

type tval struct {
	T *types.T
	V values.T
}

func (e *Expr) k(sess *Session, env *values.Env, ident string, k func(vs []values.T) (values.T, error), subs ...interface{}) (values.T, error) {
	return stdEvalK.Continue(e, sess, env, ident, k, subs...)
}

func (e *Expr) flatten(sess *Session, env *values.Env, ident string, list values.List, t *types.T, k evalK) (values.T, error) {
	tvals := make([]interface{}, len(list))
	for i := range list {
		tvals[i] = tval{t, list[i]}
	}
	return k.Continue(e, sess, env, ident, func(vs []values.T) (values.T, error) {
		var list values.List
		for _, v := range vs {
			for _, el := range v.(values.List) {
				list = append(list, el)
			}
		}
		return list, nil
	}, tvals...)
}

// indexer is used to mint fresh indices and look them up later.
type indexer struct {
	i   int
	ids map[string]int
}

func newIndexer() *indexer {
	return &indexer{ids: map[string]int{}}
}

// Lookup returns the index associated with an id.
func (x *indexer) Lookup(id string) (int, bool) {
	i, ok := x.ids[id]
	return i, ok
}

// Index returns the index associated with id. If no association
// exists, it returns the next index and stores the new association.
func (x *indexer) Index(id string) int {
	i, ok := x.ids[id]
	if !ok {
		i = x.i
		x.i++
		x.ids[id] = i
	}
	return i
}

// N returns the number of associations made.
func (x *indexer) N() int {
	return x.i
}

func quotequote(s string) string {
	return strings.Replace(s, "%", "%%", -1)
}

// evalK is the type of evaluation continuation. It is abstracted
// over the function that computes the digest of the delayed
// computation. This allows us to reuse this code when the expression
// does not provide enough context to compute the digest, as is the
// case in ExprCompr.
type evalK func(*Expr, *values.Env, io.Writer)

// Continue continues the evaluation of expression e once the
// subcomputations (expressions or tvals) subs are evaluated. If the
// dependencies are available immediately, kfn is invoked inline;
// otherwise a delayed K node is returned which represents the
// implied dependency graph and continuation.
func (k evalK) Continue(e *Expr, sess *Session, env *values.Env, ident string, kfn func(vs []values.T) (values.T, error), subs ...interface{}) (values.T, error) {
	var (
		deps  []*flow.Flow
		depsi []int
		vs    = make([]values.T, len(subs))
		ts    = make([]*types.T, len(subs))
		dw    = reflow.Digester.NewWriter()
	)
	// TODO(marius): push down sorting of dependencies here?
	for i, sub := range subs {
		switch sub := sub.(type) {
		case *Expr:
			var err error
			vs[i], err = sub.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
			ts[i] = sub.Type
		case tval:
			vs[i] = sub.V
			ts[i] = sub.T
		default:
			panic(fmt.Sprintf("invalid sub argument type %T", sub))
		}
		f, ok := vs[i].(*flow.Flow)
		if !ok {
			continue
		}
		deps = append(deps, f)
		depsi = append(depsi, i)
	}

	// If all dependencies are resolved, we evaluate directly,
	// except if we're evaluating a delay operation.
	if len(deps) == 0 && (e.Kind != ExprBuiltin || e.Op != "delay") {
		return kfn(vs)
	}

	// If we need to continue then we also have to incorporate the digests
	// of dependent values.
	for i, v := range vs {
		if _, ok := v.(*flow.Flow); ok {
			continue
		}
		values.WriteDigest(dw, v, ts[i])
	}

	// Otherwise, the node cannot be immediately evaluated; we defer its
	// evaluation until all of its dependencies are resolved.
	//
	// We first compute the (single-node) digest to identify the
	// operation.
	//
	// Note that, except for operations that delay evaluating part of
	// the tree, all dependencies are captured either directly through
	// value digests or else through the flow dependencies in the K
	// below. Thus, we need only include the logical operation itself
	// here.
	k(e, env, dw)

	return &flow.Flow{
		Op:         flow.K,
		Deps:       deps,
		FlowDigest: dw.Digest(),
		K: func(vs1 []values.T) *flow.Flow {
			for i, v := range vs1 {
				vs[depsi[i]] = v
			}
			v, err := kfn(vs)
			if err != nil {
				return &flow.Flow{Op: flow.Val, Err: errors.Recover(err)}
			}
			if f, ok := v.(*flow.Flow); ok {
				return f
			}
			return &flow.Flow{
				Ident:      ident,
				Op:         flow.Val,
				FlowDigest: values.Digest(v, e.Type),
				Value:      v,
			}
		},
	}, nil
}

// The standard evalK, computing single-node digests for known
// expressions.
var stdEvalK evalK = func(e *Expr, env *values.Env, dw io.Writer) {
	// We construct an environment with indexed identifiers. This is ok
	// since we're capturing the structure of an expression, and the
	// expressions dependencies are captured independently.
	e.digest1(dw)
	switch e.Kind {
	case ExprCond:
		e.Left.digest(dw, env)
		e.Right.digest(dw, env)
	case ExprBinop:
		// These are short-circuit operations, and so their dependencies
		// aren't captured by the evaluator directly.
		if e.Op == "||" || e.Op == "&&" {
			e.Right.digest(dw, env)
		}
	case ExprApply:
		for _, f := range e.Fields {
			f.Expr.digest(dw, env)
		}
	case ExprCompr:
		panic("stdEvalK used for ExprCompr")
	case ExprBlock:
		env2 := env.Push()
		i := 0
		for _, decl := range e.Decls {
			for _, id := range decl.Pat.Idents(nil) {
				env2.Bind(id, i)
				i++
			}
		}
		e.Left.digest(dw, env2)
	}
}

// makeResources constructs a resource specification
// from a value environment, where "mem", "cpu", and
// "disk" are integers; "cpufeatures" is a list of strings.
// Missing values are taken to be the zero value.
func makeResources(env *values.Env) reflow.Resources {
	f64 := func(id string) float64 {
		v := env.Value(id)
		if v == nil {
			return 0
		}
		switch arg := v.(type) {
		case *big.Int:
			return float64(arg.Uint64())
		case *big.Float:
			f64, _ := arg.Float64()
			return f64
		default:
			panic("invalid type")
		}
	}
	resources := reflow.Resources{
		"mem":  f64("mem"),
		"cpu":  f64("cpu"),
		"disk": f64("disk"),
	}
	v := env.Value("cpufeatures")
	if v == nil {
		return resources
	}
	for _, feature := range v.(values.List) {
		// We assign one feature per CPU request.
		resources[feature.(string)] = resources["cpu"]
	}
	return resources
}
