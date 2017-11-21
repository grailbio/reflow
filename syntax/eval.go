// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"fmt"
	"log"
	"math/big"
	"runtime/debug"
	"strings"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

var (
	coerceExecOutputDigest = reflow.Digester.FromString("grail.com/reflow/syntax.Eval.coerceExecOutput")
	sequenceDigest         = reflow.Digester.FromString("grail.com/reflow/syntax.Eval.~>")
)

// Eval evaluates the expression e and returns its value (or error).
// Evaluation is lazy with respect to *reflow.Flow, and thus values
// may be delayed. Delayed values are returned as *reflow.Flow
// values. Note that this relationship holds recursively: a composite
// value may contain other delayed values--evaluation is fully lazy
// with respect to *reflow.Flow.
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
		if f, ok := val.(*reflow.Flow); ok {
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
		return env.Value(e.Ident), nil
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
			leftFlow, ok := left.(*reflow.Flow)
			if !ok {
				return right, nil
			}
			return &reflow.Flow{
				Deps:       []*reflow.Flow{leftFlow},
				Op:         reflow.OpK,
				FlowDigest: sequenceDigest,
				K: func(vs []values.T) *reflow.Flow {
					if f, ok := right.(*reflow.Flow); ok {
						return f
					}
					return &reflow.Flow{
						Op:         reflow.OpVal,
						FlowDigest: values.Digest(right, e.Right.Type),
						Value:      right,
					}
				},
			}, nil
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
	case ExprConst:
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
			for id, m := range d.Pat.Matchers() {
				w, err := coerceMatch(v, d.Type, m.Path())
				if err != nil {
					return nil, err
				}
				env.Bind(id, w)
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
				v := make(values.Map)
				for i := range vs {
					var err error
					v[vs[i]], err = e.Map[sortedKeys[i]].eval(sess, env, ident)
					if err != nil {
						return nil, err
					}
				}
				return v, nil
			},
			keys...)
	case ExprExec:
		// Execs are special. The interpolation environment also has the
		// output ids.
		narg := len(e.Template.Args)
		outputs := map[string]*types.T{}
		for _, f := range e.Type.Tupled().Fields {
			outputs[f.Name] = f.T
		}
		varg := make([]values.T, narg)
		for i, ae := range e.Template.Args {
			if ae.Kind == ExprIdent && outputs[ae.Ident] != nil {
				continue
			}
			var err error
			varg[i], err = ae.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
			// Execs need to use the actual values, so we must force
			// their evaluation.
			varg[i] = Force(varg[i], ae.Type)
		}
		// TODO(marius): it might be useful to allow the declarations inside
		// of an exec to just override the ones outside. This way, we can for
		// example declare a module-wide image.
		env2 := values.NewEnv()
		for _, d := range e.Decls {
			// These are all guaranteed to be const expressions by the
			// type checker, so we can safely evaluate them here to an
			// immediate result.
			v, err := d.Expr.eval(sess, env, d.ID(ident))
			if err != nil {
				return nil, err
			}
			if !d.Pat.BindValues(env2, v) {
				return nil, errMatch
			}
		}
		image := env2.Value("image").(string)
		u64 := func(id string) uint64 {
			v := env2.Value(id)
			if v == nil {
				return 0
			}
			return v.(*big.Int).Uint64()
		}
		resources := reflow.Resources{
			Memory: u64("mem"),
			CPU:    uint16(u64("cpu")),
			Disk:   u64("disk"),
		}

		// Now for each argument that must be evaluated through the flow
		// evaluator, we attach as a dependency. Other arguments are inlined.
		var (
			deps    []*reflow.Flow
			earg    []reflow.ExecArg
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
				earg = append(earg, reflow.ExecArg{Out: true, Index: indexer.Index(ae.Ident)})
			} else if f, ok := varg[i].(*reflow.Flow); ok {
				// Runtime dependency: we attach this to our exec nodes, and let
				// the runtime perform argument substitution. Only files and dirs
				// are allowed as dynamic dependencies. These are both
				// represented by reflow.Fileset, and can be substituted by the
				// runtime. Input arguments are indexed by dependency.
				//
				// Because OpExec expects filesets, we have to coerce the input by
				// type.
				//
				// TODO(marius): collapse OpVals here
				f = coerceFlowToFileset(ae.Type, f)
				b.WriteString("%s")
				deps = append(deps, f)
				earg = append(earg, reflow.ExecArg{Index: len(deps) - 1})
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
					deps = append(deps, &reflow.Flow{
						Op:    reflow.OpVal,
						Value: coerceToFileset(e.Template.Args[i].Type, v),
					})
					earg = append(earg, reflow.ExecArg{Index: len(deps) - 1})
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
		return &reflow.Flow{
			Ident: ident,

			Deps: []*reflow.Flow{{
				Op:        reflow.OpExec,
				Ident:     ident,
				Position:  e.Position.String(), // XXX TODO full path
				Image:     image,
				Resources: resources,
				// TODO(marius): use a better interpolation scheme that doesn't
				// require us to do these gymnastics wrt string interpolation.
				Cmd:         b.String(),
				Deps:        deps,
				Argmap:      earg,
				Argstrs:     argstrs,
				OutputIsDir: dirs,
			}},

			Op:         reflow.OpCoerce,
			FlowDigest: coerceExecOutputDigest,
			Coerce: func(v values.T) (values.T, error) {
				list := v.(reflow.Fileset).List
				if got, want := len(list), indexer.N(); got != want {
					return nil, fmt.Errorf("bad exec result: expected size %d, got %d (deps %v, argmap %v, outputisdir %v)", want, got, deps, earg, dirs)
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
								return nil, errors.New("empty file")
							}
							v = values.File(file)
						case types.DirKind:
							dir := make(values.Dir)
							for k, file := range fs.Map {
								dir[k] = values.File(file)
							}
							v = dir
						default:
							panic("bad result type")
						}
						tup[i] = v
					} else {
						switch outputs[f.Name].Kind {
						case types.FileKind:
							tup[i] = values.File{}
						case types.DirKind:
							tup[i] = make(values.Dir)
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
	case ExprCond:
		return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
			if vs[0].(bool) {
				return e.Left.eval(sess, env, ident)
			}
			return e.Right.eval(sess, env, ident)
		}, e.Cond)
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
		return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
			v, ok := vs[0].(values.Map)[vs[1]]
			if !ok {
				return nil, fmt.Errorf("key %s not found", values.Sprint(vs[1], e.Right.Type))
			}
			return v, nil
		}, e.Left, e.Right)
	case ExprCompr:
		return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
			switch e.Left.Type.Kind {
			default:
				panic("invalid expr")
			case types.ListKind:
				left := vs[0].(values.List)
				list := make(values.List, len(left))
				for i, v := range left {
					env2 := env.Push()
					for id, m := range e.Pat.Matchers() {
						w, err := coerceMatch(v, e.Left.Type.Elem, m.Path())
						if err != nil {
							return nil, err
						}
						env2.Bind(id, w)
					}
					var err error
					list[i], err = e.ComprExpr.eval(sess, env2, ident)
					if err != nil {
						return nil, err
					}
				}
				return list, nil
			case types.MapKind:
				left := vs[0].(values.Map)
				var list values.List
				for k, v := range left {
					env2 := env.Push()
					for id, matcher := range e.Pat.Matchers() {
						tup := values.Tuple{k, v}
						w, err := coerceMatch(tup, types.Tuple(&types.Field{T: e.Left.Type.Index}, &types.Field{T: e.Left.Type.Elem}), matcher.Path())
						if err != nil {
							return nil, err
						}
						env2.Bind(id, w)
					}
					ev, err := e.ComprExpr.eval(sess, env2, ident)
					if err != nil {
						return nil, err
					}
					list = append(list, ev)
				}
				return list, nil
			}
		}, e.Left)
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
			for id, m := range d.Pat.Matchers() {
				w, err := coerceMatch(v, d.Type, m.Path())
				if err != nil {
					return nil, err
				}
				if e.Module.Eager() {
					w = Force(w, params[id].Type)
				}
				args = append(args, tval{d.Type, w})
				argIds = append(argIds, id)
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
				switch e.Left.Type.Kind {
				case types.FileKind:
					file := vs[0].(values.File)
					return values.NewInt(file.Size), nil
				case types.DirKind:
					dir := vs[0].(values.Dir)
					return values.NewInt(int64(len(dir))), nil
				case types.ListKind:
					list := vs[0].(values.List)
					return values.NewInt(int64(len(list))), nil
				case types.MapKind:
					m := vs[0].(values.Map)
					return values.NewInt(int64(len(m))), nil
				default:
					panic("bug")
				}
			}, e.Left)
		case "int":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				f := vs[0].(*big.Float)
				i, _ := f.Int64()
				return values.NewInt(i), nil
			}, e.Left)
		case "float":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				i := vs[0].(*big.Int)
				f := float64(i.Int64())
				return values.NewFloat(f), nil
			}, e.Left)
		case "zip":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				var (
					left  = vs[0].(values.List)
					right = vs[1].(values.List)
					zip   = make(values.List, len(left))
				)
				if len(left) != len(right) {
					return nil, fmt.Errorf("list sizes do not match: %d vs %d", len(left), len(right))
				}
				for i := range left {
					zip[i] = values.Tuple{left[i], right[i]}
				}
				return zip, nil
			}, e.Left, e.Right)
		case "unzip":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				list := vs[0].(values.List)
				tuples := make([]interface{}, len(list))
				for i, v := range list {
					tuples[i] = tval{e.Left.Type.Elem, v}
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
			}, e.Left)
		case "flatten":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				// Traverse the outer list so we can get its length.
				outer := vs[0].(values.List)
				tvals := make([]interface{}, len(outer))
				for i := range outer {
					tvals[i] = tval{types.List(e.Type.Elem), outer[i]}
				}
				return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
					var list values.List
					for _, v := range vs {
						for _, el := range v.(values.List) {
							list = append(list, el)
						}
					}
					return list, nil
				}, tvals...)
			}, e.Left)
		case "map":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				switch e.Left.Type.Kind {
				case types.ListKind:
					// We have to unpack both the tuples and the key of the tuples
					// here.
					list := vs[0].(values.List)
					tuples := make([]interface{}, len(list))
					for i, v := range list {
						tuples[i] = tval{e.Left.Type.Elem, v}
					}
					return e.k(sess, env, ident, func(tuples []values.T) (values.T, error) {
						keys := make([]interface{}, len(tuples))
						for i, v := range tuples {
							k := tval{e.Left.Type.Elem.Fields[0].T, v.(values.Tuple)[0]}
							keys[i] = k
						}
						return e.k(sess, env, ident, func(keys []values.T) (values.T, error) {
							m := make(values.Map)
							for i, v := range tuples {
								m[keys[i]] = v.(values.Tuple)[1]
							}
							return m, nil
						}, keys...)
					}, tuples...)
				case types.DirKind:
					m := make(values.Map)
					d := vs[0].(values.Dir)
					for k, v := range d {
						m[k] = v
					}
					return m, nil
				default:
					panic("bug")
				}
			}, e.Left)
		case "list":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				var list values.List
				switch e.Left.Type.Kind {
				case types.MapKind:
					for k, v := range vs[0].(values.Map) {
						list = append(list, values.Tuple{k, v})
					}
				case types.DirKind:
					for k, v := range vs[0].(values.Dir) {
						list = append(list, values.Tuple{k, v})
					}
				default:
					panic("bug")
				}
				return list, nil
			}, e.Left)
		case "panic":
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				return nil, fmt.Errorf("panic: %s", vs[0].(string))
			}, e.Left)
		case "delay":
			// Delay deliberately introduces delayed evaluation, which is
			// useful for testing and debugging. It is handled specially in
			// (*Expr).k so that it does not return immediately if the value
			// is already resolved.
			return e.k(sess, env, ident, func(vs []values.T) (values.T, error) {
				return vs[0], nil
			}, e.Left)
		}
	case ExprRequires:
		min, max, err := e.evalRequirements(sess, env, ident)
		if err != nil {
			return nil, err
		}
		v, err := e.Left.eval(sess, env, ident)
		if err != nil {
			return nil, err
		}
		v = Force(v, e.Type)
		if f, ok := v.(*reflow.Flow); ok {
			f = &reflow.Flow{
				Deps: []*reflow.Flow{f},
				Op:   reflow.OpRequirements,
			}
			f.FlowRequirements.Min, f.FlowRequirements.Max = min, max
			return f, nil
		}
		return v, nil
	}
	panic("eval bug " + e.String())
}

func (e *Expr) evalRequirements(sess *Session, env *values.Env, ident string) (min, max reflow.Resources, err error) {
	env2 := values.NewEnv()
	for _, d := range e.Decls {
		v, err := d.Expr.eval(sess, env, d.ID(ident))
		if err != nil {
			return min, max, err
		}
		if !d.Pat.BindValues(env2, v) {
			return min, max, errMatch
		}
	}
	u64 := func(id string) uint64 {
		v := env2.Value(id)
		if v == nil {
			return 0
		}
		return v.(*big.Int).Uint64()
	}
	min = reflow.Resources{
		Memory: u64("mem"),
		CPU:    uint16(u64("cpu")),
		Disk:   u64("disk"),
	}
	if v := env2.Value("wide"); v != nil && v.(bool) {
		max = reflow.MaxResources
	} else {
		max = min
	}
	return min, max, nil
}

func (e *Expr) evalBinop(vs []values.T) (values.T, error) {
	left, right := vs[0], vs[1]
	switch e.Op {
	case "+":
		switch e.Left.Type.Kind {
		case types.StringKind:
			return left.(string) + right.(string), nil
		case types.IntKind:
			v := new(big.Int)
			v.Add(left.(*big.Int), right.(*big.Int))
			return v, nil
		case types.FloatKind:
			v := new(big.Float)
			v.Add(left.(*big.Float), right.(*big.Float))
			return v, nil
		case types.ListKind:
			var (
				left  = left.(values.List)
				right = right.(values.List)
			)
			return values.List(append(append(make(values.List, 0, len(left)+len(right)), left...), right...)), nil
		case types.MapKind:
			m := make(values.Map)
			for k, v := range left.(values.Map) {
				m[k] = v
			}
			for k, v := range right.(values.Map) {
				m[k] = v
			}
			return m, nil
		default:
			panic("bug")
		}
	case "*":
		switch e.Left.Type.Kind {
		case types.IntKind:
			v := new(big.Int)
			v.Mul(left.(*big.Int), right.(*big.Int))
			return values.T(v), nil
		case types.FloatKind:
			v := new(big.Float)
			v.Mul(left.(*big.Float), right.(*big.Float))
			return values.T(v), nil
		default:
			panic("bug")
		}
	case "==":
		switch e.Left.Type.Kind {
		case types.FloatKind:
			return left.(*big.Float).Cmp(right.(*big.Float)) == 0, nil
		default:
			return values.Equal(left, right), nil
		}
	case "!=":
		switch e.Left.Type.Kind {
		case types.FloatKind:
			return left.(*big.Float).Cmp(right.(*big.Float)) != 0, nil
		default:
			return !values.Equal(left, right), nil
		}
	case ">", "<", "<=", ">=":
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
			panic("bug")
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

type tval struct {
	T *types.T
	V values.T
}

func (e *Expr) k(sess *Session, env *values.Env, ident string, k func(vs []values.T) (values.T, error), subs ...interface{}) (values.T, error) {
	var (
		deps  []*reflow.Flow
		depsi []int
		vs    = make([]values.T, len(subs))
		dw    = reflow.Digester.NewWriter()
	)
	// TODO(marius): push down sorting of dependencies here?
	for i, sub := range subs {
		var T *types.T
		switch sub := sub.(type) {
		case *Expr:
			var err error
			vs[i], err = sub.eval(sess, env, ident)
			if err != nil {
				return nil, err
			}
			T = sub.Type
		case tval:
			vs[i] = sub.V
			T = sub.T
		default:
			panic(fmt.Sprintf("invalid sub argument type %T", sub))
		}
		f, ok := vs[i].(*reflow.Flow)
		if !ok {
			values.WriteDigest(dw, vs[i], T)
			continue
		}
		deps = append(deps, f)
		depsi = append(depsi, i)
	}

	// If all dependencies are resolved, we evaluate directly,
	// except if we're evaluating a delay operation.
	if len(deps) == 0 && (e.Kind != ExprBuiltin || e.Op != "delay") {
		return k(vs)
	}

	// Otherwise, we compute the single-node digest. Note that, except
	// for operations that delay evaluating part of the three, all
	// dependencies are captured either directly through value digests
	// or else through the flow dependencies in the OpK below. Thus, we
	// need only include the operation itself.
	//
	// In the case of expressions with delayed evaluation, we construct
	// an environment with indexed identifiers. This is ok since we're
	// capturing the structure of an expression, and the expressions
	// dependencies are captured independently.
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
		env2 := env.Push()
		for i, id := range e.Pat.Idents(nil) {
			env2.Bind(id, digestN(i))
		}
		e.ComprExpr.digest(dw, env2)
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

	// This node cannot be immediately evaluated, so we defer it
	// until all its dependencies are resolved. Evaluation continues
	// in (*Expr).eval.
	return &reflow.Flow{
		Op:         reflow.OpK,
		Deps:       deps,
		FlowDigest: dw.Digest(),
		K: func(vs1 []values.T) *reflow.Flow {
			for i, v := range vs1 {
				vs[depsi[i]] = v
			}
			v, err := k(vs)
			if err != nil {
				return &reflow.Flow{Op: reflow.OpVal, Err: errors.Recover(err)}
			}
			if f, ok := v.(*reflow.Flow); ok {
				return f
			}
			return &reflow.Flow{
				Ident:      ident,
				Op:         reflow.OpVal,
				FlowDigest: values.Digest(v, e.Type),
				Value:      v,
			}
		},
	}, nil
}

// indexer is used to mind fresh indices and look them up later.
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
