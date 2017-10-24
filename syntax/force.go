package syntax

import (
	"log"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

var forceDigest = reflow.Digester.FromString("grail.com/reflow/syntax.Eval.Force")

// Force produces a strict version of v. Force either returns an
// immediate value v, or else a *reflow.Flow that will produce the
// immediate value.
func Force(v values.T, t *types.T) values.T {
	if f, ok := v.(*reflow.Flow); ok {
		return &reflow.Flow{
			Deps:       []*reflow.Flow{f},
			Op:         reflow.OpK,
			FlowDigest: forceDigest,
			K: func(vs []values.T) *reflow.Flow {
				v := vs[0]
				return flow(Force(v, t), t)
			},
		}
	}
	switch t.Kind {
	case types.ErrorKind:
		panic("bad type")
	case types.BottomKind:
		panic("bottom value")
	case types.IntKind, types.StringKind, types.BoolKind,
		types.FileKind, types.DirKind, types.UnitKind, types.FuncKind:
		// These types are always strict.
		return v
	case types.ListKind:
		if _, ok := v.(values.List); !ok {
			log.Printf("expected a list, got %v", v)
		}
		var (
			list = v.(values.List)
			copy = make(values.List, len(list))
			r    = newResolver(copy, t)
		)
		for i := range list {
			copy[i] = Force(list[i], t.Elem)
			r.Add(&copy[i], t.Elem)
		}
		return r.Resolve(nil)
	case types.MapKind:
		var (
			m    = v.(values.Map)
			copy = make(values.Map)
			r    = newResolver(copy, t)
			kvs  []kpvp
		)
		for k := range m {
			kk := Force(k, t.Index)
			vv := Force(m[k], t.Elem)
			kv := kpvp{&kk, &vv}
			kvs = append(kvs, kv)
			r.Add(kv.K, t.Index)
			r.Add(kv.V, t.Elem)
		}
		return r.Resolve(func() {
			for _, kv := range kvs {
				copy[*kv.K] = *kv.V
			}
		})
	case types.TupleKind:
		var (
			tup  = v.(values.Tuple)
			copy = make(values.Tuple, len(tup))
			r    = newResolver(copy, t)
		)
		for i := range tup {
			copy[i] = Force(tup[i], t.Fields[i].T)
			r.Add(&copy[i], t.Fields[i].T)
		}
		return r.Resolve(nil)
	case types.StructKind:
		var (
			s    = v.(values.Struct)
			copy = make(values.Struct)
			fm   = t.FieldMap()
			r    = newResolver(copy, t)
			kvs  []kvp
		)
		for k := range s {
			vv := Force(s[k], fm[k])
			copy[k] = vv
			kv := kvp{k, &vv}
			kvs = append(kvs, kv)
			r.Add(kv.V, fm[k])
		}
		return r.Resolve(func() {
			for _, kv := range kvs {
				copy[kv.K.(string)] = *kv.V
			}
		})
	case types.ModuleKind:
		var (
			m    = v.(values.Module)
			copy = make(values.Module)
			r    = newResolver(copy, t)
			fm   = t.FieldMap()
			kvs  []kvp
		)
		for k := range m {
			vv := Force(m[k], fm[k])
			copy[k] = vv
			kv := kvp{k, &vv}
			kvs = append(kvs, kv)
			r.Add(kv.V, fm[k])
		}
		return r.Resolve(func() {
			for _, kv := range kvs {
				copy[kv.K.(string)] = *kv.V
			}
		})
	}
	panic("bad value")
}

// flow produces a flow from value v. If v is already a flow,
// it is returned immediately; otherwise it's wrapped in a
// Val flow.
func flow(v values.T, t *types.T) *reflow.Flow {
	if f, ok := v.(*reflow.Flow); ok {
		return f
	}
	return &reflow.Flow{
		Op:         reflow.OpVal,
		Value:      v,
		FlowDigest: values.Digest(v, t),
	}
}

type resolver struct {
	dw   digest.Writer
	deps []*reflow.Flow
	vps  []*values.T
	v    values.T
	t    *types.T
}

func newResolver(v values.T, t *types.T) *resolver {
	return &resolver{
		v:  v,
		t:  t,
		dw: reflow.Digester.NewWriter(),
	}
}

func (r *resolver) Add(vp *values.T, t *types.T) {
	if f, ok := (*vp).(*reflow.Flow); ok {
		r.deps = append(r.deps, f)
		r.vps = append(r.vps, vp)
	} else {
		values.WriteDigest(r.dw, *vp, t)
	}
}

func (r *resolver) Resolve(proc func()) values.T {
	if len(r.deps) == 0 {
		if proc != nil {
			proc()
		}
		return r.v
	}
	writeN(r.dw, int(r.t.Kind))
	return &reflow.Flow{
		Op:         reflow.OpK,
		Deps:       r.deps,
		FlowDigest: r.dw.Digest(),
		K: func(vs []values.T) *reflow.Flow {
			for i := range vs {
				*r.vps[i] = vs[i]
			}
			if proc != nil {
				proc()
			}
			return flow(r.v, r.t)
		},
	}
}

type kpvp struct {
	K *values.T
	V *values.T
}

type kvp struct {
	K values.T
	V *values.T
}
