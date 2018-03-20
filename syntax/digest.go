// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/values"
)

var (
	exprDigest [maxExpr]digest.Digest
	nDigest    [32]digest.Digest
)

func init() {
	for i := range exprDigest {
		reflow.Digester.FromBytes([]byte{byte(i)})
	}
	for i := range nDigest {
		nDigest[i] = reflow.Digester.FromBytes([]byte{byte(i)})
	}
}

func digestN(i int) digest.Digest {
	if i >= 0 && i < len(nDigest) {
		return nDigest[i]
	}
	return reflow.Digester.FromBytes([]byte{byte(i)})
}

// Digest computes an identifier that uniquely identifies what Expr e
// will compute with the given environment. i.e., it can be used as a
// key to identify the computation represented by Expr e and an
// environment.
//
// On a semantic level, Digest is one-to-many: that is, there are
// many candidate digests for a given semantic computation. However,
// we go through some lengths to normalize, for example by using De
// Bruijn indices (levels) to remove dependence on concrete names. In
// the future, we could consider canonicalizing the expression tree
// as well (e.g., by exploiting commutatvity, etc.)
func (e *Expr) Digest(env *values.Env) digest.Digest {
	w := reflow.Digester.NewWriter()
	e.digest(w, env)
	return w.Digest()
}

var zero34 = make([]byte, 34)

func (e *Expr) digest(w io.Writer, env *values.Env) {
	switch e.Kind {
	case ExprAscribe:
		e.Left.digest(w, env)
		return
	}

	// TODO(marius): fix later.
	// We need to write a zero digest for reflow to maintain backward compatibility.
	// Since digest.WriteDigest no longer allows crypto.Hash of type 0 to be passed
	// to it, we are writing directly 34 zeros which is the size of a SHA256 digest.
	if _, err := w.Write(zero34); err != nil {
		panic(err)
	}
	switch e.Kind {
	case ExprIdent:
		writeN(w, env.Level(e.Ident))
		digest.WriteDigest(w, env.Digest(e.Ident, e.Type))
	case ExprBinop:
		io.WriteString(w, e.Op)
		e.Left.digest(w, env)
		e.Right.digest(w, env)
	case ExprUnop:
		io.WriteString(w, e.Op)
		e.Left.digest(w, env)
	case ExprApply:
		for _, f := range e.Fields {
			f.Expr.digest(w, env)
		}
		e.Left.digest(w, env)
	case ExprConst:
		digest.WriteDigest(w, values.Digest(e.Val, e.Type))
	case ExprBlock:
		for _, decl := range e.Decls {
			d := decl.Expr.Digest(env)
			env = env.Push()
			for _, id := range decl.Pat.Idents(nil) {
				env.Bind(id, d)
			}
		}
		e.Left.digest(w, env)
	case ExprFunc:
		env = env.Push()
		// Note that it is sufficient to produce a digest for the
		// argument's position here because ExprIdent also digests the
		// environment's level; together these make a proper De Bruijn
		// index.
		for i, a := range e.Args {
			env.Bind(a.Name, digestN(i))
		}
		e.Left.digest(w, env)
	case ExprTuple:
		writeN(w, len(e.Fields))
		for _, f := range e.Fields {
			digest.WriteDigest(w, f.Digest(env))
		}
	case ExprStruct:
		writeN(w, len(e.Fields))
		fm := map[string]*FieldExpr{}
		var fields []string
		for _, fe := range e.Fields {
			fields = append(fields, fe.Name)
			fm[fe.Name] = fe
		}
		sort.Strings(fields)
		for _, k := range fields {
			io.WriteString(w, k)
			fm[k].Expr.digest(w, env)
		}
	case ExprList:
		writeN(w, len(e.List))
		for _, ee := range e.List {
			ee.digest(w, env)
		}
	case ExprMap:
		writeN(w, len(e.Map))
		for _, ke := range e.sortedMapKeys(env) {
			ke.digest(w, env)
			e.Map[ke].digest(w, env)
		}
	case ExprExec:
		for _, d := range e.Decls {
			if d.Pat.Ident == "image" {
				// TODO(marius): actually evaluate this directly; though it
				// would require us to carry a true value environment
				// (for const expressions)
				d.Expr.digest(w, env)
				break
			}
		}
		// TODO(marius): normalize this to strip out identifier names;
		// instead rely on indices.
		io.WriteString(w, e.Template.FormatString())
		fm := e.Type.Tupled().FieldMap()
		for i, ae := range e.Template.Args {
			if ae.Kind == ExprIdent && fm[ae.Ident] != nil {
				// We use position here so that we can change output
				// names without changing the exec's digest.
				writeN(w, i)
				continue
			}
			ae.digest(w, env)
		}
	case ExprCond:
		e.Cond.digest(w, env)
		e.Left.digest(w, env)
		e.Right.digest(w, env)
	case ExprDeref:
		e.Left.digest(w, env)
		io.WriteString(w, e.Ident)
	case ExprIndex:
		e.Left.digest(w, env)
		e.Right.digest(w, env)
	case ExprCompr:
		env = env.Push()
		for _, clause := range e.ComprClauses {
			switch clause.Kind {
			case ComprEnum:
				d := clause.Expr.Digest(env)
				for _, id := range clause.Pat.Idents(nil) {
					env.Bind(id, d)
				}
			case ComprFilter:
				clause.Expr.digest(w, env)
			}
		}
		e.ComprExpr.digest(w, env)
	case ExprThunk:
		e.Left.digest(w, e.Env)
	case ExprMake:
		// TODO(marius): Module path (e.Left) should probably be normalized somehow.
		// TODO(marius): sort declarations
		e.Left.digest(w, env)
		for _, d := range e.Decls {
			for _, id := range d.Pat.Idents(nil) {
				io.WriteString(w, id)
			}
			d.Expr.digest(w, env)
		}
	case ExprBuiltin:
		io.WriteString(w, e.Op)
		switch e.Op {
		default:
			panic("bad builtin " + e.Op)
		case "len", "unzip", "panic", "map", "list", "flatten", "delay", "trace", "range":
			e.Left.digest(w, env)
		case "zip":
			e.Right.digest(w, env)
			e.Left.digest(w, env)
		}
	case ExprRequires:
		e.Left.digest(w, e.Env)
	default:
		panic("invalid expression " + e.String())
	}
}

// digest1 computes the "single op" digest: it is used to express just the
// operation performed by this expression. In effect, digest1 summarizes
// the arguments of the expression's operation.
func (e *Expr) digest1(w io.Writer) {
	switch e.Kind {
	case ExprAscribe, ExprRequires:
		return
	}

	// TODO(marius): fix later. Same hack in digest() elsewhere in this file.
	if _, err := w.Write(zero34); err != nil {
		panic(err)
	}
	switch e.Kind {
	case ExprIdent:
		io.WriteString(w, e.Ident)
	case ExprBinop, ExprUnop:
		io.WriteString(w, e.Op)
	case ExprApply:
	case ExprConst:
		digest.WriteDigest(w, values.Digest(e.Val, e.Type))
	case ExprBlock:
	case ExprFunc:
		panic("ExprFunc invalid for digest1")
	case ExprTuple, ExprStruct, ExprList, ExprMap:
	case ExprExec:
		panic("ExprExec invalid for digest1")
	case ExprCond:
	case ExprDeref:
		io.WriteString(w, e.Ident)
	case ExprIndex, ExprCompr:
	case ExprThunk:
	case ExprMake:
		io.WriteString(w, e.Left.Val.(string))
	case ExprBuiltin:
		io.WriteString(w, e.Op)
	default:
		panic("invalid expression " + e.String())
	}
}

func writeN(w io.Writer, n int) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(n))
	w.Write(b[:])
}
