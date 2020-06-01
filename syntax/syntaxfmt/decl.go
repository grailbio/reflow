// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package fmt

import (
	"fmt"
	"strings"

	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
)

func (f *Fmt) writeParamDecls(ds []*syntax.Decl) sourceLine {
	if len(ds) <= 1 {
		panic(ds)
	}
	if f.opts.DebugWritePos {
		f.w.writeString(fmt.Sprintf("%d:%d:%.4q[", ds[0].Line, ds[0].Column, ds[0].Comment))
		defer f.w.writeString("]")
	}
	for i, d := range ds {
		if d.Kind != syntax.DeclDeclare {
			panic(ds)
		}
		if i > 0 {
			f.w.writeString(", ")
		}
		f.w.writeString(d.Ident)
	}
	f.w.writeString(" ")
	f.w.writeString(ds[0].Type.String())
	return sourceLine(ds[0].Line)
}

func (f *Fmt) writeParamDecl(d *syntax.Decl) sourceLine {
	if f.opts.DebugWritePos {
		f.w.writeString(fmt.Sprintf("%d:%d:%.4q[", d.Line, d.Column, d.Comment))
		defer f.w.writeString("]")
	}
	switch d.Kind {
	case syntax.DeclAssign:
		switch d.Expr.Kind {
		case syntax.ExprAscribe:
			f.w.writeString(d.Pat.String())
			f.w.writeString(" ")
			f.w.writeString(d.Expr.Type.String())
			f.w.writeString(" =")
			nBreaks := f.nBreakExpr(sourceLine(d.Position.Line), d.Expr.Left)
			if nBreaks > 0 {
				f.w.writeString(strings.Repeat("\n", nBreaks))
				f.w.indent()
			} else {
				f.w.writeString(" ")
			}
			last := f.writeExpr(d.Expr.Left)
			if nBreaks > 0 {
				f.w.unindent()
			}
			return last
		default:
			f.w.writeString(d.Pat.String())
			f.w.writeString(" =")
			nBreaks := f.nBreakExpr(sourceLine(d.Position.Line), d.Expr)
			if nBreaks > 0 {
				f.w.writeString(strings.Repeat("\n", nBreaks))
				f.w.indent()
			} else {
				f.w.writeString(" ")
			}
			last := f.writeExpr(d.Expr)
			if nBreaks > 0 {
				f.w.unindent()
			}
			return last
		}
	case syntax.DeclDeclare:
		f.w.writeString(d.Ident)
		f.w.writeString(" ")
		f.w.writeString(d.Type.String())
		return sourceLine(d.Line)
	default:
		panic(d)
	}
}

func (f *Fmt) writeDeclExpr(last sourceLine, d *syntax.Decl, e *syntax.Expr, comment string) sourceLine {
	if f.opts.DebugWritePos {
		f.w.writeString(fmt.Sprintf("%d:%d:%d:%.4q[", d.Line, d.Column, last, d.Comment))
		defer f.w.writeString("]")
	}
	if last > 0 {
		var nBreaks int
		if d.Kind == syntax.DeclAssign && e.Kind == syntax.ExprRequires {
			requiresLine := e.Line
			// The @requires(...) annotation always occupies one line.
			nBreaks = nExtraDeclBreaks(last, sourceLine(requiresLine), comment) - 1
			nBreaks--
		} else {
			nBreaks = nExtraDeclBreaks(last, sourceLine(d.Line), comment)
		}
		if last > 0 && nBreaks > 0 {
			if nBreaks > f.opts.MaxBlankLines {
				nBreaks = f.opts.MaxBlankLines
			}
			f.w.writeString(strings.Repeat("\n", nBreaks))
		}
	}
	f.writeComment(comment)
	switch d.Kind {
	case syntax.DeclType:
		f.w.writeString("type ")
		f.w.writeString(d.Ident)
		f.w.writeString(" ")
		f.w.writeString(d.Type.String())
		return sourceLine(d.Line)
	case syntax.DeclAssign:
		switch e.Kind {
		case syntax.ExprFunc:
			return f.writeFunc(e, d.Pat, nil)
		case syntax.ExprAscribe:
			switch e.Left.Kind {
			case syntax.ExprFunc:
				return f.writeFunc(e.Left, d.Pat, e.Type)
			default:
				f.w.writeString("val ")
				f.w.writeString(d.Pat.String())
				f.w.writeString(" ")
				f.w.writeString(e.Type.String())
				f.w.writeString(" =")
				nBreaks := f.nBreakExpr(sourceLine(d.Position.Line), e.Left)
				if nBreaks > 0 {
					f.w.writeString(strings.Repeat("\n", nBreaks))
					f.w.indent()
				} else {
					f.w.writeString(" ")
				}
				last := f.writeExpr(e.Left)
				if nBreaks > 0 {
					f.w.unindent()
				}
				return last
			}
		case syntax.ExprRequires:
			f.w.writeString("@requires(")
			for i, decl := range e.Decls {
				if i > 0 {
					f.w.writeString(", ")
				}
				_ = f.writeDeclExpr(0, decl, decl.Expr, decl.Comment)
			}
			f.w.writeString(")\n")
			return f.writeDeclExpr(0, d, e.Left, "")
		default:
			switch d.Pat.Kind {
			case syntax.PatIdent:
				if e.Kind == syntax.ExprIdent && e.Ident == d.Pat.Ident {
					f.w.writeString(d.Pat.Ident)
					return sourceLine(d.Line)
				}
				f.w.writeString(d.Pat.String())
				f.w.writeString(" :=")
				nBreaks := f.nBreakExpr(sourceLine(d.Position.Line), e)
				if nBreaks > 0 {
					f.w.writeString(strings.Repeat("\n", nBreaks))
					f.w.indent()
				} else {
					f.w.writeString(" ")
				}
				last := f.writeExpr(e)
				if nBreaks > 0 {
					f.w.unindent()
				}
				return last
			default:
				f.w.writeString("val ")
				f.w.writeString(d.Pat.String())
				f.w.writeString(" =")
				nBreaks := f.nBreakExpr(sourceLine(d.Position.Line), e)
				if nBreaks > 0 {
					f.w.writeString(strings.Repeat("\n", nBreaks))
					f.w.indent()
				} else {
					f.w.writeString(" ")
				}
				last := f.writeExpr(e)
				if nBreaks > 0 {
					f.w.unindent()
				}
				return last
			}
		}
	default:
		panic([]interface{}{d, e})
	}
}

func (f *Fmt) writeFunc(e *syntax.Expr, ident *syntax.Pat, returnType *types.T) sourceLine {
	f.w.writeString("func ")
	f.w.writeString(ident.String())
	f.w.writeString("(")
	for i, arg := range e.Args {
		if i > 0 {
			f.w.writeString(", ")
		}
		f.w.writeString(arg.Name)
		if i >= len(e.Args)-1 || !e.Args[i+1].T.StructurallyEqual(arg.T) {
			f.w.writeString(" ")
			f.w.writeString(e.Args[i].T.String())
		}
	}
	f.w.writeString(") ")
	if returnType != nil {
		f.w.writeString(returnType.Elem.String())
		f.w.writeString(" ")
	}
	f.w.writeString("=")
	nBreaks := f.nBreakExpr(sourceLine(e.Line), e.Left)
	if nBreaks > 0 {
		f.w.writeString(strings.Repeat("\n", nBreaks))
		f.w.indent()
	} else {
		f.w.writeString(" ")
	}
	last := f.writeExpr(e.Left)
	if nBreaks > 0 {
		f.w.unindent()
	}
	return last
}
