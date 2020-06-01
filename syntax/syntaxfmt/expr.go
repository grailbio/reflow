// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package fmt

import (
	"fmt"
	"sort"
	"strings"

	"github.com/grailbio/reflow/syntax"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

func (f *Fmt) writeExpr(e *syntax.Expr) sourceLine {
	if f.opts.DebugWritePos {
		f.w.writeString(fmt.Sprintf("%d:%d:%.4q[", e.Line, e.Column, e.Comment))
		defer f.w.writeString("]")
	}
	f.writeComment(e.Comment)
	switch e.Kind {
	case syntax.ExprError, syntax.ExprThunk:
		panic(e)
	case syntax.ExprIdent:
		f.w.writeString(e.Ident)
		return sourceLine(e.Line)
	case syntax.ExprBinop:
		if e.Fmt.IsSet(syntax.FmtAppendArgs) {
			f.w.writeString("[")
			breakPrev := sourceLine(e.Line)
			broken := false
			switch findAppendOpKind(e) {
			case syntax.ExprList:
				_ = f.writeListAppendArgs(e, &breakPrev, &broken)
			case syntax.ExprMap:
				_ = f.writeMapAppendArgs(e, &breakPrev, &broken)
			}
			f.w.writeString("]")
			if broken {
				f.w.unindent()
			}
			return breakPrev
		}
		parenthesizeRight := false
		if l, r := e.Prec(), e.Right.Prec(); r <= l && l != 0 && r != 0 {
			parenthesizeRight = true
		}
		last := f.writeExpr(e.Left)
		f.w.writeString(" ")
		f.w.writeString(e.Op)
		nBreaks := f.nBreakExpr(last, e.Right)
		if nBreaks > 0 {
			f.w.writeString(strings.Repeat("\n", nBreaks))
			f.w.indent()
		} else {
			f.w.writeString(" ")
		}
		if parenthesizeRight {
			f.w.writeString("(")
		}
		last = f.writeExpr(e.Right)
		if parenthesizeRight {
			f.w.writeString(")")
		}
		if nBreaks > 0 {
			f.w.unindent()
		}
		return last
	case syntax.ExprUnop:
		f.w.writeString(e.Op)
		return f.writeExpr(e.Left)
	case syntax.ExprApply:
		breakPrev := f.writeExpr(e.Left)
		f.w.writeString("(")
		broken := false
		for i, field := range e.Fields {
			if i > 0 {
				f.w.writeString(",")
			}
			if n := f.nBreakExpr(breakPrev, field.Expr); n > 0 {
				f.w.writeString(strings.Repeat("\n", n))
				if !broken {
					f.w.indent()
				}
				broken = true
			} else if i > 0 {
				f.w.writeString(" ")
			}
			breakPrev = f.writeExpr(field.Expr)
		}
		f.w.writeString(")")
		if broken {
			f.w.unindent()
		}
		return breakPrev
	case syntax.ExprConst:
		switch e.Type {
		case types.Bool, types.Int, types.Float:
			f.w.writeString(values.Sprint(e.Val, e.Type))
			return sourceLine(e.Line)
		case types.String:
			val := e.Val.(string)
			// TODO: Consider canonicalizing escaped vs. raw.
			if e.Fmt.IsSet(syntax.FmtRawString) {
				f.w.writeString("`")
				f.w.setIndentationSuspended(true)
				f.w.writeString(val)
				f.w.setIndentationSuspended(false)
				f.w.writeString("`")
			} else {
				f.w.writeString(`"`)
				f.w.writeString(val)
				f.w.writeString(`"`)
			}
			return sourceLine(e.Line)
		default:
			// The Parser should not produce ExprConst for any other types.
			panic(e)
		}
	case syntax.ExprAscribe:
		panic(e)
	case syntax.ExprBlock:
		f.w.writeString("{\n")
		f.w.indent()
		last := sourceLine(e.Line)
		for _, decl := range e.Decls {
			last = f.writeDeclExpr(last, decl, decl.Expr, decl.Comment)
			f.w.writeString("\n")
		}
		fmt.Println("!!!", last, sourceLine(e.Left.Line), e.Left.Comment)
		f.w.writeString(strings.Repeat("\n", nExtraDeclBreaks(last, sourceLine(e.Left.Line), e.Left.Comment)))
		last = f.writeExpr(e.Left)
		f.w.unindent()
		f.w.writeString("\n}")
		return last
	case syntax.ExprFunc:
		panic(e)
	case syntax.ExprTuple:
		f.w.writeString("(")
		breakPrev := sourceLine(e.Line)
		broken := false
		for i, field := range e.Fields {
			if i > 0 {
				f.w.writeString(",")
			}
			if n := f.nBreakExpr(breakPrev, field.Expr); n > 0 {
				f.w.writeString(strings.Repeat("\n", n))
				if !broken {
					f.w.indent()
				}
				broken = true
			} else if i > 0 {
				f.w.writeString(" ")
			}
			breakPrev = f.writeExpr(field.Expr)
		}
		f.w.writeString(")")
		if broken {
			f.w.unindent()
		}
		return breakPrev
	case syntax.ExprStruct:
		f.w.writeString("{")
		breakPrev := sourceLine(e.Line)
		broken := false
		for i, field := range e.Fields {
			if i > 0 {
				f.w.writeString(",")
			}
			if n := f.nBreakExpr(breakPrev, field.Expr); n > 0 {
				f.w.writeString(strings.Repeat("\n", n))
				if !broken {
					f.w.indent()
				}
				broken = true
			} else if i > 0 {
				f.w.writeString(" ")
			}
			if field.Expr.Kind == syntax.ExprIdent && field.Expr.Ident == field.Name {
				f.w.writeString(field.Name)
				breakPrev = sourceLine(field.Expr.Line)
			} else {
				f.w.writeString(field.Name)
				f.w.writeString(":")
				breakPrev = f.writeExpr(field.Expr)
			}
		}
		if broken {
			f.w.writeString(",\n")
			f.w.unindent()
			f.w.writeString("}")
		} else {
			f.w.writeString("}")
		}
		return breakPrev
	case syntax.ExprList:
		f.w.writeString("[")
		breakPrev := sourceLine(e.Line)
		broken := false
		f.writeListAppendArgs(e, &breakPrev, &broken)
		f.w.writeString("]")
		if broken {
			f.w.unindent()
		}
		return breakPrev
	case syntax.ExprMap:
		f.w.writeString("[")
		breakPrev := sourceLine(e.Line)
		broken := false
		f.writeMapAppendArgs(e, &breakPrev, &broken)
		f.w.writeString("]")
		if broken {
			f.w.unindent()
		}
		return breakPrev
	case syntax.ExprExec:
		f.w.writeString("exec(")
		for i, decl := range e.Decls {
			if i > 0 {
				f.w.writeString(", ")
			}
			f.writeDeclExpr(0, decl, decl.Expr, decl.Comment)
		}
		f.w.writeString(") ")
		f.w.writeString(e.Type.String())
		f.w.writeString(" {\"")
		f.w.setIndentationSuspended(true)
		// Disable indentation in the template to avoid invalidating exec caching.
		f.w.writeString(e.Template.Text)
		f.w.setIndentationSuspended(false)
		f.w.writeString("\"}")
		return sourceLine(e.Line + strings.Count(e.Template.Text, "\n"))
	case syntax.ExprCond:
		f.w.writeString("if ")
		_ = f.writeExpr(e.Cond)
		f.w.writeString(" ")
		_ = f.writeExpr(e.Left)
		f.w.writeString(" else ")
		return f.writeExpr(e.Right)
	case syntax.ExprDeref:
		last := f.writeExpr(e.Left)
		f.w.writeString(".")
		f.w.writeString(e.Ident)
		return last
	case syntax.ExprIndex:
		last := f.writeExpr(e.Left)
		f.w.writeString("[")
		nBreaks := f.nBreakExpr(last, e.Right)
		if nBreaks > 0 {
			f.w.writeString(strings.Repeat("\n", nBreaks))
			f.w.indent()
		}
		last = f.writeExpr(e.Right)
		f.w.writeString("]")
		if nBreaks > 0 {
			f.w.unindent()
		}
		return last
	case syntax.ExprCompr:
		f.w.writeString("[ ")
		broken := false
		if n := f.nBreakExpr(sourceLine(e.Line), e.ComprExpr); n > 0 {
			f.w.writeString("\n") // Disallow blank lines.
			f.w.indent()
			broken = true
		}
		breakPrev := f.writeExpr(e.ComprExpr)
		f.w.writeString(" | ")
		for i, clause := range e.ComprClauses {
			if i > 0 {
				f.w.writeString(",")
			}
			if n := f.nBreakExpr(breakPrev, clause.Expr); n > 0 {
				f.w.writeString(strings.Repeat("\n", n))
				if !broken {
					f.w.indent()
				}
				broken = true
			} else if i > 0 {
				f.w.writeString(" ")
			}
			switch clause.Kind {
			case syntax.ComprEnum:
				f.w.writeString(clause.Pat.String())
				f.w.writeString(" <- ")
				breakPrev = f.writeExpr(clause.Expr)
			case syntax.ComprFilter:
				f.w.writeString("if ")
				breakPrev = f.writeExpr(clause.Expr)
			default:
				panic(e)
			}
		}
		f.w.writeString(" ]")
		if broken {
			f.w.unindent()
		}
		return breakPrev
	case syntax.ExprMake:
		f.w.writeString("make(")
		breakPrev := f.writeExpr(e.Left)
		broken := false
		for _, decl := range e.Decls {
			f.w.writeString(",")
			if n := nExtraDeclBreaks(breakPrev, sourceLine(decl.Line), decl.Comment) + 1; n > 0 {
				f.w.writeString(strings.Repeat("\n", n))
				if !broken {
					f.w.indent()
				}
				broken = true
				f.writeComment(decl.Comment)
			} else {
				f.w.writeString(" ")
			}
			// Disable writeDeclExpr line breaks and comments because it doesn't indent.
			breakPrev = f.writeDeclExpr(0, decl, decl.Expr, "")
		}
		f.w.writeString(")")
		if broken {
			f.w.unindent()
		}
		return breakPrev
	case syntax.ExprBuiltin:
		f.w.writeString(e.Op)
		f.w.writeString("(")
		var last sourceLine
		if e.Left != nil {
			last = f.writeExpr(e.Left)
		}
		if e.Right != nil {
			f.w.writeString(", ")
			last = f.writeExpr(e.Right)
		}
		f.w.writeString(")")
		return last
	case syntax.ExprRequires:
		panic(e)
	default:
		panic("unhandled expression " + e.String())
	}
}

func (f *Fmt) nBreakExpr(left sourceLine, right *syntax.Expr) int {
	if right.Line == 0 {
		return 0 // Parser leaves Position unset for some expressions.
	}
	nBreak := right.Line - int(left)
	nBreak -= strings.Count(right.Comment, "\n")
	if nBreak < 0 {
		panic([]interface{}{left, right})
	}
	if nBreak > f.opts.MaxBlankLines+1 {
		nBreak = f.opts.MaxBlankLines + 1
	}
	return nBreak
}

// findAppendOpKind inspects expression e to determine if it's either a list or map append BinOp.
// Returns ExprList or ExprMap, respectively. e MUST be an append BinOp.
// For example, in expression [1, 2, ...a ... b], parsing to ([1, 2] + a) + b, we recursively
// inspect the tree until encountering [1, 2] of kind ExprList.
func findAppendOpKind(e *syntax.Expr) syntax.ExprKind {
	// List append BinOps are left-leaning by construction.
	child := e
	for child.Kind == syntax.ExprBinop {
		child = child.Left
	}
	if child.Kind == syntax.ExprList {
		return syntax.ExprList
	}

	// Map append BinOps are right-leaning.
	child = e
	for child.Kind == syntax.ExprBinop {
		child = child.Right
	}
	if child.Kind == syntax.ExprMap {
		return syntax.ExprMap
	}

	panic(e)
}

func (f *Fmt) writeListAppendArgs(e *syntax.Expr, breakPrev *sourceLine, broken *bool) (leaf bool) {
	switch e.Kind {
	case syntax.ExprList:
		for i, v := range e.List {
			if i > 0 {
				f.w.writeString(",")
			}
			if n := f.nBreakExpr(*breakPrev, v); n > 0 {
				f.w.writeString(strings.Repeat("\n", n))
				if !*broken {
					f.w.indent()
				}
				*broken = true
			} else if i > 0 {
				f.w.writeString(" ")
			}
			*breakPrev = f.writeExpr(v)
		}
		return true
	case syntax.ExprBinop:
		if !e.Fmt.IsSet(syntax.FmtAppendArgs) {
			panic(e)
		}
		wasLeaf := f.writeListAppendArgs(e.Left, breakPrev, broken)
		if n := f.nBreakExpr(*breakPrev, e.Right); n > 0 {
			if wasLeaf {
				f.w.writeString(",")
			}
			f.w.writeString(strings.Repeat("\n", n))
			if !*broken {
				f.w.indent()
			}
			*broken = true
		} else {
			f.w.writeString(" ")
		}
		f.w.writeString("...")
		*breakPrev = f.writeExpr(e.Right)
		return false
	default:
		panic(e)
	}
}

func (f *Fmt) writeMapAppendArgs(e *syntax.Expr, breakPrev *sourceLine, broken *bool) (leaf bool) {
	switch e.Kind {
	case syntax.ExprMap:
		// Sort map keys to preserve source ordering.
		var keys []*syntax.Expr
		for ke := range e.Map {
			keys = append(keys, ke)
		}
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].Line == keys[j].Line {
				return keys[i].Column < keys[j].Column
			}
			return keys[i].Line < keys[j].Line
		})
		i := 0
		for _, ke := range keys {
			if i > 0 {
				f.w.writeString(",")
			}
			if n := f.nBreakExpr(*breakPrev, ke); n > 0 {
				f.w.writeString(strings.Repeat("\n", n))
				if !*broken {
					f.w.indent()
				}
				*broken = true
			} else if i > 0 {
				f.w.writeString(" ")
			}
			*breakPrev = f.writeExpr(ke)
			brokenEntry := f.nBreakExpr(*breakPrev, e.Map[ke]) > 0 // Disallow blank lines.
			if brokenEntry {
				f.w.writeString(":\n")
				f.w.indent()
			} else {
				f.w.writeString(": ")
			}
			*breakPrev = f.writeExpr(e.Map[ke])
			if brokenEntry {
				f.w.unindent()
			}
			i++
		}
		return true
	case syntax.ExprBinop:
		if !e.Fmt.IsSet(syntax.FmtAppendArgs) {
			panic(e)
		}
		wasLeaf := f.writeMapAppendArgs(e.Right, breakPrev, broken)
		if n := f.nBreakExpr(*breakPrev, e.Left); n > 0 {
			if wasLeaf {
				f.w.writeString(",")
			}
			f.w.writeString(strings.Repeat("\n", n))
			if !*broken {
				f.w.indent()
			}
			*broken = true
		} else {
			f.w.writeString(" ")
		}
		f.w.writeString("...")
		*breakPrev = f.writeExpr(e.Left)
		return false
	default:
		panic(e)
	}
}
