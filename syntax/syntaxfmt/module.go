// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package fmt

import (
	"strings"

	"github.com/grailbio/reflow/syntax"
)

// sourceLine is a file line number from the original (unformatted) file, generally representing
// where a Decl or Expr was located.
type sourceLine int

// WriteModule writes a formatted representation of module to out.
func (f *Fmt) WriteModule(module *syntax.ModuleImpl) error {
	var last sourceLine

	if len(module.ParamDecls) > 0 {
		// Invariant: len(x) > 0 for all x in byLine.
		byLine := [][]*syntax.Decl{{module.ParamDecls[0]}}
		for _, decl := range module.ParamDecls[1:] {
			if decl.Position.Line == byLine[len(byLine)-1][0].Line {
				byLine[len(byLine)-1] = append(byLine[len(byLine)-1], decl)
			} else {
				byLine = append(byLine, []*syntax.Decl{decl})
			}
		}

		if len(byLine) > 1 {
			f.w.writeString("param (\n")
			f.w.indent()
		} else {
			f.w.writeString("param ")
		}

		for _, decls := range byLine {
			if n := nExtraDeclBreaks(last, sourceLine(decls[0].Line), decls[0].Comment); last > 0 && n > 0 {
				f.w.writeString(strings.Repeat("\n", n))
			}
			f.writeComment(decls[0].Comment)
			if len(decls) > 1 {
				last = f.writeParamDecls(decls)
			} else {
				last = f.writeParamDecl(decls[0])
			}
			f.w.writeString("\n")
		}

		if len(byLine) > 1 {
			f.w.unindent()
			f.w.writeString(")\n")
		}

		f.w.writeString("\n")
	}

	last = 0
	for _, decl := range module.Decls {
		last = f.writeDeclExpr(last, decl, decl.Expr, decl.Comment)
		f.w.writeString("\n")
	}

	return f.w.err
}

func (f *Fmt) writeComment(comment string) {
	if len(comment) == 0 {
		return
	}
	for _, line := range strings.Split(strings.TrimSuffix(comment, "\n"), "\n") {
		f.w.writeString("// ")
		f.w.writeString(line)
		f.w.writeString("\n")
	}
}

func nExtraDeclBreaks(prev, next sourceLine, comment string) int {
	return int(next-prev) - strings.Count(comment, "\n") - 1
}
