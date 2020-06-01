// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/grailbio/reflow/syntax"
	syntaxfmt "github.com/grailbio/reflow/syntax/syntaxfmt"
)

const (
	codeBadSyntax = 10
	codeBadFormat = 11
)

func (c *Cmd) fmt(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("fmt", flag.ExitOnError)
	sideBySide := flags.Bool("side-by-side", false,
		"Paste original and formatted side-by-side, for comparison")
	debugCompare := flags.Bool("debug-compare", false,
		"Compare original and formatted ASTs and print differences")
	debugWritePos := flags.Bool("debug-write-pos", false,
		"Print debugging information about token positions")
	maxBlankLines := flags.Int("max-blank-lines", 1,
		"Formatter will eliminate sequences of blank lines longer than this")
	overwrite := flags.Bool("w", false, "write result to (source) file instead of stdout")
	help := fmt.Sprintf(
		`Fmt formats a single reflow file and writes it to stdout.

It's fairly new so keep backups of source code, just to be safe.
It currently does not handle comments on expressions well:

  sum := a +
    // b is special.
    b

The comment on b won't be preserved.

As a self-test, if the formatted result is syntactically invalid or
not identical to the input, the formatter exits with codes %d and %d,
respectively. Please report such cases as bugs.`,
		codeBadSyntax, codeBadFormat)
	c.Parse(flags, args, help, "fmt path")

	if flags.NArg() == 0 {
		flags.Usage()
	}

	for _, path := range flags.Args() {
		source, err := ioutil.ReadFile(path)
		if err != nil {
			c.Fatalf("error reading file: %v", err)
		}

		srcParser := &syntax.Parser{
			File: path,
			Body: bytes.NewReader(source),
			Mode: syntax.ParseModule,
		}
		if err := srcParser.Parse(); err != nil {
			c.Fatalf("error parsing file: %v", err)
		}

		out := new(bytes.Buffer)
		bw := bufio.NewWriter(out)
		formatter := syntaxfmt.NewWithOpts(bw, syntaxfmt.Opts{
			DebugWritePos: *debugWritePos,
			MaxBlankLines: *maxBlankLines,
		})
		if err := formatter.WriteModule(srcParser.Module); err != nil {
			c.Fatalf("formatting %s: %v", path, err)
		}
		if err := bw.Flush(); err != nil {
			c.Fatalf("formatting %s: %v", path, err)
		}
		formatted := out.Bytes()

		fmtParser := &syntax.Parser{
			File: path,
			Body: bytes.NewReader(formatted),
			Mode: syntax.ParseModule,
		}
		if err := fmtParser.Parse(); err != nil {
			fmt.Fprintln(os.Stderr, "error: invalid syntax")
			os.Exit(codeBadSyntax)
		} else {
			if !srcParser.Module.Equal(fmtParser.Module) {
				fmt.Fprintln(os.Stderr, "error: internal error; please report")

				if *debugCompare {
					for i := 0; i < len(srcParser.Module.ParamDecls) || i < len(fmtParser.Module.ParamDecls); i++ {
						if i >= len(srcParser.Module.ParamDecls) {
							fmt.Fprintln(os.Stderr, i, "new in fmt: ", fmtParser.Module.ParamDecls[i])
							continue
						}
						if i >= len(fmtParser.Module.ParamDecls) {
							fmt.Fprintln(os.Stderr, i, "missing in fmt: ", srcParser.Module.ParamDecls[i])
							continue
						}
						if !srcParser.Module.ParamDecls[i].Equal(fmtParser.Module.ParamDecls[i]) {
							fmt.Fprintln(os.Stderr, i, "diff src", srcParser.Module.ParamDecls[i])
							fmt.Fprintln(os.Stderr, i, "diff fmt", fmtParser.Module.ParamDecls[i])
						}
					}
					for i := 0; i < len(srcParser.Module.Decls) || i < len(fmtParser.Module.Decls); i++ {
						if i >= len(srcParser.Module.Decls) {
							fmt.Fprintln(os.Stderr, i, "new in fmt: ", fmtParser.Module.Decls[i])
							continue
						}
						if i >= len(fmtParser.Module.Decls) {
							fmt.Fprintln(os.Stderr, i, "missing in fmt: ", srcParser.Module.Decls[i])
							continue
						}
						if !srcParser.Module.Decls[i].Equal(fmtParser.Module.Decls[i]) {
							fmt.Fprintln(os.Stderr, i, "diff src", srcParser.Module.Decls[i])
							fmt.Fprintln(os.Stderr, i, "diff fmt", fmtParser.Module.Decls[i])
						}
					}
				}

				os.Exit(codeBadFormat)
			}
		}

		var outFile *os.File
		if *overwrite {
			outFile, err = os.Create(path)
			if err != nil {
				c.Fatalf("failed to open %s: %v\n", path, err)
			}
		} else {
			outFile = os.Stdout
		}

		if *sideBySide {
			bw := bufio.NewWriter(outFile)
			if err := paste(bw, string(source), string(formatted)); err != nil {
				c.Fatalf("formatting %s: %v", path, err)
			}
			if err := bw.Flush(); err != nil {
				c.Fatalf("formatting %s: %v", path, err)
			}
		} else {
			if _, err := outFile.Write(formatted); err != nil {
				c.Fatalf("failed to write %s: %v\n", path, err)
			}
		}

		if *overwrite {
			if err := outFile.Close(); err != nil {
				c.Fatalf("failed to close %s: %v\n", path, err)
			}
		}
	}
}

func paste(w *bufio.Writer, l, r string) error {
	normalizeWidth := func(s string) string {
		return strings.Replace(s, "\t", "  ", -1)
	}
	llines := strings.Split(normalizeWidth(l), "\n")
	rlines := strings.Split(normalizeWidth(r), "\n")

	maxLeftLength := 0
	for i := 0; i < len(llines) || i < len(rlines); i++ {
		if i < len(llines) && len(llines[i]) > maxLeftLength {
			maxLeftLength = len(llines[i])
		}
	}

	for i := 0; i < len(llines) || i < len(rlines); i++ {
		if i < len(llines) {
			if _, err := w.WriteString(llines[i]); err != nil {
				return err
			}
			if _, err := w.WriteString(strings.Repeat(" ", maxLeftLength-len(llines[i]))); err != nil {
				return err
			}
		} else {
			if _, err := w.WriteString(strings.Repeat(" ", maxLeftLength)); err != nil {
				return err
			}
		}
		if _, err := w.WriteString("|"); err != nil {
			return err
		}
		if i < len(rlines) {
			if _, err := w.WriteString(rlines[i]); err != nil {
				return err
			}
		}
		if _, err := w.WriteString("\n"); err != nil {
			return err
		}
	}
	return nil
}
