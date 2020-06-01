// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package fmt

import (
	"bufio"
	"strings"
)

type writer struct {
	w                      *bufio.Writer
	level                  int
	isAlreadyIndented      bool
	isIndentationSuspended bool

	err error
}

func newWriter(w *bufio.Writer) *writer {
	return &writer{w: w}
}

func (w *writer) writeString(s string) {
	if w.err != nil {
		return
	}

	lines := strings.SplitAfter(s, "\n")
	if strings.HasSuffix(s, "\n") {
		// Avoid writing an empty string after the last newline in s because we may want to change the
		// indent of that line before writing actual content.
		lines = lines[:len(lines)-1]
	}

	for _, line := range lines {
		isLineEnd := strings.HasSuffix(line, "\n")
		if isLineEnd {
			line = line[:len(line)-1]
		}

		// Indent if not already done, but only if the content of this is non-empty, to avoid lines
		// with only spaces.
		if !w.isAlreadyIndented && len(line) > 0 {
			// If indentation is suspended, we just skip writing the tabs. We still do the bookkeeping
			// in case something is written on the same line after suspension ends.
			if !w.isIndentationSuspended {
				if _, err := w.w.WriteString(strings.Repeat("\t", w.level)); err != nil {
					w.err = err
					return
				}
			}
			w.isAlreadyIndented = true
		}

		if _, err := w.w.WriteString(line); err != nil {
			w.err = err
			return
		}
		if isLineEnd {
			if _, err := w.w.WriteString("\n"); err != nil {
				w.err = err
				return
			}
			w.isAlreadyIndented = false
		}
	}
}

func (w *writer) setIndentationSuspended(new bool) {
	w.isIndentationSuspended = new
}

func (w *writer) indent() {
	w.level++
}

func (w *writer) unindent() {
	if w.level <= 0 {
		panic(w.level)
	}
	w.level--
}
