// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import (
	"bytes"
	"io"
)

// prefixWriter is an io.Writer that outputs a prefix before each line.
type prefixWriter struct {
	w          io.Writer
	prefix     string
	needPrefix bool
}

func newPrefixWriter(w io.Writer, prefix string) *prefixWriter {
	return &prefixWriter{w: w, prefix: prefix, needPrefix: true}
}

func (w *prefixWriter) Write(p []byte) (n int, err error) {
	if w.needPrefix {
		if n, err := io.WriteString(w.w, w.prefix); err != nil {
			return n, err
		}
		w.needPrefix = false
	}
	for {
		i := bytes.Index(p, []byte{'\n'})
		switch i {
		case len(p) - 1:
			w.needPrefix = true
			fallthrough
		case -1:
			return w.w.Write(p)
		default:
			n, err := w.w.Write(p[:i+1])
			if err != nil {
				return n, err
			}
			_, err = io.WriteString(w.w, w.prefix)
			if err != nil {
				return n, err
			}
			p = p[i+1:]
		}
	}
}
