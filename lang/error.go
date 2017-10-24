// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package lang

import (
	"fmt"
	"io"
	"text/scanner"
)

// Error implements error reporting during parsing, typechecking, and
// evaluation. It is not safe for concurrent access.
type Error struct {
	W io.Writer // The io.Writer to which errors are reported.
	N int       // The error count.
}

// Errorf formats and reports an error.
func (e *Error) Errorf(pos scanner.Position, format string, args ...interface{}) {
	fmt.Fprintf(e.W, "%s: %s\n", pos, fmt.Sprintf(format, args...))
	e.N++
}
