// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"bytes"
	"fmt"

	"grail.com/reflow/syntax/scanner"
)

// posError attaches a position to an error.
type posError struct {
	scanner.Position
	err error
}

func (e posError) Error() string {
	return e.Position.String() + ": " + e.err.Error()
}

// posErrors represents multiple posErrors.
type posErrors []posError

func (e posErrors) Error() string {
	b := new(bytes.Buffer)
	for i, err := range e {
		b.WriteString(err.Error())
		if i != len(e)-1 {
			b.WriteString("\n")
		}
	}
	return b.String()
}

// errorf formats, and then returns a posErrors.
func errorf(pos scanner.Position, format string, args ...interface{}) error {
	return posErrors{{pos, fmt.Errorf(format, args...)}}
}

// errlist accumulates posErrors.
type errlist []posError

func (e errlist) Error(pos scanner.Position, err error) errlist {
	if err == nil {
		return e
	}
	return append(e, posError{pos, err})
}

func (e errlist) Errorf(pos scanner.Position, format string, args ...interface{}) errlist {
	return e.Error(pos, fmt.Errorf(format, args...))
}

func (e errlist) Append(err error) errlist {
	if err == nil {
		return e
	}
	switch err := err.(type) {
	case posError:
		return append(e, err)
	case posErrors:
		return append(e, err...)
	default:
		panic("only posError[s] allowed")
	}
}

func (e errlist) Make() error {
	if len(e) > 0 {
		return posErrors(e)
	}
	return nil
}
