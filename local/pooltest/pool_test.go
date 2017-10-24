// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package pooltest

import (
	"testing"

	. "github.com/grailbio/reflow/local/testutil"
)

// TestAlloc tests the whole lifetime of an alloc. One is created, an
// exec is created; it is then not maintained and is garbage
// collected. It becomes a zombie.
func TestAlloc(t *testing.T) {
	p, cleanup := NewTestPoolOrSkip(t)
	defer cleanup()
	TestPool(t, p)
}
