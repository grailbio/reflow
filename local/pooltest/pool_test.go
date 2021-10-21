// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package pooltest

import (
	"testing"

	"github.com/grailbio/reflow/local"
	. "github.com/grailbio/reflow/local/testutil"
	"github.com/grailbio/testutil"
)

// TestAlloc tests the whole lifetime of an alloc. One is created, an
// exec is created; it is then not maintained and is garbage
// collected. It becomes a zombie.
func TestAlloc(t *testing.T) {
	p, cleanup := NewTestPoolOrSkip(t)
	defer cleanup()
	TestPool(t, p)
}

func TestPoolFailsLessThanExpectedMem(t *testing.T) {
	// We put this in /tmp because it's one of the default locations
	// that are bindable from Docker for Mac.
	dir, cleanup := testutil.TempDir(t, "/tmp", "reflowtest")
	defer cleanup()
	p := &local.Pool{
		Client: NewDockerClientOrSkip(t),
		Dir:    dir,
	}
	err := p.Start(10 << 40 /* 10TiB, insane expectation */)
	if err != nil {
		t.Logf("expected error observed: %v", err)
	} else {
		t.Fatalf("pool did not fail")
	}
}
