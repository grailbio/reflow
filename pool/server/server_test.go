// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package server

import (
	"testing"

	"github.com/grailbio/reflow/local/testutil"
)

func TestClientServer(t *testing.T) {
	p, cleanup := testutil.NewTestPoolOrSkip(t)
	defer cleanup()
	testutil.TestPool(t, p)
}
