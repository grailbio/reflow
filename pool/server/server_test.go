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
