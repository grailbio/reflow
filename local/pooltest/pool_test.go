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
