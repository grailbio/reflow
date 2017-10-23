package reflow

import (
	"bytes"
	"io"
	"testing"
)

func TestPrefixWriter(t *testing.T) {
	var b bytes.Buffer
	w := newPrefixWriter(&b, "prefix: ")
	io.WriteString(w, "hello")
	io.WriteString(w, "\nworld\n\n")
	io.WriteString(w, "another\ntest\nthere\n")
	if got, want := b.String(), `prefix: hello
prefix: world
prefix: 
prefix: another
prefix: test
prefix: there
`; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
