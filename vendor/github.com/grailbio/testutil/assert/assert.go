package assert

import (
	"fmt"

	"github.com/grailbio/testutil/h"
)

// TB stands for either *testing.T or *testing.B.
type TB interface {
	Fatal(sargs ...interface{})
}

// That checks if the gives value matches the matcher. If msgs... is not empty,
// msgs[0] must be a format string, and they are printed using fmt.Printf on
// error.
func That(t TB, val interface{}, m *h.Matcher, msgs ...interface{}) {
	r := m.Match(val)
	if r.Status() == h.Match {
		return
	}
	var msg string
	switch len(msgs) {
	case 0:
	case 1:
		msg = " " + msgs[0].(string)
	default:
		f := msgs[0].(string)
		msg = " " + fmt.Sprintf(f, msgs[1:]...)
	}
	t.Fatal(r.String() + msg)
}
