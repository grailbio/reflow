package liveset

import (
	"github.com/grailbio/base/digest"
)

// A Liveset contains a possibly approximate judgement about live
// objects.
type Liveset interface {
	// Contains returns true if the given object definitely is in the
	// set; it may rarely return true when the object does not.
	Contains(digest.Digest) bool
}
