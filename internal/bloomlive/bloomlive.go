package bloomlive

import (
	"bytes"
	"encoding/json"

	"github.com/grailbio/base/digest"
	"github.com/willf/bloom"
)

// T implements a reflow.Liveset using a concrete bloom filter.
// The bloom filter stores each digest according to its bytewise
// representation.
type T struct {
	*bloom.BloomFilter
	buf bytes.Buffer
}

// New creates a new T from a bloom filter.
func New(b *bloom.BloomFilter) *T {
	return &T{BloomFilter: b}
}

// Contains tells whether the digest d is definitely in the set. Contains
// is not safe to call concurrently.
func (b *T) Contains(d digest.Digest) bool {
	b.buf.Reset()
	if _, err := digest.WriteDigest(&b.buf, d); err != nil {
		panic("failed to write digest " + d.String() + ": " + err.Error())
	}
	return b.BloomFilter.Test(b.buf.Bytes())
}

// MarshalJSON serializes the liveset into JSON.
func (b *T) MarshalJSON() ([]byte, error) {
	return json.Marshal(b.BloomFilter)
}

// UnmarshalJSON deserializes the liveset from JSON.
func (b *T) UnmarshalJSON(p []byte) error {
	b.BloomFilter = new(bloom.BloomFilter)
	return json.Unmarshal(p, b.BloomFilter)
}
