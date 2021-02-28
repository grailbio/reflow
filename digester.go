// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

import (
	"crypto"
	_ "crypto/sha256"

	"github.com/grailbio/base/digest"
)

var Digester = digest.Digester(crypto.SHA256)

// StringDigest holds any string and its digest.
type StringDigest struct {
	s string
	d digest.Digest
}

// NewStringDigest creates a StringDigest based on the given string.
func NewStringDigest(s string) StringDigest {
	return StringDigest{
		s: s,
		d: Digester.FromString(s),
	}
}

// String returns the underlying string.
func (i StringDigest) String() string {
	return i.s
}

// Digest returns the digest of the underlying string.
func (i StringDigest) Digest() digest.Digest {
	return i.d
}

// IsValid returns whether this StringDigest is valid (ie, the underlying string is non-empty).
func (i StringDigest) IsValid() bool {
	return i.s != ""
}
