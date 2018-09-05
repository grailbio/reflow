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
