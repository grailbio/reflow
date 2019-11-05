// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow

// This is for exporting otherwise unexported members for testing purposes.

type LegacyAssertionKey = assertionKey

func (a LegacyAssertionKey) Less(b LegacyAssertionKey) bool {
	return a.less(b)
}
