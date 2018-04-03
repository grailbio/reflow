// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

const (
	// The amount of outstanding number of transfers
	// between each repository.
	defaultTransferLimit = 20

	// The number of concurrent stat operations that can
	// be performed against a repository.
	statLimit = 200

	// The number of concurrent cache operations allowed.
	cacheOpConcurrency = 512
)

// TransferLimit returns the configured transfer limit.
func (c *Cmd) TransferLimit() int {
	lim := c.Config.Value("transferlimit")
	if lim == nil {
		return defaultTransferLimit
	}
	v, ok := lim.(int)
	if !ok {
		c.Fatalf("non-integer limit %v", lim)
	}
	return v
}
