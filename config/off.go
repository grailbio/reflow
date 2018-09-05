// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import "github.com/grailbio/reflow/flow"

func init() {
	options := []struct {
		name, doc string
		mode      flow.CacheMode
	}{
		{"off", "turn caching off", flow.CacheOff},
		{"read", "read-only caching", flow.CacheRead},
		{"write", "write-only caching", flow.CacheWrite},
		{"read+write", "read and write caching", flow.CacheRead | flow.CacheWrite},
	}
	for _, opt := range options {
		opt := opt
		Register("cache", opt.name, "", opt.doc,
			func(cfg Config, arg string) (Config, error) {
				return &cacheMode{cfg, opt.mode}, nil
			},
		)
	}
}

type cacheMode struct {
	Config
	mode flow.CacheMode
}

func (c *cacheMode) CacheMode() flow.CacheMode {
	return c.mode
}
