// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"github.com/grailbio/reflow"
)

func init() {
	options := []struct {
		name, doc string
		mode      reflow.CacheMode
	}{
		{"off", "turn caching off", reflow.CacheOff},
		{"read", "read-only caching", reflow.CacheRead},
		{"write", "write-only caching", reflow.CacheWrite},
		{"read+write", "read and write caching", reflow.CacheRead | reflow.CacheWrite},
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
	mode reflow.CacheMode
}

func (c *cacheMode) CacheMode() reflow.CacheMode {
	return c.mode
}
