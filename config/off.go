// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import "github.com/grailbio/reflow/infra"

func init() {
	options := []struct {
		name, doc string
		mode      infra.CacheMode
	}{
		{"off", "turn caching off", infra.CacheOff},
		{"read", "read-only caching", infra.CacheRead},
		{"write", "write-only caching", infra.CacheWrite},
		{"read+write", "read and write caching", infra.CacheRead | infra.CacheWrite},
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
	mode infra.CacheMode
}

func (c *cacheMode) CacheMode() infra.CacheMode {
	return c.mode
}
