// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"github.com/grailbio/reflow"
)

func init() {
	Register("cache", "off", "", "turn caching off",
		func(cfg Config, arg string) (Config, error) {
			return &cacheOff{cfg}, nil
		},
	)
}

type cacheOff struct {
	Config
}

func (c *cacheOff) Cache() (reflow.Cache, error) {
	// A nil cache is just an off cache.
	return nil, nil
}
