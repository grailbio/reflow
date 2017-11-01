// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"errors"
	"strings"
)

func init() {
	Register(User, "local", "username", "provide a local username",
		func(cfg Config, arg string) (Config, error) {
			if !strings.Contains(arg, "@") {
				return nil, errors.New("local: username must contain @")
			}
			return &localUser{cfg, arg}, nil
		},
	)
}

type localUser struct {
	Config
	user string
}

func (c *localUser) User() (string, error) {
	return c.user, nil
}
