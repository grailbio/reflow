// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"flag"
	"fmt"
)

// Flag exposes a FlagSet that overrides a set of config keys.
type Flag struct {
	Config

	vals map[string]*string
}

// Initialize this Flag config with the provided flag set.
// A flag is registered for each key in the top level AllKeys.
func (f *Flag) Init(flags *flag.FlagSet) {
	f.vals = make(map[string]*string)
	for _, key := range AllKeys {
		f.vals[key] = flags.String(key, "", fmt.Sprintf("override %s from config", key))
	}
}

// Value returns the flag override value for key key, or else the
// value from the layered configuration.
func (f *Flag) Value(key string) interface{} {
	s := f.vals[key]
	if s != nil && *s != "" {
		return *s
	}
	return f.Config.Value(key)
}
