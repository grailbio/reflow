// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"io"

	"github.com/grailbio/reflow/types"
)

// RegisterModule is a hook to register custom reflow intrinsics.
func RegisterModule(name string, m *ModuleImpl) {
	mu.Lock()
	lib[name] = m
	lib[name].Init(nil, types.NewEnv())
	mu.Unlock()
}

// ParseAndRegisterModule is like RegisterModule, but parses module from reflow source.
// This function has the advantage of being able to define module parameters.
func ParseAndRegisterModule(name string, source io.Reader) error {
	p := Parser{Mode: ParseModule, Body: source}
	if err := p.Parse(); err != nil {
		return err
	}
	RegisterModule(name, p.Module)
	return nil
}
