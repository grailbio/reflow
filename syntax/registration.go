// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import "github.com/grailbio/reflow/types"

// RegisterModule is a hook to register custom reflow intrinsics.
func RegisterModule(name string, m *ModuleImpl) {
	mu.Lock()
	lib[name] = m
	lib[name].Init(nil, types.NewEnv())
	mu.Unlock()
}
