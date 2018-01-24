package syntax

import "github.com/grailbio/reflow/types"

// RegisterModule is a hook to register custom reflow intrinsics.
func RegisterModule(name string, m *ModuleImpl) {
	mu.Lock()
	lib[name] = m
	lib[name].Init(nil, types.NewEnv())
	mu.Unlock()
}
