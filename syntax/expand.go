// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import "github.com/grailbio/reflow/types"

// Expands expands any aliases present in the type t, with respect to
// the environment env. We look up type aliases directly in the
// environment, or through module definitions whose types are
// bound in the value dictionary.
func expand(t *types.T, env *types.Env) *types.T {
	if t.Error != nil {
		return t
	}
	if t.Kind == types.RefKind {
		switch len(t.Path) {
		case 0:
			return types.Errorf("invalid alias type")
		case 1:
			id := t.Path[0]
			u := env.Alias(id)
			if u == nil {
				return types.Errorf("type alias %s not found", id)
			}
			u = u.Copy()
			// Carry the path for pretty printing.
			u.Path = t.Path
			return types.Promote(u)
		default:
			var u *types.T
			for i, el := range t.Path {
				switch i {
				case 0:
					u = env.Type(el)
				default:
					u = u.Alias(el)
				}
				if i == len(t.Path)-1 {
					break
				}
				if u == nil {
					return types.Errorf("type alias %s not found: no such module %s", t.Ident(), el)
				}
				if u.Kind != types.ModuleKind {
					return types.Errorf("type alias %s not found: %s is not a module", t.Ident(), el)
				}
			}
			if u == nil {
				return types.Errorf("type alias %s not found", t.Ident())
			}
			u = u.Copy()
			// Carry the path for pretty printing.
			u.Path = t.Path
			return types.Promote(u)
		}
	}

	u := t.Copy()
	if t.Index != nil {
		u.Index = expand(t.Index, env)
	}
	if t.Elem != nil {
		u.Elem = expand(t.Elem, env)
	}
	if t.Fields != nil {
		u.Fields = make([]*types.Field, len(t.Fields))
		for i, f := range t.Fields {
			u.Fields[i] = &types.Field{
				Name: f.Name,
				T:    expand(f.T, env),
			}
		}
	}
	if t.Aliases != nil {
		u.Aliases = make([]*types.Field, len(t.Aliases))
		for i, f := range t.Aliases {
			u.Aliases[i] = &types.Field{
				Name: f.Name,
				T:    expand(f.T, env),
			}
		}
	}
	return types.Promote(u)
}
