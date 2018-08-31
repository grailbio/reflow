// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package syntax

import (
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/types"
	"github.com/grailbio/reflow/values"
)

func fileToFileset(file reflow.File) reflow.Fileset {
	return reflow.Fileset{
		Map: map[string]reflow.File{
			".": file,
		},
	}
}

func dirToFileset(dir values.Dir) reflow.Fileset {
	fs := reflow.Fileset{Map: map[string]reflow.File{}}
	for k, file := range dir {
		fs.Map[k] = reflow.File(file)
	}
	return fs
}

func coerceToFileset(t *types.T, v values.T) reflow.Fileset {
	switch t.Kind {
	case types.FileKind:
		return fileToFileset(v.(reflow.File))
	case types.DirKind:
		return dirToFileset(v.(values.Dir))
	case types.ListKind:
		list := v.(values.List)
		fs := reflow.Fileset{List: make([]reflow.Fileset, len(list))}
		for i := range list {
			fs.List[i] = coerceToFileset(t.Elem, list[i])
		}
		return fs
	default:
		panic("invalid input type")
	}
}

var coerceFlowToFilesetDigest = reflow.Digester.FromString("grail.com/reflow/syntax.coerceFlowToFileset")

func coerceFlowToFileset(t *types.T, f *flow.Flow) *flow.Flow {
	return &flow.Flow{
		Op:         flow.Coerce,
		Deps:       []*flow.Flow{f},
		FlowDigest: coerceFlowToFilesetDigest,
		Coerce: func(v values.T) (values.T, error) {
			return coerceToFileset(t, v), nil
		},
	}
}
