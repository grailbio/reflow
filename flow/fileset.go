// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import (
	"fmt"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/values"
)

// filesetFlow returns the Flow which evaluates to the constant Value v.
func filesetFlow(v reflow.Fileset) *Flow {
	return &Flow{Op: Val, Value: values.T(v), State: Done}
}

// ApplyAssertions applies Assertions to the given fileset (based on the exec config).
// For each file in the fileset appropriate assertions are applied based on the type of operation.
func ApplyAssertions(v *reflow.Fileset, cfg reflow.ExecConfig) {
	switch cfg.Type {
	case "intern":
		// When files are 'intern'ed, their properties will be set, but the corresponding
		// assertions are not applied by the executor, so we apply them here.
		v.Replace(func(f reflow.File) reflow.File {
			f.Assertions = blob.Assertions(f)
			return f
		})
	case "extern":
		// When files are 'extern'ed, the result fileset contains the same files as the input,
		// but are missing assertions.  So we apply them each file mapped by its digest.
		v.MapAssertionsByFile(cfg.Args[0].Fileset.Files())
	case "exec":
		// Execs are handled in flow.Eval.Mutate
	default:
		panic(fmt.Sprintf("unknown execconfig type %s", cfg.Type))
	}
}
