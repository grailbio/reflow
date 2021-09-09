// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package flow

import (
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/values"
)

// filesetFlow returns the Flow which evaluates to the constant Value v.
func filesetFlow(v reflow.Fileset) *Flow {
	return &Flow{Op: Val, Value: values.T(v), State: Done}
}
