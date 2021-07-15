// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package sched

import "github.com/grailbio/reflow"

func Requirements(tasks []*Task) reflow.Requirements {
	return requirements(tasks)
}

func (t *Task) NonDirectTransfer() bool {
	return t.nonDirectTransfer
}

func (t *Task) WithRepo(repo reflow.Repository) *Task {
	t.Repository = repo
	return t
}
