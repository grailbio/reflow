// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflow_test

import (
	"testing"
	"time"

	"docker.io/go-docker/api/types"
	"github.com/grailbio/reflow"
)

func TestRuntime(t *testing.T) {
	var (
		e = new(reflow.ExecInspect)
		b = new(types.ContainerJSONBase)
		d = new(types.ContainerJSON)
		s = new(types.ContainerState)
	)
	b.State = s
	d.ContainerJSONBase = b
	e.Docker = *d

	for _, tt := range []struct {
		sd, ed string
		want   time.Duration
	}{
		// End is around 0.01s before start. In this case, the runtime should be 0s.
		{"2019-05-30T22:09:34.945074271Z", "2019-05-30T22:09:34.910391085Z", time.Duration(0)},
		// End is 10 seconds after start, rounded to the nearest second.
		{"2019-05-30T22:09:34.945074271Z", "2019-05-30T22:09:44.910391085Z", 10 * time.Second},
		// End is 10 seconds after start, rounded to the nearest second. End is also missing a digit (8 decimals instead of 9).
		{"2019-05-30T22:09:34.945074271Z", "2019-05-30T22:09:44.91039105Z", 10 * time.Second},
	} {
		s.StartedAt, s.FinishedAt = tt.sd, tt.ed
		if got, want := e.Runtime().Round(time.Second), tt.want; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
