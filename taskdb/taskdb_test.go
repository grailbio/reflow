// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package taskdb

import (
	"testing"

	"github.com/grailbio/reflow"
)

func TestRunID(t *testing.T) {
	digest := reflow.Digester.Rand(nil)
	digestString := digest.String()
	digestStringHex4 := digest.HexN(4)
	runid := RunID(digest)
	if got, want := runid.ID(), digestString; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if got, want := runid.IDShort(), digestStringHex4; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if !runid.IsValid() {
		t.Errorf("runid %s invalid", runid.ID())
	}
	invalidDigest := RunID{}
	if invalidDigest.IsValid() {
		t.Errorf("runid %s valid", invalidDigest.ID())
	}
}

func TestTaskID(t *testing.T) {
	digest := reflow.Digester.Rand(nil)
	digestString := digest.String()
	digestStringHex4 := digest.HexN(4)
	taskid := TaskID(digest)
	if got, want := taskid.ID(), digestString; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if got, want := taskid.IDShort(), digestStringHex4; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if !taskid.IsValid() {
		t.Errorf("taskid %s invalid", taskid.ID())
	}
	invalidDigest := TaskID{}
	if invalidDigest.IsValid() {
		t.Errorf("taskid %s valid", invalidDigest.ID())
	}
}
