// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package taskdb

import (
	"context"
	"testing"
	"time"

	"github.com/grailbio/base/retry"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
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

func TestImgCmdID(t *testing.T) {
	var (
		image    = "testimage"
		cmd      = "testcmd"
		imgCmdID = NewImgCmdID(image, cmd)
		validID  = "sha256:bfc35fa41b55e036e8303eeaff9824b8880629114303d1c86e2137694f08b8b5"
	)
	if !imgCmdID.IsValid() {
		t.Fatalf("id invalid")
	}
	if got, want := imgCmdID.ID(), validID; got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

type mockTaskDB struct {
	TaskDB
	keepRunAliveErrs []error
}

func (db *mockTaskDB) KeepRunAlive(ctx context.Context, id RunID, keepalive time.Time) error {
	var current error
	current, db.keepRunAliveErrs = db.keepRunAliveErrs[0], db.keepRunAliveErrs[1:]
	return current
}

func TestKeepRunAlive(t *testing.T) {
	keepaliveInterval = time.Microsecond
	wait = time.Nanosecond
	ivOffset = time.Nanosecond
	keepaliveTries = 2
	policy = retry.Backoff(time.Microsecond, time.Microsecond, 1)

	for _, test := range []struct {
		name            string
		db              TaskDB
		expectedErrKind errors.Kind
	}{
		{
			"fatal",
			&mockTaskDB{
				keepRunAliveErrs: []error{
					errors.E(errors.Fatal),
				},
			},
			errors.Fatal,
		},
		{
			"temporary_fatal",
			&mockTaskDB{
				keepRunAliveErrs: []error{
					errors.E(errors.Temporary),
					errors.E(errors.Fatal),
				},
			},
			errors.Fatal,
		},
		{
			"success_fatal",
			&mockTaskDB{
				keepRunAliveErrs: []error{
					nil,
					errors.E(errors.Fatal),
				},
			},
			errors.Fatal,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := KeepRunAlive(context.Background(), test.db, RunID{}); !errors.Is(test.expectedErrKind, err) {
				t.Errorf("expected KeepRunAlive to return errKind %s but got %s", test.expectedErrKind.String(), errors.Recover(err).Kind.String())
			}
		})
	}
}
