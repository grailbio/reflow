// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package taskdb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/grailbio/base/digest"

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
	kaErrs     []error
	kaN        int
	endTimeErr error
	endTimeN   int
}

func (db *mockTaskDB) KeepRunAlive(ctx context.Context, id RunID, keepalive time.Time) error {
	return db.KeepIDAlive(ctx, digest.Digest(id), keepalive)
}

func (db *mockTaskDB) KeepIDAlive(ctx context.Context, id digest.Digest, keepalive time.Time) error {
	var current error
	if len(db.kaErrs) > 0 {
		current, db.kaErrs = db.kaErrs[0], db.kaErrs[1:]
	}
	db.kaN += 1
	return current
}

func (db *mockTaskDB) SetEndTime(ctx context.Context, id digest.Digest, end time.Time) (err error) {
	if db.endTimeN == 0 {
		err = db.endTimeErr
	}
	db.endTimeN += 1
	return
}

func TestKeepRunAlive(t *testing.T) {
	keepaliveInterval = time.Microsecond
	policy = retry.Backoff(time.Microsecond, time.Microsecond, 1)

	for _, test := range []struct {
		name            string
		db              *mockTaskDB
		expectedErrKind errors.Kind
	}{
		{
			"fatal",
			&mockTaskDB{
				kaErrs: []error{
					errors.E(errors.Fatal),
				},
			},
			errors.Fatal,
		},
		{
			"temporary_fatal",
			&mockTaskDB{
				kaErrs: []error{
					errors.E(errors.Temporary),
					errors.E(errors.Fatal),
				},
			},
			errors.Fatal,
		},
		{
			"success_fatal",
			&mockTaskDB{
				kaErrs: []error{
					nil,
					errors.E(errors.Fatal),
				},
			},
			errors.Fatal,
		},
	} {
		wantCount := len(test.db.kaErrs)
		if err := KeepRunAlive(context.Background(), test.db, RunID{}); !errors.Is(test.expectedErrKind, err) {
			t.Errorf("expected KeepRunAlive to return errKind %s but got %s", test.expectedErrKind.String(), errors.Recover(err).Kind.String())
		}
		if got, want := test.db.kaN, wantCount; got != want {
			t.Errorf("got %d want %d", got, want)
		}
	}
}

func TestKeepIDAliveAndEnd(t *testing.T) {
	keepaliveInterval = 2 * time.Millisecond
	policy = retry.Backoff(10*time.Microsecond, 100*time.Microsecond, 1.2)

	for _, test := range []struct {
		db           *mockTaskDB
		wantMinKaN   int
		wantEndTimeN int
		wantEndErr   error
	}{
		{
			&mockTaskDB{kaErrs: []error{errors.E(errors.Fatal)}},
			1,
			0,
			errors.E(errors.Fatal),
		},
		{
			&mockTaskDB{kaErrs: []error{errors.E(errors.Temporary)}},
			40,
			1,
			nil,
		},
		{
			&mockTaskDB{endTimeErr: errors.E(errors.Temporary)},
			40,
			2,
			nil,
		},
		{
			&mockTaskDB{endTimeErr: errors.E(errors.Fatal)},
			40,
			1,
			errors.E(errors.Fatal),
		},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if got := KeepIDAliveAndEnd(ctx, test.db, digest.Digest{}, 10*time.Millisecond); !reflect.DeepEqual(got, test.wantEndErr) {
			t.Errorf("got %v, want %v", got, test.wantEndErr)
		}
		if got, want := test.db.kaN, test.wantMinKaN; got < want {
			t.Errorf("got %d want >%d", got, want)
		}
		if got, want := test.db.endTimeN, test.wantEndTimeN; got != want {
			t.Errorf("got %d want %d", got, want)
		}
	}
}
