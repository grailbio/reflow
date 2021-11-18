// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package errors

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"

	baseerrors "github.com/grailbio/base/errors"
)

func roundtripJSON(in interface{}, out interface{}) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, out)
}

func TestMarshalKind(t *testing.T) {
	for k := Other; k < maxKind; k++ {
		var (
			e1 = E("op", "arg", k)
			e2 = new(Error)
		)
		if err := roundtripJSON(e1, e2); err != nil {
			t.Error(err)
			continue
		}
		if !Match(e1, e2) {
			t.Errorf("%v does not match %v", e1, e2)
		}
	}
}

func TestMarshalChain(t *testing.T) {
	var (
		e1 = E("op1", Timeout, E("op2", Temporary))
		e2 = new(Error)
	)
	if err := roundtripJSON(e1, e2); err != nil {
		t.Fatal(err)
	}
	if !Match(e1, e2) {
		t.Errorf("%v does not match %v", e1, e2)
	}
}

func TestMarshalOrdinary(t *testing.T) {
	var (
		underlying = New(`ordinary error /&#@$%"hello"`)
		e1         = E("op1", underlying)
		e2         = new(Error)
	)
	if err := roundtripJSON(e1, e2); err != nil {
		t.Fatal(err)
	}
	if !Match(e1, e2) {
		t.Errorf("%v does not match %v", e1, e2)
	}
}

func TestE(t *testing.T) {
	e := E("fetch", context.DeadlineExceeded)
	if got, want := e, E("fetch", Timeout); !Match(want, got) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Collapse errors
	e = E("fetch", Timeout, E("lookup", Timeout))
	if got, want := e, E("fetch", Timeout, E("lookup")); !Match(want, got) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestError(t *testing.T) {
	e := E("open", "x://google.com", NotSupported, New(`scheme "x" not recognized`))
	if got, want := e.Error(), `open x://google.com: operation not supported: scheme "x" not recognized`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	e = E("read", "/dev/null", E(NotAllowed))
	if got, want := e.Error(), "read /dev/null: access denied"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	e = E("read", "/dev/null", E("open", "/dev/null", NotAllowed, os.ErrPermission))
	if got, want := e.Error(), "read /dev/null: access denied:\n\topen /dev/null: permission denied"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestErrorUnsupportedArg(t *testing.T) {
	e := E("open", "x://google.com", 10, New(`scheme "x" not recognized`))
	if got, want := e.Error(), `open x://google.com illegal (int 10 from errors_test.go:100): scheme "x" not recognized`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

type isTemporary bool

func (t isTemporary) Error() string   { return "maybe a temporary error" }
func (t isTemporary) Temporary() bool { return bool(t) }

func TestIs(t *testing.T) {
	for kind := Other; kind < maxKind; kind++ {
		if got, want := Is(kind, E(kind)), kind != Other; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	for _, temp := range []bool{true, false} {
		if got, want := Is(Temporary, isTemporary(temp)), temp; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
	if got, want := Is(OOM, nil), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestRestartable(t *testing.T) {
	for _, tc := range []struct {
		err         error
		transient   bool
		restartable bool
	}{
		{New("some error"), false, false},
		{E(Timeout, "some timeout error"), true, true},
		{E(TooManyTries, "some too many tries error"), true, true},
		{E(Eval, E(Timeout, "some timeout error")), true, true},
		{E(Eval, E(TooManyTries, "some too many tries error")), true, true},
		{E(Eval, E(Integrity, "some integrity error")), false, false},
		{E(Fatal, E(Timeout, "some timeout error")), false, false},
		{E(Eval, E(OOM, "some oom error")), false, false},
		{E(OOM, "some oom error"), false, false},
		{E(NotAllowed, E(Timeout, "some timeout error")), false, false},
		{E(Net, "some network error"), false, true},
		{E(Eval, baseerrors.E(baseerrors.TooManyTries, "some too many tries error")), true, true},
	} {
		if got, want := Restartable(tc.err), tc.restartable; got != want {
			t.Errorf("Restartable(): got %v, want %v: for error %v", got, want, tc.err)
		}
		if got, want := Restartable(Recover(tc.err)), tc.restartable; got != want {
			t.Errorf("Restartable(Recover()): got %v, want %v: for error %v", got, want, tc.err)
		}
	}
}

func TestWithRetryableKinds(t *testing.T) {
	for _, tc := range []struct {
		name     string
		existing map[Kind]bool
		added    []Kind
		want     map[Kind]bool
	}{
		{
			name:     "no_existing",
			existing: map[Kind]bool{},
			added:    []Kind{NotExist},
			want:     map[Kind]bool{NotExist: true},
		},
		{
			name:     "with_existing",
			existing: map[Kind]bool{Unavailable: true},
			added:    []Kind{NotExist},
			want:     map[Kind]bool{Unavailable: true, NotExist: true},
		},
		{
			name:     "none_added",
			existing: map[Kind]bool{Unavailable: true},
			added:    []Kind{},
			want:     map[Kind]bool{Unavailable: true},
		},
		{
			name:     "duplicates_added",
			existing: map[Kind]bool{Unavailable: true},
			added:    []Kind{Unavailable, NotExist, TooManyTries, NotExist},
			want:     map[Kind]bool{Unavailable: true, NotExist: true, TooManyTries: true},
		},
		{
			name:     "empty",
			existing: map[Kind]bool{},
			added:    []Kind{},
			want:     map[Kind]bool{},
		},
		{
			name:     "nil",
			existing: nil,
			added:    nil,
			want:     map[Kind]bool{},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.existing != nil {
				ctx = context.WithValue(ctx, retryableErrorKey{}, tc.existing)
			}
			if tc.added != nil {
				ctx = WithRetryableKinds(ctx, tc.added...)
			}
			got := GetRetryableKinds(ctx)
			if len(got) != len(tc.want) {
				t.Errorf("got %s, want %s", got, getKeys(tc.want))
			}
			for _, k := range got {
				if !tc.want[k] {
					t.Errorf("got %s, want %s", got, getKeys(tc.want))
				}
			}
		})
	}
}

func TestMulti(t *testing.T) {
	for _, tc := range []struct {
		errs []error
		want error
	}{
		{nil, nil},
		{[]error{nil, nil}, nil},
		{[]error{nil, fmt.Errorf("only one error")}, fmt.Errorf("only one error")},
		{
			[]error{fmt.Errorf("first error"), nil, E("second", "error", fmt.Errorf("another"))},
			fmt.Errorf("[2 errs]: [first error], [second error: another]"),
		},
	} {
		var (
			m  Multi
			wg sync.WaitGroup
		)
		for _, err := range tc.errs {
			err := err
			wg.Add(1)
			go func() {
				defer wg.Done()
				m.Add(err)
			}()
		}
		wg.Wait()
		if want := tc.want; want == nil {
			if got := m.Combined(); got != want {
				t.Errorf("got %s, want %s", got, want)
			}
			continue
		} else {
			if got, want := m.Combined().Error(), tc.want.Error(); got != want {
				t.Errorf("got %s, want %s", got, want)
			}
		}
	}
}
