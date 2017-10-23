package errors

import (
	"context"
	"encoding/json"
	"os"
	"testing"
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
