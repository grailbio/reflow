// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rest

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/grailbio/reflow/errors"
)

type errorNode struct {
	err error
}

func (errorNode) Walk(context.Context, *Call, string) Node {
	return nil
}

func (n errorNode) Do(_ context.Context, call *Call) {
	call.Error(n.err)
}

func TestError(t *testing.T) {
	err2 := errors.E("op1", errors.NotExist)
	mux := Mux{
		"err1": errorNode{errors.New("random error")},
		"err2": errorNode{err2},
	}
	h := Handler(mux, nil)

	r := httptest.NewRequest("GET", "/err1", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got, want := w.Result().StatusCode, http.StatusInternalServerError; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	e := new(errors.Error)
	if err := json.NewDecoder(w.Result().Body).Decode(e); err != nil {
		t.Fatal(err)
	}

	r = httptest.NewRequest("GET", "/err2", nil)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got, want := w.Result().StatusCode, http.StatusNotFound; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if err := json.NewDecoder(w.Result().Body).Decode(e); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(errors.NotExist, e) {
		t.Errorf("expected %v to be NotExist", e)
	}
	if got, want := e.Error(), "op1: resource does not exist"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
