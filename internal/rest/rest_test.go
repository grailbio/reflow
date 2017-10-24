// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestEndToEnd(t *testing.T) {
	mux := Mux{
		"foo": WalkFunc(func(path string) Node {
			return DoFunc(func(ctx context.Context, call *Call) {
				if !call.Allow("GET") {
					return
				}
				call.Reply(http.StatusOK, path)
			})
		}),
		"bar": DoFunc(func(ctx context.Context, call *Call) {
			if !call.Allow("POST") {
				return
			}
			var m string
			if call.Unmarshal(&m) != nil {
				return
			}
			call.Reply(http.StatusOK, m)
		}),
	}

	srv := httptest.NewServer(Handler(mux, nil))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(nil, u, nil)
	ctx := context.Background()

	call := client.Call("GET", "foo/bar")
	code, err := call.Do(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := code, http.StatusOK; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	var m string
	if err := call.Unmarshal(&m); err != nil {
		t.Fatal(err)
	}
	if got, want := m, "bar"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	call.Close()

	call = client.Call("GET", "foos/bar")
	code, err = call.Do(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := code, http.StatusNotFound; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	call.Close()

	call = client.Call("POST", "bar")
	code, err = call.DoJSON(ctx, "hello, world!")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := code, http.StatusOK; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	if err := call.Unmarshal(&m); err != nil {
		t.Fatal(err)
	}
	if got, want := m, "hello, world!"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	call.Close()
}
