// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package resolver

import (
	"context"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

func TestMux(t *testing.T) {
	m := make(Mux)
	m.HandleFunc("test", func(ctx context.Context, url string) (reflow.Fileset, error) {
		return reflow.Fileset{Map: map[string]reflow.File{".": reflow.File{Source: url}}}, nil
	})

	ctx := context.Background()
	_, err := m.Resolve(ctx, "s3://testbucket/testpath")
	if !errors.Is(errors.NotSupported, err) {
		t.Errorf("expected NotSupported error, got %v", err)
	}

	fs, err := m.Resolve(ctx, "test://testx/testy")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fs.N(), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := fs.Map["."].Source, "test://testx/testy"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
