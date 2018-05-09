// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package local

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/walker"
	"github.com/grailbio/testutil"
)

const (
	dockerSocket = "/var/run/docker.sock"
	bashImage    = "yikaus/alpine-bash" // the default alpine image doesn't have Bash.
	// We put this in /tmp because it's one of the default locations
	// that are bindable from Docker for Mac.
	tmpDir = "/tmp"
)

func newTestExecutorOrSkip(t *testing.T, creds *credentials.Credentials) (*Executor, func()) {
	dir, cleanup := testutil.TempDir(t, tmpDir, "reflowtest")
	x := &Executor{
		Client:   newDockerClientOrSkip(t),
		Dir:      dir,
		AWSImage: "619867110810.dkr.ecr.us-west-2.amazonaws.com/awstool",
		AWSCreds: creds,
	}
	x.SetResources(reflow.Resources{
		"mem":  1 << 30,
		"cpu":  2,
		"disk": 1e10,
	})
	if err := x.Start(); err != nil {
		cleanup()
		t.Fatal(err)
	}
	cleanup = func() {}
	return x, cleanup
}

func TestExec(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	x, cleanup := newTestExecutorOrSkip(t, nil)
	defer cleanup()
	ctx := context.Background()
	id := reflow.Digester.FromString("hello world!")
	exec, err := x.Put(ctx, id, reflow.ExecConfig{
		Type:  "exec",
		Image: bashImage,
		Cmd:   "echo foobar > $tmp/x; cat $tmp/x > $out",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Give it some time to fetch the image, etc.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = exec.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res2 := reflow.Result{Fileset: reflow.Fileset{
		Map: map[string]reflow.File{".": {ID: reflow.Digester.FromString("foobar\n"), Size: 7}},
	}}
	if got, want := res, res2; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestLocalfile(t *testing.T) {
	x, cleanup := newTestExecutorOrSkip(t, nil)
	defer cleanup()
	dir, cleanupDir := testutil.TempDir(t, tmpDir, "files")
	defer cleanupDir()
	testutil.CreateDirectoryTree(t, dir, 3, 2, 2)
	id := reflow.Digester.FromString("TestLocalFile")

	ctx := context.Background()
	exec, err := x.Put(ctx, id, reflow.ExecConfig{
		Type: "intern",
		URL:  "localfile://" + dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = exec.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var w walker.Walker
	w.Init(dir)
	for w.Scan() {
		if w.Info().IsDir() {
			continue
		}
		p := w.Relpath()
		_, ok := res.Fileset.Map[p]
		if !ok {
			t.Errorf("missing file %q", p)
		}
		delete(res.Fileset.Map, p)
	}
	for p := range res.Fileset.Map {
		t.Errorf("extraneous file %q", p)
	}
}

// TestExecRestore simulates an executor crash & exec restore.
func TestExecRestore(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	x, cleanup := newTestExecutorOrSkip(t, nil)
	defer cleanup()
	ctx := context.Background()
	id := reflow.Digester.FromString("sleepy")
	exec, err := x.Put(ctx, id, reflow.ExecConfig{
		Type:  "exec",
		Image: bashImage,
		Cmd:   "sleep 2",
	})
	if err != nil {
		t.Fatal(err)
	}
	x.cancel()
	err = exec.Wait(ctx)
	if got, want := errors.Recover(err).Err, context.Canceled; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}

	// This resets the executor's state, as if it had started anew.
	if err := x.Start(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	exec, err = x.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := exec.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	res, err := exec.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Err != nil {
		t.Fatal(res.Err)
	}
}
