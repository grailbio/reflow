// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/repository/file"
	"grail.com/testutil"
	"grail.com/testutil/s3test"
)

type staticS3client struct {
	Client s3iface.S3API
}

func (c *staticS3client) New(user *aws.Config) s3iface.S3API {
	return c.Client
}

func newS3Test(t *testing.T, bucket, prefix string) (s3 *s3Exec, client *s3test.Client, repo *file.Repository, cleanup func()) {
	var dir string
	dir, cleanup = testutil.TempDir(t, "", "s3test")
	repo = &file.Repository{Root: filepath.Join(dir, "repo")}
	client = s3test.NewClient(t, bucket)
	client.Region = "us-west-2"
	s3 = &s3Exec{
		S3Client:    &staticS3client{client},
		Repository:  repo,
		Root:        filepath.Join(dir, "exec"),
		FileLimiter: limiter.New(),
		ExecID:      reflow.Digester.FromString("s3test"),
	}
	s3.staging.Root = filepath.Join(dir, "staging")
	s3.FileLimiter.Release(10)
	s3.Config = reflow.ExecConfig{
		Type: "intern",
		URL:  "s3://" + bucket + "/" + prefix,
	}
	s3.Init(nil)
	return
}

func TestS3ExecPrefix(t *testing.T) {
	const (
		bucket = "testbucket"
		prefix = "prefix/"
	)
	s3, client, repo, cleanup := newS3Test(t, bucket, prefix)
	defer cleanup()

	files := []string{"a", "a/b", "d", "d/e/f/g", "abcdefg"}
	val := reflow.Fileset{
		Map: map[string]reflow.File{},
	}
	for _, file := range files {
		client.SetFileContent(prefix+file, []byte(file))
		val.Map[file] = reflow.File{
			ID:   reflow.Digester.FromString(file),
			Size: int64(len(file)),
		}
	}

	ctx := context.Background()
	go s3.Go(ctx)
	if err := s3.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	inspect, err := s3.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := inspect.Error; err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if got, want := inspect.State, "complete"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	res2, err := s3.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := res2, (reflow.Result{Fileset: val}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if err := s3.Promote(ctx); err != nil {
		t.Fatal(err)
	}

	// Verify that everything is in the repository.
	for _, file := range val.Map {
		ok, err := repo.Contains(file.ID)
		if err != nil {
			t.Error(err)
		}
		if !ok {
			t.Errorf("repo is missing %v", file.ID)
		}
	}
}

func TestS3ExecPath(t *testing.T) {
	const (
		bucket   = "testbucket"
		key      = "somefile"
		contents = "file contents"
	)
	s3, client, repo, cleanup := newS3Test(t, bucket, key)
	defer cleanup()

	client.SetFileContent(key, []byte(contents))
	client.SetFileContent(key+"suffix", []byte(contents))
	client.SetFileContent("someotherfile", []byte("blah"))

	ctx := context.Background()
	go s3.Go(ctx)
	if err := s3.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	inspect, err := s3.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := inspect.Error; err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if got, want := inspect.State, "complete"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	got, err := s3.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s3.Promote(ctx); err != nil {
		t.Fatal(err)
	}
	want := reflow.Result{Fileset: reflow.Fileset{
		Map: map[string]reflow.File{
			".": reflow.File{ID: reflow.Digester.FromString(contents), Size: int64(len(contents))},
		},
	}}
	if !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	ok, err := repo.Contains(want.Fileset.Map["."].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Errorf("file is missing from repository")
	}
}
