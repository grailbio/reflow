// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"path/filepath"
	"testing"

	"crypto/md5"
	"fmt"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository/filerepo"
	reflowtestutil "github.com/grailbio/reflow/test/testutil"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/s3test"
)

type testStore map[string]blob.Bucket

func (s testStore) Bucket(ctx context.Context, name string) (blob.Bucket, error) {
	bucket, ok := s[name]
	if !ok {
		return nil, errors.E("testStore.Bucket", name, errors.NotExist)
	}
	return bucket, nil
}

func newS3Test(t *testing.T, bucket, prefix string, transferType string) (exec *blobExec, client *s3test.Client, repo *filerepo.Repository, cleanup func()) {
	var dir string
	dir, cleanup = testutil.TempDir(t, "", "s3test")
	repo = &filerepo.Repository{Root: filepath.Join(dir, "repo")}
	client = s3test.NewClient(t, bucket)
	client.Region = "us-west-2"
	store := testStore{"testbucket": s3blob.NewBucket("testbucket", client)}
	exec = &blobExec{
		Blob:         blob.Mux{"s3": store},
		Repository:   repo,
		Root:         filepath.Join(dir, "exec"),
		ExecID:       reflow.Digester.FromString("s3test"),
		transferType: transferType,
	}
	if transferType == intern {
		exec.staging.Root = filepath.Join(dir, "staging")
	}
	exec.Config = reflow.ExecConfig{
		Type: transferType,
		URL:  "s3://" + bucket + "/" + prefix,
	}
	exec.Init(nil)
	return
}

func executeAndGetResult(ctx context.Context, t *testing.T, s3 *blobExec) reflow.Result {
	t.Helper()
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

	res, err := s3.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestS3ExecInternPrefix(t *testing.T) {
	const (
		bucket = "testbucket"
		prefix = "prefix/"
	)
	s3, client, repo, cleanup := newS3Test(t, bucket, prefix, intern)
	defer cleanup()

	files := []string{"a", "a/b", "d", "d/e/f/g", "abcdefg"}
	val := reflow.Fileset{
		Map: map[string]reflow.File{},
	}
	for _, file := range files {
		client.SetFile(prefix+file, []byte(file), "unused")
		// Get the file to access the LastModified which is set as time.Now() by the test client.
		fc, _ := client.GetFile(prefix + file)
		rf := reflow.File{
			ID:           reflow.Digester.FromString(file),
			Source:       fmt.Sprintf("s3://%s/%s%s", bucket, prefix, file),
			ETag:         fmt.Sprintf("%x", md5.Sum([]byte(file))),
			LastModified: fc.LastModified,
			Size:         int64(len(file)),
		}
		rf.Assertions = blob.Assertions(rf)
		val.Map[file] = rf
	}

	ctx := context.Background()
	res2 := executeAndGetResult(ctx, t, s3)

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

func TestS3ExecExternPrefix(t *testing.T) {
	const (
		bucket = "testbucket"
		prefix = "prefix/"
	)
	s3, client, repo, cleanup := newS3Test(t, bucket, prefix, extern)
	defer cleanup()

	files := []string{"a", "a/b", "d", "d/e/f/g", "abcdefg"}
	fileset := reflowtestutil.WriteFiles(repo, files...)
	s3.Config.Args = []reflow.Arg{{Fileset: &fileset}}

	ctx := context.Background()
	res := executeAndGetResult(ctx, t, s3)

	if got, want := res, (reflow.Result{Fileset: fileset}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Verify that everything is in the blob.
	for _, file := range files {
		content, ok := client.GetFile(prefix + file)
		if !ok {
			t.Errorf("blob is missing %v", file)
		}
		if content.Content.Size() != int64(len(file)) {
			t.Errorf("blob content mismatch %v", file)
		}
	}
}

func TestS3ExecExternFileFileset(t *testing.T) {
	const (
		bucket = "testbucket"
		prefix = "filename"
	)
	s3, client, repo, cleanup := newS3Test(t, bucket, prefix, extern)
	defer cleanup()

	contents := "abcdefg"
	file := reflowtestutil.WriteFile(repo, contents)
	fileset := reflow.Fileset{
		Map: map[string]reflow.File{
			".": file,
		},
	}
	s3.Config.Args = []reflow.Arg{{Fileset: &fileset}}

	ctx := context.Background()
	res := executeAndGetResult(ctx, t, s3)

	if got, want := res, (reflow.Result{Fileset: fileset}); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Verify that everything is in the blob.
	content, ok := client.GetFile(prefix)
	if !ok {
		t.Errorf("blob is missing %v, filename %v", file, prefix)
	}
	if content.Content.Size() != int64(len(contents)) {
		t.Errorf("blob content mismatch %v", file)
	}
}

func TestS3ExecPath(t *testing.T) {
	const (
		bucket   = "testbucket"
		key      = "somefile"
		contents = "file contents"
	)
	s3, client, repo, cleanup := newS3Test(t, bucket, key, intern)
	defer cleanup()

	client.SetFile(key, []byte(contents), "unused")
	client.SetFile(key+"suffix", []byte(contents), "unused")
	client.SetFile("someotherfile", []byte("blah"), "unused")

	ctx := context.Background()
	got := executeAndGetResult(ctx, t, s3)
	if err := s3.Promote(ctx); err != nil {
		t.Fatal(err)
	}

	// Get the file to access the LastModified which is set as time.Now() by the test client.
	fc, _ := client.GetFile(key)
	rf := reflow.File{
		ID:           reflow.Digester.FromString(contents),
		Source:       fmt.Sprintf("s3://%s/%s", bucket, key),
		ETag:         fmt.Sprintf("%x", md5.Sum([]byte(contents))),
		LastModified: fc.LastModified,
		Size:         int64(len(contents)),
	}
	rf.Assertions = blob.Assertions(rf)

	want := reflow.Result{Fileset: reflow.Fileset{
		Map: map[string]reflow.File{".": rf},
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
