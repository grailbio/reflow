// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package local

import (
	"bytes"
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/grailbio/reflow/repository/file"
	"github.com/grailbio/reflow/s3/s3client"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/s3test"
)

func TestS3Downloader(t *testing.T) {
	const bucket = "bucket"
	dir, cleanup := testutil.TempDir(t, "", "s3test")
	defer cleanup()
	repo := &file.Repository{Root: filepath.Join(dir, "repo")}
	client := s3test.NewClient(t, bucket)
	client.Region = "us-west-2"

	dl := newS3downloader(&s3client.Static{client}, nil, dir)
	var content testutil.ByteContent
	content.Set([]byte("abcdefg"))
	client.SetFileContentAt("blah", &content, "")

	ctx := context.Background()
	file, err := dl.Download(ctx, repo, bucket, "blah", content.Size(), content.Checksum())
	if err != nil {
		t.Fatal(err)
	}
	rc, err := repo.Get(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	p, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := p, content.Data; bytes.Compare(got, want) != 0 {
		t.Errorf("got %v, want %v", got, want)
	}
}
