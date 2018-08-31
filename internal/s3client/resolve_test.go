// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package s3client

import (
	"context"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/s3test"
)

func TestResolve(t *testing.T) {
	const bucket = "testbucket"
	client := s3test.NewClient(t, bucket)
	client.Region = "us-west-2"
	resolver := Resolver(&Static{client})
	ctx := context.Background()

	_, err := resolver.Resolve(ctx, "s3://testbucket/foobar")
	if !errors.Is(errors.NotExist, err) {
		t.Errorf("got %v, want NotExist", err)
	}

	fs, err := resolver.Resolve(ctx, "s3://testbucket/")
	if err != nil {
		t.Fatal(err)
	}
	if fs.N() != 0 {
		t.Errorf("expected empty fileset, got %v", fs)
	}

	c := content("hello")
	client.SetFileContentAt("foo/bar/baz", c, "")
	fs, err = resolver.Resolve(ctx, "s3://testbucket/foo/bar/baz")
	if err != nil {
		t.Fatal(err)
	}
	expect := reflow.Fileset{
		Map: map[string]reflow.File{
			".": reflow.File{
				Size:   5,
				Source: "s3://testbucket/foo/bar/baz",
				ETag:   c.Checksum(),
			},
		},
	}
	if got, want := fs, expect; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	expect.Map["bar/baz"] = expect.Map["."]
	delete(expect.Map, ".")
	fs, err = resolver.Resolve(ctx, "s3://testbucket/foo/")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fs, expect; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	c = content("")
	client.SetFileContentAt("foo/biz/foo", c, "")

	expect.Map["biz/foo"] = reflow.File{
		Size:   0,
		Source: "s3://testbucket/foo/biz/foo",
		ETag:   c.Checksum(),
	}
	fs, err = resolver.Resolve(ctx, "s3://testbucket/foo/")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fs, expect; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func content(s string) testutil.ContentAt {
	return &testutil.ByteContent{Data: []byte(s)}
}
