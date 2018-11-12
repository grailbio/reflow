// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package s3walker

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/grailbio/base/admit"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/testutil/s3test"
)

const bucket = "test"

func checkScan(t *testing.T, w *S3Walker, want []string) {
	t.Helper()
	var got []string
	for w.Scan(context.Background()) {
		got = append(got, aws.StringValue(w.Object().Key))
	}
	if err := w.Err(); err != nil {
		t.Error(err)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func setup(t *testing.T) (client *s3test.Client, want []string) {
	t.Helper()
	client = s3test.NewClient(t, bucket)
	want = []string{"test/x", "test/y", "test/z/foobar"}
	keys := append([]string{"unrelated"}, want...)
	for _, key := range keys {
		client.SetFile(key, []byte(key), "unused")
	}
	return
}

func TestS3Walker(t *testing.T) {
	client, want := setup(t)
	w := &S3Walker{S3: client, Bucket: bucket, Prefix: "test/"}
	checkScan(t, w, want)
}

func TestS3WalkerWithPolicy(t *testing.T) {
	rp := retry.MaxTries(retry.Backoff(100*time.Millisecond, time.Minute, 1.5), 1)
	policy := admit.ControllerWithRetry(10, 10, rp)
	client, want := setup(t)
	w := &S3Walker{S3: client, Bucket: bucket, Prefix: "test/", Policy: policy}
	if err := policy.Acquire(context.Background(), 10); err != nil {
		t.Errorf("acquire failed!")
	}
	ctx, _ := context.WithTimeout(context.Background(), time.Millisecond)
	// all tokens in use, so must get false
	if want, got := false, w.Scan(ctx); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	policy.Release(10, true)
	// Setup new S3Walker with same policy (previous will be in err state).
	w = &S3Walker{S3: client, Bucket: bucket, Prefix: "test/", Policy: policy}
	checkScan(t, w, want)
}

func TestS3WalkerFile(t *testing.T) {
	client := s3test.NewClient(t, bucket)
	const key = "path/to/a/file"
	client.SetFile(key, []byte("contents"), "unused")
	ctx := context.Background()
	w := &S3Walker{S3: client, Bucket: bucket, Prefix: key}
	var got []string
	for w.Scan(ctx) {
		got = append(got, aws.StringValue(w.Object().Key))
	}
	if err := w.Err(); err != nil {
		t.Error(err)
	}
	if want := []string{key}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
