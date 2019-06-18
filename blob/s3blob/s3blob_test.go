// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package s3blob

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/base/retry"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/s3test"
)

func content(s string) *testutil.ByteContent {
	return &testutil.ByteContent{Data: []byte(s)}
}

const (
	name        = "testbucket"
	errorbucket = "errorbucket"
)

var testKeys = map[string]*testutil.ByteContent{
	"test/x":        content("x"),
	"test/y":        content("y"),
	"test/z/foobar": content("foobar"),
	"unrelated":     content("unrelated"),
}

var errorKeys = map[string]error{
	"key_awscanceled":       awserr.New(request.CanceledErrorCode, "test", nil),
	"key_nosuchkey":         awserr.New(s3.ErrCodeNoSuchKey, "test", nil),
	"key_canceled":          context.Canceled,
	"key_deadlineexceeded":  context.DeadlineExceeded,
	"key_awsrequesttimeout": awserr.New("RequestTimeout", "test", nil),
}

func testFile(key string) reflow.File {
	return reflow.File{
		Size:   testKeys[key].Size(),
		Source: "s3://testbucket/" + key,
		ETag:   testKeys[key].Checksum(),
	}
}

func newTestBucket(t *testing.T) *Bucket {
	t.Helper()
	client := s3test.NewClient(t, name)
	client.Region = "us-west-2"
	bucket := NewBucket(name, client)
	for k, v := range testKeys {
		client.SetFileContentAt(k, v, "")
	}
	return bucket
}

func newErrorBucket(t *testing.T) *Bucket {
	client := s3test.NewClient(t, errorbucket)
	client.Region = "us-west-2"
	client.Err = func(api string, input interface{}) error {
		if api != "HeadObjectRequestWithContext" {
			return nil
		}
		if hoi, ok := input.(*s3.HeadObjectInput); ok {
			if *hoi.Bucket != errorbucket {
				return nil
			}
			err := errorKeys[*hoi.Key]
			return err
		}
		return nil
	}
	return NewBucket(errorbucket, client)
}

func TestSnapshot(t *testing.T) {
	bucket := newTestBucket(t)
	ctx := context.Background()

	_, err := bucket.Snapshot(ctx, "foobar")
	if !errors.Is(errors.NotExist, err) {
		t.Errorf("got %v, want NotExist", err)
	}

	fs, err := bucket.Snapshot(ctx, "blah/")
	if err != nil {
		t.Fatal(err)
	}
	if fs.N() != 0 {
		t.Errorf("expected empty fileset, got %v", fs)
	}

	fs, err = bucket.Snapshot(ctx, "test/z/foobar")
	if err != nil {
		t.Fatal(err)
	}
	expect := reflow.Fileset{
		Map: map[string]reflow.File{".": testFile("test/z/foobar")},
	}
	if got, want := fs, expect; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	expect.Map["foobar"] = expect.Map["."]
	delete(expect.Map, ".")
	fs, err = bucket.Snapshot(ctx, "test/z/")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := fs, expect; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestScanner(t *testing.T) {
	bucket := newTestBucket(t)
	ctx := context.Background()
	scan := bucket.Scan("test/")

	var got, want []string
	for scan.Scan(ctx) {
		got = append(got, scan.Key())
	}
	if err := scan.Err(); err != nil {
		t.Fatal(err)
	}
	for k := range testKeys {
		if strings.HasPrefix(k, "test/") {
			want = append(want, k)
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestGet(t *testing.T) {
	bucket := newTestBucket(t)
	ctx := context.Background()

	_, _, err := bucket.Get(ctx, "xyz", "")
	if !errors.Is(errors.NotExist, err) {
		t.Errorf("expected NotExist, got %v", err)
	}

	rc, file, err := bucket.Get(ctx, "test/x", "")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := file, testFile("test/x"); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	p, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Error(err)
	}
	if got, want := p, testKeys["test/x"].Data; bytes.Compare(got, want) != 0 {
		t.Errorf("got %v, want %v", got, want)
	}

	_, _, err = bucket.Get(ctx, "test/x", "random etag")
	if !errors.Is(errors.Precondition, err) {
		t.Errorf("expected Precondition, got %v", err)
	}
	_, _, err = bucket.Get(ctx, "test/x", testFile("test/x").ETag)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func TestPut(t *testing.T) {
	bucket := newTestBucket(t)
	ctx := context.Background()

	c := content("new content")
	if err := bucket.Put(ctx, "newkey", 0, bytes.NewReader(c.Data)); err != nil {
		t.Fatal(err)
	}
	rc, file, err := bucket.Get(ctx, "newkey", "")
	if err != nil {
		t.Fatal(err)
	}
	p, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := p, c.Data; bytes.Compare(got, want) != 0 {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := file.Size, c.Size(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	// ETag generation is technically opaque to us but the s3 test client
	// uses the content's MD5.
	if got, want := file.ETag, c.Checksum(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestDownload(t *testing.T) {
	bucket := newTestBucket(t)
	ctx := context.Background()

	b := aws.NewWriteAtBuffer(nil)
	_, err := bucket.Download(ctx, "notexist", "", 0, b)
	if !errors.Is(errors.NotExist, err) {
		t.Errorf("expected NotExist, got %v", err)
	}

	_, err = bucket.Download(ctx, "test/z/foobar", "", 0, b)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := b.Bytes(), testKeys["test/z/foobar"].Data; bytes.Compare(got, want) != 0 {
		t.Errorf("got %v, want %v", got, want)
	}

	_, err = bucket.Download(ctx, "test/z/foobar", "random etag", 0, b)
	if !errors.Is(errors.Precondition, err) {
		t.Errorf("expected Precondition, got %v", err)
	}
	_, err = bucket.Download(ctx, "test/z/foobar", testFile("test/z/foobar").ETag, 0, b)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func TestTimeoutPolicy(t *testing.T) {
	p := timeoutPolicy(minBPS)
	if got, want := timeout(p, 0), 60*time.Second; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := timeout(p, 1), 90*time.Second; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := timeout(p, 100), 180*time.Second; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	p = timeoutPolicy(100 * minBPS)
	if got, want := timeout(p, 0), 100*time.Second; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := timeout(p, 1), 150*time.Second; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := timeout(p, 100), 300*time.Second; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestFileErrors(t *testing.T) {
	bucket := newErrorBucket(t)
	bucket.retrier = retry.MaxTries(retry.Jitter(retry.Backoff(20*time.Millisecond, 100*time.Millisecond, 1.5), 0.25), defaultMaxRetries)
	ctx := context.Background()
	for _, tc := range []struct {
		key   string
		wantK errors.Kind
		wantE error
	}{
		{"key_nosuchkey", errors.NotExist, nil},
		{"key_deadlineexceeded", errors.Other, fmt.Errorf("s3blob.File errorbucket key_deadlineexceeded: gave up after 3 tries: too many tries")},
		{"key_awsrequesttimeout", errors.Other, fmt.Errorf("s3blob.File errorbucket key_awsrequesttimeout: gave up after 3 tries: too many tries")},
		{"key_canceled", errors.Canceled, nil},
		{"key_awscanceled", errors.Canceled, nil},
	} {
		_, got := bucket.File(ctx, tc.key)
		if got == nil {
			t.Errorf("want error, got none")
			continue
		}
		if tc.wantK != errors.Other {
			if !errors.Is(tc.wantK, got) {
				t.Errorf("want kind %v, got %v", tc.wantK, got)
			}
			continue
		}
		if got.Error() != tc.wantE.Error() {
			t.Errorf("got %v, want %v", got, tc.wantE)
		}
	}
}

func TestShouldRetry(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{awserr.New(request.CanceledErrorCode, "test", nil), false},
		{awserr.New(s3.ErrCodeNoSuchKey, "test", nil), false},
		{awserr.New("MultipartUpload", "test", context.Canceled), false},
		{awserr.New("MultipartUpload", "test", context.DeadlineExceeded), false},
		{aws.ErrMissingRegion, false},
		{aws.ErrMissingEndpoint, false},
		{context.Canceled, false},
		{context.DeadlineExceeded, true},
		{errors.E("test", errors.Temporary), true},
		{awserr.New("RequestTimeout", "test", nil), true},
	} {
		if got, want := retryable(tc.err), tc.want; got != want {
			t.Errorf("got %v, want %v: %v", got, want, tc.err)
		}
	}
}
