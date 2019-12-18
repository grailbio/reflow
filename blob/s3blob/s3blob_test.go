// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.
package s3blob

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/base/digest"

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
	"key_badrequest":        awserr.New("BadRequest", "test", nil),
	"key_canceled":          context.Canceled,
	"key_deadlineexceeded":  context.DeadlineExceeded,
	"key_awsrequesttimeout": awserr.New("RequestTimeout", "test", nil),
	"key_nestedEOFrequest":  awserr.New("MultipartUpload", "test", awserr.New("SerializationError", "test2", fmt.Errorf("unexpected EOF"))),
}

func testFile(key string, withContentHash bool) reflow.File {
	f := reflow.File{
		Size:   testKeys[key].Size(),
		Source: "s3://testbucket/" + key,
		ETag:   testKeys[key].Checksum(),
	}
	if withContentHash {
		f.ContentHash = reflow.Digester.FromBytes(testKeys[key].Data)
	}
	return f
}

func newTestBucket(t *testing.T) *Bucket {
	t.Helper()
	client := s3test.NewClient(t, name)
	client.Region = "us-west-2"
	bucket := NewBucket(name, client)
	for k, v := range testKeys {
		client.SetFileContentAt(k, v, reflow.Digester.FromBytes(v.Data).Hex())
	}
	return bucket
}

func newErrorBucket(t *testing.T) *Bucket {
	t.Helper()
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
			return errorKeys[*hoi.Key]
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
		Map: map[string]reflow.File{".": testFile("test/z/foobar", true)},
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
	if got, want := file, testFile("test/x", false); !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	p, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Close(); err != nil {
		t.Error(err)
	}
	if got, want := p, testKeys["test/x"].Data; !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	_, _, err = bucket.Get(ctx, "test/x", "random etag")
	if !errors.Is(errors.Precondition, err) {
		t.Errorf("expected Precondition, got %v", err)
	}
	_, _, err = bucket.Get(ctx, "test/x", testFile("test/x", false).ETag)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}
}

func checkObject(t *testing.T, bucket *Bucket, key string, c *testutil.ByteContent, d digest.Digest) {
	t.Helper()
	rc, file, err := bucket.Get(context.Background(), key, "")
	if err != nil {
		t.Fatal(err)
	}
	p, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := p, c.Data; !bytes.Equal(got, want) {
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
	if d.IsZero() && !file.ContentHash.IsZero() {
		t.Errorf("got non-zero content hash %v, want zero", file.ContentHash)
	}
	if !d.IsZero() {
		if got, want := file.ContentHash, d; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func TestPut(t *testing.T) {
	bucket := newTestBucket(t)
	ctx := context.Background()

	c := content("new content")
	if err := bucket.Put(ctx, "newkey", 0, bytes.NewReader(c.Data), ""); err != nil {
		t.Fatal(err)
	}
	checkObject(t, bucket, "newkey", c, digest.Digest{})
}

func TestPutWithContentHash(t *testing.T) {
	bucket := newTestBucket(t)
	ctx := context.Background()

	c := content("new content")
	d := reflow.Digester.FromBytes(c.Data)
	if err := bucket.Put(ctx, "newkey2", 0, bytes.NewReader(c.Data), d.Hex()); err != nil {
		t.Fatal(err)
	}
	checkObject(t, bucket, "newkey2", c, d)
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
	if got, want := b.Bytes(), testKeys["test/z/foobar"].Data; !bytes.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	_, err = bucket.Download(ctx, "test/z/foobar", "random etag", 0, b)
	if !errors.Is(errors.Precondition, err) {
		t.Errorf("expected Precondition, got %v", err)
	}
	_, err = bucket.Download(ctx, "test/z/foobar", testFile("test/z/foobar", false).ETag, 0, b)
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
	for _, tc := range []struct {
		key       string
		wantK     errors.Kind
		cancelCtx bool
		wantE     error
	}{
		{"key_nosuchkey", errors.NotExist, false, nil},
		{"key_deadlineexceeded", errors.Other, false, fmt.Errorf("s3blob.File errorbucket key_deadlineexceeded: gave up after 3 tries: too many tries")},
		{"key_awsrequesttimeout", errors.Other, false, fmt.Errorf("s3blob.File errorbucket key_awsrequesttimeout: gave up after 3 tries: too many tries")},
		{"key_canceled", errors.Canceled, true, nil},
		{"key_awscanceled", errors.Canceled, false, nil},
	} {
		ctx := context.Background()
		if tc.cancelCtx {
			var cancel context.CancelFunc
			ctx, cancel = context.WithCancel(context.Background())
			cancel()
		}
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
		{awserr.New("MultipartUpload", "test", awserr.New("RequestTimeout", "test2", nil)), true},
		{awserr.New("MultipartUpload", "test", awserr.New("SerializationError", "test2", fmt.Errorf("unexpected EOF"))), true},
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

func TestCopy(t *testing.T) {
	bucket := newTestBucket(t)
	ctx := context.Background()
	c := content("new content")
	d := reflow.Digester.FromBytes(c.Data)

	if err := bucket.Put(ctx, "src_no_hash", 0, bytes.NewReader(c.Data), ""); err != nil {
		t.Fatal(err)
	}
	checkObject(t, bucket, "src_no_hash", c, digest.Digest{})

	if err := bucket.Put(ctx, "src_with_hash", 0, bytes.NewReader(c.Data), d.Hex()); err != nil {
		t.Fatal(err)
	}
	checkObject(t, bucket, "src_with_hash", c, d)

	if err := bucket.Copy(ctx, "src_no_hash", "src_no_hash_to_dst_no_hash", ""); err != nil {
		t.Fatal(err)
	}
	checkObject(t, bucket, "src_no_hash_to_dst_no_hash", c, digest.Digest{})

	if err := bucket.Copy(ctx, "src_no_hash", "src_no_hash_to_dst_with_hash", d.Hex()); err != nil {
		t.Fatal(err)
	}
	checkObject(t, bucket, "src_no_hash_to_dst_with_hash", c, d)

	if err := bucket.Copy(ctx, "src_with_hash", "src_with_hash_to_dst1", ""); err != nil {
		t.Fatal(err)
	}
	checkObject(t, bucket, "src_with_hash_to_dst1", c, d)

	if err := bucket.Copy(ctx, "src_with_hash", "src_with_hash_to_dst2", "something_else"); err != nil {
		t.Fatal(err)
	}
	checkObject(t, bucket, "src_with_hash_to_dst2", c, d)
}

// failN returns true n times when fail() is called and then returns false, until its reset.
type failN struct {
	n, i int
}

func (p *failN) fail() bool {
	if p.i < p.n {
		p.i++
		return true
	}
	return false
}

func (p *failN) reset() {
	p.i = 0
}

func newCopyErrorBucket(t *testing.T, fn *failN) *Bucket {
	t.Helper()
	client := s3test.NewClient(t, errorbucket)
	client.Region = "us-west-2"
	client.Err = func(api string, input interface{}) error {
		switch api {
		case "UploadPartCopyWithContext":
			if upc, ok := input.(*s3.UploadPartCopyInput); ok {
				// Possibly fail the first part with an error based on the key
				if *upc.PartNumber == int64(1) && fn.fail() {
					return errorKeys[*upc.Key]
				}
			}
		}
		return nil
	}
	bucket := NewBucket(errorbucket, client)
	bucket.retrier = retry.MaxTries(retry.Jitter(retry.Backoff(20*time.Millisecond, 100*time.Millisecond, 1.5), 0.25), defaultMaxRetries)
	return bucket
}

func TestCopyMultipart(t *testing.T) {
	bctx := context.Background()
	testBucket := newTestBucket(t)
	fn2, fnMax := &failN{n: 2}, &failN{n: defaultMaxRetries + 1}
	errorBucket := newCopyErrorBucket(t, fn2)
	failMaxBucket := newCopyErrorBucket(t, fnMax)
	for _, tc := range []struct {
		bucket                 *Bucket
		dstKey                 string
		size, limit, partsize  int64
		useShortCtx, cancelCtx bool
		wantErr                bool
	}{
		// 100KiB of data, multi-part limit 50KiB, part size 10KiB
		{testBucket, "dst1", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		// 50KiB of data, multi-part limit 50KiB, part size 10KiB
		{testBucket, "dst2", 50 << 10, 50 << 10, 10 << 10, false, false, false},
		{testBucket, "dst3", 100 << 10, 50 << 10, 10 << 10, true, false, true},
		{testBucket, "dst4", 100 << 10, 50 << 10, 10 << 10, false, true, true},
		{errorBucket, "key_badrequest", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		{errorBucket, "key_deadlineexceeded", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		{errorBucket, "key_awsrequesttimeout", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		{errorBucket, "key_nestedEOFrequest", 100 << 10, 50 << 10, 10 << 10, false, false, false},
		{errorBucket, "key_canceled", 100 << 10, 50 << 10, 10 << 10, false, false, true},
		{failMaxBucket, "key_badrequest", 100 << 10, 50 << 10, 10 << 10, false, false, true},
	} {
		fn2.reset()
		fnMax.reset()
		b := make([]byte, tc.size)
		tc.bucket.s3ObjectCopySizeLimit = tc.limit
		tc.bucket.s3MultipartCopyPartSize = tc.partsize
		if _, err := rand.Read(b); err != nil {
			t.Fatal(err)
		}
		c := &testutil.ByteContent{Data: b}
		d := reflow.Digester.FromBytes(b)
		if err := tc.bucket.Put(bctx, "src", 0, bytes.NewReader(c.Data), d.Hex()); err != nil {
			t.Fatal(err)
		}
		checkObject(t, tc.bucket, "src", c, d)
		ctx := bctx
		var cancel context.CancelFunc
		if tc.useShortCtx {
			ctx, cancel = context.WithTimeout(bctx, 10*time.Nanosecond)
		} else if tc.cancelCtx {
			ctx, cancel = context.WithCancel(bctx)
			cancel()
		}
		err := tc.bucket.Copy(ctx, "src", tc.dstKey, "")
		if cancel != nil {
			cancel()
		}
		if tc.wantErr {
			if err == nil {
				t.Errorf("%s got no error, want error", tc.dstKey)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		checkObject(t, tc.bucket, tc.dstKey, c, d)
		if t.Failed() {
			t.Logf("case: %v", tc)
		}
	}
}
