package s3test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"grail.com/testutil"
)

// awsContentSha256Key is the header used to store the sha256 of
// a file's content in the grail.com/pipeline.
const awsContentSha256Key = "Content-Sha256"

// Client implements s3iface.S3API by using an AWS SDK client and
// overriding methods under test: HeadObject, ListObjectsV2,
// PutObjectRequest, CreateMultipartUploadRequest, UploadPartRequest,
// AbortMultipartUploadRequest, CompleteMultipartUploadRequest,
// GetObjectRequest, CopyObject, and DeleteObject. (These methods are
// sufficient to use with the S3 upload and download managers.)
//
// File contents (and their checksums) are provided by the user.
type Client struct {
	// IgnoreMissingSha256 suppresses checks for the
	// content checksum header.
	IgnoreMissingSha256 bool

	// Region holds the region of the bucket returned by
	// GetBucketLocationRequest.
	Region string

	// NumMaxRetries configures the maximum number of retries permitted
	// for operations involving this client.
	NumMaxRetries int

	s3iface.S3API
	svc      s3iface.S3API
	bucket   string
	m        sync.Mutex
	content  map[string]testutil.ContentAt // maps s3 key to ContentAt.
	partial  map[int64][]byte              // maps part number to file content bytes
	sha256   map[string][]byte             // maps s3 key to file checksum bytes
	apiCount map[string]int                // maps the s3 api methods to occurence counts
	t        *testing.T
}

// NewClient constructs a new S3 client under test. The client
// reports errors to the given testing.T, and expects to receive
// requests for the given bucket.
func NewClient(t *testing.T, bucket string) *Client {
	// There are different ways of handling the XXXRequest vs XXX API methods.
	// - The XXX methods directly return a result so that's easy,
	//   just return a custom result.
	// - The XXXRequest methods as used by s3manager, return a request
	//   that s3manager tweaks and then calls its Send() method.  Here
	//   we subcontract the building of the request out to the real S3API
	//   implementation, get rid of its Handlers, patch up the output, and
	//   then insert a noop Send Handler
	svc := s3.New(session.New(), nil)
	svc.Handlers.Clear()
	return &Client{
		svc:      svc,
		bucket:   bucket,
		content:  make(map[string]testutil.ContentAt),
		partial:  make(map[int64][]byte),
		sha256:   make(map[string][]byte),
		apiCount: make(map[string]int),
		t:        t,
	}
}

// MaxRetries returns the maximum number of retries permitted for operations
// using this client.
func (c *Client) MaxRetries() int {
	return c.NumMaxRetries
}

// SetFileSha256 sets the sha256 for the given key.
func (c *Client) SetFileSha256(key string, sha256 []byte) {
	c.m.Lock()
	defer c.m.Unlock()
	c.sha256[key] = sha256
}

// GetFileSha256 returns the sha256 defined by SetFileSha256.
func (c *Client) GetFileSha256(key string) []byte {
	c.m.Lock()
	defer c.m.Unlock()
	return c.sha256[key]
}

// SetFileContent defines the body for key.
func (c *Client) SetFileContent(key string, content []byte) {
	c.m.Lock()
	defer c.m.Unlock()
	c.content[key] = &testutil.ByteContent{content}
}

// SetFileContentReader sets the underlying TestReader for content.
func (c *Client) SetFileContentAt(key string, content testutil.ContentAt) {
	c.m.Lock()
	defer c.m.Unlock()
	c.content[key] = content
}

// HasFileContent returns whether the given key was set by SetFileContent.
func (c *Client) HasFileContent(key string) bool {
	c.m.Lock()
	defer c.m.Unlock()
	_, ok := c.content[key]
	return ok
}

// GetFileContent returns ReaderAt defined by SetFileContent.
func (c *Client) GetFileContent(key string) testutil.ContentAt {
	c.m.Lock()
	defer c.m.Unlock()
	return c.content[key]
}

// GetFileContentBytes returns the byte slice representation of the contents for key.
func (c *Client) GetFileContentBytes(key string) []byte {
	c.m.Lock()
	defer c.m.Unlock()
	result := make([]byte, c.content[key].Size())
	c.content[key].ReadAt(result, 0)
	return result
}

// SetPartialContent defines the body for part.
func (c *Client) SetPartialContent(part int64, content []byte) {
	c.m.Lock()
	defer c.m.Unlock()
	c.partial[part] = content
}

// HasPartialContent returns whether the given part was set by SetPartialContent.
func (c *Client) HasPartialContent(part int64) bool {
	c.m.Lock()
	defer c.m.Unlock()
	_, ok := c.partial[part]
	return ok
}

// SetFileFromPartialContent collects the content from partial and sets key in content with the result.
func (c *Client) SetFileFromPartialContent(key string) {
	c.m.Lock()
	defer c.m.Unlock()
	size := 0
	max := int64(1)
	for i, b := range c.partial {
		size += len(b)
		if i > max {
			max = i
		}
	}
	buf := make([]byte, size)
	pos := 0
	for i := int64(1); i <= max; i++ {
		part, ok := c.partial[i]
		if !ok {
			c.t.Errorf("Missing part %d", i)
		} else {
			copy(buf[pos:], part)
			// buf[pos:pos+len(part)] = part
			pos += len(part)
			delete(c.partial, i)
		}
	}
	c.content[key] = &testutil.ByteContent{buf}
}

func (c *Client) copyFile(src, dst string) {
	c.m.Lock()
	defer c.m.Unlock()
	c.content[dst] = c.content[src]
	if _, ok := c.sha256[src]; ok {
		c.sha256[dst] = c.sha256[src]
	}
}

func (c *Client) deleteFile(key string) {
	c.m.Lock()
	defer c.m.Unlock()
	delete(c.content, key)
	delete(c.sha256, key)
}

func (c *Client) incApiCount(api string) {
	c.m.Lock()
	defer c.m.Unlock()
	c.apiCount[api]++
}

// GetApiCount returns the number of invocations for the given API
// GetApiCount returns call. counts only for methods that are under
// GetApiCount returns test.
func (c *Client) GetApiCount(api string) int {
	c.m.Lock()
	defer c.m.Unlock()
	return c.apiCount[api]
}

// HeadObject is used in s3-loader to determine if an object in S3 and
// the local matching object are identical.
func (c *Client) HeadObject(
	input *s3.HeadObjectInput) (output *s3.HeadObjectOutput, err error) {
	c.incApiCount("HeadObject")
	//c.t.Logf("HeadObject input: %v", input)
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("HeadObject received unexpected bucket got: %s want %s", got, want)
	}

	key := aws.StringValue(input.Key)
	if !c.HasFileContent(key) {
		return nil, awserr.New("NoSuchKey", "Object not found", nil)
	}
	b := c.GetFileContent(key)
	output = &s3.HeadObjectOutput{
		ContentLength: aws.Int64(b.Size()),
	}
	if b := c.GetFileSha256(key); len(b) > 0 {
		output.Metadata = map[string]*string{
			awsContentSha256Key: aws.String(string(b)),
		}
	}
	return output, nil
}

func (c *Client) HeadObjectRequest(input *s3.HeadObjectInput) (req *request.Request, out *s3.HeadObjectOutput) {
	c.incApiCount("HeadObjectRequest")
	var err error
	req, out = c.svc.HeadObjectRequest(input)
	out1, err := c.HeadObject(input)
	if err != nil {
		panic(err)
	}
	*out = *out1
	req.Handlers.Send.Clear()
	req.Handlers.Clear()
	return
}

// ListObjectsV2 is used by DownloadDirTree to detemine all the files
// to download.
func (c *Client) ListObjectsV2(
	input *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	c.incApiCount("ListObjectsV2")
	// c.t.Logf("ListObjectsV2 input: %v", input)
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("ListObjectsV2 received unexpected bucket got: %s want %s", got, want)
	}
	prefix := aws.StringValue(input.Prefix)
	output := &s3.ListObjectsV2Output{}

	c.m.Lock()
	defer c.m.Unlock()
	for key, content := range c.content {
		if strings.HasPrefix(key, prefix) {
			object := s3.Object{Key: aws.String(key), Size: aws.Int64(content.Size())}
			output.Contents = append(output.Contents, &object)
		}
	}
	return output, nil
}

// ListObjectsV2Request implements the request variant of ListObjectsV2.
func (c *Client) ListObjectsV2Request(
	input *s3.ListObjectsV2Input) (req *request.Request, output *s3.ListObjectsV2Output) {
	c.incApiCount("ListObjectsV2Request")
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("ListObjectsV2 received unexpected bucket got: %s want %s", got, want)
	}
	req, output = c.svc.ListObjectsV2Request(input)

	outputp, err := c.ListObjectsV2(input)
	if err != nil {
		panic("unexpected error: " + err.Error())
	}
	*output = *outputp
	return
}

// PutObjectRequest is used within s3manager to upload single part files.
func (c *Client) PutObjectRequest(
	input *s3.PutObjectInput) (req *request.Request, output *s3.PutObjectOutput) {
	c.incApiCount("PutObjectRequest")
	// c.t.Logf("PutObjectRequest input: %v", input)
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("PutObjectRequest received unexpected bucket got: %s want %s", got, want)
	}
	req, output = c.svc.PutObjectRequest(input)

	key := aws.StringValue(input.Key)
	if b, err := ioutil.ReadAll(input.Body); err != nil {
		c.t.Errorf("PutObjectRequest when reading input.Body: %s", err)
	} else {
		c.SetFileContent(key, b)
	}
	if sha256, ok := input.Metadata[awsContentSha256Key]; ok {
		c.SetFileSha256(key, []byte(aws.StringValue(sha256)))
	}
	req.Handlers.Send.PushBack(func(r *request.Request) {
		// c.t.Logf("put: %T\n", r.Params)
	})
	return
}

// CreateMultipartUploadWithContext stubs the corresponding s3iface.API method.
func (c *Client) CreateMultipartUploadWithContext(
	ctx aws.Context, input *s3.CreateMultipartUploadInput, opts ...request.Option) (
	*s3.CreateMultipartUploadOutput, error) {
	name := "CreateMultipartUploadWithContext"
	c.incApiCount(name)
	req, out := c.CreateMultipartUploadRequest(input)
	req.SetContext(ctx)
	req.ApplyOptions(opts...)
	return out, req.Send()
}

// UploadPartWithContext stubs the corresponding s3iface.API method.
func (c *Client) UploadPartWithContext(
	ctx aws.Context, input *s3.UploadPartInput, opts ...request.Option) (
	*s3.UploadPartOutput, error) {
	name := "UploadPartWithContext"
	c.incApiCount(name)
	req, out := c.UploadPartRequest(input)
	req.Handlers.Unmarshal.Clear()
	req.SetContext(ctx)
	req.ApplyOptions(opts...)
	return out, req.Send()
}

// UploadPartCopyWithContext stubs the corresponding s3iface.API method.
func (c *Client) UploadPartCopyWithContext(
	ctx aws.Context, input *s3.UploadPartCopyInput, opts ...request.Option) (
	*s3.UploadPartCopyOutput, error) {
	name := "UploadPartCopyWithContext"
	c.incApiCount(name)
	req, out := c.UploadPartCopyRequest(input)
	req.Handlers.Unmarshal.Clear()
	req.SetContext(ctx)
	req.ApplyOptions(opts...)
	return out, req.Send()
}

// CompleteMultipartUploadWithContext stubs the corresponding s3iface.API method.
func (c *Client) CompleteMultipartUploadWithContext(
	ctx aws.Context, input *s3.CompleteMultipartUploadInput, opts ...request.Option) (
	*s3.CompleteMultipartUploadOutput, error) {
	name := "CompleteMultipartUploadWithContext"
	c.incApiCount(name)
	req, out := c.CompleteMultipartUploadRequest(input)
	req.Handlers.Unmarshal.Clear()
	req.SetContext(ctx)
	req.ApplyOptions(opts...)
	return out, req.Send()
}

// CreateMultipartUploadRequest stubs the corresponding s3iface.API method.
func (c *Client) CreateMultipartUploadRequest(
	input *s3.CreateMultipartUploadInput) (req *request.Request, output *s3.CreateMultipartUploadOutput) {
	name := "CreateMultipartUploadRequest"
	c.incApiCount(name)
	// c.t.Logf("%s input: %v", name, input)
	key := aws.StringValue(input.Key)
	req, output = c.svc.CreateMultipartUploadRequest(input)
	if sha256, ok := input.Metadata[awsContentSha256Key]; ok {
		c.SetFileSha256(key, []byte(aws.StringValue(sha256)))
	}
	output.SetUploadId("Id42")
	req.Handlers.Send.PushBack(func(r *request.Request) {
		// c.t.Logf("%s: %T\n", name r.Params)
	})
	return req, output
}

// UploadPartRequest stubs the corresponding s3iface.API method.
func (c *Client) UploadPartRequest(
	input *s3.UploadPartInput) (req *request.Request, output *s3.UploadPartOutput) {
	name := "UploadPartRequest"
	c.incApiCount(name)
	// c.t.Logf("%s input: %v", name, input)
	req, output = c.svc.UploadPartRequest(input)
	output.SetETag("etag")
	if b, err := ioutil.ReadAll(input.Body); err != nil {
		c.t.Errorf("UploadPartRequest when reading input.Body: %s", err)
	} else {
		c.SetPartialContent(aws.Int64Value(input.PartNumber), b)
	}
	req.Handlers.Send.PushBack(func(r *request.Request) {
		// c.t.Logf("%s: %T\n", name, r.Params)
	})
	return req, output
}

// UploadPartCopyRequest stubs the corresponding s3iface.API method.
func (c *Client) UploadPartCopyRequest(
	input *s3.UploadPartCopyInput) (req *request.Request, output *s3.UploadPartCopyOutput) {
	name := "UploadPartCopyRequest"
	c.incApiCount(name)
	// c.t.Logf("%s input: %v", name, input)
	req, output = c.svc.UploadPartCopyRequest(input)
	source, err := Decode(aws.StringValue(input.CopySource))
	if err != nil {
		c.t.Errorf("UploadPartCopyRequest could not unescape CopySource: %s", aws.StringValue(input.CopySource))
	}
	if !strings.HasPrefix(source, c.bucket+"/") {
		c.t.Errorf("UploadPartCopyRequest expected copy source from the same bucket, got: %v", source)
	}
	src := strings.TrimPrefix(source, c.bucket+"/")
	b := c.GetFileContent(src)
	start := int64(0)
	last := b.Size() - 1
	if input.CopySourceRange != nil {
		both := strings.TrimPrefix(aws.StringValue(input.CopySourceRange), "bytes=")
		parts := strings.Split(both, "-")
		var err error
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			start = 0
			c.t.Errorf("UploadPartCopyRequest could not parse start from: %s", aws.StringValue(input.CopySourceRange))
		}
		last, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			last = b.Size() - 1
			c.t.Errorf("UploadPartCopyRequest could not parse start from: %s", aws.StringValue(input.CopySourceRange))
		}
	}

	data := make([]byte, last-start+1)
	if _, err := b.ReadAt(data, start); err != nil {
		c.t.Fatal(err)
	}
	c.SetPartialContent(aws.Int64Value(input.PartNumber), data)
	output.SetCopyPartResult(&s3.CopyPartResult{
		ETag: aws.String("etag"),
	})
	req.Handlers.Send.PushBack(func(r *request.Request) {
		// c.t.Logf("%s: %T\n", name, r.Params)
	})
	return req, output
}

// AbortMultipartUploadRequest stubs the corresponding s3iface.API method.
func (c *Client) AbortMultipartUploadRequest(
	input *s3.AbortMultipartUploadInput) (req *request.Request, output *s3.AbortMultipartUploadOutput) {
	name := "AbortMultipartUploadRequest"
	c.incApiCount(name)
	// c.t.Logf("%s input: %v", name, input)
	req, output = c.svc.AbortMultipartUploadRequest(input)
	req.Handlers.Send.PushBack(func(r *request.Request) {
		// c.t.Logf("%s: %T\n", name, r.Params)
	})
	return req, output
}

// CompleteMultipartUploadRequest stubs the corresponding s3iface.API method.
func (c *Client) CompleteMultipartUploadRequest(
	input *s3.CompleteMultipartUploadInput) (req *request.Request, output *s3.CompleteMultipartUploadOutput) {
	name := "CompleteMultipartUploadRequest"
	c.incApiCount(name)
	// c.t.Logf("%s input: %v", name, input)
	req, output = c.svc.CompleteMultipartUploadRequest(input)
	key := aws.StringValue(input.Key)
	c.SetFileFromPartialContent(key)
	req.Handlers.Send.PushBack(func(r *request.Request) {
		// c.t.Logf("%s: %T\n", name, r.Params)
	})
	return req, output
}

// GetObjectRequest is used by by s3manager (aws-sdk < 1.8.0) to downoad files.
// GetObjectRequest is used by GetObjectWithContext by s3manager (aws-sdk >= 1.8.0) to downoad files.
func (c *Client) GetObjectRequest(
	input *s3.GetObjectInput) (req *request.Request, output *s3.GetObjectOutput) {
	c.incApiCount("GetObjectRequest")
	// c.t.Logf("GetObjectRequest input: %v", input)
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("GetObjectRequest received unexpected bucket got: %s want %s", got, want)
	}
	req, output = c.svc.GetObjectRequest(input)

	key := aws.StringValue(input.Key)
	start := int64(0)
	last := int64(-1)
	if input.Range != nil {
		both := strings.TrimPrefix(aws.StringValue(input.Range), "bytes=")
		parts := strings.Split(both, "-")
		var err error
		start, err = strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			start = 0
			c.t.Errorf("GetObjectRequest could not parse start from: %s", aws.StringValue(input.Range))
		}
		last, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			last = -1
			c.t.Errorf("GetObjectRequest could not parse start from: %s", aws.StringValue(input.Range))
		}
	}
	if !c.HasFileContent(key) {
		c.t.Logf("GetObjectRequest no file content for: %s", key)
		output.Body = ioutil.NopCloser(bytes.NewReader(make([]byte, 0)))
		output.ContentLength = aws.Int64(0)
	} else {
		b := c.GetFileContent(key)
		if last == -1 || (last+1) >= b.Size() {
			output.Body = ioutil.NopCloser(io.NewSectionReader(b, start, b.Size()-start))
			if start > 0 {
				last = b.Size() - 1
				output.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, last, b.Size()))
			}
			output.ContentLength = aws.Int64(b.Size() - start)
		} else {
			output.Body = ioutil.NopCloser(io.NewSectionReader(b, start, last-start+1))
			output.ContentRange = aws.String(fmt.Sprintf("bytes %d-%d/%d", start, last, b.Size()))
			output.ContentLength = aws.Int64(last - start + 1)
		}
	}
	// c.t.Logf("GetObjectRequest output: %v", output)
	req.Handlers.Send.PushBack(func(r *request.Request) {
		// c.t.Logf("get params: %T\n", r.Params)
	})
	return
}

// CopyObjectRequest implements the Request model of server side object copying.
func (c *Client) CopyObjectRequest(
	input *s3.CopyObjectInput) (req *request.Request, output *s3.CopyObjectOutput) {
	c.incApiCount("CopyObjectRequest")
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("CopyObject received unexpected bucket got: %s want %s", got, want)
	}
	req, output = c.svc.CopyObjectRequest(input)
	req.Handlers.Unmarshal.Clear()

	// c.t.Logf("CopyObjectRequest input: %v", *input)
	source, err := Decode(aws.StringValue(input.CopySource))
	if err != nil {
		c.t.Errorf("UploadPartCopyRequest could not unescape CopySource: %s", aws.StringValue(input.CopySource))
	}
	if !strings.HasPrefix(source, c.bucket+"/") {
		c.t.Errorf("CopyObject expected copy source from the same bucket, got: %v", source)
	}
	src, dst := strings.TrimPrefix(source, c.bucket+"/"), aws.StringValue(input.Key)
	c.copyFile(src, dst)
	if sha256, ok := input.Metadata[awsContentSha256Key]; ok {
		c.SetFileSha256(dst, []byte(aws.StringValue(sha256)))
	}
	req.Handlers.Send.PushBack(func(r *request.Request) {
		// c.t.Logf("get params: %T\n", r.Params)
	})
	return
}

// CopyObject implements S3-side object copying.
func (c *Client) CopyObject(input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
	c.incApiCount("CopyObject")
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("CopyObject received unexpected bucket got: %s want %s", got, want)
	}
	// c.t.Logf("CopyObject input: %v", *input)
	source, err := Decode(aws.StringValue(input.CopySource))
	if err != nil {
		c.t.Errorf("UploadPartCopyRequest could not unescape CopySource: %s", aws.StringValue(input.CopySource))
	}
	if !strings.HasPrefix(source, c.bucket+"/") {
		c.t.Errorf("CopyObject expected copy source from the same bucket, got: %v", source)
	}
	src, dst := strings.TrimPrefix(source, c.bucket+"/"), aws.StringValue(input.Key)
	c.copyFile(src, dst)
	if sha256, ok := input.Metadata[awsContentSha256Key]; ok {
		c.SetFileSha256(dst, []byte(aws.StringValue(sha256)))
	}
	return &s3.CopyObjectOutput{}, nil
}

// DeleteObject removes an object from the bucket.
func (c *Client) DeleteObject(input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	c.incApiCount("DeleteObject")
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("DeleteObject received unexpected bucket got: %s want %s", got, want)
	}
	key := aws.StringValue(input.Key)
	c.deleteFile(key)
	return &s3.DeleteObjectOutput{}, nil
}

// GetObject retrieves an object from the bucket.
func (c *Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	c.incApiCount("GetObject")
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("GetObject received unexpected bucket got: %s want %s", got, want)
	}

	output := s3.GetObjectOutput{}
	key := aws.StringValue(input.Key)
	if !c.HasFileContent(key) {
		c.t.Logf("GetObject no file content for: %s", key)
		output.Body = ioutil.NopCloser(bytes.NewReader(make([]byte, 0)))
		output.ContentLength = aws.Int64(0)
	} else {
		b := c.GetFileContent(key)
		output.Body = ioutil.NopCloser(io.NewSectionReader(b, 0, b.Size()))
		output.ContentLength = aws.Int64(b.Size())
	}

	return &output, nil
}

// GetObjectWithContext is used within s3manager (aws-sdk >= 1.8.0) to downoad files,
// we leverage GetObjectRequest (from above) internally to do the work.
func (c *Client) GetObjectWithContext(
	ctx aws.Context, input *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {

	c.incApiCount("GetObjectWithContext")

	// This implementation taken from svc.GetObjectWithContext()
	req, out := c.GetObjectRequest(input)
	req.SetContext(ctx)
	req.ApplyOptions(opts...)
	return out, req.Send()
}

// GetBucketLocationRequest implements the bucket location (Client.Region)
// request.
func (c *Client) GetBucketLocationRequest(input *s3.GetBucketLocationInput) (req *request.Request, output *s3.GetBucketLocationOutput) {
	c.incApiCount("GetBucketLocationRequest")
	if got, want := aws.StringValue(input.Bucket), c.bucket; got != want {
		c.t.Errorf("GetBucketLocationRequest received unexpected bucket got: %s want %s", got, want)
	}
	req, output = c.svc.GetBucketLocationRequest(input)
	output.SetLocationConstraint(c.Region)
	req.Handlers.Send.Clear()
	req.Handlers.Clear()
	return
}

// PutObjectAcl sets the ACL of an object already in the bucket.
func (c *Client) PutObjectAcl(input *s3.PutObjectAclInput) (*s3.PutObjectAclOutput, error) {
	c.incApiCount("PutObjectAcl")
	output := s3.PutObjectAclOutput{}
	return &output, nil
}
