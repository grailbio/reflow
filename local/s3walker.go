package local

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// s3Walker traverses s3 keys through a prefix scan.
type s3Walker struct {
	// S3 is the S3 client to be used.
	S3 s3iface.S3API
	// Bucket and Prefix name the location of the scan.
	Bucket, Prefix string

	object  *s3.Object
	objects []*s3.Object
	token   *string
	err     error
	done    bool
}

// Scan scans the next key; it returns false when no more keys can
// be scanned, or if there was an error.
func (w *s3Walker) Scan(ctx context.Context) bool {
	if w.err != nil {
		return false
	}
	w.err = ctx.Err()
	if w.err != nil {
		return false
	}
	if len(w.objects) > 0 {
		w.object, w.objects = w.objects[0], w.objects[1:]
		return true
	}
	if w.done {
		return false
	}
	req, res := w.S3.ListObjectsV2Request(&s3.ListObjectsV2Input{
		Bucket:            aws.String(w.Bucket),
		ContinuationToken: w.token,
		Prefix:            aws.String(w.Prefix),
	})
	req.HTTPRequest = req.HTTPRequest.WithContext(ctx)
	w.err = req.Send()
	if w.err != nil {
		return false
	}
	w.token = res.NextContinuationToken
	w.objects = res.Contents
	w.done = len(w.objects) == 0 || !aws.BoolValue(res.IsTruncated)
	return w.Scan(ctx)
}

// Err returns an error, if any.
func (w *s3Walker) Err() error {
	return w.err
}

// Object returns the last object that was scanned.
func (w *s3Walker) Object() *s3.Object {
	return w.object
}
