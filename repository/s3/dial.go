package s3

import (
	"net/url"
	"sync"

	amzs3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/repository"
)

var (
	mu            sync.Mutex
	clients       = map[string]*amzs3.S3{}
	defaultClient *amzs3.S3
)

func init() {
	repository.RegisterScheme("s3r", Dial)
}

// SetClient sets the default s3 client to use for dialling repositories.
// If non-nil, it is used when there is not a more specific per-bucket client.
func SetClient(client *amzs3.S3) {
	defaultClient = client
}

// Dial dials an s3 repository. The URL must have the form:
//
//	s3r://bucket/prefix
//
// TODO(marius): we should support shipping authentication
// information in the URL also.
func Dial(u *url.URL) (reflow.Repository, error) {
	if u.Scheme != "s3r" {
		return nil, errors.E("dial", u.String(), errors.NotSupported, errors.Errorf("unknown scheme %v", u.Scheme))
	}
	bucket := u.Host
	mu.Lock()
	client := clients[bucket]
	mu.Unlock()
	if client == nil {
		client = defaultClient
	}
	if client == nil {
		return nil, errors.E("dial", u.String(), errors.NotSupported, "bucket %s not registered for dialing", bucket)
	}
	return &Repository{
		Client: client,
		Bucket: bucket,
		Prefix: u.Path,
	}, nil
}
