package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository/blobrepo"
)

func init() {
	infra.Register("s3", new(Repository))
}

// Repository is a s3 backed blob repository.
type Repository struct {
	// Repository is the underlying blob repository implementation for s3.
	*blobrepo.Repository
	infra2.BucketNameFlagsTrait
}

// Help implements infra.Provider
func (Repository) Help() string {
	return "configure a repository using a S3 bucket"
}

// Init implements infra.Provider
func (r *Repository) Init(sess *session.Session) (err error) {
	r.Repository, err = InitRepo(sess, r.BucketName)
	return
}

func InitRepo(sess *session.Session, bucketName string) (*blobrepo.Repository, error) {
	blob := s3blob.New(sess)
	blobrepo.Register("s3", blob)
	ctx := context.Background()
	bucket, err := blob.Bucket(ctx, bucketName)
	if err != nil {
		return nil, errors.E("repo.InitRepo", bucketName, err)
	}
	return &blobrepo.Repository{Bucket: bucket}, nil
}

// Setup implements infra.Provider
func (r *Repository) Setup(sess *session.Session, log *log.Logger) error {
	return CreateS3Bucket(sess, r.BucketNameFlagsTrait.BucketName, log)
}

func CreateS3Bucket(sess *session.Session, bucketName string, log *log.Logger) error {
	log.Printf("creating s3 bucket %s", bucketName)
	req := &s3.CreateBucketInput{Bucket: aws.String(bucketName)}
	if region := *sess.Config.Region; region != "us-east-1" {
		req.CreateBucketConfiguration = &s3.CreateBucketConfiguration{LocationConstraint: aws.String(region)}
	}
	_, err := s3.New(sess).CreateBucket(req)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				log.Printf("s3 bucket %s is already owned by someone else", bucketName)
				return nil
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				log.Printf("s3 bucket %s already exists; not created", bucketName)
				return nil
			default:
				return fmt.Errorf("request: %v failed: %v", req, err)
			}
		} else {
			return fmt.Errorf("request: %v failed: %v", req, err)
		}
	}
	log.Printf("created s3 bucket %s", bucketName)
	return nil
}
