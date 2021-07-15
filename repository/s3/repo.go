package s3

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	"github.com/grailbio/reflow/repository/blobrepo"
)

func init() {
	infra.Register("s3", new(Repository))
}

// Repository is a s3 backed blob repository.
type Repository struct {
	// Repository is the underlying blob repository implementation for s3.
	*blobrepo.Repository
	repository.RepoFlagsTrait
}

// Help implements infra.Provider
func (Repository) Help() string {
	return "configure a repository using a S3 bucket"
}

// Init implements infra.Provider
func (r *Repository) Init(sess *session.Session) error {
	blob := s3blob.New(sess)
	blobrepo.Register("s3", blob)
	ctx := context.Background()
	bucket, err := blob.Bucket(ctx, r.RepoFlagsTrait.BucketName)
	if err != nil {
		return err
	}
	r.Repository = &blobrepo.Repository{Bucket: bucket}
	return nil
}

// Setup implements infra.Provider
func (r *Repository) Setup(sess *session.Session, log *log.Logger) error {
	log.Printf("creating s3 bucket %s", r.RepoFlagsTrait.BucketName)
	_, err := s3.New(sess).CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(r.RepoFlagsTrait.BucketName),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(*sess.Config.Region),
		},
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case s3.ErrCodeBucketAlreadyExists:
				log.Printf("s3 bucket %s is already owned by someone else", r.RepoFlagsTrait.BucketName)
				return nil
			case s3.ErrCodeBucketAlreadyOwnedByYou:
				log.Printf("s3 bucket %s already exists; not created", r.RepoFlagsTrait.BucketName)
				return nil
			default:
				return err
			}
		} else {
			return err
		}
	}
	log.Printf("created s3 bucket %s", r.RepoFlagsTrait.BucketName)
	return nil
}
