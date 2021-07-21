package s3

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/test/testutil"
)

func TestS3RepositoryInfra(t *testing.T) {
	const bucket = "reflow-unittest"
	testutil.SkipIfNoCreds(t)
	var schema = infra.Schema{
		"session":    new(session.Session),
		"logger":     new(log.Logger),
		"repository": new(reflow.Repository),
	}
	config, err := schema.Make(infra.Keys{
		"session":    "awssession",
		"logger":     "logger",
		"repository": fmt.Sprintf("s3,bucket=%v", bucket),
	})
	if err != nil {
		t.Fatal(err)
	}

	var repo reflow.Repository
	config.Must(&repo)
	var s3repos *Repository
	config.Must(&s3repos)
	if got, want := s3repos.BucketName, bucket; got != want {
		t.Errorf("got %v, want %v", s3repos.BucketName, bucket)
	}
	if got, want := repo.URL().String(), "s3://reflow-unittest/"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
