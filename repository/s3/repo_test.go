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
		"session":    "github.com/grailbio/infra/aws.Session",
		"logger":     "github.com/grailbio/reflow/log.Logger",
		"repository": fmt.Sprintf("github.com/grailbio/reflow/repository/s3.Repository,bucket=%v", bucket),
	})
	if err != nil {
		t.Fatal(err)
	}

	var repos reflow.Repository
	config.Must(&repos)
	var s3repos *Repository
	config.Must(&s3repos)
	if got, want := s3repos.Bucket, bucket; got != want {
		t.Errorf("got %v, want %v", s3repos.Bucket, bucket)
	}
	if got, want := repos.URL().String(), "s3://reflow-unittest//"; got != want {
		t.Errorf("got %v, want %v", repos.URL(), "s3://reflow-unittest//")
	}
}
