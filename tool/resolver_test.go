package tool

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/test/testutil"
)

func TestResolveImages(t *testing.T) {
	if testing.Short() {
		t.Skip("requires network access")
	}
	testutil.SkipIfNoCreds(t)
	var schema = infra.Schema{
		"session": new(session.Session),
	}
	config, err := schema.Make(infra.Keys{
		"session": "awssession",
	})
	if err != nil {
		t.Fatal(err)
	}
	var sess *session.Session
	config.Must(&sess)
	const (
		repository = "619867110810.dkr.ecr.us-west-2.amazonaws.com"
		resolved   = "619867110810.dkr.ecr.us-west-2.amazonaws.com/awstool@sha256:df811e08963f5e3ebba7efc8a003ec6fbde401ea272dd34378a9f2aa24b708db"
	)
	r := ImageResolver{Authenticator: ec2authenticator.New(sess)}
	for _, img := range []string{
		repository + "/awstool",
		repository + "/awstool$aws",
		repository + "/awstool$docker",
		repository + "/awstool$aws$docker",
		repository + "/awstool$docker$aws",
	} {
		canonical, err := r.ResolveImages(context.Background(), []string{img})
		if err != nil {
			t.Errorf("error while getting canonical name for %s: %v", img, err)
			continue
		}
		if got, want := canonical[img], resolved; got != want {
			t.Errorf("expected %s, got %s", want, got)
		}
	}
}
