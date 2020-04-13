package localcluster

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	"github.com/grailbio/infra/aws"
	_ "github.com/grailbio/infra/aws/test"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	_ "github.com/grailbio/reflow/assoc/test"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	_ "github.com/grailbio/reflow/repository/s3/test"
	"github.com/grailbio/reflow/runner"
)

func getTestReflowConfig() infra.Config {
	schema := infra.Schema{
		infra2.Assoc:      new(assoc.Assoc),
		infra2.AWSCreds:   new(credentials.Credentials),
		infra2.AWSTool:    new(aws.AWSTool),
		infra2.Cache:      new(infra2.CacheProvider),
		infra2.Cluster:    new(runner.Cluster),
		infra2.Labels:     make(pool.Labels),
		infra2.Log:        new(log.Logger),
		infra2.Repository: new(reflow.Repository),
		infra2.Session:    new(session.Session),
		infra2.TLS:        new(tls.Certs),
		infra2.Username:   new(infra2.User),
	}
	keys := getTestReflowConfigKeys()
	cfg, err := schema.Make(keys)
	if err != nil {
		log.Fatal(err)
	}
	return cfg
}

func getTestReflowConfigKeys() infra.Keys {
	return infra.Keys{
		infra2.Assoc:      "fakeassoc",
		infra2.AWSCreds:   "fakecreds",
		infra2.AWSTool:    "awstool,awstool=grailbio/awstool:latest",
		infra2.Cache:      "readwrite",
		infra2.Cluster:    "localcluster",
		infra2.Labels:     "kv",
		infra2.Log:        "logger,level=debug",
		infra2.Repository: "fakes3",
		infra2.Session:    "fakesession",
		infra2.TLS:        "tls,file=/tmp/ca.reflow",
		infra2.Username:   "user",
	}
}

func TestLocalClusterAllocateAndOffers(t *testing.T) {
	log.Std.Level = log.DebugLevel
	config := getTestReflowConfig()
	var (
		cluster runner.Cluster
		ok      bool
		logger  *log.Logger
		err     error
	)

	if err = config.Instance(&cluster); err != nil {
		t.Fatal(err)
	}
	if _, ok = cluster.(*Cluster); !ok {
		t.Fatal("error: cluster is not a local cluster")
	}
	if err = config.Instance(&logger); err != nil {
		t.Fatal(err)
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), 1*time.Minute)
	alloc, err := cluster.Allocate(ctx, reflow.Requirements{Min: reflow.Resources{"cpu": 1.0}}, make(pool.Labels))
	cancelFunc()
	if err != nil {
		t.Fatal(err)
	}
	offers, err := alloc.Pool().Offers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(offers) != 1 {
		t.Fatalf("expected 1 offer, got %v", len(offers))
	}
}
