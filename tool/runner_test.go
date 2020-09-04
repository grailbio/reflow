package tool

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/status"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws/test"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	_ "github.com/grailbio/reflow/assoc/test"
	_ "github.com/grailbio/reflow/ec2cluster/test"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/repository"
	_ "github.com/grailbio/reflow/repository/s3/test"
	"github.com/grailbio/reflow/runner"
	wg2 "github.com/grailbio/reflow/wg"
	"v.io/x/lib/vlog"
)

func getTestReflowConfig() infra.Config {
	schema := infra.Schema{
		infra2.Log:        new(log.Logger),
		infra2.Repository: new(reflow.Repository),
		infra2.Cluster:    new(runner.Cluster),
		infra2.Assoc:      new(assoc.Assoc),
		infra2.Cache:      new(infra2.CacheProvider),
		infra2.Session:    new(session.Session),
		infra2.TLS:        new(tls.Certs),
	}
	keys := getTestReflowConfigKeys()
	cfg, err := schema.Make(keys)
	if err != nil {
		vlog.Fatal(err)
	}
	return cfg
}

func getTestReflowConfigKeys() infra.Keys {
	return infra.Keys{
		infra2.Assoc:      "fakeassoc",
		infra2.Cache:      "readwrite",
		infra2.Cluster:    "fakecluster",
		infra2.Log:        "logger,level=debug",
		infra2.Repository: "fakes3",
		infra2.Session:    "fakesession",
		infra2.TLS:        "tls,file=/tmp/ca.reflow",
	}
}

func TestSchedulerDefaultPendingTransferLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := getTestReflowConfig()
	scheduler, err := NewScheduler(ctx, config, &wg2.WaitGroup{}, nil, log.Std, new(status.Status))
	if err != nil {
		t.Fatal(err)
	}
	// This is fragile. Due to lack of better ideas, for now...
	// TODO(pgopal): may be expose this as an API?
	manager, ok := scheduler.Transferer.(*repository.Manager)
	if !ok {
		t.Fatal("scheduler transferer not repository.Manager")
	}
	expectedLimit := fmt.Sprintf("limits{def:%d overrides:", defaultTransferLimit)
	if !strings.HasPrefix(manager.PendingTransfers.String(), expectedLimit) {
		t.Fatalf("expected prefix %s, got %s", expectedLimit, manager.PendingTransfers.String())
	}
}
