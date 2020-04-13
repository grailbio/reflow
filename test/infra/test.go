package infra

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	_ "github.com/grailbio/infra/aws/test"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	_ "github.com/grailbio/reflow/assoc/test"
	_ "github.com/grailbio/reflow/ec2cluster/test"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	_ "github.com/grailbio/reflow/repository/s3/test"
	"github.com/grailbio/reflow/runner"
)

// GetTestReflowConfig returns a dummy reflow config.
func GetTestReflowConfig() infra.Config {
	schema := infra.Schema{
		infra2.Log:        new(log.Logger),
		infra2.Repository: new(reflow.Repository),
		infra2.Cluster:    new(runner.Cluster),
		infra2.Assoc:      new(assoc.Assoc),
		infra2.Cache:      new(infra2.CacheProvider),
		infra2.Session:    new(session.Session),
		infra2.TLS:        new(tls.Certs),
	}
	keys := GetTestReflowConfigKeys()
	cfg, err := schema.Make(keys)
	if err != nil {
		log.Fatal(err)
	}
	return cfg
}

// GetTestReflowConfigKeys returns a dummy set of infra keys.
func GetTestReflowConfigKeys() infra.Keys {
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
