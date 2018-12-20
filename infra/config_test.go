// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package infra_test

import (
	"errors"
	"flag"
	"testing"

	"github.com/grailbio/reflow/infra"
)

type testCreds string

func (c *testCreds) User() string { return string(*c) }

func (*testCreds) Init() error {
	return nil
}

func (c *testCreds) Config() interface{} {
	return c
}

func (c *testCreds) Flags(flags *flag.FlagSet) {
	flags.StringVar((*string)(c), "user", "", "the user name")
}

type testCluster struct {
	User         string `yaml:"-"`
	InstanceType string `yaml:"instance_type"`
	NumInstances int    `yaml:"num_instances"`
	SetupUser    string `yaml:"setup_user"`
}

func (c *testCluster) Init(creds *testCreds) error {
	c.User = string(*creds)
	return nil
}

func (c *testCluster) Config() interface{} {
	return c
}

func (c *testCluster) Setup(creds *testCreds) error {
	if string(*creds) == "" {
		return errors.New("no user specified")
	}
	c.InstanceType = "xxx"
	c.NumInstances = 123
	c.SetupUser = string(*creds)
	return nil
}

func (c *testCluster) Version() int {
	return 1
}

func init() {
	infra.Register("testcreds", new(testCreds))
	infra.Register("testcluster", new(testCluster))
}

var schema = infra.Schema{
	"creds":   new(testCreds),
	"cluster": new(testCluster),
}

func TestConfig(t *testing.T) {
	config, err := schema.Make(infra.Keys{
		"creds":   "testcreds,user=testuser",
		"cluster": "testcluster",
	})
	if err != nil {
		t.Fatal(err)
	}
	var cluster *testCluster
	config.Must(&cluster)
	if got, want := cluster.User, "testuser"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestConfigUnmarshal(t *testing.T) {
	config, err := schema.Unmarshal([]byte(`creds: testcreds,user=unmarshaled
cluster: testcluster
testcluster:
  instance_type: xyz
  num_instances: 123
`))
	if err != nil {
		t.Fatal(err)
	}
	var cluster *testCluster
	config.Must(&cluster)
	if got, want := *cluster, (testCluster{"unmarshaled", "xyz", 123, ""}); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestConfigInterface(t *testing.T) {
	type credentials interface {
		User() string
	}
	schema := infra.Schema{"creds": new(credentials)}
	config, err := schema.Make(
		infra.Keys{"creds": "testcreds,user=interface"},
	)
	if err != nil {
		t.Fatal(err)
	}
	var creds credentials
	config.Must(&creds)
	if got, want := creds.User(), "interface"; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSetup(t *testing.T) {
	config, err := schema.Make(infra.Keys{
		"creds":   "testcreds",
		"cluster": "testcluster",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Setup(); err == nil || err.Error() != "setup testcluster: no user specified" {
		t.Fatal(err)
	}
	config, err = schema.Make(infra.Keys{
		"creds":   "testcreds,user=xyz",
		"cluster": "testcluster",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := config.Setup(); err != nil {
		t.Fatal(err)
	}
	// Make sure
	p, err := config.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(p), `cluster: testcluster
creds: testcreds,user=xyz
testcluster:
  instance_type: xxx
  num_instances: 123
  setup_user: xyz
testcreds: xyz
versions:
  testcluster: 1
`; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
