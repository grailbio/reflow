// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	"github.com/grailbio/infra/aws"
	_ "github.com/grailbio/infra/ec2metadata"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	_ "github.com/grailbio/reflow/assoc/dydbassoc"
	_ "github.com/grailbio/reflow/ec2cluster"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	_ "github.com/grailbio/reflow/repository/s3"
	"github.com/grailbio/reflow/runner"
	"github.com/grailbio/reflow/taskdb"
	_ "github.com/grailbio/reflow/taskdb/dynamodbtask"
	"github.com/grailbio/reflow/tool"
	"github.com/grailbio/reflow/trace"
	_ "github.com/grailbio/reflow/trace"
	_ "github.com/grailbio/reflow/trace/xraytrace"
)

// version is set by the linker when building the binary.
var version = "broken"

var configFile = os.ExpandEnv("$HOME/.reflow/config.yaml")

// TODO(dnicolaou): Make open source reflow use reflowbootstrap.

// bootstrapimage is the docker URI for the bootstrap image.
const bootstrapimage = "grailbio/reflowlet:bootstrap"

const intro = `Cluster computing and caching

Additional configuration is required to use a cluster for Reflow
jobs. Reflow may be set up to make use of a cluster of reflowlets
(Reflow server processes), or to make use of its own cluster manager,
which elastically provisions (and tears down) compute resources as
they are needed.

The command setup-ec2 configures an AWS account to be used by
Reflow's cluster manager.

Reflow may also use a distributed cache to automatically store and
reuse intermediate results. Caching requires setting up a global
repository and association table. A global repository may be
configured to use S3, and the association table may be configured to
use DynamoDB. Command setup-s3-repository and setup-dynamodb-assoc
provisions the necessary resources in an AWS account. 

See the following for more details:

	reflow setup-ec2 -help
	reflow setup-s3-repository -help
	reflow setup-dynamodb-assoc -help`

func main() {
	// TODO(swami):  Don't marshal reflowlet and version in the config
	// because they shouldn't be changeable by the user once bootstrapping is rolled out.

	cmd := &tool.Cmd{
		// Turn caching off by default. This way we can run a vanilla Reflow
		// binary in local mode without any additional configuration.
		DefaultConfigFile: configFile,
		Version:           version,
		Intro:             intro,
		Commands: map[string]tool.Func{
			"setup-ec2":            setupEC2,
			"setup-s3-repository":  setupS3Repository,
			"setup-dynamodb-assoc": setupDynamoDBAssoc,
		},
	}
	cmd.Schema = infra.Schema{
		infra2.AWSCreds:   new(credentials.Credentials),
		infra2.Assoc:      new(assoc.Assoc),
		infra2.AWSTool:    new(aws.AWSTool),
		infra2.Cache:      new(infra2.CacheProvider),
		infra2.Cluster:    new(runner.Cluster),
		infra2.Labels:     make(pool.Labels),
		infra2.Log:        new(log.Logger),
		infra2.Bootstrap:  new(infra2.BootstrapImage),
		infra2.Reflow:     new(infra2.ReflowVersion),
		infra2.Repository: new(reflow.Repository),
		infra2.Session:    new(session.Session),
		infra2.SSHKey:     new(infra2.SshKey),
		infra2.TLS:        new(tls.Authority),
		infra2.Username:   new(infra2.User),
		infra2.Tracer:     new(trace.Tracer),
		infra2.TaskDB:     new(taskdb.TaskDB),
	}
	cmd.SchemaKeys = infra.Keys{
		infra2.AWSCreds:  "awscreds",
		infra2.AWSTool:   "awstool,awstool=grailbio/awstool:latest",
		infra2.Cache:     "off",
		infra2.Labels:    "kv",
		infra2.Log:       "logger",
		infra2.Bootstrap: fmt.Sprintf("bootstrapimage,uri=%s", bootstrapimage),
		infra2.Reflow:    fmt.Sprintf("reflowversion,version=%s", version),
		infra2.Session:   "awssession",
		infra2.SSHKey:    "key",
		infra2.TLS:       "tls,file=/tmp/ca.reflow",
		infra2.Username:  "user",
		infra2.Tracer:    "xray",
	}
	cmd.Flags().Parse(os.Args[1:])
	cmd.Main()
}
