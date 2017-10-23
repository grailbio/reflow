package main

import (
	"os"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/config"
	_ "github.com/grailbio/reflow/config/awsenvconfig"
	_ "github.com/grailbio/reflow/config/dockerconfig"
	_ "github.com/grailbio/reflow/config/ec2metadataconfig"
	_ "github.com/grailbio/reflow/config/httpscaconfig"
	_ "github.com/grailbio/reflow/config/httpsconfig"
	_ "github.com/grailbio/reflow/config/s3config"
	_ "github.com/grailbio/reflow/ec2cluster"
	"github.com/grailbio/reflow/tool"
)

var configFile = os.ExpandEnv("$HOME/.reflow/config.yaml")

const intro = `Cluster computing and caching

Additional configuration is required to use a cluster for Reflow
jobs. Reflow may be set up to make use of a cluster of reflowlets
(Reflow server processes), or to make use of its own cluster manager,
which elastically provisions (and tears down) compute resources as
they are needed.

The command setup-ec2 configures an AWS account to be used by
Reflow's cluster manager.

Reflow may also use a distributed cache to automatically store and
reuse intermediate results. Reflow may be configured to use S3 (and
dynamoDB) to implement such a cache. Command setup-s3-cache
provisions the necessary resources in an AWS account.

See the following for more details:

	reflow setup-ec2 -help
	reflow setup-s3-cache -help`

func main() {
	var cfg config.Config = make(config.Base)
	cfg = defaultConfig{cfg}
	cfg = &config.KeyConfig{cfg, "reflowlet", reflowlet}
	cfg = &config.KeyConfig{cfg, "aws", "awsenv"}
	cmd := &tool.Cmd{
		// Turn caching off by default. This way we can run a vanilla Reflow
		// binary in local mode without any additional configuration.
		Config:            cfg,
		DefaultConfigFile: configFile,
		Version:           version,
		Intro:             intro,
		Commands: map[string]tool.Func{
			"setup-ec2":      setupEC2,
			"setup-s3-cache": setupS3Cache,
		},
	}
	cmd.Flags().Parse(os.Args[1:])
	cmd.Main()
}

// defaultConfig provides configuration defaults for the reflow distribution.
type defaultConfig struct {
	config.Config
}

func (c defaultConfig) Marshal(keys config.Keys) error {
	keys[config.Cache] = "off"
	keys[config.AWSTool] = "docker,grailbio/awstool:latest"
	if err := c.Config.Marshal(keys); err != nil {
		return err
	}
	return nil
}

func (defaultConfig) Cache() (reflow.Cache, error) {
	return nil, nil
}

func (defaultConfig) AWSTool() (string, error) {
	return "grailbio/awstool:latest", nil
}
