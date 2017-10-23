package dockerconfig

import (
	"errors"

	"github.com/grailbio/reflow/config"
)

func init() {
	config.Register(config.AWSTool, "docker", "image", "use the given docker image containing the AWS CLI",
		func(cfg config.Config, arg string) (config.Config, error) {
			if arg == "" {
				return nil, errors.New("no image name provided")
			}
			return &docker{cfg, arg}, nil
		},
	)
}

type docker struct {
	config.Config
	awstool string
}

func (c *docker) AWSTool() (string, error) {
	return c.awstool, nil
}
