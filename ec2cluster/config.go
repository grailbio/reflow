// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/state"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/internal/ec2authenticator"
	"github.com/grailbio/reflow/runner"
	"golang.org/x/net/http2"
	yaml "gopkg.in/yaml.v2"
)

const (
	defaultMaxInstances = 100
	ec2cluster          = "ec2cluster"
)

func init() {
	config.Register("cluster", "ec2cluster", "", "configure a cluster using AWS EC2 compute nodes",
		func(cfg config.Config, arg string) (config.Config, error) {
			c := &Config{Config: cfg}
			return c, c.Init()
		},
	)
}

// Config defines a configuration for ec2cluster, providing
// Config.Cluster. It may be configured with the parameters defined
// directly in the struct. This configuration is overriden by the
// configuration specified in the key "ec2cluster".
type Config struct {
	config.Config `yaml:"-"`

	// SecurityGroup defines the EC2 security group with which to launch
	// new instances.
	SecurityGroup string `yaml:"securitygroup,omitempty"`

	// Region specifies the AWS region used for launching EC2 instances.
	// Instances are launched into any availability zone.
	Region string `yaml:"region,omitempty"`

	// Spot determines whether to use spot instances.
	Spot bool `yaml:"spot,omitempty"`
	// DiskType defines the EBS disk type (e.g., gp2) to use when
	// configuring EBS volumes.
	DiskType string `yaml:"disktype"`
	// DiskSpace determines the amount of EBS disk space to allocate for
	// each node, in gigabytes.
	DiskSpace int `yaml:"diskspace"`
	// AMI defines the AMI to use when launching new instances. CoreOS
	// is assumed.
	AMI string `yaml:"ami"`
	// MaxInstances limits the number of instances that that may be
	// running at any given time.
	MaxInstances int `yaml:"maxinstances,omitempty"`
	// InstanceTypes defines the set of allowable EC2 instance types for
	// this cluster. If empty, all instance types are permitted.
	InstanceTypes []string `yaml:"instancetypes,omitempty"`
	// Additional public SSH key to add to the instance.
	SshKey string
	// KeyName is the AWS SSH key with which to launch new instances.
	// If unspecified, instances are launched without keys.
	KeyName string
	// Immortal determines whether instances should be made immortal.
	Immortal bool `yaml:"immortal,omitempty"`
}

// Init initializes this EC2 configuration from the underlying configuration
// key "ec2cluster". It returns an error if this key cannot be unmarshaled
// into a valid EC2 configuration.
func (c *Config) Init() error {
	v := c.Value(ec2cluster)
	if v == nil {
		return nil
	}
	// Transcoding via YAML is really not very great. It would be nice
	// if the YAML package exposed an intermediate representation that
	// we could use for keys, but alas.
	//
	// Alternatively, we could use reflection to translate interface{}
	// into a struct.
	b, err := yaml.Marshal(v)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(b, c)
}

// Marshal adds this config's ec2 parameters.
func (c *Config) Marshal(keys config.Keys) error {
	if err := c.Config.Marshal(keys); err != nil {
		return err
	}
	keys[ec2cluster] = c
	return nil
}

// Cluster returns an EC2-based cluster using the provided parameters.
func (c *Config) Cluster() (runner.Cluster, error) {
	clientConfig, _, err := c.HTTPS()
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{TLSClientConfig: clientConfig}
	http2.ConfigureTransport(transport)
	httpClient := &http.Client{Transport: transport}
	log, err := c.Logger()
	if err != nil {
		return nil, err
	}
	sess, err := c.AWS()
	if err != nil {
		return nil, err
	}
	svc := ec2.New(sess, &aws.Config{MaxRetries: aws.Int(13)})
	path := filepath.Join(os.ExpandEnv("$HOME/.reflow") /*c.Version,*/, "ec2cluster" /*+c.Config.EC2ClusterName*/)
	state, err := state.Open(path)
	if err != nil {
		return nil, err
	}
	id, err := c.User()
	if err != nil {
		log.Errorf("retrieving username: %s", err)
		id = "unknown"
	}
	var ok bool
	reflowlet, ok := c.Value("reflowlet").(string)
	if !ok {
		return nil, errors.New("key \"reflowlet\" is not a string")
	}
	cluster := &Cluster{
		// This is a little sketchy with layering, etc. e.g., keys for
		// providers on top of us may not be available, etc.
		Config:        c,
		EC2:           svc,
		File:          state,
		Authenticator: ec2authenticator.New(sess),
		HTTPClient:    httpClient,
		Log:           log.Tee(nil, "ec2cluster: "),
		Tag:           fmt.Sprintf("%s (reflow)", id),
		/* XXX
		Labels: pool.Labels{
			"grail:user":    id,
			"grail:project": c.Flag.Project,
		},
		*/
		Spot:           c.Spot,
		SecurityGroup:  c.SecurityGroup,
		Region:         c.Region,
		ReflowletImage: reflowlet,
		MaxInstances:   c.MaxInstances,
		DiskType:       c.DiskType,
		DiskSpace:      c.DiskSpace,
		AMI:            c.AMI,
		SshKey:         c.SshKey,
		KeyName:        c.KeyName,
		Immortal:       c.Immortal,
	}
	if cluster.MaxInstances == 0 {
		cluster.MaxInstances = defaultMaxInstances
	}
	if len(c.InstanceTypes) > 0 {
		cluster.InstanceTypes = make(map[string]bool)
		for _, typ := range c.InstanceTypes {
			cluster.InstanceTypes[typ] = true
		}
	}
	if err := cluster.Init(); err != nil {
		return nil, err
	}
	return cluster, nil
}
