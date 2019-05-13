// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"fmt"
	"net/http"
	"regexp"

	"sort"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ecr"
	"github.com/aws/aws-sdk-go/service/ecr/ecriface"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/ec2cluster/instances"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/runner"
	"golang.org/x/net/http2"
	"gopkg.in/yaml.v2"
)

const (
	defaultMaxInstances = 100
	defaultClusterName  = "default"
	ec2cluster          = "ec2cluster"
)

var ecrURI = regexp.MustCompile(`^[0-9]+\.dkr\.ecr\.[a-z0-9-]+\.amazonaws.com/(.*):(.*)$`)

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

	// Name is the name of the cluster config, which defaults to defaultClusterName.
	// Multiple clusters can be launched/maintained simultaneously by using different names.
	Name string `yaml:"name,omitempty"`

	// InstanceProfile defines the EC2 instance profile with which to launch
	// new instances.
	InstanceProfile string `yaml:"instanceprofile,omitempty"`

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
	// DiskSlices is the number of EBS volumes to use. When DiskSlices > 1,
	// the volumes are arranged in a RAID0 array to increase throughput.
	DiskSlices int `yaml:"diskslices"`
	// AMI defines the AMI to use when launching new instances. CoreOS
	// is assumed.
	AMI string `yaml:"ami"`
	// MaxInstances limits the number of instances that that may be
	// running at any given time.
	MaxInstances int `yaml:"maxinstances,omitempty"`
	// InstanceTypesMap defines the set of allowable EC2 instance types for
	// this cluster. If empty, all instance types are permitted.
	InstanceTypes []string `yaml:"instancetypes,omitempty"`
	// Additional public SSH key to add to the instance.
	SshKey string
	// KeyName is the AWS SSH key with which to launch new instances.
	// If unspecified, instances are launched without keys.
	KeyName string
	// Immortal determines whether instances should be made immortal.
	Immortal bool `yaml:"immortal,omitempty"`

	// SpotProbeDepth determines the depth of the capacity probing used
	// to determine whether spot instances are likely to be available.
	SpotProbeDepth int `yaml:"spotprobedepth,omitempty"`

	// CloudConfig stores a cloud config that is merged into ec2cluster's
	// cloud config. It is merged before the reflowlet unit is added, so
	// the user has an opportunity to introduce additional systemd units.
	CloudConfig cloudConfig `yaml:"cloudconfig"`
}

// Init initializes this EC2 configuration from the underlying configuration
// key "ec2cluster". It returns an error if this key cannot be unmarshaled
// into a valid EC2 configuration.
func (c *Config) Init() error {
	// If InstanceTypes are not defined, include all known types.
	if len(c.InstanceTypes) == 0 {
		c.InstanceTypes = make([]string, len(instances.Types))
		for i := range instances.Types {
			c.InstanceTypes[i] = instances.Types[i].Name
		}
		sort.Strings(c.InstanceTypes)
	}
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

func (c *Config) name() string {
	if c.Name != "" {
		return c.Name
	}
	return defaultClusterName
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
	id, err := c.User()
	if err != nil {
		log.Errorf("retrieving username: %s", err)
		id = "unknown"
	}
	var reflowlet string
	if reflowlet, err = getString(c, "reflowlet"); err != nil {
		return nil, err
	}
	var reflowVersion string
	if reflowVersion, err = getString(c.Config, "reflowversion"); err != nil {
		return nil, err
	}
	if reflowVersion == "" {
		return nil, errors.New("no version specified in cluster configuration")
	}
	if err := validateReflowletImage(ecr.New(sess), reflowlet, log); err != nil {
		return nil, err
	}
	labels := c.Labels().Copy()
	qtags := make(map[string]string)
	qtags["Name"] = fmt.Sprintf("%s (reflow)", id)
	qtags["cluster"] = c.name()
	cluster := &Cluster{
		// This is a little sketchy with layering, etc. e.g., keys for
		// providers on top of us may not be available, etc.
		Config:          c,
		EC2:             svc,
		Authenticator:   ec2authenticator.New(sess),
		HTTPClient:      httpClient,
		Log:             log.Tee(nil, "ec2cluster: "),
		InstanceTags:    qtags,
		Labels:          labels,
		Spot:            c.Spot,
		InstanceProfile: c.InstanceProfile,
		SecurityGroup:   c.SecurityGroup,
		Region:          c.Region,
		ReflowletImage:  reflowlet,
		ReflowVersion:   reflowVersion,
		MaxInstances:    c.MaxInstances,
		DiskType:        c.DiskType,
		DiskSpace:       c.DiskSpace,
		DiskSlices:      c.DiskSlices,
		AMI:             c.AMI,
		SshKey:          c.SshKey,
		KeyName:         c.KeyName,
		SpotProbeDepth:  c.SpotProbeDepth,
		Immortal:        c.Immortal,
		CloudConfig:     c.CloudConfig,
	}
	if cluster.MaxInstances == 0 {
		cluster.MaxInstances = defaultMaxInstances
	}
	if len(c.InstanceTypes) > 0 {
		cluster.InstanceTypesMap = make(map[string]bool)
		for _, typ := range c.InstanceTypes {
			cluster.InstanceTypesMap[typ] = true
		}
	}
	if err := cluster.initialize(); err != nil {
		return nil, err
	}
	return cluster, nil
}

func validateReflowletImage(ecrApi ecriface.ECRAPI, reflowlet string, log *log.Logger) error {
	matches := ecrURI.FindStringSubmatch(reflowlet)
	if len(matches) != 3 {
		log.Debugf("cannot determine repository name and/or image tag from: %s", reflowlet)
		return nil
	}
	dii := &ecr.DescribeImagesInput{
		RepositoryName: &matches[1],
		ImageIds:       []*ecr.ImageIdentifier{{ImageTag: &matches[2]}},
	}
	if _, err := ecrApi.DescribeImages(dii); err != nil {
		return fmt.Errorf("required reflowlet image not found on AWS: %v", err)
	}
	return nil
}

func getString(c config.Config, key string) (string, error) {
	val, ok := c.Value(key).(string)
	if !ok {
		return "", fmt.Errorf("key \"%s\" is not a string", key)
	}
	return val, nil
}
