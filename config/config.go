// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package config defines an interface for configuring a Reflow
// instance. This interface can be composed in multiple ways,
// allowing for layered configuration and also for distributions of
// Reflow to supply custom configuration.
//
// A configuration is a set of keys (corresponding to toplevel keys
// in a YAML document). A subset of keys, defined by the package's
// AllKeys, correspond to objects that are configured by the Config
// interface. These keys may be provisioned by globally registered
// providers; the keys must be string formatted, and contain the
// (registered) name of the provider, followed by an optional comma
// and string argument. For example:
//
//	cache: s3,bucket,dynamodb
//
// Configures the cache key (corresponding to Config.Cache) using the
// s3 provider; the argument "bucket,dynamodb" is used to configure
// it.
//
// Providers may themselves look up keys to supply further
// configuration. For example, we may provision the cluster key
// (corresponding to Config.Cluster) with the ec2cluster provider,
// which is further configured by the ec2cluster key, for example:
//
//	cluster: ec2cluster
//
//	ec2cluster:
//	  zone: us-west-2a
//	  ami: ami-12345
//	  disktype: gp2
//	  ...
//
// Configuration providers are registered globally, allowing for
// different distributions to provide different backends or
// configuration mechanisms.
//
// Configuration providers may also supply additional keys (i.e., not
// present in the original concrete input configuration) that can be
// used to transmit additional configuration information across
// systems boundaries, for example to pass credentials from the
// Reflow cluster controller to individual reflowlet instances.
package config

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	golog "log"
	"os"
	"os/user"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/runner"
	yaml "gopkg.in/yaml.v2"
)

// The following are the set of keys provisioned by Config.
const (
	Logger  = "logger"
	AWS     = "aws"
	AWSTool = "awstool"
	User    = "user"
	HTTPS   = "https"
	// Cache is maintained as a key for backwards compatibility purposes.
	Cache      = "cache"
	Cluster    = "cluster"
	Assoc      = "assoc"
	Repository = "repository"
)

// AllKeys defines the order in which configuration keys are
// provisioned. Thus, providers for keys later in the list may use
// configuration provided by providers for keys earlier in the list.
var AllKeys = []string{
	Logger,
	AWS,
	AWSTool,
	User,
	HTTPS,
	Cache,
	Assoc,
	Repository,
	Cluster,
}

// Keys is a map of string keys to configuration values.
type Keys map[string]interface{}

// A Config provides a number of methods to mint new objects
// that are used in Reflow. It is safe to call each method multiple
// times, but they should not be called concurrently.
type Config interface {
	// CacheMode returns the configured cache mode.
	CacheMode() reflow.CacheMode

	// Assoc returns this configuration's assoc.
	Assoc() (assoc.Assoc, error)

	// Repository returns this configuration's repository.
	Repository() (reflow.Repository, error)

	// AWS returns this configuration's AWS session.
	AWS() (*session.Session, error)

	// AWSCreds provides AWS credentials directly. This differs from AWS
	// in that these credentials my be extended to user code, and should
	// be permanent.
	AWSCreds() (*credentials.Credentials, error)

	// AWSRegion returns the region to be used for all AWS operations.
	AWSRegion() (string, error)

	// AWSTool is the Docker image to be used for the AWS CLI.
	AWSTool() (string, error)

	// HTTPS returns this configuration's client and server configs.
	HTTPS() (client, server *tls.Config, err error)

	// User returns the user's id.
	User() (string, error)

	// Logger returns the configured logger.
	Logger() (*log.Logger, error)

	// Cluster returns the configured cluster.
	Cluster() (runner.Cluster, error)

	// Value returns the value of the given key.
	Value(key string) interface{}

	// Marshal marshals the current configuration into keys.
	Marshal(keys Keys) error

	// Keys returns all the keys as defined by this config.
	Keys() Keys
}

// Base defines a base configuration with reasonable defaults
// where they apply.
type Base Keys

// Assoc returns a nil assoc.
func (b Base) Assoc() (assoc.Assoc, error) {
	return nil, nil
}

// Repository returns a nil repository.
func (b Base) Repository() (reflow.Repository, error) {
	return nil, nil
}

// CacheMode returns the default cache mode, reflow.CacheOff.
func (b Base) CacheMode() reflow.CacheMode {
	return reflow.CacheRead | reflow.CacheWrite
}

// Cache returns an error indicating no cache was configured.
func (b Base) Cache() (reflow.Cache, error) {
	return nil, errors.New("no cache")
}

// AWS returns new default AWS credentials.
func (b Base) AWS() (*session.Session, error) {
	return nil, errors.New("AWS session not configured")
}

// AWSCreds returns new default AWS credentials.
func (b Base) AWSCreds() (*credentials.Credentials, error) {
	return nil, errors.New("AWS credentials not configured")
}

// AWSRegion the region in the key "awsregion", or else
// the default region us-west-2.
func (b Base) AWSRegion() (string, error) {
	v, ok := b["awsregion"]
	if ok {
		s, ok := v.(string)
		if !ok {
			return "", fmt.Errorf("invalid AWS region value: %v", v)
		}
		return s, nil
	}
	return "us-west-2", nil
}

// AWSTool returns an error indicating no AWS CLI tool was configured.
func (b Base) AWSTool() (string, error) {
	return "", errors.New("AWSTool not configured")
}

// HTTPS returns an error indicating no TLS certificates were configured.
func (b Base) HTTPS() (client, server *tls.Config, err error) {
	return nil, nil, errors.New("TLS not configured")
}

// User returns the current system user, or else "unknown" if not supported.
func (b Base) User() (string, error) {
	u, err := user.Current()
	if err != nil {
		log.Printf("error retrieving current user: %v", err)
		return "unknown@localhost", nil
	}
	return u.Username + "@localhost", nil
}

// Logger returns a logger that outputs to standard error.
func (b Base) Logger() (*log.Logger, error) {
	return log.New(golog.New(os.Stderr, "", golog.LstdFlags), log.InfoLevel), nil
}

// Cluster returns an error indicating no cluster was configured.
func (b Base) Cluster() (runner.Cluster, error) {
	return nil, errors.New("cluster not configured")
}

// Keys returns the configured keys.
func (b Base) Keys() Keys {
	return Keys(b)
}

// Value returns the value for the provided key.
func (b Base) Value(key string) interface{} {
	return b[key]
}

// Marshal populates the provided key dictionary with the keys
// present in this configuration. It also inlines AWS credentials
// so that the configuration is sealed.
func (b Base) Marshal(keys Keys) error {
	for k, v := range b {
		keys[k] = v
	}
	return nil
}

// Unmarshal unmarshals the (YAML-configured) configuration in b into
// keys.
func Unmarshal(b []byte, keys Keys) error {
	return yaml.Unmarshal(b, keys)
}

// Marshal marshals the given keys into YAML-formatted bytes.
func Marshal(cfg Config) ([]byte, error) {
	keys := make(Keys)
	if err := cfg.Marshal(keys); err != nil {
		return nil, err
	}
	return yaml.Marshal(keys)
}

// Make evaluates a config's keys: for each key in AllKeys (and in
// the order defined by AllKeys), Make parses its provider, and
// provisions the key accordingly. Make returns errors if a provider
// cannot be found or if the provider fails to configure the given
// key.
func Make(cfg Config) (Config, error) {
	for _, key := range AllKeys {
		v := cfg.Value(key)
		if v == nil {
			continue
		}
		vstr, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("expected string for key %s, got %T", key, v)
		}
		name, arg := peel(vstr, ",")
		provider, ok := Lookup(key, name)
		if !ok {
			return nil, fmt.Errorf("provider %s not defined for key %s", name, key)
		}
		var err error
		cfg, err = provider.Configure(cfg, arg)
		if err != nil {
			return nil, fmt.Errorf("configuring key %s with provider %s: %v", key, name, err)
		}
	}
	return cfg, nil
}

// Parse parses and provisions a configuration from the
// YAML-formatted bytes b.
func Parse(b []byte) (Config, error) {
	base := make(Base)
	if err := Unmarshal(b, Keys(base)); err != nil {
		return nil, err
	}
	return Make(base)
}

// ParseFile reads and then parses the configuration from the
// provided filename.
func ParseFile(filename string) (Config, error) {
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// A Provider provisions a single key in a configuration. Providers
// must be registered via the package's Register function.
type Provider struct {
	Configure        func(cfg Config, arg string) (Config, error)
	Kind, Arg, Usage string
}

var (
	providers = make(map[string]map[string]Provider)
	mu        sync.Mutex
)

// Register the configuration provider kind for the given key. The
// arg and usage string should describe the provider's argument.
func Register(key, kind, arg, usage string, configure func(Config, string) (Config, error)) {
	// TODO: check if it's in AllKeys

	mu.Lock()
	defer mu.Unlock()
	kindmap := providers[key]
	if kindmap == nil {
		providers[key] = make(map[string]Provider)
		_, ok := providers[kind]
		if ok {
			panic(fmt.Sprintf("provider %s already registered for key %s", kind, key))
		}
		kindmap = providers[key]
	}
	kindmap[kind] = Provider{
		Configure: configure,
		Kind:      kind,
		Arg:       arg,
		Usage:     usage,
	}
}

// Lookup returns the Provider of kind for key.
func Lookup(key, kind string) (Provider, bool) {
	mu.Lock()
	defer mu.Unlock()
	p, ok := providers[key][kind]
	return p, ok
}

// Usage contains usage information for a provider.
type Usage struct {
	Kind, Arg, Usage string
}

// Help returns Usages, organized by key.
func Help() map[string][]Usage {
	mu.Lock()
	defer mu.Unlock()
	help := make(map[string][]Usage)
	for key, keyProviders := range providers {
		var usages []Usage
		for name, provider := range keyProviders {
			usages = append(usages, Usage{
				Kind:  name,
				Arg:   provider.Arg,
				Usage: provider.Usage,
			})
		}
		help[key] = usages
	}
	return help
}

func peel(s, sep string) (head, tail string) {
	switch parts := strings.SplitN(s, sep, 2); len(parts) {
	case 1:
		return parts[0], ""
	case 2:
		return parts[0], parts[1]
	default:
		panic("bug")
	}
}
