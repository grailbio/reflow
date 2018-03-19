// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package awsenvconfig configures AWS configuration to be derived from
// the user's environment in accordance with the AWS SDK.
package awsenvconfig

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/reflow/config"
	yaml "gopkg.in/yaml.v2"
)

const awsenv = "awsenv"

func init() {
	config.Register(config.AWS, "awsenv", "", "configure AWS credentials from the user's environment",
		func(cfg config.Config, arg string) (config.Config, error) {
			if v := cfg.Value(awsenv); v != nil {
				p, err := yaml.Marshal(v)
				if err != nil {
					return nil, err
				}
				v := new(credentialsSessionValue)
				if err := yaml.Unmarshal(p, &v); err != nil {
					return nil, err
				}
				v.Config = cfg
				return v, nil
			}
			return &credentialsSession{Config: cfg}, nil
		},
	)
}

// A credentialsSessionValue is a Config that contains a marshaled
// (i.e., snapshotted in time) credentialsSession. It
// CredentialsSessions marshal into credentialsSessionValues.
type credentialsSessionValue struct {
	config.Config `yaml:"-"`
	Credentials   credentials.Value
	Region        string

	sessionOnce sync.Once
	session     *session.Session
	err         error
}

func (v *credentialsSessionValue) Marshal(keys config.Keys) error {
	if err := v.Config.Marshal(keys); err != nil {
		return err
	}
	keys[awsenv] = *v
	return nil
}

func (v *credentialsSessionValue) AWSCreds() (*credentials.Credentials, error) {
	return credentials.NewStaticCredentialsFromCreds(v.Credentials), nil
}

func (v *credentialsSessionValue) AWS() (*session.Session, error) {
	v.sessionOnce.Do(func() {
		creds, _ := v.AWSCreds()
		v.session, v.err = session.NewSession(&aws.Config{
			Credentials: creds,
			Region:      aws.String(v.Region),
		})
	})
	return v.session, v.err
}

// AcredentialsSession implements derives AWS configuration
// A(AWSCreds, WSRegion, AWS) from the user's environment, using the
// ASDK's defaults.
type credentialsSession struct {
	config.Config
	sessionOnce sync.Once
	session     *session.Session
	err         error
}

func (c *credentialsSession) Marshal(keys config.Keys) error {
	if err := c.Config.Marshal(keys); err != nil {
		return err
	}
	creds, _ := c.AWSCreds()
	val, err := creds.Get()
	if err != nil {
		return err
	}
	region, err := c.AWSRegion()
	if err != nil {
		return err
	}
	keys[awsenv] = credentialsSessionValue{
		Credentials: val,
		Region:      region,
	}
	return nil
}

func (c *credentialsSession) AWSCreds() (*credentials.Credentials, error) {
	sess, err := c.AWS()
	if err != nil {
		return nil, err
	}
	return sess.Config.Credentials, nil
}

func (c *credentialsSession) AWSRegion() (string, error) {
	sess, err := c.AWS()
	if err != nil {
		return "", err
	}
	if region := sess.Config.Region; region != nil {
		return *region, nil
	}
	return "", nil
}

func (c *credentialsSession) AWS() (*session.Session, error) {
	c.sessionOnce.Do(func() {
		// session.NewSession() uses a chain provider that looks for
		// credentials first in the environment variables, then in shared
		// credential locations (e.g. ~/.aws/config.yaml), then at remote
		// credential providers (EC2 or ECS roles, ...).  We do not want
		// remote credential providers here, as the credentials are
		// then temporary and cannot be passed to reflowlets,
		// see https://github.com/grailbio/reflow/issues/29.
		// That's why we have a custom chain provider here, without
		// the remote credential providers.
		credProvider := &credentials.ChainProvider{
			VerboseErrors: true,
			Providers: []credentials.Provider{
				&credentials.EnvProvider{},
				&credentials.SharedCredentialsProvider{
					Filename: "",
					Profile:  "",
				},
			},
		}
		// We do a retrieval here to catch NoCredentialProviders errors
		// early on.
		if _, err := credProvider.Retrieve(); err != nil {
			c.err = fmt.Errorf("cannot retrieve AWS credentials: %v", err)
			return
		}
		c.session, c.err = session.NewSession(&aws.Config{
			Credentials: credentials.NewCredentials(credProvider),
		})
	})
	return c.session, c.err
}
