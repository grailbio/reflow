// Package awsenvconfig configures AWS credentials to be derived from
// the user's environment. These credentials are also marshaled in
// the configuration so that they may be used by reflowlets.
package awsenvconfig

import (
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
				c := &static{Config: cfg}
				p, err := yaml.Marshal(v)
				if err != nil {
					return nil, err
				}
				if err := yaml.Unmarshal(p, &c.creds); err != nil {
					return nil, err
				}
				return &credentialsSession{c}, nil
			}
			return &credentialsSession{&env{cfg}}, nil
		},
	)
}

type static struct {
	config.Config
	creds credentials.Value
}

func (s *static) AWSCreds() (*credentials.Credentials, error) {
	return credentials.NewStaticCredentialsFromCreds(s.creds), nil
}

type env struct {
	config.Config
}

func (e *env) Marshal(keys config.Keys) error {
	if err := e.Config.Marshal(keys); err != nil {
		return err
	}
	creds, _ := e.AWSCreds()
	val, err := creds.Get()
	if err != nil {
		return err
	}
	keys[awsenv] = val
	return nil
}

func (e *env) AWSCreds() (*credentials.Credentials, error) {
	return credentials.NewEnvCredentials(), nil
}

type credentialsSession struct {
	config.Config
}

func (c *credentialsSession) AWS() (*session.Session, error) {
	creds, err := c.AWSCreds()
	if err != nil {
		return nil, err
	}
	region, err := c.AWSRegion()
	if err != nil {
		return nil, err
	}
	return session.NewSession(&aws.Config{
		Credentials: creds,
		Region:      aws.String(region),
	})
}
