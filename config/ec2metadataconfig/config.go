// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package ec2config defines and registers configuration providers
// using Amazon's EC2 metadata service. It is imported for its side
// effects. Note that ec2config does not marshal its key material.
// In this configuration, it's expected that reflowlets can also derive
// their credentials directly from the ec2 metdata service.
package ec2metadataconfig

import (
	"sync"

	awspkg "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/ec2rolecreds"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/reflow/config"
)

func init() {
	config.Register(config.AWS, "ec2metadata", "", "use EC2/IAM role credentials",
		func(cfg config.Config, arg string) (config.Config, error) {
			return &aws{Config: cfg}, nil
		},
	)
}

type aws struct {
	config.Config

	docOnce sync.Once
	sess    *session.Session
	doc     ec2metadata.EC2InstanceIdentityDocument
	creds   *credentials.Credentials
	err     error
}

func (a *aws) get() {
	a.sess, a.err = session.NewSession()
	if a.err != nil {
		return
	}
	metaClient := ec2metadata.New(a.sess)
	provider := &ec2rolecreds.EC2RoleProvider{Client: metaClient}
	a.creds = credentials.NewCredentials(provider)
	a.doc, a.err = metaClient.GetInstanceIdentityDocument()
}

func (a *aws) AWS() (*session.Session, error) {
	a.docOnce.Do(a.get)
	if a.err != nil {
		return nil, a.err
	}
	return session.NewSession(&awspkg.Config{
		Credentials: a.creds,
		Region:      awspkg.String(a.doc.Region),
	})
}

func (a *aws) AWSRegion() (string, error) {
	a.docOnce.Do(a.get)
	return a.doc.Region, a.err
}

func (*aws) AWSCreds() (*credentials.Credentials, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	metaClient := ec2metadata.New(sess)
	provider := &ec2rolecreds.EC2RoleProvider{Client: metaClient}
	creds := credentials.NewCredentials(provider)
	return creds, nil
}
