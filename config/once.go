// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"crypto/tls"
	"sync"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
)

// OnceConfig memoizes the first call of the following methods to the
// underlying config: Assoc, Repository, AWS, AWCreds, and HTTPS.
type OnceConfig struct {
	Config

	assocOnce sync.Once
	assoc     assoc.Assoc
	assocErr  error

	repositoryOnce sync.Once
	repository     reflow.Repository
	repositoryErr  error

	awsOnce sync.Once
	aws     *session.Session
	awsErr  error

	awsCredsOnce sync.Once
	awsCreds     *credentials.Credentials
	awsCredsErr  error

	httpsOnce                sync.Once
	httpsClient, httpsServer *tls.Config
	httpsErr                 error
}

// Once constructs a new OnceConfig using the provided
// underlying configuration.
func Once(cfg Config) *OnceConfig {
	return &OnceConfig{Config: cfg}
}

// Assoc returns the result of the first call to the underlying
// configuration's Assoc.
func (o *OnceConfig) Assoc() (assoc.Assoc, error) {
	o.assocOnce.Do(func() {
		o.assoc, o.assocErr = o.Config.Assoc()
	})
	return o.assoc, o.assocErr
}

// Repository returns the result of the first call to the underlying
// configuration's Repository.
func (o *OnceConfig) Repository() (reflow.Repository, error) {
	o.repositoryOnce.Do(func() {
		o.repository, o.repositoryErr = o.Config.Repository()
	})
	return o.repository, o.repositoryErr
}

// AWS returns the result of the first call to the underlying
// configuration's AWS.
func (o *OnceConfig) AWS() (*session.Session, error) {
	o.awsOnce.Do(func() {
		o.aws, o.awsErr = o.Config.AWS()
	})
	return o.aws, o.awsErr
}

// AWSCreds returns the result of the first call to the underlying
// configuration's AWSCreds.
func (o *OnceConfig) AWSCreds() (*credentials.Credentials, error) {
	o.awsCredsOnce.Do(func() {
		o.awsCreds, o.awsCredsErr = o.Config.AWSCreds()
	})
	return o.awsCreds, o.awsCredsErr
}

// HTTPS returns the result of the first call to the underlying
// configuration's HTTPS.
func (o *OnceConfig) HTTPS() (client, server *tls.Config, err error) {
	o.httpsOnce.Do(func() {
		o.httpsClient, o.httpsServer, o.httpsErr = o.Config.HTTPS()
	})
	return o.httpsClient, o.httpsServer, o.httpsErr
}
