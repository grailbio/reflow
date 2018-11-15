// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package config

import (
	"crypto/tls"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
) // OnceConfig memoizes the first call of the following methods to the
// underlying config: Assoc, Repository, AWS, AWCreds, and HTTPS.
type OnceConfig struct {
	Config

	assocOnce once.Task
	assoc     assoc.Assoc

	repositoryOnce once.Task
	repository     reflow.Repository

	awsOnce once.Task
	aws     *session.Session

	awsCredsOnce once.Task
	awsCreds     *credentials.Credentials

	httpsOnce                once.Task
	httpsClient, httpsServer *tls.Config
}

// Once constructs a new OnceConfig using the provided
// underlying configuration.
func Once(cfg Config) *OnceConfig {
	return &OnceConfig{Config: cfg}
}

// Assoc returns the result of the first call to the underlying
// configuration's Assoc.
func (o *OnceConfig) Assoc() (assoc.Assoc, error) {
	err := o.assocOnce.Do(func() (err error) {
		o.assoc, err = o.Config.Assoc()
		return
	})
	return o.assoc, err
}

// Repository returns the result of the first call to the underlying
// configuration's Repository.
func (o *OnceConfig) Repository() (reflow.Repository, error) {
	err := o.repositoryOnce.Do(func() (err error) {
		o.repository, err = o.Config.Repository()
		return
	})
	return o.repository, err
}

// AWS returns the result of the first call to the underlying
// configuration's AWS.
func (o *OnceConfig) AWS() (*session.Session, error) {
	err := o.awsOnce.Do(func() (err error) {
		o.aws, err = o.Config.AWS()
		return
	})
	return o.aws, err
}

// AWSCreds returns the result of the first call to the underlying
// configuration's AWSCreds.
func (o *OnceConfig) AWSCreds() (*credentials.Credentials, error) {
	err := o.awsCredsOnce.Do(func() (err error) {
		o.awsCreds, err = o.Config.AWSCreds()
		return
	})
	return o.awsCreds, err
}

// HTTPS returns the result of the first call to the underlying
// configuration's HTTPS.
func (o *OnceConfig) HTTPS() (client, server *tls.Config, err error) {
	err = o.httpsOnce.Do(func() (err error) {
		o.httpsClient, o.httpsServer, err = o.Config.HTTPS()
		return
	})
	return o.httpsClient, o.httpsServer, err
}
