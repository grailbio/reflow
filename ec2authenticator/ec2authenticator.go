// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package ec2authenticator implements Docker repository
// authentication for ECR using an AWS SDK session and a root.
package ec2authenticator

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"docker.io/go-docker/api/types"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecr"
)

var ecrURI = regexp.MustCompile(`^[0-9]+\.dkr\.ecr\.[a-z0-9-]+\.amazonaws.com/.*$`)

// T is a Docker repository authenticator for ECR repositories.
type T struct {
	// Session is an AWS SDK session that is authorized
	// to generate authorization tokens for the account's
	// ECR repository.
	Session *session.Session
}

// New returns a new ec2authenticator based on the provided session.
func New(sess *session.Session) *T {
	return &T{sess}
}

// Authenticates tells whether the authenticator can authenticate the provided image.
func (a *T) Authenticates(ctx context.Context, image string) (bool, error) {
	return ecrURI.MatchString(image), nil
}

// Authenticate deposits Docker repository authentication material
// for the ECR repository into the provided cfg object.
func (a *T) Authenticate(ctx context.Context, cfg *types.AuthConfig) error {
	if a.Session == nil {
		return errors.New("AWS credentials not present")
	}
	resp, err := ecr.New(a.Session).GetAuthorizationToken(&ecr.GetAuthorizationTokenInput{})
	if err != nil {
		return err
	}
	if len(resp.AuthorizationData) == 0 {
		return errors.New("no authorization data from ECR")
	}
	auth := resp.AuthorizationData[0]
	if auth.AuthorizationToken == nil || auth.ProxyEndpoint == nil || auth.ExpiresAt == nil {
		return errors.New("bad authorization data from ECR")
	}
	b, err := base64.StdEncoding.DecodeString(*auth.AuthorizationToken)
	if err != nil {
		return err
	}
	up := strings.SplitN(string(b), ":", 2)
	if len(up) != 2 {
		return fmt.Errorf("malformed authorization token %q", string(b))
	}
	cfg.Username, cfg.Password = up[0], up[1]
	u, err := url.Parse(*auth.ProxyEndpoint)
	if err != nil {
		return err
	}
	cfg.ServerAddress = u.Host
	return nil
}
