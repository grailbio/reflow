// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package ecrauth provides an interface and utilities for
// authenticating AWS EC2 ECR Docker repositories.
package ecrauth

import (
	"context"
	"fmt"

	"github.com/docker/docker/api/types"
)

// Interface is the interface that is implemented by ECR
// authentication providers.
type Interface interface {
	// Authenticates tells whether this authenticator can authenticate the
	// provided image URI.
	Authenticates(ctx context.Context, image string) (bool, error)

	// Authenticate writes authentication information into the provided config struct.
	Authenticate(ctx context.Context, cfg *types.AuthConfig) error
}

// Login authenticates via the provided authenticator and then
// returns the corresponding Docker login command.
func Login(ctx context.Context, auth Interface) (string, error) {
	var cfg types.AuthConfig
	if err := auth.Authenticate(context.TODO(), &cfg); err != nil {
		return "", err
	}
	return fmt.Sprintf(
		"docker login -u %s -p %s https://%s",
		cfg.Username, cfg.Password, cfg.ServerAddress), nil
}
