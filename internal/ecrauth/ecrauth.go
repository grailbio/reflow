// Package ecrauth provides an interface and utilities for
// authenticating AWS EC2 ECR Docker repositories.
package ecrauth

import (
	"context"
	"fmt"

	"github.com/docker/engine-api/types"
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
		"docker login -u %s -p %s -e none https://%s",
		cfg.Username, cfg.Password, cfg.ServerAddress), nil
}
