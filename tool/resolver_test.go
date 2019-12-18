package tool

import (
	"context"
	"testing"

	"docker.io/go-docker/api/types"
)

type nilAuthenticator struct{}

// Authenticate implements ecrauth.Interface.
func (a nilAuthenticator) Authenticates(ctx context.Context, image string) (bool, error) {
	return false, nil
}

// Authenticates implements ecrauth.Interface.
func (a nilAuthenticator) Authenticate(ctx context.Context, cfg *types.AuthConfig) error {
	return nil
}

func TestResolveImages(t *testing.T) {
	const resolved = "index.docker.io/grailbio/awstool@sha256:b9a5e983e2de3f5319bca2fc015d279665096af20a27013c90583ac899c8b35a"
	if testing.Short() {
		t.Skip("requires network access")
	}
	r := ImageResolver{Authenticator: nilAuthenticator{}}
	for _, img := range []string{
		"grailbio/awstool",
		"grailbio/awstool$aws",
		"grailbio/awstool$docker",
		"grailbio/awstool$aws$docker",
		"grailbio/awstool$docker$aws",
	} {
		canonical, err := r.ResolveImages(context.Background(), []string{img})
		if err != nil {
			t.Errorf("error while getting canonical name for %s: %v", img, err)
			continue
		}
		if got, want := canonical[img], resolved; got != want {
			t.Errorf("expected %s, got %s", want, got)
		}
	}
}
