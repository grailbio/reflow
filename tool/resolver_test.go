package tool

import (
	"context"
	"testing"

	"github.com/docker/docker/api/types"
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
	if testing.Short() {
		t.Skip("requires network access")
	}

	testCases := []struct {
		image     string
		canonical string
	}{
		{
			image:     "grailbio/awstool",
			canonical: "index.docker.io/grailbio/awstool@sha256:b9a5e983e2de3f5319bca2fc015d279665096af20a27013c90583ac899c8b35a",
		},
	}

	r := imageResolver{
		authenticator: nilAuthenticator{},
	}
	for _, testCase := range testCases {
		canonical, err := r.resolveImages(context.Background(), []string{testCase.image})
		if err != nil {
			t.Errorf("error while getting canonical name for %s", testCase.image)
		}
		if got, want := canonical[testCase.image], testCase.canonical; got != want {
			t.Errorf("expected %s, got %s", want, got)
		}
	}
}
