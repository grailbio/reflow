package tool

import (
	"context"
	"fmt"
	"sync"

	"github.com/docker/docker/api/types"
	"github.com/google/go-containerregistry/pkg/authn"
	imgname "github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/internal/ecrauth"
) // ImageResolver maintains maps of image descriptions to their canonical values
// (fully qualified registry host, and digest based references)
type imageResolver struct {
	// Authenticator will have a nil AWS session if the config does not have
	// AWS credentials, in which case it is expected that none of the images are
	// ECR images.
	authenticator ecrauth.Interface

	// Cached ECR credentials from the authenticator, populated only if needed.
	ecrCreds *types.AuthConfig
	authOnce once.Task

	// TODO(sbagaria): When using this object for batch reflow runs, memoize
	// the calls to resolveImage.
}

func (r *imageResolver) resolveImages(ctx context.Context, images []string) (map[string]string, error) {
	var mu sync.Mutex
	imageMap := make(map[string]string)
	err := traverse.Each(len(images), func(i int) error {
		image := images[i]
		resolved, err := r.resolveImage(ctx, image)
		if err != nil {
			return err
		}
		mu.Lock()
		imageMap[image] = resolved
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	return imageMap, nil
}

func (r *imageResolver) resolveImage(ctx context.Context, image string) (string, error) {
	ecrImage, err := r.authenticator.Authenticates(ctx, image)
	if err != nil {
		return "", err
	}
	var auth authn.Authenticator
	if ecrImage {
		if err = r.authenticate(ctx); err != nil {
			return "", err
		}
		auth = &authn.Basic{Username: r.ecrCreds.Username, Password: r.ecrCreds.Password}
		if err != nil {
			return "", fmt.Errorf("making creds for %q: %v", image, err)
		}
	} else {
		auth = authn.Anonymous
	}

	ref, err := imageDigestReference(image, auth)
	if err != nil {
		return "", err
	}

	return ref, nil
}

func (r *imageResolver) authenticate(ctx context.Context) error {
	return r.authOnce.Do(func() error {
		if r.ecrCreds != nil {
			return nil
		}
		r.ecrCreds = &types.AuthConfig{}
		return r.authenticator.Authenticate(ctx, r.ecrCreds)
	})
}

func imageDigestReference(image string, auth authn.Authenticator) (string, error) {
	ref, err := imgname.ParseReference(image, imgname.WeakValidation)
	if err != nil {
		return "", fmt.Errorf("parsing tag %q: %v", ref, err)
	}

	img, err := remote.Image(ref, remote.WithAuth(auth))
	if err != nil {
		return "", fmt.Errorf("obtaining image for %q: %v", ref, err)
	}

	hash, err := img.Digest()
	if err != nil {
		return "", fmt.Errorf("obtaining manifest digest for %q: %v", ref, err)
	}
	return ref.Context().Name() + "@" + hash.String(), nil
}
