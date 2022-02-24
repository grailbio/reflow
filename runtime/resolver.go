package runtime

import (
	"context"
	"sync"
	"time"

	"docker.io/go-docker/api/types"
	"github.com/google/go-containerregistry/pkg/authn"
	imgname "github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/flow"
	"github.com/grailbio/reflow/internal/ecrauth"
	"github.com/grailbio/reflow/log"
)

// ImageResolver maintains maps of image descriptions to their canonical values
// (fully qualified registry host, and digest based references)
type ImageResolver struct {
	// Authenticator will have a nil AWS session if the config does not have
	// AWS credentials, in which case it is expected that none of the images are
	// ECR images.
	Authenticator ecrauth.Interface

	// Cached ECR credentials from the Authenticator, populated only if needed.
	ecrCreds *types.AuthConfig
	authOnce once.Task

	// TODO(sbagaria): When using this object for batch reflow runs, memoize
	// the calls to resolveImage.
}

func (r *ImageResolver) ResolveImages(ctx context.Context, images []string) (map[string]string, error) {
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

func (r *ImageResolver) resolveImage(ctx context.Context, image string) (string, error) {
	ecrImage, err := r.Authenticator.Authenticates(ctx, image)
	if err != nil {
		return "", err
	}
	var auth authn.Authenticator
	if ecrImage {
		if err = r.authenticate(ctx); err != nil {
			return "", err
		}
		auth = &authn.Basic{Username: r.ecrCreds.Username, Password: r.ecrCreds.Password}
	} else {
		auth = authn.Anonymous
	}

	ref, err := imageDigestReference(ctx, image, auth)
	if err != nil {
		return "", err
	}

	return ref, nil
}

func (r *ImageResolver) authenticate(ctx context.Context) error {
	return r.authOnce.Do(func() error {
		if r.ecrCreds != nil {
			return nil
		}
		r.ecrCreds = &types.AuthConfig{}
		return r.Authenticator.Authenticate(ctx, r.ecrCreds)
	})
}

func imageDigestReference(ctx context.Context, image string, auth authn.Authenticator) (string, error) {
	image, _, _ = flow.ImageQualifiers(image)
	ref, err := imgname.ParseReference(image, imgname.WeakValidation)
	if err != nil {
		return "", errors.E(err, "tool.imageDigestReference", "parse", image)
	}

	var img v1.Image
	retryPolicy := retry.MaxRetries(retry.Backoff(time.Second, 10*time.Second, 1.5), 5)
	for retries := 0; ; retries++ {
		img, err = remote.Image(ref, remote.WithAuth(auth))
		if err == nil {
			break
		}
		log.Printf("retrying connecting to container registry for %q: %v", ref.Name(), err)
		if err := retry.Wait(ctx, retryPolicy, retries); err != nil {
			// Mark error as kind Unavailable to indicate that this could be a
			// transient error.  If we do not explicitly mark as Unavailable, then
			// errors of type TooManyTries from grailbio/base do not get assigned to
			// the right reflow/errors kind.
			return "", errors.E(errors.Unavailable, err, "tool.imageDigestReference", "remote", ref.Name())
		}
	}

	for retries := 0; ; retries++ {
		hash, err := img.Digest()
		if err == nil {
			return ref.Context().Name() + "@" + hash.String(), nil
		}
		// Do not retry on certain errors.
		if e, ok := err.(*transport.Error); ok {
			for _, d := range e.Errors {
				switch d.Code {
				case transport.ManifestUnknownErrorCode,
					transport.NameInvalidErrorCode,
					transport.NameUnknownErrorCode,
					transport.UnauthorizedErrorCode,
					transport.DeniedErrorCode,
					transport.UnsupportedErrorCode:
					return "", errors.E(err, "tool.imageDigestReference", ref.Name())
				}
			}
		}
		log.Printf("retrying getting image manifest for %q: %v", ref.Name(), err)
		if err := retry.Wait(ctx, retryPolicy, retries); err != nil {
			return "", errors.E(errors.Unavailable, err, "tool.imageDigestReference", ref.Name())
		}
	}
}
