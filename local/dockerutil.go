// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"time"

	"docker.io/go-docker"
	"docker.io/go-docker/api/types"
	"github.com/docker/distribution/reference"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/ecrauth"
	"github.com/grailbio/reflow/log"
)

// imageExists checks whether an image exists at a Docker client.
func imageExists(ctx context.Context, client *docker.Client, id string) (bool, error) {
	ref, err := reference.Parse(id)
	if err != nil {
		return false, err
	}
	images, err := client.ImageList(ctx, types.ImageListOptions{})
	if err != nil {
		return false, err
	}

	var useDigest bool
	switch r := ref.(type) {
	case reference.Digested:
		useDigest = true
	case reference.Tagged:
		// Do nothing; needed for excluding tagged images in below case.
	case reference.Named:
		// Does not have digest or tag.
		ref, err = reference.WithTag(r, "latest")
		if err != nil {
			return false, err
		}
	}

	refStr := ref.String()
	for _, image := range images {
		if useDigest {
			for _, digest := range image.RepoDigests {
				if digest == refStr {
					return true, nil
				}
			}
		} else {
			for _, tag := range image.RepoTags {
				if tag == refStr {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

// pullImage pulls an image (by reference) to a Docker client using an authenticator.
func pullImage(ctx context.Context, client docker.APIClient, authenticator ecrauth.Interface, ref string, log *log.Logger) error {
	var options types.ImagePullOptions
	if authenticator != nil {
		if ok, err := authenticator.Authenticates(ctx, ref); ok && err == nil {
			var auth types.AuthConfig
			if err := authenticator.Authenticate(ctx, &auth); err != nil {
				return err
			}
			b, err := json.Marshal(auth)
			if err != nil {
				return err
			}
			options.RegistryAuth = base64.URLEncoding.EncodeToString(b)
		} else if err != nil {
			return err
		}
	}
	var (
		resp     io.ReadCloser
		maxTries = 5
		policy   = retry.MaxTries(retry.Backoff(time.Second, 10*time.Second, 1.5), maxTries)
	)
	for retries := 0; ; retries++ {
		log.Printf("pulling image (try %d/%d): %s", retries+1, maxTries, ref)
		var err error
		resp, err = client.ImagePull(ctx, ref, options)
		if err == nil {
			break
		}
		// docker/go-docker does not always report "not found" errors as such due to a bug,
		// so check the error string manually.
		if strings.HasSuffix(err.Error(), "not found") {
			return errors.E("pull image", ref, errors.NotExist, err)
		}
		if strings.Contains(err.Error(), "net/http") {
			return errors.E("pull image", ref, errors.Net, err)
		}
		if err := retry.Wait(ctx, policy, retries); err != nil {
			log.Printf("pull image got error: %s, will retry", err)
			// if we've exhausted the retry policy and the error is not one that we recognize,
			// return an errors.Other so that the scheduler does not retry it on the same alloc
			return errors.E("pull image", ref, err)
		}
	}
	defer resp.Close()
	decoder := json.NewDecoder(resp)
	// Docker sends status messages (e.g., "x% downloaded").
	// We don't currently display these, but nonetheless have to
	// consume them.
	for {
		var msg jsonmessage.JSONMessage
		if err := decoder.Decode(&msg); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if msg.Error != nil {
			return msg.Error
		}
	}
	return nil
}

// image manages the status of a single image that is either pulled
// or in the process of being pulled. It is used to rendezvous
// multiple execs that are pulling a single image.
type image struct {
	sync.Mutex
	*sync.Cond
	done bool
	err  error
}

var (
	clientMu sync.Mutex
	clientIm = map[*docker.Client]map[string]*image{}
)

// ensureImage returns nil when the image is known to be present
// at the given Docker client. ensureImage ensures that there is only
// one concurrent pull per image, per client.
func ensureImage(ctx context.Context, client *docker.Client, authenticator ecrauth.Interface, ref string, log *log.Logger) error {
	clientMu.Lock()
	images := clientIm[client]
	if images == nil {
		images = map[string]*image{}
		clientIm[client] = images
	}
	im := images[ref]
	if im != nil {
		clientMu.Unlock()
		im.Lock()
		for !im.done {
			im.Wait()
		}
		im.Unlock()
		return im.err
	}
	im = &image{}
	im.Cond = sync.NewCond(im)
	images[ref] = im
	clientMu.Unlock()
	defer func() {
		im.Lock()
		im.done = true
		im.Broadcast()
		im.Unlock()
	}()
	if ok, _ := imageExists(ctx, client, ref); ok {
		return nil
	}
	im.err = pullImage(ctx, client, authenticator, ref, log)
	if im.err != nil {
		// Let subsequent fetches retry.
		clientMu.Lock()
		delete(images, ref)
		clientMu.Unlock()
	}
	return im.err
}
