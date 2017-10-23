package local

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"strings"
	"sync"

	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/grailbio/reflow/internal/ecrauth"
)

// imageExists checks whether an image exists at a Docker client.
func imageExists(ctx context.Context, client *client.Client, id string) (bool, error) {
	images, err := client.ImageList(ctx, types.ImageListOptions{})
	if err != nil {
		return false, err
	}
	for _, image := range images {
		for _, tag := range image.RepoTags {
			i := strings.LastIndex(tag, ":")
			if i > 0 {
				tag = tag[0:i]
			}
			if tag == id {
				return true, nil
			}
		}
	}
	return false, nil
}

// pullImage pulls an image (by reference) to a Docker client using an authenticator.
func pullImage(ctx context.Context, client *client.Client, authenticator ecrauth.Interface, ref string) error {
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
	resp, err := client.ImagePull(ctx, ref, options)
	if err != nil {
		return err
	}
	// TODO(marius): report progress up the chain.
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
	clientIm = map[*client.Client]map[string]*image{}
)

// ensureImage returns nil when the image is known to be present
// at the given Docker client. ensureImage ensures that there is only
// one concurrent pull per image, per client.
func ensureImage(ctx context.Context, client *client.Client, authenticator ecrauth.Interface, ref string) error {
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
	im.err = pullImage(ctx, client, authenticator, ref)
	if im.err != nil {
		// Let subsequent fetches retry.
		clientMu.Lock()
		delete(images, ref)
		clientMu.Unlock()
	}
	return im.err
}
