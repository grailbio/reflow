// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

//go:build !unit || integration
// +build !unit integration

package local

import (
	"context"
	"fmt"
	"io"
	"testing"

	"docker.io/go-docker"
	"docker.io/go-docker/api/types"
	"github.com/grailbio/reflow/errors"
)

func newDockerClientOrSkip(t *testing.T) *docker.Client {
	client, err := docker.NewClient(
		"unix:///var/run/docker.sock", "1.22", /*client.DefaultVersion*/
		nil, map[string]string{"user-agent": "reflow"})
	if err != nil {
		t.Skip("error instantiating docker client:", err)
	}
	return client
}

func TestPullImage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	ctx := context.Background()
	// TODO(marius): test ECR authentication too.
	client := newDockerClientOrSkip(t)
	// Multiple representations of the same image.
	images := []string{
		"grailbio/awstool",
		"grailbio/awstool:latest",
		"grailbio/awstool@sha256:b9a5e983e2de3f5319bca2fc015d279665096af20a27013c90583ac899c8b35a",
	}
	err := pullImage(ctx, client, nil, images[0], nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, image := range images {
		ok, err := imageExists(ctx, client, image)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("image %s was not pulled", image)
		}
	}
}

type testDockerClient struct {
	docker.Client
	err        string
	imagePullN int
}

func (tdc *testDockerClient) ImagePull(ctx context.Context, refStr string, options types.ImagePullOptions) (io.ReadCloser, error) {
	tdc.imagePullN++
	return nil, fmt.Errorf(tdc.err)
}

func TestPullImageErrors(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	ctx := context.Background()
	tests := []struct {
		name                string
		image               string
		err                 string
		wantKind            errors.Kind
		wantNImagePullCalls int
	}{
		{"image not found", "invalid-test-image", "manifest for ... not found", errors.NotExist, 1},
		{"network error", "grailbio/awstool", "net/http: TLS handshake timeout", errors.Net, 1},
		{"other error", "grailbio/awstool", "some other error", errors.TooManyTries, 6},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cli := &testDockerClient{
				err: tt.err,
			}
			gotErr := pullImage(ctx, cli, nil, tt.image, nil)
			if got, want := errors.Recover(gotErr).Kind, tt.wantKind; got != want {
				t.Errorf("got %v, want %v", got, want)
			}
			if got, want := cli.imagePullN, tt.wantNImagePullCalls; got != want {
				t.Errorf("got %v, want %v calls to ImagePull", got, want)
			}
		})
	}
}
