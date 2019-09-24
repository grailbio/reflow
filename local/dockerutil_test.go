// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package local

import (
	"context"
	"testing"

	"docker.io/go-docker"
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
	err := pullImage(ctx, client, nil, images[0])
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
