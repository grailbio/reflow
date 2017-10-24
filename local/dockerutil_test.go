// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit integration

package local

import (
	"context"
	"testing"

	dockerclient "github.com/docker/engine-api/client"
)

func newDockerClientOrSkip(t *testing.T) *dockerclient.Client {
	client, err := dockerclient.NewClient(
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
	const image = "grailbio/awstool"
	err := pullImage(ctx, client, nil, image)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := imageExists(ctx, client, image)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("image was not pulled")
	}
}
