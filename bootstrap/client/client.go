// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package client implements a remote client for the reflow bootstrap.
package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/grailbio/reflow/bootstrap/common"

	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/rest"
)

// Client implements an http Client. Client serves two purposes:
// 1. To get the status of the bootstrap.
// 2. To push an image to bootstrap.
type Client struct {
	*rest.Client
	host string
}

// New creates a new Client which connects to a given host using the
// provided http.Client. If http.Client is nil, the default client is
// used. If logger is not nil, Client logs detailed request/response
// information to it.
func New(baseurl string, client *http.Client, log *log.Logger) (*Client, error) {
	url, err := url.Parse(baseurl)
	if err != nil {
		return nil, err
	}
	return &Client{Client: rest.NewClient(client, url, log), host: url.Host}, nil
}

// Status gets the status from the servlet.
func (c *Client) Status(ctx context.Context) error {
	call := c.Call("GET", "status")
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return err
	}
	if code != http.StatusOK {
		return fmt.Errorf("status %v: %v", code, call.Error())
	}
	return nil
}

// InstallImage instructs the bootstrap instance to install and run a new image.
func (c *Client) InstallImage(ctx context.Context, image common.Image) error {
	call := c.Call("POST", "execimage")
	defer call.Close()

	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	code, err := call.DoJSON(ctx, image)
	if err != nil && !errors.Is(errors.Net, err) {
		return fmt.Errorf("client.InstallImage: %v", err)
	}
	// If the call was successful and the image got 'exec'ed, we should get zero.
	// Anything else should be treated as an error.
	if code != 0 {
		return call.Error()
	}
	return nil
}
