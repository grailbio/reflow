// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package client implements repository REST client.
package client

import (
	"context"
	"io"
	"net/http"
	"net/url"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/rest"
)

// Client is a Repository that dispatches requests to a server
// implementing the Repository REST API.
type Client struct {
	*rest.Client
	Short string
}

func (c *Client) ShortString() string {
	if c.Short != "" {
		return c.Short
	}
	return c.String()
}

func (c *Client) String() string {
	return "remote:" + c.Client.URL().String()
}

// Stat queries the repository for the file metadata for the given object.
func (c *Client) Stat(ctx context.Context, id digest.Digest) (reflow.File, error) {
	call := c.Call("HEAD", "%s", id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return reflow.File{}, errors.E("stat", id, err)
	}
	// HEAD requests are special: their bodies are dropped by Go's
	// HTTP library. So we need to reconstruct errors here.
	switch code {
	case http.StatusOK:
		return reflow.File{ID: id, Size: call.ContentLength()}, nil
	case http.StatusNotFound:
		return reflow.File{}, errors.E(errors.NotExist, errors.Errorf("file %v does not exist", id))
	default:
		return reflow.File{}, call.Error()
	}
}

// Get retrieves the object with digest id.
func (c *Client) Get(ctx context.Context, id digest.Digest) (io.ReadCloser, error) {
	call := c.Call("GET", "%s", id)
	code, err := call.Do(ctx, nil)
	if err != nil {
		return nil, errors.E("get", id, err)
	}
	if code != http.StatusOK {
		defer call.Close()
		return nil, call.Error()
	}
	return call, nil
}

// Put writes the object in body to the repository.
func (c *Client) Put(ctx context.Context, body io.Reader) (digest.Digest, error) {
	call := c.Call("POST", "")
	defer call.Close()
	code, err := call.Do(ctx, body)
	if err != nil {
		return digest.Digest{}, errors.E("put", "<body>", err)
	}
	if code != http.StatusOK {
		return digest.Digest{}, call.Error()
	}
	var d digest.Digest
	err = call.Unmarshal(&d)
	return d, err
}

// WriteTo writes the object with digest id directly to the repository at URL u.
func (c *Client) WriteTo(ctx context.Context, id digest.Digest, u *url.URL) error {
	call := c.Call("POST", "%s/transfers", id)
	code, err := call.DoJSON(ctx, u.String())
	if err != nil {
		return errors.E("writeto", id, u.String(), err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}

// ReadFrom reads the object with digest id directly from the repository at URL u.
func (c *Client) ReadFrom(ctx context.Context, id digest.Digest, u *url.URL) error {
	call := c.Call("PUT", "%s", id)
	defer call.Close()
	call.Header.Add("reflow-read-from", u.String())
	code, err := call.Do(ctx, nil)
	if err != nil {
		return errors.E("readfrom", id, u.String(), err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}

// Collect instructs the repository collect all objects not in the liveset.
func (c *Client) Collect(ctx context.Context, live reflow.Liveset) error {
	call := c.Call("POST", "collect")
	defer call.Close()
	code, err := call.DoJSON(ctx, live)
	if err != nil {
		return errors.E("collect", err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}
