// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"golang.org/x/net/context/ctxhttp"
)

// Client is a REST client.
type Client struct {
	url    *url.URL
	client *http.Client
	log    *log.Logger
}

// NewClient returns a new REST client given an HTTP client and root URL.
func NewClient(client *http.Client, u *url.URL, log *log.Logger) *Client {
	return &Client{client: client, url: u, log: log}
}

// Walk constructs a new client based on client c with a root URL based
// on resolving the URL's relative reference vis-a-vis the formatted string.
func (c *Client) Walk(format string, args ...interface{}) (*Client, error) {
	u, err := url.Parse(fmt.Sprintf(format, args...))
	if err != nil {
		return nil, err
	}
	return &Client{
		url:    c.url.ResolveReference(u),
		client: c.client,
		log:    c.log,
	}, nil
}

// URL returns the client's root URL.
func (c *Client) URL() *url.URL { return c.url }

// Call constructs a ClientCall with the given method and path
// (relative to the client's root URL).
func (c *Client) Call(method, format string, args ...interface{}) *ClientCall {
	return &ClientCall{Client: c, Header: http.Header{}, method: method, path: url.QueryEscape(fmt.Sprintf(format, args...))}
}

// ClientCall represents a single call. It handles the entire call lifecycle.
// ClientCalls must be closed in order to relinquish resources. Typically,
// client code will invoke Close with a defer statement:
//
//	call := client.Call(...)
//	defer call.Close()
//
// ClientCall is also an io.ReadCloser for the reply body.
type ClientCall struct {
	*Client
	Header http.Header
	method string
	path   string
	resp   *http.Response
	err    error
}

// Err returns the call's error, if any.
func (c *ClientCall) Err() error {
	return c.err
}

// Error unmarshals a reflow error from the call's reply.
func (c *ClientCall) Error() *errors.Error {
	err := new(errors.Error)
	if err := c.Unmarshal(err); err != nil {
		return errors.Recover(err)
	}
	return err
}

// Message unmarshals a string message from the client.
func (c *ClientCall) Message() (string, error) {
	var m string
	err := c.Unmarshal(&m)
	return m, err
}

// Do performs a call with the given context and body. It returns the
// HTTP status code for the reply, or a non-nil error if one occured.
func (c *ClientCall) Do(ctx context.Context, body io.Reader) (int, error) {
	var r *http.Request
	r, c.err = http.NewRequest(c.method, "", body)
	if c.err != nil {
		return 0, c.err
	}
	u, err := url.Parse(c.path)
	if err != nil {
		return 0, err
	}
	r.URL = c.url.ResolveReference(u)
	r.Header = c.Header
	if c.log.At(log.DebugLevel) {
		b, err := httputil.DumpRequest(r, true)
		if err != nil {
			return 0, err
		}
		c.log.Debugf("request %s", string(b))
	}
	c.resp, err = ctxhttp.Do(ctx, c.client, r)
	switch err {
	case nil:
		c.err = nil
	case context.Canceled, context.DeadlineExceeded:
		c.err = errors.Recover(err)
	default:
		c.err = errors.E(errors.Net, err)
	}
	if c.log.At(log.DebugLevel) {
		if c.resp != nil {
			b, err := httputil.DumpResponse(c.resp, true)
			if err != nil {
				return 0, err
			}
			c.log.Debugf("response %s", string(b))
		} else if c.err != nil {
			c.log.Debugf("response error %s", c.err)
		}
	}
	if c.resp == nil {
		return 0, c.err
	}
	return c.resp.StatusCode, c.err
}

// DoJSON is like Do, except the request req is marshalled using Go's
// JSON encoder.
func (c *ClientCall) DoJSON(ctx context.Context, req interface{}) (int, error) {
	var body io.Reader
	if req != nil {
		b := new(bytes.Buffer)
		c.err = json.NewEncoder(b).Encode(req)
		if c.err != nil {
			return 0, c.err
		}
		body = b
	}
	return c.Do(ctx, body)
}

// Unmarshal unmarshals the call's reply using Go's JSON decoder.
func (c *ClientCall) Unmarshal(reply interface{}) error {
	if c.err != nil {
		return c.err
	}
	if reply != nil {
		c.err = json.NewDecoder(c.resp.Body).Decode(reply)
	}
	return c.err
}

// Read implements io.Reader for the call's reply body.
func (c *ClientCall) Read(p []byte) (n int, err error) {
	return c.resp.Body.Read(p)
}

// Close relinquishes resources associated with the call.
func (c *ClientCall) Close() error {
	if c.resp != nil {
		return c.resp.Body.Close()
	}
	return nil
}
