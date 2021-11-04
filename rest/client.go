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
	"strings"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
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
	return &ClientCall{
		Client:      c,
		Header:      http.Header{},
		method:      method,
		path:        fmt.Sprintf(format, args...),
		queryParams: map[string]string{},
	}
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
	Header      http.Header
	method      string
	path        string
	queryParams map[string]string
	resp        *http.Response
	err         error
}

func (c *ClientCall) String() string {
	return fmt.Sprintf("%s %s", c.method, c.path)
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

// SetQueryParam adds or overwrites a query parameter on the call.
func (c *ClientCall) SetQueryParam(k, v string) {
	c.queryParams[k] = v
}

// Do performs a call with the given context and body. It returns the
// HTTP status code for the reply, or a non-nil error if one occured.
func (c *ClientCall) Do(ctx context.Context, body io.Reader) (int, error) {
	var r *http.Request
	r, c.err = http.NewRequest(c.method, "", body)
	if c.err != nil {
		return 0, c.err
	}
	// We need to be careful to split the query portion of the path
	// (which should be escaped) and the path itself.
	var path, query string
	parts := strings.SplitN(c.path, "?", 2)
	path = parts[0]
	if len(parts) == 2 {
		query = parts[1]
	}
	r.URL = c.url.ResolveReference(&url.URL{Path: path, RawQuery: query})
	// add query parameters
	q := r.URL.Query()
	for k, v := range c.queryParams {
		q.Set(k, v)
	}
	r.URL.RawQuery = q.Encode()
	r.Header = c.Header
	if c.log.At(log.DebugLevel) {
		b, err := httputil.DumpRequest(r, true)
		if err != nil {
			return 0, err
		}
		c.log.Debugf("request %s", string(b))
	}
	var err error
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
			c.log.Debugf("response %s body %v", c, c.resp.Body)
			b, err := httputil.DumpResponse(c.resp, true)
			if err != nil {
				return 0, err
			}
			c.log.Debugf("response %s [%d] %s", c, c.resp.ContentLength, string(b))
		} else if c.err != nil {
			c.log.Debugf("response %s error %s", c, c.err)
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

func (c *ClientCall) DoFileset(ctx context.Context, fs reflow.Fileset) (int, error) {
	r, w := io.Pipe()
	var streamErr error
	go func() {
		streamErr = fs.Write(w, assoc.FilesetV2, true, false)
		_ = w.CloseWithError(streamErr)
	}()
	code, err := c.Do(ctx, r)
	if streamErr != nil {
		return 0, streamErr
	}
	return code, err
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

// Body returns the reader for the call's reply body.
func (c *ClientCall) Body() io.ReadCloser {
	return c.resp.Body
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

// ContentLength returns the content length of the reply.
// Unless the request's method is HEAD, this is the number
// of bytes that may be read from the call.
func (c *ClientCall) ContentLength() int64 {
	return c.resp.ContentLength
}
