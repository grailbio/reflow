// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package rest provides a framework for serving and accessing
// hierarchical resource-based APIs. A REST server exports a node
// tree. The tree is explored in the manner of a filesystem: we walk
// a tree along a path, and, when our destination is reached, an
// operation is invoked on it.
//
// This structure encourages good "proper" REST implementations--each
// node represents a resource, and also promotes good separation of
// concerns: for example, the existence of an object is checked while
// walking the path.
//
// The REST client is not (yet) so fully committed to this idea;
// instead it provides a convenient API to access REST services.
//
// Package rest uses the reflow errors package in order to unify
// error handling.
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
	"path"
	"strings"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

// Call represents an incoming call to be serviced. Calls encapsulate
// the complete lifecycle of a REST transaction, including error
// handling.
type Call struct {
	writer http.ResponseWriter
	req    *http.Request
	err    error
	code   int
	reply  interface{}
	done   bool
	log    *log.Logger
}

func (c *Call) String() string {
	return fmt.Sprintf("%s %s", c.Method(), c.URL())
}

// Allow admits a set of methods to this call. If the call's method
// is not among the ones passed in, Allow returns false and fails the
// call with a http.StatusMethodNotAllowed error.
func (c *Call) Allow(methods ...string) bool {
	for _, m := range methods {
		if c.req.Method == m {
			return true
		}
	}
	c.code = http.StatusMethodNotAllowed
	c.reply = errors.E(c.req.Method, c.req.URL.String(), errors.NotSupported)
	return false
}

// StreamingCall implements the writer interface to write a chunk of
// bytes to the response writer and flush.
type StreamingCall struct {
	*Call
}

// Write writes the data to the response writer and flushes it.
func (c *StreamingCall) Write(p []byte) (n int, err error) {
	m, err := io.Copy(c.writer, bytes.NewReader(p))
	n = int(m)
	if f, ok := c.writer.(http.Flusher); ok {
		f.Flush()
	}
	return
}

// Write sets the call's status code and replies with the contents
// of the passed-in io.Reader.
func (c *Call) Write(code int, r io.Reader) error {
	c.code = code
	c.writer.WriteHeader(c.code)
	_, err := io.Copy(c.writer, r)
	c.done = true
	return err
}

// Method returns the method used in this call.
func (c *Call) Method() string {
	return c.req.Method
}

// Header returns the HTTP headers set by the client.
func (c *Call) Header() http.Header {
	return c.req.Header
}

// ReplyHeader returns the HTTP headers used in the call's reply.
func (c *Call) ReplyHeader() http.Header {
	return c.writer.Header()
}

// URL returns the url of this call.
func (c *Call) URL() *url.URL {
	return c.req.URL
}

// GetQueryParam returns the query parameter string for key k on the
// call request.
func (c *Call) GetQueryParam(k string) string {
	u := c.req.URL
	return u.Query().Get(k)
}

// Done tells if the call is done--whether it is replied to
// or has failed.
func (c *Call) Done() bool {
	return c.err != nil || c.code != 0
}

// Err returns the error of this call, if any.
func (c *Call) Err() error {
	return c.err
}

// Error sets an error for this call.
func (c *Call) Error(err error) {
	c.err = err
}

// Body returns an io.Reader containing the request
// body of the call.
func (c *Call) Body() io.Reader {
	return c.req.Body
}

// Unmarshal unmarshal's the call's request body into v using Go's
// JSON decoder. If unmarshalling fails, Unmarshal returns an error
// and also fails the call with a http.StatusBadRequest error.
func (c *Call) Unmarshal(v interface{}) error {
	err := json.NewDecoder(c.req.Body).Decode(v)
	if err != nil {
		c.code = http.StatusBadRequest
		c.reply = errors.E("unmarshal", fmt.Sprint(v), err)
	}
	return err
}

func (c *Call) UnmarshalFileset(fs *reflow.Fileset) error {
	err := fs.Read(c.req.Body, assoc.FilesetV2)
	if err != nil {
		c.code = http.StatusBadRequest
		c.reply = errors.E("unmarshal fileset v2", err)
	}
	return err
}

// Reply replies to the call with the given code and reply. The reply
// is marshalled using Go's JSON encoder.
func (c *Call) Reply(code int, reply interface{}) {
	if c.Done() {
		return
	}
	c.code, c.reply = code, reply
}

// Replyf formats a string and uses it to reply to the call.
func (c *Call) Replyf(code int, format string, args ...interface{}) {
	c.Reply(code, fmt.Sprintf(format, args...))
}

func (c *Call) flush() {
	var (
		reply interface{}
		code  int
	)
	if c.done {
		return
	}
	if c.err != nil {
		code, reply = errorToHTTP(c.err)
	} else if c.code == 0 {
		code, reply = errorToHTTP(errors.New("server failed to respond"))
	} else {
		code = c.code
		reply = c.reply
	}
	if c.log.At(log.DebugLevel) {
		c.log.Debugf("response %s %d %v", c, code, reply)
	}
	if reply != nil {
		c.writer.Header().Set("Content-Type", "application/json; charset=UTF-8")
	}
	c.writer.WriteHeader(code)
	if reply != nil {
		if fs, ok := reply.(reflow.Fileset); ok {
			if err := fs.Write(c.writer, assoc.FilesetV2, true, false); err != nil {
				panic(err)
			}
		} else {
			if err := json.NewEncoder(c.writer).Encode(reply); err != nil {
				panic(err)
			}
		}
	}
}

// Node is a node in a REST resource tree.
type Node interface {
	// Walk returns the child node named path while servicing the given
	// call. Walk returns nil when no such child exist.
	Walk(ctx context.Context, call *Call, path string) Node

	// Do services a call on this node. The call must be serviced by the
	// time Do returns.
	Do(ctx context.Context, call *Call)
}

// Mux is a node multiplexer.
type Mux map[string]Node

// Walk returns the entry path in Mux.
func (m Mux) Walk(ctx context.Context, call *Call, path string) Node {
	return m[path]
}

// Do replies to the call with http.StatusNotFound.
func (m Mux) Do(ctx context.Context, call *Call) {
	call.Reply(http.StatusNotFound, nil)
}

// WalkFunc is an adapter that allows the use of ordinary functions
// as multiplexer nodes.
type WalkFunc func(string) Node

// Walk invokes the function WalkFunc
func (f WalkFunc) Walk(ctx context.Context, call *Call, path string) Node { return f(path) }

// Do replies to the call with http.StatusNotFound
func (f WalkFunc) Do(ctx context.Context, call *Call) { call.Reply(http.StatusNotFound, nil) }

// DoFunc is an adapter that allows the use of ordinary functions
// as call nodes.
type DoFunc func(context.Context, *Call)

// Walk returns nil.
func (f DoFunc) Walk(ctx context.Context, call *Call, path string) Node {
	return nil
}

// Do invokes DoFunc.
func (f DoFunc) Do(ctx context.Context, call *Call) { f(ctx, call) }

type nodeHandler struct {
	root Node
	log  *log.Logger
}

// Handler creates a http.Handler from a root node. The returned
// handler serves requests by walking the URL's path starting from
// the root node, constructing a Call, and invoking the node's Do
// method. It returns http.StatusNotFound if there is no node
// corresponding to the given path.
func Handler(root Node, log *log.Logger) http.Handler {
	return &nodeHandler{root, log}
}

func (h *nodeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.log.At(log.DebugLevel) {
		b, err := httputil.DumpRequest(r, true)
		if err != nil {
			panic(err)
		}
		h.log.Debugf("request %s", string(b))
	}
	ctx := r.Context()
	call := &Call{writer: w, req: r, log: h.log}
	defer call.flush()
	path := path.Clean(r.URL.Path)
	elems := strings.Split(path, "/")
	n := h.root
	for _, e := range elems {
		if e == "" {
			continue
		}
		n = n.Walk(ctx, call, e)
		if call.Done() {
			return
		}
		if n == nil {
			call.Reply(http.StatusNotFound, errors.E("servehttp", path, e, errors.NotExist))
			return
		}
	}
	n.Do(ctx, call)
}

type doFuncHandler struct {
	node DoFunc
	log  *log.Logger
}

// DoFuncHandler creates a http.Handler from a DoFunc. The returned
// handler serves requests by simply invoking the node's Do
// method.
// This handler should be registered directly on a leaf path.
func DoFuncHandler(node DoFunc, log *log.Logger) http.Handler {
	return &doFuncHandler{node, log}
}

func (h *doFuncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.log.At(log.DebugLevel) {
		b, err := httputil.DumpRequest(r, true)
		if err != nil {
			panic(err)
		}
		h.log.Debugf("request %s", string(b))
	}
	ctx := r.Context()
	call := &Call{writer: w, req: r, log: h.log}
	defer call.flush()
	h.node.Do(ctx, call)
}

type doProxyHandler struct {
	url string
	log *log.Logger
}

// DoProxyHandler creates an http.Handler from a url. The returned
// handler serves requests by returning the result of a GET request
// to this URL.
func DoProxyHandler(url string, log *log.Logger) http.Handler {
	return &doProxyHandler{url, log}
}

func (h *doProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.log.At(log.DebugLevel) {
		b, err := httputil.DumpRequest(r, true)
		if err != nil {
			panic(err)
		}
		h.log.Debugf("request %s", string(b))
	}
	resp, getErr := http.Get(h.url)
	if getErr != nil {
		panic(getErr)
	}
	if _, copyErr := io.Copy(w, resp.Body); copyErr != nil {
		panic(copyErr)
	}
}

func errorToHTTP(err error) (code int, reply interface{}) {
	e := errors.Recover(err)
	return e.HTTPStatus(), e
}
