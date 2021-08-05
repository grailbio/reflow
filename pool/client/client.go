// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package client implements a remoting client for reflow pools.
package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	repositoryclient "github.com/grailbio/reflow/repository/client"
	"github.com/grailbio/reflow/rest"
	"golang.org/x/sync/singleflight"
	"gopkg.in/yaml.v2"
)

// Client implements a reflow pool by dispatching calls to a remote
// implementation.
type Client struct {
	*rest.Client
	host  string
	group singleflight.Group
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

// ID returns the client's host name.
func (c *Client) ID() string { return c.host }

// Alloc looks up an alloc by name.
func (c *Client) Alloc(ctx context.Context, id string) (pool.Alloc, error) {
	call := c.Call("GET", "allocs/%s", id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return nil, errors.E("alloc", id, err)
	}
	if code != http.StatusOK {
		return nil, call.Error()
	}

	var json pool.AllocInspect
	if err := call.Unmarshal(&json); err != nil {
		return nil, errors.E("alloc", id, err)
	}
	return &clientAlloc{c, id, json.Resources}, nil
}

// Allocs enumerates all allocs available on the remote node.
func (c *Client) Allocs(ctx context.Context) ([]pool.Alloc, error) {
	call := c.Call("GET", "allocs/")
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return nil, errors.E("allocs", err)
	}
	if code != http.StatusOK {
		return nil, call.Error()
	}
	var jsons []pool.AllocInspect
	if err := call.Unmarshal(&jsons); err != nil {
		return nil, errors.E("allocs", err)
	}
	allocs := make([]pool.Alloc, len(jsons))
	for i, json := range jsons {
		allocs[i] = &clientAlloc{c, json.ID, json.Resources}
	}
	return allocs, nil
}

// Config retrieves the reflowlet instance's reflow config.
func (c *Client) Config(ctx context.Context) (infra.Keys, error) {
	call := c.Call("GET", "config")
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return nil, errors.E("config", err)
	}
	if code != http.StatusOK {
		return nil, call.Error()
	}
	var cfgStr string
	if err := call.Unmarshal(&cfgStr); err != nil {
		return nil, errors.E("unmarshal config ", err)
	}
	var keys infra.Keys
	err = yaml.Unmarshal([]byte(cfgStr), &keys)
	if err != nil {
		return nil, errors.E("parse config", err)
	}
	return keys, nil
}

// ExecImage retrieves the reflowlet instance's executable image info.
func (c *Client) ExecImage(ctx context.Context) (digest.Digest, error) {
	var d digest.Digest
	call := c.Call("GET", "execimage")
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return d, errors.E("execimage", err)
	}
	if code != http.StatusOK {
		return d, call.Error()
	}
	if err := call.Unmarshal(&d); err != nil {
		return d, errors.E("unmarshall reflowlet execimage", err)
	}
	return d, nil
}

// InstallImage instructs the reflowlet instance to install and run a new image.
// The image is referenced by the digest (in a format returned by digest.String())
// and is expected to exist in the repository (or the call will fail).
func (c *Client) InstallImage(ctx context.Context, d digest.Digest) error {
	// install the image on the reflowlet and check if it worked
	// by comparing the execimage digest again after waiting for some time
	// (for the reflowlet to have restarted).
	call := c.Call("POST", "execimage")
	defer call.Close()
	// Install the image onto the reflowlet. This will make the
	// machine unresponsive, because it will not have a chance to reply
	// to the exec call. We give it some time to recover.
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	// We expect an error since the reflowlet would've started the new image
	// before it has a chance to reply.
	// We check at least that the error comes from the right place in the stack
	code, err := call.DoJSON(ctx, d)
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

type clientAlloc struct {
	*Client
	id        string
	resources reflow.Resources
}

func (a *clientAlloc) Pool() pool.Pool             { return a.Client }
func (a *clientAlloc) Resources() reflow.Resources { return a.resources }
func (a *clientAlloc) ID() string                  { return a.Client.ID() + "/" + a.id }
func (a *clientAlloc) Repository() reflow.Repository {
	c, err := a.Client.Walk("allocs/%s/repository/", a.id)
	if err != nil {
		panic(err)
	}
	return &repositoryclient.Client{
		Client: c,
		Short:  a.ID(),
	}
}

// Keepalive issues a keepalive request to a remote alloc.
func (a *clientAlloc) Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error) {
	call := a.Call("POST", "allocs/%s/keepalive", a.id)
	defer call.Close()
	arg := struct {
		Interval time.Duration
	}{interval}
	code, err := call.DoJSON(ctx, arg)
	if err != nil {
		return time.Duration(0), errors.E("keepalive", a.ID(), fmt.Sprint(interval), err)
	}
	if code != http.StatusOK {
		return time.Duration(0), call.Error()
	}
	var iv struct{ Interval time.Duration }
	if err := call.Unmarshal(&iv); err != nil {
		return time.Duration(0), errors.E("keepalive", a.ID(), fmt.Sprint(interval), err)
	}
	return iv.Interval, nil
}

// Free frees a remote alloc.
func (a *clientAlloc) Free(ctx context.Context) error {
	call := a.Call("DELETE", "allocs/%s", a.id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return errors.E("free", a.ID(), err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}

// Inspect returns metadata for the alloc.
func (a *clientAlloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	call := a.Call("GET", "allocs/%s", a.id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return pool.AllocInspect{}, errors.E("inspect", a.ID(), err)
	}
	if code != http.StatusOK {
		return pool.AllocInspect{}, call.Error()
	}
	var inspect pool.AllocInspect
	if err := call.Unmarshal(&inspect); err != nil {
		return pool.AllocInspect{}, errors.E("inspect", a.ID(), err)
	}
	return inspect, nil
}

// Put idempotently creates a new exec in the alloc.
func (a *clientAlloc) Put(ctx context.Context, id digest.Digest, cfg reflow.ExecConfig) (reflow.Exec, error) {
	call := a.Call("PUT", "allocs/%s/execs/%s/", a.id, id)
	defer call.Close()
	code, err := call.DoJSON(ctx, &cfg)
	if err != nil {
		return nil, errors.E("put", a.ID(), id, fmt.Sprint(cfg), err)
	}
	if code != http.StatusOK {
		return nil, call.Error()
	}
	return &clientExec{a.Client, a.id, id}, nil
}

// Get retrieves the exec named id.
func (a *clientAlloc) Get(ctx context.Context, id digest.Digest) (reflow.Exec, error) {
	call := a.Call("HEAD", "allocs/%s/execs/%s/", a.id, id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return nil, errors.E("get", a.ID(), id, err)
	}
	if code != http.StatusOK {
		return nil, call.Error()
	}
	return &clientExec{a.Client, a.id, id}, nil
}

// Remove removes the exec named id.
func (a *clientAlloc) Remove(ctx context.Context, id digest.Digest) error {
	call := a.Call("DELETE", "allocs/%s/execs/%s", a.id, id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return errors.E("remove", a.ID(), id, err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}

// Execs enumerates the execs in this alloc.
func (a *clientAlloc) Execs(ctx context.Context) ([]reflow.Exec, error) {
	call := a.Call("GET", "allocs/%s/execs", a.id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return nil, errors.E("execs", a.ID(), err)
	}
	if code != http.StatusOK {
		return nil, call.Error()
	}
	var list []digest.Digest
	if err := call.Unmarshal(&list); err != nil {
		return nil, errors.E("execs", a.ID(), err)
	}
	execs := make([]reflow.Exec, len(list))
	for i := range list {
		execs[i] = &clientExec{a.Client, a.id, list[i]}
	}
	return execs, nil
}

// Load loads the fileset into the alloc repository.
func (a *clientAlloc) Load(ctx context.Context, repo *url.URL, fs reflow.Fileset) (reflow.Fileset, error) {
	call := a.Call("POST", "allocs/%s/load", a.id)
	defer call.Close()
	arg := struct {
		Fileset reflow.Fileset
		SrcUrl  *url.URL
	}{fs, repo}
	code, err := call.DoJSON(ctx, arg)
	if err != nil {
		return reflow.Fileset{}, errors.E("load", a.ID(), err)
	}
	if code != http.StatusOK {
		return reflow.Fileset{}, call.Error()
	}
	fs = reflow.Fileset{}
	err = call.Unmarshal(&fs)
	return fs, err
}

// VerifyIntegrity verifies the integrity of the given set of files
func (a *clientAlloc) VerifyIntegrity(ctx context.Context, fs reflow.Fileset) error {
	call := a.Call("POST", "allocs/%s/verify", a.id)
	defer func() { _ = call.Close() }()
	code, err := call.DoJSON(ctx, fs)
	if err != nil {
		return errors.E("verify", a.ID(), err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}

// Unload unloads the fileset from the alloc repository.
func (a *clientAlloc) Unload(ctx context.Context, fs reflow.Fileset) error {
	call := a.Call("POST", "allocs/%s/unload", a.id)
	defer call.Close()
	code, err := call.DoJSON(ctx, fs)
	if err != nil {
		return errors.E("unload", a.ID(), err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}

type clientExec struct {
	*Client
	allocID string
	id      digest.Digest
}

// URI returns the URI of this exec.
func (o *clientExec) URI() string {
	return fmt.Sprintf("%s/%s/%s", o.Client.ID(), o.allocID, o.id.Hex())
}

// ID returns the exec ID of this exec.
func (o *clientExec) ID() digest.Digest {
	return o.id
}

// Inspect returns the exec's metadata.
func (o *clientExec) Inspect(ctx context.Context, repo *url.URL) (inspect reflow.ExecInspect, d digest.Digest, err error) {
	var call *rest.ClientCall
	var code int
	if repo != nil {
		call = o.Call("POST", "allocs/%s/execs/%s", o.allocID, o.id)
		code, err = call.DoJSON(ctx, repo)
	} else {
		call = o.Call("GET", "allocs/%s/execs/%s", o.allocID, o.id)
		code, err = call.Do(ctx, nil)
	}
	defer call.Close()

	if err != nil {
		err = errors.E("inspect", o.URI(), err)
		return
	}
	if code != http.StatusOK {
		err = call.Error()
		return
	}
	reply := struct {
		Inspect       reflow.ExecInspect
		InspectDigest digest.Digest
	}{}
	err = call.Unmarshal(&reply)
	return reply.Inspect, reply.InspectDigest, err
}

// Logs returns this exec's logs.
func (o *clientExec) Logs(ctx context.Context, stdout, stderr, follow bool) (io.ReadCloser, error) {
	var which string
	if stderr && stdout {
		which = "logs"
	} else if stderr {
		which = "stderr"
	} else if stdout {
		which = "stdout"
	} else {
		return nil, errors.E(
			"logs", o.URI(), fmt.Sprint(stdout), fmt.Sprint(stderr),
			errors.New("at least one of stdout, stderr must be set"))
	}
	call, code, err := o.makeLogsCall(ctx, which, follow)
	if err != nil {
		call.Close()
		return nil, errors.E("logs", o.URI(), fmt.Sprint(stdout), fmt.Sprint(stderr), err)
	}
	if code != http.StatusOK {
		err := call.Error()
		call.Close()
		return nil, errors.E("logs", o.URI(), fmt.Sprint(stdout), fmt.Sprint(stderr), err)
	}
	return call, err
}

// Logs returns this exec's remote logs reference.
func (o *clientExec) RemoteLogs(ctx context.Context, stdout bool) (reflow.RemoteLogs, error) {
	which := "stderrloc"
	if stdout {
		which = "stdoutloc"
	}
	call, code, err := o.makeLogsCall(ctx, which, false)
	if err != nil {
		return reflow.RemoteLogs{}, errors.E("logslocation", o.URI(), which, err)
	}
	defer func() { _ = call.Close() }()
	if code != http.StatusOK {
		return reflow.RemoteLogs{}, call.Error()
	}
	var loc reflow.RemoteLogs
	if err := call.Unmarshal(&loc); err != nil {
		return reflow.RemoteLogs{}, errors.E("logslocation", o.URI(), which, err)
	}
	return loc, nil
}

func (o *clientExec) makeLogsCall(ctx context.Context, which string, follow bool) (*rest.ClientCall, int, error) {
	followParam := ""
	if follow {
		followParam = "?follow=true"
	}
	call := o.Call("GET", "allocs/%s/execs/%s/%s%s", o.allocID, o.id, which, followParam)
	code, err := call.Do(ctx, nil)
	return call, code, err
}

type conn struct {
	io.ReadCloser
	io.Writer
}

func (o *clientExec) Shell(ctx context.Context) (io.ReadWriteCloser, error) {
	s := fmt.Sprintf("allocs/%s/execs/%s/shell", o.allocID, o.id)
	call := o.Call("POST", s)
	r, w := io.Pipe()
	code, err := call.Do(ctx, r)
	if err != nil {
		call.Close()
		return nil, err
	}
	if code != http.StatusOK {
		err := call.Error()
		call.Close()
		return nil, err
	}
	return &conn{call, w}, nil
}

// Wait blocks until the exec has completed.
func (o *clientExec) Wait(ctx context.Context) error {
	call := o.Call("GET", "allocs/%s/execs/%s/wait", o.allocID, o.id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return errors.E("wait", o.URI(), err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}

// Value returns this exec's value.
func (o *clientExec) Result(ctx context.Context) (reflow.Result, error) {
	call := o.Call("GET", "allocs/%s/execs/%s/result", o.allocID, o.id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return reflow.Result{}, errors.E("value", o.URI(), err)
	}
	if code != http.StatusOK {
		return reflow.Result{}, call.Error()
	}
	var r reflow.Result
	if err := call.Unmarshal(&r); err != nil {
		return reflow.Result{}, errors.E("value", o.URI(), err)
	}
	return r, nil
}

func (o *clientExec) Promote(ctx context.Context) error {
	call := o.Call("POST", "allocs/%s/execs/%s/promote", o.allocID, o.id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return errors.E("promote", o.URI(), err)
	}
	if code != http.StatusOK {
		return call.Error()
	}
	return nil
}

// Offer looks up the offer named id.
func (c *Client) Offer(ctx context.Context, id string) (pool.Offer, error) {
	call := c.Call("GET", "offers/%s", id)
	defer call.Close()
	code, err := call.Do(ctx, nil)
	if err != nil {
		return nil, errors.E("offer", c.ID(), id, err)
	}
	if code != http.StatusOK {
		return nil, call.Error()
	}
	var json pool.OfferJSON
	if err := call.Unmarshal(&json); err != nil {
		return nil, errors.E("offer", c.ID(), id, err)
	}
	return &clientOffer{c, id, json.Available}, nil
}

// Offers enumerates all available offers in this pool.
func (c *Client) Offers(ctx context.Context) ([]pool.Offer, error) {
	v, err, _ := c.group.Do("offers", func() (interface{}, error) {
		call := c.Call("GET", "offers/")
		defer call.Close()
		code, err := call.Do(ctx, nil)
		if err != nil {
			return nil, err
		}
		if code != http.StatusOK {
			return nil, call.Error()
		}

		var jsons []pool.OfferJSON
		if err := call.Unmarshal(&jsons); err != nil {
			return nil, err
		}
		offers := make([]pool.Offer, len(jsons))
		for i, json := range jsons {
			offers[i] = &clientOffer{c, json.ID, json.Available}
		}
		return offers, nil
	})
	if err != nil {
		return nil, errors.E("offers", c.ID(), err)
	}
	return v.([]pool.Offer), nil
}

type clientOffer struct {
	*Client
	id        string
	available reflow.Resources
}

func (c *clientOffer) ID() string                  { return c.Client.ID() + "/" + c.id }
func (c *clientOffer) Pool() pool.Pool             { return c.Client }
func (c *clientOffer) Available() reflow.Resources { return c.available }

// Accept accepts a subset of this offer.
func (c *clientOffer) Accept(ctx context.Context, meta pool.AllocMeta) (pool.Alloc, error) {
	call := c.Call("POST", "offers/%s", c.id)
	defer call.Close()
	code, err := call.DoJSON(ctx, meta)
	if err != nil {
		return nil, errors.E("accept", c.ID(), err)
	}
	if code != http.StatusOK {
		return nil, call.Error()
	}
	var json pool.AllocInspect
	if err := call.Unmarshal(&json); err != nil {
		return nil, errors.E("accept", c.ID(), err)
	}
	return &clientAlloc{c.Client, json.ID, json.Resources}, nil
}
