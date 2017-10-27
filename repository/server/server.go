// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package server implements a Repository REST server.
package server

import (
	"context"
	"net/http"
	"net/url"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/bloomlive"
	"github.com/grailbio/reflow/rest"
)

// Node is a REST node serving a Repository.
type Node struct {
	Repository reflow.Repository
}

// Walk walks the Node tree to path.
func (n Node) Walk(ctx context.Context, call *rest.Call, path string) rest.Node {
	switch {
	case path == "collect":
		return collectNode{n.Repository}
	default:
		id, err := reflow.Digester.Parse(path)
		if err != nil {
			call.Error(errors.E("walk", path, err))
			return nil
		}
		return fileNode{n.Repository, id}
	}
}

// Do performs call on node n.
func (n Node) Do(ctx context.Context, call *rest.Call) {
	if !call.Allow("POST") {
		return
	}
	d, err := n.Repository.Put(ctx, call.Body())
	if err != nil {
		call.Error(err)
		return
	}
	call.Reply(http.StatusOK, d)
}

type collectNode struct{ reflow.Repository }

func (n collectNode) Walk(ctx context.Context, call *rest.Call, path string) rest.Node {
	return nil
}

func (n collectNode) Do(ctx context.Context, call *rest.Call) {
	if !call.Allow("POST") {
		return
	}
	var live bloomlive.T
	if call.Unmarshal(&live) != nil {
		return
	}
	err := n.Repository.Collect(ctx, &live)
	if err != nil {
		call.Error(err)
		return
	}
	call.Reply(http.StatusOK, nil)
}

type fileNode struct {
	r  reflow.Repository
	id digest.Digest
}

func (n fileNode) Walk(ctx context.Context, call *rest.Call, path string) rest.Node {
	switch path {
	case "transfers":
		return transfersNode{n.r, n.id}
	}
	return nil
}

func (n fileNode) Do(ctx context.Context, call *rest.Call) {
	if !call.Allow("GET", "PUT", "POST") {
		return
	}
	switch call.Method() {
	case "HEAD":
		file, err := n.r.Stat(ctx, n.id)
		if err != nil {
			call.Error(err)
			return
		}
		call.Reply(http.StatusOK, file)
	case "GET":
		rc, err := n.r.Get(ctx, n.id)
		if err != nil {
			call.Error(err)
			return
		}
		call.Write(http.StatusOK, rc)
		rc.Close()
	case "POST":
		id, err := n.r.Put(ctx, call.Body())
		if err != nil {
			call.Error(err)
		}
		call.Reply(http.StatusOK, id)
	case "PUT":
		rawurl := call.Header().Get("reflow-read-from")
		switch rawurl {
		case "":
			call.Error(errors.New("file PUT: source missing"))
		default:
			u, err := url.Parse(rawurl)
			if err != nil {
				call.Error(err)
				return
			}
			if err := n.r.ReadFrom(ctx, n.id, u); err != nil {
				call.Error(err)
				return
			}
			call.Reply(http.StatusOK, nil)
		}
	}
}

type transfersNode struct {
	r  reflow.Repository
	id digest.Digest
}

func (n transfersNode) Walk(ctx context.Context, call *rest.Call, path string) rest.Node { return nil }

func (n transfersNode) Do(ctx context.Context, call *rest.Call) {
	if !call.Allow("POST") {
		return
	}
	var rawurl string
	if call.Unmarshal(&rawurl) != nil {
		return
	}
	u, err := url.Parse(rawurl)
	if err != nil {
		call.Error(errors.E("transfer", rawurl, err))
		return
	}
	if err := n.r.WriteTo(ctx, n.id, u); err != nil {
		call.Error(err)
		return
	}
	call.Reply(http.StatusOK, nil)
}
