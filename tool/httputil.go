package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/pool"
)

// TODO(pgopal) - Move all the url construction logic to a common library that pool/client
// and httputil can use.

func (c *Cmd) allocInspect(ctx context.Context, n name) (pool.AllocInspect, error) {
	httpClient, err := c.httpClient()
	httpClient.Timeout = httpTimeout
	if err != nil {
		c.Fatal(err)
	}
	switch n.Kind {
	case allocName:
		url := constructHTTPUrl(n)
		resp, err := httpClient.Get(url)
		if err != nil {
			c.Log.Errorf("error inspecting alloc %q: %s", url, err)
			return pool.AllocInspect{}, err
		}
		defer resp.Body.Close()
		var inspect pool.AllocInspect
		err = json.NewDecoder(resp.Body).Decode(&inspect)
		if err != nil {
			c.Log.Errorf("error decoding alloc ExecInspect  %q: %s", url, err)
			return pool.AllocInspect{}, err
		}
		return inspect, nil
	default:
		return pool.AllocInspect{}, fmt.Errorf("not an alloc id: %v", n)
	}
}

const (
	httpTimeout = 2 * time.Second
)

func (c *Cmd) allocExecs(ctx context.Context, n name) ([]reflow.Exec, error) {
	httpClient, err := c.httpClient()
	httpClient.Timeout = httpTimeout
	if err != nil {
		c.Fatal(err)
	}
	switch n.Kind {
	case allocName:
		url := constructHTTPUrl(n) + "/" + "execs"
		resp, err := httpClient.Get(url)
		if err != nil {
			c.Log.Errorf("error get execs %q: %s", url, err)
			return []reflow.Exec{}, err
		}
		defer resp.Body.Close()
		var execs []reflow.Exec
		err = json.NewDecoder(resp.Body).Decode(&execs)
		if err != nil {
			c.Log.Errorf("error decoding alloc execs %q: %s", url, err)
			return []reflow.Exec{}, err
		}
		return execs, nil
	default:
		return []reflow.Exec{}, fmt.Errorf("not an alloc id: %v", n)
	}
}

func (c *Cmd) liveExecInspect(ctx context.Context, n name) (reflow.ExecInspect, error) {
	httpClient, err := c.httpClient()
	httpClient.Timeout = httpTimeout
	if err != nil {
		c.Fatal(err)
	}
	switch n.Kind {
	case execName:
		url := constructHTTPUrl(n)
		resp, err := httpClient.Get(url)
		if err != nil {
			c.Log.Debugf("error inspecting exec %q: %s", url, err)
			return reflow.ExecInspect{}, err
		}
		defer resp.Body.Close()
		var inspect reflow.ExecInspect
		err = json.NewDecoder(resp.Body).Decode(&inspect)
		if err != nil {
			c.Log.Errorf("error decoding exec %q: %s", url, err)
			return reflow.ExecInspect{}, err
		}
		return inspect, nil
	default:
		return reflow.ExecInspect{}, fmt.Errorf("not an exec id: %v", n)
	}
}

func (c *Cmd) liveExecResult(ctx context.Context, n name) (reflow.Result, error) {
	httpClient, err := c.httpClient()
	httpClient.Timeout = httpTimeout
	if err != nil {
		c.Fatal(err)
	}
	switch n.Kind {
	case execName:
		url := constructHTTPUrl(n) + "/" + "result"
		resp, err := httpClient.Get(url)
		if err != nil {
			c.Log.Errorf("error inspecting exec %q: %s", url, err)
			return reflow.Result{}, err
		}
		defer resp.Body.Close()
		var result reflow.Result
		err = json.NewDecoder(resp.Body).Decode(&result)
		if err != nil {
			c.Log.Errorf("error decoding exec result  %q: %s", url, err)
			return reflow.Result{}, err
		}
		return result, nil
	default:
		return reflow.Result{}, fmt.Errorf("not an exec id: %v", n)
	}
}

func (c *Cmd) reposExecInspect(ctx context.Context, d digest.Digest) (reflow.ExecInspect, error) {
	var repo reflow.Repository
	err := c.Config.Instance(&repo)
	if err != nil {
		log.Fatal("repository: ", err)
	}
	rc, err := repo.Get(ctx, d)
	if err != nil {
		return reflow.ExecInspect{}, err
	}
	dec := json.NewDecoder(rc)
	var ins reflow.ExecInspect
	err = dec.Decode(&ins)
	if err != nil {
		return reflow.ExecInspect{}, err
	}
	return ins, nil
}

func (c *Cmd) execLogs(ctx context.Context, stdout, follow bool, n name) (io.ReadCloser, error) {
	httpClient, err := c.httpClient()
	httpClient.Timeout = httpTimeout
	if err != nil {
		c.Fatal(err)
	}
	switch n.Kind {
	case execName:
		path := "/stderr"
		if stdout {
			path = "/stdout"
		}
		if follow {
			path = path + fmt.Sprintf("?follow=%t", follow)
		}
		url := constructHTTPUrl(n) + path
		resp, err := httpClient.Get(url)
		if err != nil {
			c.Log.Errorf("error inspecting exec %q: %s", url, err)
			return nil, err
		}
		return resp.Body, nil
	default:
		return nil, fmt.Errorf("not an exec id: %v", n)
	}
}

func constructHTTPUrl(n name) string {
	var url string
	prefix := "https://" + n.InstanceID + "/v1"
	switch n.Kind {
	case allocName:
		url = prefix + "/allocs/" + n.AllocID
	case execName:
		exec := n.ID.Hex()
		url = prefix + "/allocs/" + n.AllocID + "/execs/" + exec
	}
	return url
}
