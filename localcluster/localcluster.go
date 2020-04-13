// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package localcluster implements a runner.Cluster using the local machine's docker.
package localcluster

import (
	"context"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"docker.io/go-docker"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/infra"
	infraaws "github.com/grailbio/infra/aws"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/blob/s3blob"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/local"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"golang.org/x/net/http2"
)

func init() {
	infra.Register("localcluster", new(Cluster))
}

// Cluster implements a cluster abstraction using the local machine.
type Cluster struct {
	// Pool is the local pool
	pool.Pool
	// Log is the logger to write logs out to.
	Log *log.Logger
	// Client is the local docker client.
	Client *docker.Client
	// Session is the aws session.
	Session *session.Session

	total     reflow.Resources
	available reflow.Resources
	dir       string
	mu        sync.Mutex
	// needed indicates the resources that are currently needed by the localcluster client
	needed reflow.Resources
}

// Need reflects the current set of resources that are pending
// allocation.
func (c *Cluster) Need() reflow.Resources {
	c.mu.Lock()
	need := c.needed
	c.mu.Unlock()
	return need
}

func (c *Cluster) updateNeed(req reflow.Requirements) (cancel func()) {
	c.mu.Lock()
	c.needed.Add(c.needed, req.Min)
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		c.needed.Sub(c.needed, req.Min)
		c.mu.Unlock()
	}
}

// Init implements infra.Provider
func (c *Cluster) Init(tls tls.Certs, session *session.Session, logger *log.Logger, tool *infraaws.AWSTool, creds *credentials.Credentials) error {
	var err error
	if c.Client, c.total, err = dockerClient(); err != nil {
		return err
	}
	c.available = c.total
	clientConfig, _, err := tls.HTTPS()
	if err != nil {
		return err
	}
	transport := &http.Transport{TLSClientConfig: clientConfig}
	if err = http2.ConfigureTransport(transport); err != nil {
		return err
	}
	c.Session = session
	c.Log = logger.Tee(nil, "localcluster: ")
	pool := &local.Pool{
		Dir:           c.dir,
		Client:        c.Client,
		Authenticator: ec2authenticator.New(session),
		AWSImage:      string(*tool),
		AWSCreds:      creds,
		Blob: blob.Mux{
			"s3": s3blob.New(session),
		},
		Log:          logger.Tee(nil, "executor: "),
		HardMemLimit: false,
	}
	if err = pool.Start(); err != nil {
		return err
	}
	c.Pool = pool
	return nil
}

// Flags implements infra.Provider
func (c *Cluster) Flags(flags *flag.FlagSet) {
	flags.StringVar(&c.dir, "dir", "/tmp/flow", "directory to store local state")
}

// Help implements infra.Provider
func (*Cluster) Help() string {
	return "configure a local cluster using the host machine."
}

const (
	allocatePoolInterval = time.Minute
)

// Allocate allocates an alloc on the local machine with the specified requirements.
func (c *Cluster) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (alloc pool.Alloc, err error) {
	if !c.total.Available(req.Min) {
		return nil, errors.E(fmt.Sprintf("required resource %v greater than available %v", req.Min, c.total), errors.ResourcesExhausted)
	}
	cancel := c.updateNeed(req)
	defer cancel()
	tick := time.NewTicker(allocatePoolInterval)
	defer tick.Stop()
	for ctx.Err() == nil {
		allocCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		alloc, err := pool.Allocate(allocCtx, c, req, labels)
		cancel()
		if err == nil {
			return alloc, nil
		}
		if errors.Is(errors.Unavailable, err) {
			select {
			case <-tick.C:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return nil, ctx.Err()
}

// Shutdown implements runner.Cluster.
func (c *Cluster) Shutdown() error {
	return nil
}

func dockerClient() (*docker.Client, reflow.Resources, error) {
	addr := os.Getenv("DOCKER_HOST")
	if addr == "" {
		addr = "unix:///var/run/docker.sock"
	}
	client, err := docker.NewClient(
		addr, "1.22", /*client.DefaultVersion*/
		nil, map[string]string{"user-agent": "reflow"})
	if err != nil {
		return nil, nil, err
	}
	info, err := client.Info(context.Background())
	if err != nil {
		return nil, nil, err
	}
	// TODO(pgopal): detect available size on the partition containing the local repository state.
	resources := reflow.Resources{
		"mem":  math.Floor(float64(info.MemTotal) * 0.95),
		"cpu":  float64(info.NCPU),
		"disk": 1e13, // Assume 10TB.
	}
	return client, resources, nil
}
