// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/docker/engine-api/client"
	"github.com/docker/engine-api/types"
	"github.com/grailbio/base/limiter"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/fs"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
)

const (
	statePath            = "state.json"
	metaPath             = "meta.json"
	allocsPath           = "allocs"
	keepaliveInterval    = 1 * time.Minute
	maxKeepaliveInterval = 2 * time.Hour
	offerID              = "1"
)

var (
	errOfferExpired = errors.New("offer expired")
	errAllocExpired = errors.New("alloc expired")
)

// Pool implements a resource pool on top of a Docker client.
// The pool itself must run on the same machine as the Docker
// instance as it performs local filesystem operations that must
// be reflected inside the container.
//
// Pool keeps all state on disk, as follows:
//
//	Prefix/Dir/state.json
//		Stores the set of currently active allocs, together with their
//		resource requirements.
//
//	Prefix/Dir/allocs/<id>/
//		The root directory for the alloc with id. The state under
//		this directory is managed by an executor instance.
type Pool struct {
	// Dir is the filesystem root of the pool. Everything under this
	// path is assumed to be owned and managed by the pool.
	Dir string
	// Prefix is prepended to paths constructed by allocs. This is to
	// permit running the pool manager inside of a Docker container.
	Prefix string
	// Client is the Docker client. We assume that the Docker daemon
	// runs on the same host from which the pool is managed.
	Client *client.Client
	// Authenticator is used to authenticate ECR image pulls.
	Authenticator interface {
		Authenticates(ctx context.Context, image string) (bool, error)
		Authenticate(ctx context.Context, cfg *types.AuthConfig) error
	}
	// AWSImage is the name of the image that contains the 'aws' tool.
	// This is used to implement directory syncing via s3.
	AWSImage string
	// AWSCreds is a credentials provider used to mint AWS credentials.
	// They are used to access AWS services.
	AWSCreds *credentials.Credentials
	// Log
	Log *log.Logger
	// DigestLimiter controls the number of digest operations that may
	// proceed concurrently.
	DigestLimiter *limiter.Limiter
	// S3FileLimiter controls the number of S3 file downloads that may
	// proceed concurrently.
	S3FileLimiter *limiter.Limiter

	mu        sync.Mutex
	allocs    map[string]*alloc // the set of active allocs
	resources reflow.Resources  // the total amount of available resources
	stopped   bool
}

// saveState saves the current state of the pool to Prefix/Dir/state.json.
// It must be called while m.mu is locked.
func (p *Pool) saveState() error {
	path := filepath.Join(p.Prefix, p.Dir, statePath)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	allocs := map[string]reflow.Resources{}
	for id, alloc := range p.allocs {
		allocs[id] = alloc.resources
	}
	if err := json.NewEncoder(file).Encode(allocs); err != nil {
		file.Close()
		os.Remove(path)
	}
	file.Close()
	return nil
}

// Start starts the pool. If the pool has a state snapshot, Start
// will restore the pool's previous state. Start will also make sure
// that all zombie allocs are collected.
func (p *Pool) Start() error {
	ctx := context.Background()
	if p.DigestLimiter == nil {
		p.DigestLimiter = limiter.New()
		p.DigestLimiter.Release(defaultDigestLimit)
	}
	if p.S3FileLimiter == nil {
		p.S3FileLimiter = limiter.New()
		p.S3FileLimiter.Release(defaultS3FileLimit)
	}

	info, err := p.Client.Info(ctx)
	if err != nil {
		return err
	}
	p.resources = reflow.Resources{
		"mem": math.Floor(float64(info.MemTotal) * 0.95),
		"cpu": float64(info.NCPU),
	}
	root := filepath.Join(p.Prefix, p.Dir)
	if err := os.MkdirAll(root, 0777); err != nil {
		log.Printf("mkdir %s: %v", root, err)
	}
	if usage, err := fs.Stat(root); err == nil {
		p.resources["disk"] = float64(usage.Total)
	} else {
		log.Printf("stat %s: %v", root, err)
		p.resources["disk"] = 2e12
	}

	if err := os.MkdirAll(filepath.Join(p.Prefix, p.Dir, allocsPath), 0777); err != nil {
		return err
	}
	allocs := map[string]reflow.Resources{}
	if file, err := os.Open(filepath.Join(p.Prefix, p.Dir, statePath)); err != nil {
		if os.IsNotExist(err) {
			p.Log.Printf("no state on disk")
		} else {
			return err
		}
	} else {
		if err := json.NewDecoder(file).Decode(&allocs); err != nil {
			p.Log.Errorf("failed to recover state: %s; starting from empty", err)
		}
		file.Close()
	}
	dir, err := os.Open(filepath.Join(p.Prefix, p.Dir, allocsPath))
	if err != nil {
		return err
	}
	defer dir.Close()
	infos, err := dir.Readdir(-1)
	if err != nil {
		return err
	}
	p.allocs = map[string]*alloc{}
	for _, info := range infos {
		if !info.IsDir() {
			continue
		}
		id := info.Name()
		alloc := p.newAlloc(id)
		if err := alloc.restore(); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		if err := alloc.Start(); err != nil {
			return err
		}
		if _, ok := allocs[id]; ok {
			delete(allocs, id)
			p.allocs[id] = alloc
		} else {
			// TODO(marius): this may be overkill, but it will do the right thing.
			// In the future, we may want to store whether an alloc was definitely
			// killed.
			go func() {
				if err := alloc.Kill(context.Background()); err != nil {
					p.Log.Errorf("error killing alloc %s: %s", alloc.ID(), err)
				}
			}()
		}
	}
	for id := range allocs {
		p.Log.Printf("orphaned alloc %s", id)
	}
	return nil
}

// available returns the amount of currently available resources:
// The total less what is occupied by active allocs.
func (p *Pool) available() reflow.Resources {
	var reserved reflow.Resources
	for _, alloc := range p.allocs {
		if !alloc.expired() {
			reserved.Add(reserved, alloc.resources)
		}
	}
	var avail reflow.Resources
	avail.Sub(p.resources, reserved)
	return avail
}

// new creates a new alloc with the given meta. new collects expired
// allocs as needed to make room for the resource requirements as
// indicated by meta.
func (p *Pool) new(ctx context.Context, meta pool.AllocMeta) (pool.Alloc, error) {
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return nil, errors.Errorf("alloc %v: shutting down", meta)
	}
	var (
		used    reflow.Resources
		expired []*alloc
	)
	for _, alloc := range p.allocs {
		used.Add(used, alloc.resources)
		if alloc.expired() {
			expired = append(expired, alloc)
		}
	}
	// ACHTUNG N²! (But n is small.)
	n := 0
	collect := expired[:]
	// TODO: preferentially prefer those allocs which will give us the
	// resource types we need.
	p.Log.Printf("alloc total%s used%s want%s", p.resources, used, meta.Want)
	var free reflow.Resources
	for {
		free.Sub(p.resources, used)
		if free.Available(meta.Want) || len(expired) == 0 {
			break
		}
		max := 0
		for i := 1; i < len(expired); i++ {
			if expired[i].expiredBy() > expired[max].expiredBy() {
				max = i
			}
		}
		alloc := expired[max]
		expired[0], expired[max] = expired[max], expired[0]
		expired = expired[1:]
		used.Sub(used, alloc.resources)
		n++
	}
	collect = collect[:n]
	if !free.Available(meta.Want) {
		p.mu.Unlock()
		return nil, errors.E("alloc", errors.NotExist, errOfferExpired)
	}
	for _, alloc := range collect {
		delete(p.allocs, alloc.id)
	}
	id := newID()
	alloc := p.newAlloc(id)
	var err error
	err = alloc.configure(meta)
	if err == nil {
		err = alloc.Start()
	}
	if err != nil {
		for _, alloc := range collect {
			p.allocs[alloc.id] = alloc
		}
		p.mu.Unlock()
		return nil, err
	}
	p.allocs[id] = alloc
	if err := p.saveState(); err != nil {
		delete(p.allocs, id)
		for _, alloc := range collect {
			p.allocs[alloc.id] = alloc
		}
		p.mu.Unlock()
		if err := alloc.Kill(context.Background()); err != nil {
			p.Log.Errorf("error killing alloc: %s", err)
		}
		return nil, err
	}
	p.mu.Unlock()
	for _, alloc := range collect {
		p.Log.Printf("alloc reclaim %s", alloc.ID())
		if err := alloc.Kill(context.Background()); err != nil {
			p.Log.Errorf("error killing alloc: %s", err)
		}
	}
	return alloc, nil
}

// free frees alloc a from this pool. It does not collect the alloc itself.
func (p *Pool) free(a *alloc) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.allocs[a.id] != a {
		return nil
	}
	delete(p.allocs, a.id)
	if err := p.saveState(); err != nil {
		p.allocs[a.id] = a
		return err
	}
	return nil
}

// alive tells whether an alloc's lease is current.
func (p *Pool) alive(a *alloc) bool {
	p.mu.Lock()
	t := p.allocs[a.id] == a
	p.mu.Unlock()
	return t
}

// ID returns the ID of the pool. It is always "local".
func (p *Pool) ID() string { return "local" }

// Offer looks up the an offer by ID.
func (p *Pool) Offer(ctx context.Context, id string) (pool.Offer, error) {
	offers, err := p.Offers(ctx)
	if err != nil {
		return nil, err
	}
	if len(offers) == 0 {
		return nil, errors.E("offer", id, errors.NotExist, errOfferExpired)
	}
	if id != offerID {
		return nil, errors.E("offer", id, errors.NotExist, errOfferExpired)
	}
	return offers[0], nil
}

// Offers enumerates all the current offers of this pool. The local
// pool always returns either no offers, when there are no more
// available resources, or 1 offer comprising the entirety of
// available resources.
func (p *Pool) Offers(ctx context.Context) ([]pool.Offer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return nil, nil
	}
	var reserved reflow.Resources
	for _, alloc := range p.allocs {
		if !alloc.expired() {
			reserved.Add(reserved, alloc.resources)
		}
	}
	var available reflow.Resources
	available.Sub(p.resources, reserved)
	if available["mem"] == 0 || available["cpu"] == 0 || available["disk"] == 0 {
		return nil, nil
	}
	return []pool.Offer{&offer{p, offerID, available}}, nil
}

// Alloc looks up an alloc by ID.
func (p *Pool) Alloc(ctx context.Context, id string) (pool.Alloc, error) {
	p.mu.Lock()
	alloc := p.allocs[id]
	p.mu.Unlock()
	if alloc != nil {
		return alloc, nil
	}
	dir := filepath.Join(p.Prefix, p.Dir, allocsPath, id)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, errors.E("alloc", id, errors.NotExist)
	}
	return &zombie{manager: p, dir: dir, id: id}, nil
}

// Allocs lists all the active allocs in the pool.
func (p *Pool) Allocs(ctx context.Context) ([]pool.Alloc, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	allocs := make([]pool.Alloc, len(p.allocs))
	i := 0
	for _, a := range p.allocs {
		allocs[i] = a
		i++
	}
	return allocs, nil
}

// StopIfIdle stops the pool if it is idle. Returns whether the pool was stopped.
func (p *Pool) StopIfIdleFor(d time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, alloc := range p.allocs {
		if alloc.expiredBy() < d {
			return false
		}
	}
	p.stopped = true
	return true
}

// Alloc implements a local alloc. It embeds a local executor which
// does the heavy-lifting, while the alloc code deals with lifecycle
// and resource concerns.
type alloc struct {
	*Executor
	mu            sync.Mutex
	id            string
	p             *Pool
	created       time.Time
	expires       time.Time
	lastKeepalive time.Time
	freed         bool
	meta          pool.AllocMeta
	remoteStream
}

// NewAlloc creates a new alloc. The returned alloc is not started.
func (p *Pool) newAlloc(id string) *alloc {
	e := &Executor{
		ID:            id,
		Client:        p.Client,
		Dir:           filepath.Join(p.Dir, allocsPath, id),
		Prefix:        p.Prefix,
		Authenticator: p.Authenticator,
		AWSImage:      p.AWSImage,
		AWSCreds:      p.AWSCreds,
		Log:           p.Log.Tee(nil, id+": "),
		DigestLimiter: p.DigestLimiter,
	}

	// TODO(pgopal) - Get this info from Config.
	cwlclient := cloudwatchlogs.New(
		session.New(
			&aws.Config{
				Credentials: e.AWSCreds,
				Region:      aws.String(defaultRegion),
			}))
	remoteStream, err := newCloudWatchLogs(cwlclient, "reflow")
	if err != nil {
		log.Errorf("create remote logger: %v", err)
	}
	e.remoteStream = remoteStream

	// Note that we refresh the keepalive time on exec restore. This is
	// probably a useful safeguard, but could be annoying when keepalive
	// intervals are large.
	//
	// TODO(marius): persist alloc states across restarts. This doesn't
	// matter too much at present, as ec2 nodes are terminated when
	// the reflowlet terminates, but it should be done for potential future
	// implementations.
	return &alloc{
		Executor:     e,
		id:           id,
		p:            p,
		created:      time.Now(),
		expires:      time.Now().Add(keepaliveInterval),
		remoteStream: remoteStream,
	}
}

// configure stores the given metadata in the alloc's directory.
func (a *alloc) configure(meta pool.AllocMeta) error {
	a.meta = meta
	a.resources.Set(a.meta.Want)
	path := filepath.Join(a.Prefix, a.Dir, metaPath)
	if err := os.MkdirAll(filepath.Dir(path), 0777); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewEncoder(file).Encode(meta)
}

// restore reads the stored metadata.
func (a *alloc) restore() error {
	path := filepath.Join(a.Prefix, a.Dir, metaPath)
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	err = json.NewDecoder(file).Decode(&a.meta)
	a.resources = a.meta.Want
	return err
}

// expired tells whether the alloc is expired, as per the keepalive interval.
func (a *alloc) expired() bool {
	a.mu.Lock()
	x := a.expires.Before(time.Now())
	a.mu.Unlock()
	return x
}

// expiredBy tells by how much the alloc is expired.
func (a *alloc) expiredBy() time.Duration {
	a.mu.Lock()
	d := time.Now().Sub(a.expires)
	a.mu.Unlock()
	return d
}

// Pool returns the pool that owns this alloc.
func (a *alloc) Pool() pool.Pool {
	return a.p
}

// ID returns this alloc's ID.
func (a *alloc) ID() string {
	return a.id
}

// Resources returns this alloc's resource allotment.
func (a *alloc) Resources() reflow.Resources {
	return a.resources
}

// Start assigns the run id and starts the alloc executor.
func (a *alloc) Start() error {
	a.RunID = a.meta.Labels["Name"]
	err := a.Executor.Start()
	return err
}

// Keepalive maintains the alloc's lease.
func (a *alloc) Keepalive(ctx context.Context, next time.Duration) (time.Duration, error) {
	if !a.p.alive(a) {
		return time.Duration(0), errors.E("keepalive", a.id, fmt.Sprint(next), errors.NotExist, errAllocExpired)
	}
	a.mu.Lock()
	if next > maxKeepaliveInterval {
		next = maxKeepaliveInterval
	}
	a.lastKeepalive = time.Now()
	a.expires = a.lastKeepalive.Add(next)
	a.mu.Unlock()
	return next, nil
}

// Inspect returns the alloc's status.
func (a *alloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	a.mu.Lock()
	i := pool.AllocInspect{
		ID:            a.id,
		Resources:     a.meta.Want,
		Meta:          a.meta,
		Created:       a.created,
		Expires:       a.expires,
		LastKeepalive: a.lastKeepalive,
	}
	a.mu.Unlock()
	return i, nil
}

// Free relinquishes this alloc from its pool and kills its
// resources. The alloc's repository is removed, but its metadata and
// logs are kept intact so that they may be examined posthumously.
func (a *alloc) Free(ctx context.Context) error {
	if err := a.p.free(a); err != nil {
		return err
	}
	a.mu.Lock()
	free := !a.freed
	a.freed = true
	a.mu.Unlock()
	if free {
		a.p.Log.Printf("killing alloc %v", a)
		a.Kill(context.Background())
	}
	if a.remoteStream != nil {
		a.remoteStream.Close()
	}

	return nil
}

type offer struct {
	m         *Pool
	id        string
	resources reflow.Resources
}

func (o *offer) ID() string                  { return o.id }
func (o *offer) Pool() pool.Pool             { return o.m }
func (o *offer) Available() reflow.Resources { return o.resources }
func (o *offer) Accept(ctx context.Context, meta pool.AllocMeta) (pool.Alloc, error) {
	return o.m.new(ctx, meta)
}

// newID generates a random hex string.
func newID() string {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b[:])
}
