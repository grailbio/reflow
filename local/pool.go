// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package local

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	"docker.io/go-docker"
	"docker.io/go-docker/api/types"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/blob"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/fs"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
)

const (
	statePath  = "state.json"
	metaPath   = "meta.json"
	allocsPath = "allocs"
	// remoteStreamCWLogGroupName is the cloudwatch log group name for remote streams.
	remoteStreamCWLogGroupName = "reflow"
)

var errAllocExpired = errors.New("alloc expired")

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
	pool.ResourcePool
	// Dir is the filesystem root of the pool. Everything under this
	// path is assumed to be owned and managed by the pool.
	Dir string
	// Prefix is prepended to paths constructed by allocs. This is to
	// permit running the pool manager inside of a Docker container.
	Prefix string
	// Client is the Docker client. We assume that the Docker daemon
	// runs on the same host from which the pool is managed.
	Client *docker.Client
	// Authenticator is used to authenticate ECR image pulls.
	Authenticator interface {
		Authenticates(ctx context.Context, image string) (bool, error)
		Authenticate(ctx context.Context, cfg *types.AuthConfig) error
	}
	// AWSCreds is a credentials provider used to mint AWS credentials.
	// They are used to access AWS services.
	AWSCreds *credentials.Credentials
	// Session is the AWS session to use for AWS API calls.
	Session *session.Session
	// Blob is the blob store implementation used to fetch data from interns.
	Blob blob.Mux

	// TaskDBPoolId is the identifier of this Pool in TaskDB
	TaskDBPoolId reflow.StringDigest
	TaskDB       taskdb.TaskDB

	// Log
	Log *log.Logger

	HardMemLimit bool

	mu sync.Mutex
}

// saveState saves the current state of the pool to Prefix/Dir/state.json.
func (p *Pool) saveState(allocs []pool.Alloc) error {
	path := filepath.Join(p.Prefix, p.Dir, statePath)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	allocResources := make(map[string]reflow.Resources, len(allocs))
	for _, alloc := range allocs {
		allocResources[alloc.ID()] = alloc.Resources()
	}
	if err := json.NewEncoder(file).Encode(allocResources); err != nil {
		file.Close()
		os.Remove(path)
	}
	file.Close()
	return nil
}

// updateDiskSize detects and updates the disk resources.
// It must be called while p.mu is locked.
func (p *Pool) updateDiskSize(r reflow.Resources) {
	root := filepath.Join(p.Prefix, p.Dir)
	diskSize := 2e12
	if existing, ok := r["disk"]; ok {
		diskSize = existing
	}
	if usage, err := fs.Stat(root); err == nil {
		r["disk"] = float64(usage.Total)
	} else {
		p.Log.Printf("refresh disk size (assuming %s), stat %s: %v", data.Size(diskSize), root, err)
		r["disk"] = diskSize
	}
}

// Start starts the pool. If the pool has a state snapshot, Start
// will restore the pool's previous state. Start will also make sure
// that all zombie allocs are collected.
func (p *Pool) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	ctx := context.Background()
	p.ResourcePool = pool.NewResourcePool(p, p.Log)
	info, err := p.Client.Info(ctx)
	if err != nil {
		return err
	}
	resources := reflow.Resources{
		"mem": math.Floor(float64(info.MemTotal) * 0.95),
		"cpu": float64(info.NCPU),
	}
	features, err := cpuFeatures()
	if err != nil {
		return err
	}
	for _, feature := range features {
		// Add one feature per CPU.
		resources[feature] = resources["cpu"]
	}
	root := filepath.Join(p.Prefix, p.Dir)
	if err := os.MkdirAll(root, 0777); err != nil {
		log.Printf("mkdir %s: %v", root, err)
	}
	p.updateDiskSize(resources)

	if err := os.MkdirAll(filepath.Join(p.Prefix, p.Dir, allocsPath), 0777); err != nil {
		return err
	}
	allocResources := map[string]reflow.Resources{}
	if file, err := os.Open(filepath.Join(p.Prefix, p.Dir, statePath)); err != nil {
		if os.IsNotExist(err) {
			p.Log.Printf("no state on disk")
		} else {
			return err
		}
	} else {
		if err := json.NewDecoder(file).Decode(&allocResources); err != nil {
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
	allocs := map[string]pool.Alloc{}
	for _, info := range infos {
		if !info.IsDir() {
			continue
		}
		id := info.Name()
		alloc := p.newAlloc(id, 0 /*keepalive*/)
		if err := alloc.restore(); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return err
		}
		if err := alloc.Start(); err != nil {
			return err
		}
		if _, ok := allocResources[id]; ok {
			delete(allocResources, id)
			allocs[id] = alloc
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
	for id := range allocResources {
		p.Log.Printf("orphaned alloc %s", id)
	}
	p.ResourcePool.Init(resources, allocs)
	return p.createCwLogGroup()
}

func (p *Pool) Resources() reflow.Resources {
	r := p.ResourcePool.Resources()
	p.updateDiskSize(r)
	return r
}

// Alloc looks up an alloc by ID.
func (p *Pool) Alloc(ctx context.Context, id string) (pool.Alloc, error) {
	alloc, err := p.ResourcePool.Alloc(ctx, id)
	if err == nil {
		return alloc, nil
	}
	// No matching live allocs, but look for dead (zombie) ones.
	if !errors.Is(errors.NotExist, err) {
		return nil, err
	}
	dir := filepath.Join(p.Prefix, p.Dir, allocsPath, id)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, errors.E("alloc", id, errors.NotExist)
	}
	return &zombie{manager: p, dir: dir, id: id}, nil
}

// Name implements `pool.AllocManager` and always returns "local".
func (p *Pool) Name() string {
	return "local"
}

// New implements `pool.AllocManager`.
// New creates a new alloc with the given id, alloc meta and initial keepalive.
// The list of other existing allocs are provided here to enable atomic saving
// of the state of all allocs.
func (p *Pool) New(ctx context.Context, id string, meta pool.AllocMeta, keepalive time.Duration, existing []pool.Alloc) (pool.Alloc, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	alloc := p.newAlloc(id, keepalive)
	if err := alloc.configure(meta); err != nil {
		return nil, err
	}
	if err := alloc.Start(); err != nil {
		return nil, err
	}
	if err := p.saveState(append(existing, alloc)); err != nil {
		p.Log.Errorf("error saving state: %s", err)
		if kerr := alloc.kill(); kerr != nil {
			p.Log.Errorf("error killing alloc: %s", kerr)
		}
		return nil, err
	}
	return alloc, nil
}

// Kill implements `pool.AllocManager` and kills the underlying alloc.
func (p *Pool) Kill(a pool.Alloc) error {
	alloc, ok := a.(*alloc)
	if !ok {
		panic(fmt.Sprintf("unexpected alloc type %T", a))
	}
	return alloc.kill()
}

// MaintainTaskDBRow maintains the taskdb row corresponding to this pool (if applicable).
// MaintainTaskDBRow blocks until the given context is done, if this pool has a taskdb
// implementation and a PoolID set,  Otherwise it'll return immediately.
// The taskdb row is expected to already exist, and this will simply update the Resources
// and maintains keepalive until ctx cancellation; and then it updates the End time of the row.
// MaintainTaskDBRow will panic if called on a Pool with no resources (ie, the pool must've been started)
func (p *Pool) MaintainTaskDBRow(ctx context.Context) {
	if p.TaskDB == nil || !p.TaskDBPoolId.IsValid() {
		return
	}
	if p.Resources().Equal(nil) {
		panic(fmt.Sprintf("MaintainTaskDBRow called on pool %v with no resources", p.TaskDBPoolId))
	}
	if err := p.TaskDB.SetResources(ctx, p.TaskDBPoolId.Digest(), p.Resources()); err != nil {
		p.Log.Debugf("taskdb pool %s SetResources: %v", p.TaskDBPoolId, err)
	}
	if err := taskdb.KeepIDAliveAndEnd(ctx, p.TaskDB, p.TaskDBPoolId.Digest(), 10*time.Second); err != nil {
		p.Log.Debugf("taskdb pool %s KeepIDAliveAndEnd: %v", p.TaskDBPoolId, err)
	}
}

// Alloc implements a local alloc. It embeds a local executor which
// does the heavy-lifting, while the alloc code deals with lifecycle
// and resource concerns.
type alloc struct {
	*Executor
	mu            sync.Mutex
	id            string
	taskDBAllocId reflow.StringDigest
	p             *Pool
	created       time.Time
	expires       time.Time
	lastKeepalive time.Time
	meta          pool.AllocMeta
	remoteStream
}

// createCwLogGroup creates the cloudwatch log group if it doesn't already exist.
func (p *Pool) createCwLogGroup() error {
	if p.Session == nil {
		return nil
	}
	cwl := cloudwatchlogs.New(p.Session)
	out, err := cwl.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{LogGroupNamePrefix: aws.String(remoteStreamCWLogGroupName)})
	if err != nil {
		p.Log.Debugf("DescribeLogGroups: %v", err)
	} else {
		var found bool
		for _, g := range out.LogGroups {
			if found = aws.StringValue(g.LogGroupName) == remoteStreamCWLogGroupName; found {
				break
			}
		}
		if found {
			return nil
		}
	}
	_, err = cwl.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(remoteStreamCWLogGroupName)})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == cloudwatchlogs.ErrCodeResourceAlreadyExistsException {
			err = nil
		}
	}
	return err
}

// NewAlloc creates a new alloc. The returned alloc is not started.
// keepalive is the duration to keep this alloc alive at the start
// (i.e. before any keepalive requests).
func (p *Pool) newAlloc(id string, keepalive time.Duration) *alloc {
	e := &Executor{
		ID:            id,
		Client:        p.Client,
		Dir:           filepath.Join(p.Dir, allocsPath, id),
		Prefix:        p.Prefix,
		Authenticator: p.Authenticator,
		AWSCreds:      p.AWSCreds,
		Blob:          p.Blob,
		Log:           p.Log.Tee(nil, id+": "),
		HardMemLimit:  p.HardMemLimit,
	}
	if p.Session != nil {
		cwlclient := cloudwatchlogs.New(p.Session)
		remoteStream, err := newCloudWatchLogs(cwlclient, remoteStreamCWLogGroupName)
		if err != nil {
			log.Errorf("create remote logger: %v", err)
		}
		e.remoteStream = remoteStream
	}
	var taskDBAllocId reflow.StringDigest
	if p.TaskDBPoolId.IsValid() {
		taskDBAllocId = reflow.NewStringDigest(p.TaskDBPoolId.String() + "/" + id)
	}
	// Note that we refresh the keepalive time on exec restore. This is
	// probably a useful safeguard, but could be annoying when keepalive
	// intervals are large.
	//
	// TODO(marius): persist alloc states across restarts. This doesn't
	// matter too much at present, as ec2 nodes are terminated when
	// the reflowlet terminates, but it should be done for potential future
	// implementations.
	return &alloc{
		Executor:      e,
		id:            id,
		taskDBAllocId: taskDBAllocId,
		p:             p,
		created:       time.Now(),
		expires:       time.Now().Add(keepalive),
		remoteStream:  e.remoteStream,
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
	if err := a.Executor.Start(); err != nil {
		return err
	}
	if a.p.TaskDB == nil || !a.taskDBAllocId.IsValid() {
		return nil
	}
	if err := a.p.TaskDB.StartAlloc(context.Background(), a.taskDBAllocId, a.p.TaskDBPoolId.Digest(), a.resources, a.created); err != nil {
		a.Log.Debugf("taskdb alloc %s StartAlloc: %v", a.taskDBAllocId, err)
	}
	return nil
}

// Keepalive maintains the alloc's lease as well as the alloc's corresponding row in taskdb (if applicable).
func (a *alloc) Keepalive(ctx context.Context, next time.Duration) (time.Duration, error) {
	if !a.p.Alive(a) {
		return time.Duration(0), errors.E("keepalive", a.id, fmt.Sprint(next), errors.NotExist, errAllocExpired)
	}
	a.mu.Lock()
	if next > pool.MaxKeepaliveInterval {
		next = pool.MaxKeepaliveInterval
	}
	a.lastKeepalive = time.Now()
	nextKeepalive := a.lastKeepalive.Add(next)
	a.expires = nextKeepalive
	a.mu.Unlock()
	if a.p.TaskDB != nil && a.taskDBAllocId.IsValid() {
		// Keepalive the alloc's taskdb row asynchronously
		go func() {
			tctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := a.p.TaskDB.KeepIDAlive(tctx, a.taskDBAllocId.Digest(), nextKeepalive); err != nil {
				a.Log.Debugf("taskdb alloc %s KeepIDAlive: %v", a.taskDBAllocId, err)
			}
		}()
	}
	a.Log.Printf("keepalive until %s", a.expires.Format(time.RFC3339))
	return next, nil
}

// Inspect returns the alloc's status.
func (a *alloc) Inspect(ctx context.Context) (pool.AllocInspect, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	i := pool.AllocInspect{
		ID:            a.id,
		TaskDBAllocID: a.taskDBAllocId.Digest(),
		Resources:     a.meta.Want,
		Meta:          a.meta,
		Created:       a.created,
		Expires:       a.expires,
		LastKeepalive: a.lastKeepalive,
	}
	return i, nil
}

// Free relinquishes this alloc from its pool.
func (a *alloc) Free(ctx context.Context) error {
	return a.p.Free(a)
}

// kill kills this alloc's executor and removes its repository,
// but its metadata and logs are kept intact so that they may be examined posthumously.
// kill will also update the end time of this alloc's task db row (if applicable).
func (a *alloc) kill() error {
	if !a.isDead() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		a.p.Log.Printf("killing alloc %s", a.id)
		if err := a.Kill(ctx); err != nil {
			a.Log.Debugf("killing alloc %s: %v", a.id, err)
		}
		if a.p.TaskDB != nil && a.taskDBAllocId.IsValid() {
			if err := a.p.TaskDB.SetEndTime(ctx, a.taskDBAllocId.Digest(), time.Now()); err != nil {
				a.Log.Debugf("taskdb alloc %s SetEndTime: %v", a.taskDBAllocId, err)
			}
		}
	}
	if a.remoteStream != nil {
		a.remoteStream.Close()
	}
	return nil
}
