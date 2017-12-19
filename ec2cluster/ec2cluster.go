// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package ec2cluster implements support for maintaining elastic
// clusters of Reflow instances on EC2.
//
// The EC2 instances created launch reflowlet agent processes that
// are given the user's profile token so that they can set up HTTPS
// servers that can perform mutual authentication to the reflow
// driver process and other reflowlets (for transferring objects) and
// also access external services like caching.
//
// The VM instances are configured to terminate if they are idle on
// EC2's billing hour boundary. They also terminate on any fatal
// reflowlet error.
package ec2cluster

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/state"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/ecrauth"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/pool/client"
)

const (
	ec2PollInterval   = 30 * time.Second
	statePollInterval = 10 * time.Second
)

// A Cluster implements a runner.Cluster backed by EC2. The cluster
// is stateful (stored by a local state.File), and expands with
// demand. Instances are configured so that they shut down when they
// are idle on a billing boundary.
//
// Cluster supports safely sharing state across many processes. In
// this case, the processes coordinate to maintain a shared cluster,
// where instances can be used by any of the constituent processes.
// In the case of Reflow, this means that multiple runs (single or batch)
// share the same cluster efficiently.
type Cluster struct {
	pool.Mux

	// HTTPClient is used to communicate to the reflowlet servers
	// running on the individual instances. In Cluster, this is done for
	// liveness/health checking.
	HTTPClient *http.Client
	// Logger for cluster events.
	Log *log.Logger
	// File stores the cluster's state.
	File *state.File
	// EC2 is the EC2 API instance through which EC2 calls are made.
	EC2 *ec2.EC2
	// Authenticator authenticates the ECR repository that stores the
	// Reflowlet container.
	Authenticator ecrauth.Interface
	// Type specifies the instance types used for this cluster.
	// If no type is specified, the cluster picks an instance type that
	// best matches the resource requirements of the requested allocs.
	Type string
	// Tag is the tag that's attached instance types created by this cluster.
	Tag string
	// Labels is the set of labels that should be associated with newly created instances.
	Labels pool.Labels
	// Spot is set to true when a spot instance is desired.
	Spot bool
	// SecurityGroup is the EC2 security group to use for cluster instances.
	SecurityGroup string
	// Region is the AWS availability region to use for launching new EC2 instances.
	Region string
	// InstanceTypes stores the set of admissible instance types.
	InstanceTypes map[string]bool
	// ReflowletImage is the Docker URI of the image used for instance reflowlets.
	// The image must be retrievable by the cluster's authenticator.
	ReflowletImage string
	// MaxInstances is the maximum number of concurrent instances permitted.
	MaxInstances int
	// DiskType is the EBS disk type to use.
	DiskType string
	// DiskSpace is the number of GiB of disk space to allocate for each node.
	DiskSpace int
	// AMI is the VM image used to launch new instances.
	AMI string
	// The config for this Reflow instantiation. Used to provide configs to
	// EC2 instances.
	Config config.Config
	// User's public SSH key.
	SshKey string
	// AWS key name for launching instances.
	KeyName string
	// Immortal determines whether instances should be made immortal.
	Immortal bool

	instanceState *instanceState
	pools         map[string]pool.Pool
	pending       []*instance
	wait          chan *waiter
}

type waiter struct {
	Min, Max reflow.Resources
	ctx      context.Context
	c        chan struct{}
}

func (w *waiter) Notify() {
	close(w.c)
}

// Init initializes the cluster's data structures. It must be called
// before use. Init also starts maintenance goroutines.
func (c *Cluster) Init() error {
	if c.MaxInstances == 0 {
		return errors.New("missing max instances parameter")
	}
	if c.DiskType == "" {
		return errors.New("missing disk type parameter")
	}
	if c.DiskSpace == 0 {
		return errors.New("missing disk space parameter")
	}
	if c.AMI == "" {
		return errors.New("missing AMI parameter")
	}
	if c.Region == "" {
		return errors.New("missing region parameter")
	}
	if c.SecurityGroup == "" {
		return errors.New("missing EC2 security group")
	}
	c.pools = map[string]pool.Pool{}
	c.wait = make(chan *waiter)

	// Construct the set of legal instances and set available disk space.
	var instances []instanceConfig
	for _, config := range instanceTypes {
		if c.InstanceTypes[config.Type] {
			config.Resources.Disk = uint64(c.DiskSpace << 30)
			instances = append(instances, config)
		}
	}
	if len(instances) == 0 {
		return errors.New("no configured instance types")
	}
	c.instanceState = newInstanceState(instances, 5*time.Minute, c.Region)

	c.update()
	go c.maintain()
	go c.loop()
	return nil
}

// Allocate reserves an alloc with within the resource requirement
// boundaries form this cluster. If an existing instance can serve
// the request, it is returned immediately; otherwise new instance(s)
// are spun up to handle the allocation.
func (c *Cluster) Allocate(ctx context.Context, min, max reflow.Resources, labels pool.Labels) (pool.Alloc, error) {
	c.Log.Debugf("allocate min(%v) max(%v)", min, max)
	if t := c.instanceState.Max(); !t.Resources.Available(min) {
		return nil, errors.E(errors.ResourcesExhausted,
			errors.Errorf("requested resources (%s) exceeds maximum available instance type %s (%s)",
				min, t.Type, t.Resources))
	}

	if c.Size() > 0 {
		c.Log.Debug("attempting to allocate from existing pool")
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		alloc, err := pool.Allocate(ctx, c, min, max, labels)
		cancel()
		if err == nil {
			return alloc, nil
		}
		c.Log.Debugf("failed to allocate from existing pool: %v; provisioning from EC2", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	needch := c.need(ctx, min, max)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-needch:
			actx, acancel := context.WithTimeout(ctx, 30*time.Second)
			alloc, err := pool.Allocate(actx, c, min, max, labels)
			acancel()
			if err == nil {
				return alloc, nil
			}
			c.Log.Errorf("failed to allocate from pool: %v; provisioning new instances", err)
			// We didn't get it--try again!
			needch = c.need(ctx, min, max)
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			alloc, err := pool.Allocate(ctx, c, min, max, labels)
			cancel()
			if err == nil {
				return alloc, nil
			}
		}
	}
}

func (c *Cluster) need(ctx context.Context, min, max reflow.Resources) <-chan struct{} {
	w := &waiter{
		Min: min,
		Max: max,
		ctx: ctx,
		c:   make(chan struct{}),
	}
	c.wait <- w
	return w.c
}

// loop services requests to expand the cluster's capacity.
func (c *Cluster) loop() {
	const maxPending = 5
	var (
		waiters  []*waiter
		pending  reflow.Resources
		npending int
		done     = make(chan *instance)
	)
	launch := func(config instanceConfig, price float64) {
		i := &instance{
			HTTPClient:     c.HTTPClient,
			ReflowConfig:   c.Config,
			Config:         config,
			Log:            c.Log,
			Authenticator:  c.Authenticator,
			EC2:            c.EC2,
			Tag:            c.Tag,
			Labels:         c.Labels,
			Spot:           c.Spot,
			SecurityGroup:  c.SecurityGroup,
			ReflowletImage: c.ReflowletImage,
			Price:          price,
			EBSType:        c.DiskType,
			EBSSize:        config.Resources.Disk >> 30,
			AMI:            c.AMI,
			SshKey:         c.SshKey,
			KeyName:        c.KeyName,
			Immortal:       c.Immortal,
		}
		i.Go(context.Background())
		done <- i
	}

	for {
		var instances map[string]*ec2.Instance
		c.File.Unmarshal(&instances)
		n := len(instances)
		var need reflow.Resources
		var needPoll bool
		maxInstance, ok := c.instanceState.MaxAvailable(c.Spot)
		if !ok {
			c.Log.Printf("no instance types are currently available")
			needPoll = true
			goto sleep
		}
		for _, w := range waiters {
			if maxInstance.Resources.LessAny(w.Min) {
				// We can't satisfy the minimum requirements of this waiter. Drop it.
				c.Log.Printf("no instance types can currently satisfy resource requirements %v", w.Min)
			} else if w.Max == reflow.MaxResources {
				// This is in the case of "wide" requests.
				// In these cases we simply opt for the biggest allowable instance type.
				// TODO(marius): this should be more sophisticated, relying on profile data
				// or otherwise deploy somewhat conservative heuristics.
				need = maxInstance.Resources
			} else {
				need = need.Add(w.Max)
			}
		}
		if len(waiters) > 0 && need.IsZeroAll() {
			c.Log.Print("resource requirements are unsatisfiable by current instance selection")
			needPoll = true
			goto sleep
		}
		for pending.LessAny(need) && npending < maxPending && n+npending < c.MaxInstances {
			var best instanceConfig
			if c.Type != "" {
				best, ok = c.instanceState.Type(c.Type)
				if !ok {
					c.Log.Printf("requested instance type %s is currently unavailable", c.Type)
					needPoll = true
					break
				}
			} else {
				// TODO(marius): set disk sizes dynamically
				// TODO(marius): use a more sophisticated scoring scheme to pick instance types
				best, ok = c.instanceState.MinAvailable(need, c.Spot)
				if !ok {
					c.Log.Printf("no instance types matching requirements %s are currently available", need)
					needPoll = true
					break
				}
			}
			pending = pending.Add(best.Resources)
			npending++
			c.Log.Debugf("launch %v need(%v) pending(%v)", best.Type, need, pending)
			go launch(best, best.Price[c.Region])
		}
	sleep:
		var pollch <-chan time.Time
		if needPoll {
			pollch = time.After(time.Minute)
		}
		select {
		case <-pollch:
		case inst := <-done:
			pending = pending.Sub(inst.Config.Resources)
			npending--
			switch {
			case inst.Err() == nil:
			case errors.Is(errors.Unavailable, inst.Err()):
				c.Log.Printf("instance type %s unavailable in region %s: %v", inst.Config.Type, c.Region, inst.Err())
				c.instanceState.Unavailable(inst.Config)
				fallthrough
			default:
				continue
			}
			c.add(inst.Instance())
			var ws []*waiter
			available := inst.Config.Resources
			for _, w := range waiters {
				if w.ctx.Err() != nil {
					continue
				}
				if w.Min.LessEqualAll(available) {
					available = available.Sub(w.Max.Min(available))
					w.Notify()
				} else {
					ws = append(ws, w)
				}
			}
			waiters = ws
			c.Log.Debugf("added instance %s resources(%s) pending(%s) npending(%d) waiters(%d)",
				inst.Config.Type, inst.Config.Resources, pending, npending, len(waiters))
		case w := <-c.wait:
			var ws []*waiter
			for _, w := range waiters {
				if w.ctx.Err() == nil {
					ws = append(ws, w)
				}
			}
			waiters = append(ws, w)
		}
	}
}

// maintain reconciles external state changes with local state.
func (c *Cluster) maintain() {
	ec2Tick := time.NewTicker(ec2PollInterval)
	updateTick := time.NewTicker(statePollInterval)
	if err := c.reconcile(); err != nil {
		c.Log.Printf("reconcile error: %v", err)
	}
	for {
		select {
		case <-ec2Tick.C:
			if err := c.reconcile(); err != nil {
				c.Log.Printf("reconcile error: %v", err)
			}
		case <-updateTick.C:
			c.update()
		}
	}
}

func (c *Cluster) updateState(update func(map[string]*ec2.Instance)) {
	c.File.Lock()
	instances := map[string]*ec2.Instance{}
	c.File.Unmarshal(&instances)
	update(instances)
	if err := c.File.Marshal(instances); err != nil {
		c.Log.Printf("marshal state error: %v", err)
	}
	c.File.Unlock()
	c.update()
}

func (c *Cluster) add(newInstances ...*ec2.Instance) {
	c.updateState(func(instances map[string]*ec2.Instance) {
		for _, inst := range newInstances {
			instances[*inst.InstanceId] = inst
		}
	})
}

func (c *Cluster) remove(instanceIds ...string) {
	c.updateState(func(instances map[string]*ec2.Instance) {
		for _, id := range instanceIds {
			delete(instances, id)
		}
	})
}

func (c *Cluster) update() {
	var instances map[string]*ec2.Instance
	if err := c.File.Unmarshal(&instances); err != nil {
		if err != state.ErrNoState {
			c.Log.Printf("error unmarshal state: %v", err)
		}
		return
	}
	for id, inst := range instances {
		if c.pools[id] == nil {
			baseurl := fmt.Sprintf("https://%s:9000/v1/", *inst.PublicDnsName)
			var err error
			c.pools[*inst.InstanceId], err = client.New(
				baseurl,
				c.HTTPClient, nil /*log.New(os.Stderr, "client: ", 0)*/)
			if err != nil {
				c.Log.Printf("client %s: %v", baseurl, err)
			}
		}
	}
	for id := range c.pools {
		if instances[id] == nil {
			delete(c.pools, id)
		}
	}
	c.SetPools(vals(c.pools))
}

func (c *Cluster) reconcile() error {
	var instances map[string]*ec2.Instance
	if err := c.File.Unmarshal(&instances); err != nil {
		if err == state.ErrNoState {
			return nil
		}
		return err
	}
	live := map[string]bool{}
	for id := range instances {
		// We have to do instances one-by-one here: if we specify multiple
		// instance IDs in a batch, and one of them has dissapeared, the
		// whole call fails with an error which makes it impossible
		// (without resorting to parsing the error message) to determine
		// which ID has dissapeared.
		input := ec2.DescribeInstancesInput{
			InstanceIds: []*string{aws.String(id)},
		}
		resp, err := c.EC2.DescribeInstances(&input)
		if err != nil {
			if err, ok := err.(awserr.Error); ok && err.Code() == "InvalidInstanceID.NotFound" {
				c.Log.Printf("marking instance %s down: not found", id)
				continue
			}
			return err
		}
		for _, resv := range resp.Reservations {
			for _, inst := range resv.Instances {
				// For some reason, we keep getting unrelated instances in these
				// requests.
				if instances[*inst.InstanceId] == nil {
					continue
				}
				switch *inst.State.Name {
				case "shutting-down", "terminated", "stopping", "stopped":
					c.Log.Printf("marking instance %s down: %s", *inst.InstanceId, *inst.State.Name)
				default:
					live[*inst.InstanceId] = true
				}
			}
		}
	}
	var dead []string
	for id := range instances {
		if !live[id] {
			dead = append(dead, id)
		}
	}
	c.remove(dead...)
	return nil
}

func vals(m map[string]pool.Pool) []pool.Pool {
	pools := make([]pool.Pool, len(m))
	i := 0
	for _, p := range m {
		pools[i] = p
		i++
	}
	return pools
}
