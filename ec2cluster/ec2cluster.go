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
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/state"
	"github.com/grailbio/base/status"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/config"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/internal/ecrauth"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/pool/client"
)

const (
	ec2PollInterval = time.Minute
	// ec2MaxFilter is the maximum number of filter expressions
	// that are permitted in EC2 API calls.
	ec2MaxFilter      = 200
	statePollInterval = 10 * time.Second
)

var zeroResources = reflow.Resources{"cpu": 0, "mem": 0, "disk": 0}

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
	EC2 ec2iface.EC2API
	// Authenticator authenticates the ECR repository that stores the
	// Reflowlet container.
	Authenticator ecrauth.Interface
	// Tag is the tag that's attached instance types created by this cluster.
	Tag string
	// Labels is the set of labels that should be associated with newly created instances.
	Labels pool.Labels
	// Spot is set to true when a spot instance is desired.
	Spot bool
	// InstanceProfile is the EC2 instance profile to use for the cluster instances.
	InstanceProfile string
	// SecurityGroup is the EC2 security group to use for cluster instances.
	SecurityGroup string
	// Region is the AWS availability region to use for launching new EC2 instances.
	Region string
	// InstanceTypes stores the set of admissible instance types.
	// If nil, all instance types are permitted.
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
	// DiskSlices is the number of EBS volumes that are used. When DiskSlices > 1,
	// they are arranged in a RAID0 array to increase throughput.
	DiskSlices int
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
	// CloudConfig is merged into the instance's cloudConfig before launching.
	CloudConfig cloudConfig
	// SpotProbeDepth is the probing depth for spot instance capacity checks.
	SpotProbeDepth int

	// Status is used to report cluster and instance status.
	Status *status.Group

	instanceState   *instanceState
	instanceConfigs map[string]instanceConfig
	pools           map[string]pool.Pool
	reconciled      chan struct{}

	wait chan *waiter
}

type waiter struct {
	reflow.Requirements
	ctx context.Context
	c   chan struct{}
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
	c.instanceConfigs = make(map[string]instanceConfig)
	for _, config := range instanceTypes {
		config.Resources["disk"] = float64(c.DiskSpace << 30)
		if c.InstanceTypes == nil || c.InstanceTypes[config.Type] {
			instances = append(instances, config)
		}
		c.instanceConfigs[config.Type] = config
	}
	if len(instances) == 0 {
		return errors.New("no configured instance types")
	}
	c.instanceState = newInstanceState(instances, 5*time.Minute, c.Region)
	c.reconciled = make(chan struct{}, 1)

	c.update()
	go c.maintain()
	go c.loop()
	return nil
}

// Allocate reserves an alloc with within the resource requirement
// boundaries form this cluster. If an existing instance can serve
// the request, it is returned immediately; otherwise new instance(s)
// are spun up to handle the allocation.
func (c *Cluster) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (alloc pool.Alloc, err error) {
	task := c.Status.Startf("allocate %s", req)
	defer func() {
		if err != nil {
			task.Print(err)
		} else {
			task.Print("alloc ", alloc.ID())
		}
		task.Done()
	}()
	c.Log.Debugf("allocate %s", req)
	if !c.instanceState.Available(req.Min) {
		return nil, errors.E(errors.ResourcesExhausted,
			errors.Errorf("requested resources %s not satisfiable by any available instance type", req))
	}

	if c.Size() > 0 {
		c.Log.Debug("attempting to allocate from existing pool")
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		alloc, err := pool.Allocate(ctx, c, req, labels)
		cancel()
		if err == nil {
			return alloc, nil
		}
		c.Log.Debugf("failed to allocate from existing pool: %v; provisioning from EC2", err)
	}
	task.Print("provisioning new instance")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	needch := c.allocate(ctx, req)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-needch:
			actx, acancel := context.WithTimeout(ctx, 30*time.Second)
			alloc, err := pool.Allocate(actx, c, req, labels)
			acancel()
			if err == nil {
				return alloc, nil
			}
			c.Log.Errorf("failed to allocate from pool: %v; provisioning new instances", err)
			// We didn't get it--try again!
			needch = c.allocate(ctx, req)
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			alloc, err := pool.Allocate(ctx, c, req, labels)
			cancel()
			if err == nil {
				return alloc, nil
			}
		}
	}
}

func (c *Cluster) allocate(ctx context.Context, req reflow.Requirements) <-chan struct{} {
	w := &waiter{
		Requirements: req,
		ctx:          ctx,
		c:            make(chan struct{}),
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
			HTTPClient:      c.HTTPClient,
			ReflowConfig:    c.Config,
			Config:          config,
			Log:             c.Log,
			Authenticator:   c.Authenticator,
			EC2:             c.EC2,
			Tag:             c.Tag,
			Labels:          c.Labels,
			Spot:            c.Spot,
			InstanceProfile: c.InstanceProfile,
			SecurityGroup:   c.SecurityGroup,
			ReflowletImage:  c.ReflowletImage,
			Price:           price,
			EBSType:         c.DiskType,
			EBSSize:         uint64(config.Resources["disk"]) >> 30,
			NEBS:            c.DiskSlices,
			AMI:             c.AMI,
			SshKey:          c.SshKey,
			KeyName:         c.KeyName,
			SpotProbeDepth:  c.SpotProbeDepth,
			Immortal:        c.Immortal,
			CloudConfig:     c.CloudConfig,
		}
		i.Task = c.Status.Startf("%s", config.Type)
		i.Go(context.Background())
		i.Task.Done()
		done <- i
	}

	for {
		instances, _ := c.unmarshalState()
		n := len(instances)
		var needPoll bool
		// Here we try to pack resource requests. First, we order each
		// request by the "magnitude" of the request (as defined by
		// (Resources).ScaledDistance) and then greedily pack the requests
		// until there is no instance type that can accomodate them.
		sort.Slice(waiters, func(i, j int) bool {
			return waiters[i].Min.ScaledDistance(nil) < waiters[j].Min.ScaledDistance(nil)
		})
		s := make([]string, len(waiters))
		if c.Log.At(log.DebugLevel) {
			for i, w := range waiters {
				s[i] = fmt.Sprintf("waiter%d%s", i, w.Min)
			}
			c.Log.Debugf("pending%s %s", pending, strings.Join(s, ", "))
		}
		var total, waiting reflow.Resources
		for _, w := range waiters {
			waiting.Add(waiting, w.Max())
		}
		var totalPrice float64
		for _, instance := range instances {
			config := c.instanceConfigs[*instance.InstanceType]
			total.Add(total, config.Resources)
			totalPrice += config.Price[c.Region]
		}

		// First skip waiters that are already getting their resources
		// satisfied.
		//
		// TODO(marius): this should take into account the actual
		// granularity of the pending instances. This doesn't matter too
		// much since allocation is ordered by size, and thus we'll make
		// progress since no instances smaller than the smallest allocation
		// are ever launched. But it could be wasteful if there's a lot of
		// churn.
		var (
			i       int
			howmuch reflow.Resources
		)
		for i < len(waiters) {
			howmuch.Add(howmuch, waiters[i].Min)
			if !pending.Available(howmuch) {
				break
			}
			i++
		}
		needMore := len(waiters) > 0 && i != len(waiters)
		var todo []instanceConfig
		for i < len(waiters) {
			var need reflow.Resources
			w := waiters[i]
			need.Add(need, w.Min)
			i++
			best, ok := c.instanceState.MinAvailable(need, c.Spot)
			if !ok {
				c.Log.Debugf("no currently available instance type can satisfy resource requirements %v", w.Min)
				continue
			}
			// For wide requests, we simply try to find the largest available
			// instance that will support some portion of the load. We don't
			// pack more waiters. The workers will attempt to allocate the
			// whole instance anyway.
			if w.Width > 0 {
				for j := 1; j < w.Width; j++ {
					need.Add(need, w.Min)
					wbest, ok := c.instanceState.MinAvailable(need, c.Spot)
					if !ok {
						break
					}
					best = wbest
				}
			} else {
				for i < len(waiters) {
					need.Add(need, waiters[i].Min)
					wbest, ok := c.instanceState.MinAvailable(need, c.Spot)
					if !ok {
						break
					}
					best = wbest
					i++
				}
			}
			todo = append(todo, best)
		}
		if needMore && len(todo) == 0 {
			c.Log.Print("resource requirements are unsatisfiable by current instance selection")
			needPoll = true
			goto sleep
		}
		for len(todo) > 0 && npending < maxPending && n+npending < c.MaxInstances {
			var config instanceConfig
			config, todo = todo[0], todo[1:]
			pending.Add(pending, config.Resources)
			npending++
			c.Log.Debugf("launch %v%v pending%v", config.Type, config.Resources, pending)
			go launch(config, config.Price[c.Region])
		}
	sleep:
		var pollch <-chan time.Time
		if needPoll {
			pollch = time.After(time.Minute)
		}
		instanceTypes := make(map[string]int)
		for _, instance := range instances {
			instanceTypes[*instance.InstanceType]++
		}
		var counts []string
		for typ, n := range instanceTypes {
			counts = append(counts, fmt.Sprintf("%s:%d", typ, n))
		}
		sort.Strings(counts)
		c.Status.Printf("%d instances: %s (<=$%.1f/hr), total%s, waiting%s, pending%s",
			n, strings.Join(counts, ","), totalPrice, total, waiting, pending)
		select {
		case <-c.reconciled:
		case <-pollch:
		case inst := <-done:
			pending.Sub(pending, inst.Config.Resources)
			npending--
			switch {
			case inst.Err() == nil:
			case errors.Is(errors.Unavailable, inst.Err()):
				c.Log.Debugf("instance type %s unavailable in region %s: %v", inst.Config.Type, c.Region, inst.Err())
				c.instanceState.Unavailable(inst.Config)
				fallthrough
			default:
				continue
			}
			c.add(inst.Instance())
			var (
				ws        []*waiter
				available = inst.Config.Resources
				nnotify   int
			)
			for _, w := range waiters {
				if w.ctx.Err() != nil {
					continue
				}
				if available.Available(w.Min) {
					var tmp reflow.Resources
					tmp.Min(w.Max(), available)
					available.Sub(available, tmp)
					w.Notify()
					nnotify++
				} else {
					ws = append(ws, w)
				}
			}
			waiters = ws
			c.Log.Debugf("added instance %s resources%s pending%s available%s npending:%d waiters:%d notified:%d",
				inst.Config.Type, inst.Config.Resources, pending, available, npending, len(waiters), nnotify)
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

func (c *Cluster) updateState(update func(map[string]*reflowletInstance)) {
	c.File.Lock()
	instances, _ := c.unmarshalState()
	update(instances)
	if err := c.File.Marshal(instances); err != nil {
		c.Log.Printf("marshal state error: %v", err)
	}
	c.File.Unlock()
	c.update()
}

func (c *Cluster) add(newInstances ...*reflowletInstance) {
	c.updateState(func(instances map[string]*reflowletInstance) {
		for _, inst := range newInstances {
			instances[*inst.InstanceId] = inst
		}
	})
}

func (c *Cluster) remove(instanceIds ...string) {
	c.updateState(func(instances map[string]*reflowletInstance) {
		for _, id := range instanceIds {
			delete(instances, id)
		}
	})
}

func (c *Cluster) update() {
	instances, err := c.unmarshalState()
	if err != nil {
		c.Log.Printf("error unmarshal state: %v", err)
		return
	}
	for id, inst := range instances {
		if c.pools[id] == nil {
			baseurl := fmt.Sprintf("https://%s:9000/v1/", *inst.PublicDnsName)
			clnt, err := client.New(baseurl, c.HTTPClient, nil /*log.New(os.Stderr, "client: ", 0)*/)
			if err != nil {
				c.Log.Printf("client %s: %v", baseurl, err)
				continue
			}
			// check version compatibility.
			if reflowVer, err := getString(c.Config, "reflowversion"); err != nil {
				c.Log.Debugf("unable to get reflow version: %v", err)
			} else if reflowVer != inst.Version {
				c.Log.Debugf("reflow (version: %s) incompatible with instance %v running reflowlet image: %s (version: %s)", reflowVer, id, c.ReflowletImage, inst.Version)
				continue
			}
			// Versions are compatible!
			c.pools[*inst.InstanceId] = clnt
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
	instances, err := c.unmarshalState()
	if err != nil {
		return err
	}
	var instanceIds []*string
	for id := range instances {
		instanceIds = append(instanceIds, aws.String(id))
	}
	// The EC2 API has a limit to the number of filters that are permissible in a single
	// call, so we have to page through our instance IDs here.
	live := map[string]bool{}
	for len(instanceIds) > 0 {
		var queryInstanceIds []*string
		if len(instanceIds) > ec2MaxFilter {
			queryInstanceIds = instanceIds[:ec2MaxFilter]
			instanceIds = instanceIds[ec2MaxFilter:]
		} else {
			queryInstanceIds = instanceIds
			instanceIds = nil
		}
		var q []string
		for _, id := range queryInstanceIds {
			q = append(q, *id)
		}
		resp, err := c.EC2.DescribeInstances(&ec2.DescribeInstancesInput{
			Filters: []*ec2.Filter{{
				Name:   aws.String("instance-id"),
				Values: queryInstanceIds,
			}},
		})
		if err != nil {
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
					c.Log.Debugf("marking instance %s down: %s", *inst.InstanceId, *inst.State.Name)
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
	select {
	case c.reconciled <- struct{}{}:
	default:
	}
	return nil
}

func (c *Cluster) unmarshalState() (map[string]*reflowletInstance, error) {
	var instances map[string]*reflowletInstance
	if err := c.File.Unmarshal(&instances); err != nil && err != state.ErrNoState {
		return nil, err
	}
	return instances, nil
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

func getString(c config.Config, key string) (string, error) {
	val, ok := c.Value(key).(string)
	if !ok {
		return "", fmt.Errorf("key \"%s\" is not a string", key)
	}
	return val, nil
}
