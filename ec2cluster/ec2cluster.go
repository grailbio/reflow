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
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/status"
	"github.com/grailbio/infra"
	"github.com/grailbio/infra/tls"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/ec2authenticator"
	"github.com/grailbio/reflow/ec2cluster/instances"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/internal/ecrauth"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/pool/client"
	"golang.org/x/net/http2"
)

func init() {
	infra.Register("ec2cluster", new(Cluster))
}

const (
	ec2PollInterval     = time.Minute
	defaultMaxInstances = 100
	defaultClusterName  = "default"
)

// validateBootstrap is func for validating the bootstrap image
var validateBootstrap = defaultValidateBootstrap

// A Cluster implements a runner.Cluster backed by EC2.  The cluster expands
// with demand.  Instances are configured so that they shut down when they
// are idle on a billing boundary.
//
// No local state is stored; state is inferred from labels managed by EC2.
// Cluster supports safely sharing state across many processes. In
// this case, the processes coordinate to maintain a shared cluster,
// where instances can be used by any of the constituent processes.
// In the case of Reflow, this means that multiple runs (single or batch)
// share the same cluster efficiently.
type Cluster struct {
	pool.Mux `yaml:"-"`
	// HTTPClient is used to communicate to the reflowlet servers
	// running on the individual instances. In Cluster, this is done for
	// liveness/health checking.
	HTTPClient *http.Client `yaml:"-"`
	// Logger for cluster events.
	Log *log.Logger `yaml:"-"`
	// EC2 is the EC2 API instance through which EC2 calls are made.
	EC2 ec2iface.EC2API `yaml:"-"`
	// Authenticator authenticates the ECR repository that stores the
	// Reflowlet container.
	Authenticator ecrauth.Interface `yaml:"-"`
	// InstanceTags is the set of EC2 tags attached to instances created by this Cluster.
	InstanceTags map[string]string `yaml:"-"`
	// Labels is the set of labels that should be added as EC2 tags (for informational purpose only).
	Labels pool.Labels `yaml:"-"`
	// Spot is set to true when a spot instance is desired.
	Spot bool `yaml:"spot,omitempty"`
	// InstanceProfile is the EC2 instance profile to use for the cluster instances.
	InstanceProfile string `yaml:"instanceprofile,omitempty"`
	// SecurityGroup is the EC2 security group to use for cluster instances.
	SecurityGroup string `yaml:"securitygroup,omitempty"`
	// Subnet is the id of the EC2 subnet to use for cluster instances.
	Subnet string `yaml:"subnet,omitempty"`
	// AvailabilityZone defines which AZ to spawn instances into.
	AvailabilityZone string `yaml:"availabilityzone,omitempty"`
	// Region is the AWS availability region to use for launching new EC2 instances.
	Region string `yaml:"region,omitempty"`
	// InstanceTypesMap stores the set of admissible instance types.
	// If nil, all instance types are permitted.
	InstanceTypesMap map[string]bool `yaml:"-"`
	// BootstrapImage is the URL of the image used for instance bootstrap.
	BootstrapImage string `yaml:"-"`
	// ReflowVersion is the version of reflow binary compatible with this cluster.
	ReflowVersion string `yaml:"-"`
	// MaxInstances is the maximum number of concurrent instances permitted.
	MaxInstances int `yaml:"-"`
	// DiskType is the EBS disk type to use.
	DiskType string `yaml:"disktype"`
	// DiskSpace is the number of GiB of disk space to allocate for each node.
	DiskSpace int `yaml:"diskspace"`
	// DiskSlices is the number of EBS volumes that are used. When DiskSlices > 1,
	// they are arranged in a RAID0 array to increase throughput.
	DiskSlices int `yaml:"diskslices"`
	// AMI is the VM image used to launch new instances.
	AMI string `yaml:"ami"`
	// Configuration for this Reflow instantiation. Used to provide configs to
	// EC2 instances.
	Configuration infra.Config `yaml:"-"`

	// User's public SSH key.
	SshKey string `yaml:"sshkey"`
	// AWS key name for launching instances.
	KeyName string `yaml:"keyname"`
	// Immortal determines whether instances should be made immortal.
	Immortal bool `yaml:"immortal,omitempty"`
	// CloudConfig is merged into the instance's cloudConfig before launching.
	CloudConfig cloudConfig `yaml:"cloudconfig"`
	// SpotProbeDepth is the probing depth for spot instance capacity checks.
	SpotProbeDepth int `yaml:"spotprobedepth,omitempty"`

	// Status is used to report cluster and instance status.
	Status *status.Group `yaml:"-"`

	// InstanceTypesMap defines the set of allowable EC2 instance types for
	// this cluster. If empty, all instance types are permitted.
	InstanceTypes []string `yaml:"instancetypes,omitempty"`
	// Name is the name of the cluster config, which defaults to defaultClusterName.
	// Multiple clusters can be launched/maintained simultaneously by using different names.
	Name string `yaml:"name,omitempty"`

	instanceState   *instanceState
	instanceConfigs map[string]instanceConfig

	// state maintains the state of the cluster by keeping it in-sync with EC2.
	state *state

	wait chan *waiter
}

type header interface {
	Head(url string) (resp *http.Response, err error)
}

func defaultValidateBootstrap(burl string, h header) error {
	u, err := url.Parse(burl)
	if err != nil {
		return errors.E(errors.Fatal, "bootstrap image", err)
	}
	if u.Scheme != "https" {
		return errors.E(errors.Fatal, "bootstrap image", fmt.Errorf("scheme %s not supported: %s", u.Scheme, burl))
	}
	resp, err := h.Head(burl)
	switch {
	case err == nil && resp.StatusCode != http.StatusOK:
		err = errors.E(errors.Fatal, "bootstrap image", fmt.Errorf("HEAD %s: %s", burl, resp.Status))
	case resp == nil:
		err = errors.E(errors.Fatal, "bootstrap image", fmt.Errorf("HEAD %s: no response", burl))
	default:
		if contentType := resp.Header.Get("Content-Type"); contentType != "binary/octet-stream" {
			err = errors.E(errors.Fatal, "bootstrap image", fmt.Errorf("Content-Type not supported: %s", contentType))
		}
	}
	return err
}

// Help implements infra.Provider
func (Cluster) Help() string {
	return "configure a cluster using AWS EC2 compute nodes"
}

// Config implements infra.Provider
func (c *Cluster) Config() interface{} {
	return c
}

// Init implements infra.Provider
func (c *Cluster) Init(tls tls.Certs, sess *session.Session, labels pool.Labels, bootstrapimage *infra2.BootstrapImage, reflowVersion *infra2.ReflowVersion, id *infra2.User, logger *log.Logger, sshKey *infra2.SshKey) error {
	// If InstanceTypes are not defined, include built-in verified instance types.
	if len(c.InstanceTypes) == 0 {
		verified := instances.VerifiedByRegion[c.Region]
		for _, typ := range instances.Types {
			if !verified[typ.Name].Attempted || verified[typ.Name].Verified {
				c.InstanceTypes = append(c.InstanceTypes, typ.Name)
			}
		}
		sort.Strings(c.InstanceTypes)
	}
	clientConfig, _, err := tls.HTTPS()
	if err != nil {
		return err
	}
	transport := &http.Transport{TLSClientConfig: clientConfig}
	http2.ConfigureTransport(transport)
	httpClient := &http.Client{Transport: transport}
	svc := ec2.New(sess, &aws.Config{MaxRetries: aws.Int(13)})
	if reflowVersion.Value() == "" {
		return errors.New("no version specified in cluster configuration")
	}
	if err := validateBootstrap(bootstrapimage.Value(), http.DefaultClient); err != nil {
		return errors.E(errors.Fatal, fmt.Sprintf("bootstrap image: %s", bootstrapimage.Value()), err)
	}
	c.EC2 = svc
	c.Authenticator = ec2authenticator.New(sess)
	c.HTTPClient = httpClient
	c.Log = logger.Tee(nil, "ec2cluster: ")
	if c.Name == "" {
		c.Name = defaultClusterName
	}
	c.Labels = labels.Copy()
	c.BootstrapImage = bootstrapimage.Value()
	c.ReflowVersion = string(*reflowVersion)
	c.SshKey = sshKey.Value()
	if c.MaxInstances == 0 {
		c.MaxInstances = defaultMaxInstances
	}
	if len(c.InstanceTypes) > 0 {
		c.InstanceTypesMap = make(map[string]bool)
		for _, typ := range c.InstanceTypes {
			c.InstanceTypesMap[typ] = true
		}
	}
	qtags := make(map[string]string)
	qtags["Name"] = fmt.Sprintf("%s (reflow)", id.User())
	qtags["cluster"] = c.Name
	c.InstanceTags = qtags

	if err := c.initialize(); err != nil {
		return err
	}
	return nil
}

type waiter struct {
	reflow.Requirements
	ctx context.Context
	c   chan struct{}
}

func (w *waiter) Notify() {
	close(w.c)
}

// Initialize initializes the cluster's data structures. It must be called
// before use. Init also starts maintenance goroutines.
func (c *Cluster) initialize() error {
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
	c.wait = make(chan *waiter)

	c.InstanceTags["managedby"] = "reflow"

	// Construct the set of legal instances and set available disk space.
	var configs []instanceConfig
	c.instanceConfigs = make(map[string]instanceConfig)
	for _, config := range instanceTypes {
		config.Resources["disk"] = float64(c.DiskSpace << 30)
		if c.InstanceTypesMap == nil || c.InstanceTypesMap[config.Type] {
			configs = append(configs, config)
		}
		c.instanceConfigs[config.Type] = config
	}
	if len(configs) == 0 {
		return errors.New("no configured instance types")
	}
	c.instanceState = newInstanceState(configs, 5*time.Minute, c.Region)
	// TODO(swami):  Pass through a context from somewhere upstream as appropriate.
	ctx := context.Background()
	c.state = &state{c: c}
	c.state.Init()
	go c.state.Maintain(ctx)
	c.state.Sync()
	go c.loop()
	return nil
}

// Allocate reserves an alloc with within the resource requirement
// boundaries form this cluster. If an existing instance can serve
// the request, it is returned immediately; otherwise new instance(s)
// are spun up to handle the allocation.
func (c *Cluster) Allocate(ctx context.Context, req reflow.Requirements, labels pool.Labels) (alloc pool.Alloc, err error) {
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ticker := time.NewTicker(30 * time.Second)
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

// QueryTags returns the list of tags to use to query for instances belonging to this cluster.
// This includes all InstanceTags that are set on any instance brought up by this cluster,
// and a "reflowlet:version" tag (set on the instance by the reflowlet once it comes up)
// to match the ReflowVersion of this cluster.
func (c *Cluster) QueryTags() map[string]string {
	qtags := make(map[string]string)
	for k, v := range c.InstanceTags {
		qtags[k] = v
	}
	qtags["reflowlet:version"] = c.ReflowVersion
	return qtags
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

// Probe attempts to instantiate an EC2 instance of the given type and returns a duration and an error.
// In case of a nil error the duration represents how long it took (single data point) for a usable
// Reflowlet to come up on that instance type.
// A non-nil error means that the reflowlet failed to come up on this instance type.  The error
// could be due to context deadline, in case we gave up waiting for it to come up.
func (c *Cluster) Probe(ctx context.Context, instanceType string) (time.Duration, error) {
	config := c.instanceConfigs[instanceType]
	i := c.newInstance(config, config.Price[c.Region])
probe:
	i.Task = c.Status.Startf("%s", config.Type)
	i.Go(context.Background())
	i.ec2TerminateInstance()
	if i.Err() != nil {
		// If the error was due to Spot unavailability, try on-demand instead.
		if err := errors.Recover(i.Err()); i.Spot && errors.Is(errors.Unavailable, err) {
			i.Task.Printf("spot unavailable, trying on-demand")
			i.Spot = false
			i.err = nil
			i.Task.Done()
			goto probe
		}
		i.Task.Printf("%v", i.Err().Error())
	}
	i.Task.Done()
	dur := i.Task.Value().End.Sub(i.Task.Value().Begin)
	return dur.Round(time.Second), i.Err()
}

func (c *Cluster) newInstance(config instanceConfig, price float64) *instance {
	return &instance{
		HTTPClient:      c.HTTPClient,
		ReflowConfig:    c.Configuration,
		Config:          config,
		Log:             c.Log,
		Authenticator:   c.Authenticator,
		EC2:             c.EC2,
		InstanceTags:    c.InstanceTags,
		Labels:          c.Labels,
		Spot:            c.Spot,
		Subnet:          c.Subnet,
		InstanceProfile: c.InstanceProfile,
		SecurityGroup:   c.SecurityGroup,
		BootstrapImage:  c.BootstrapImage,
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
		i := c.newInstance(config, price)
		i.Task = c.Status.Startf("%s", config.Type)
		i.Go(context.Background())
		i.Task.Done()
		done <- i
	}

	for {
		var needPoll bool
		// Here we try to pack resource requests. First, we order each
		// request by the "magnitude" of the request (as defined by
		// (Resources).ScaledDistance) and then greedily pack the requests
		// until there is no instance type that can accommodate them.
		sort.Slice(waiters, func(i, j int) bool {
			return waiters[i].Min.ScaledDistance(nil) < waiters[j].Min.ScaledDistance(nil)
		})
		s := make([]string, len(waiters))
		if c.Log.At(log.DebugLevel) {
			for i, w := range waiters {
				s[i] = fmt.Sprintf("waiter%d%s", i, w.Min)
			}
			if len(pending) > 0 {
				c.Log.Debugf("pending%s %s", pending, strings.Join(s, ", "))
			}
		}
		var waiting reflow.Resources
		for _, w := range waiters {
			waiting.Add(waiting, w.Max())
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
		n := c.state.InstancesCount()
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
		var (
			counts     []string
			totalPrice float64
			total      reflow.Resources
		)
		n = 0
		for typ, ntyp := range c.state.InstanceTypeCounts() {
			counts = append(counts, fmt.Sprintf("%s:%d", typ, ntyp))
			config := c.instanceConfigs[typ]
			var r reflow.Resources
			r.Scale(config.Resources, float64(ntyp))
			total.Add(total, r)
			totalPrice += config.Price[c.Region] * float64(ntyp)
			n += ntyp
		}
		sort.Strings(counts)
		c.Status.Printf("%d instances: %s (<=$%.1f/hr), total%s, waiting%s, pending%s",
			n, strings.Join(counts, ","), totalPrice, total, waiting, pending)
		select {
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
			// TODO(swami): Deal with Fatal errors appropriately by propagating them up the stack.
			// In case of Fatal errors, retrying is going to result in the same error, so its better
			// to just escalate up the stack and stop trying.
			// case errors.Is(errors.Fatal, inst.Err()):
			default:
				continue
			}
			// Initiate a sync and wait for it to be done.
			c.state.Sync()
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

type reflowletPool struct {
	inst *reflowletInstance
	pool pool.Pool
}

// state helps maintain the state of the underlying cluster.
type state struct {
	c         *Cluster
	reconcile func(ctx context.Context) error

	mu   sync.Mutex
	pool map[string]reflowletPool

	smu  sync.Mutex
	sync chan struct{}

	pollInterval time.Duration
}

// Init initializes the state.
func (s *state) Init() {
	if s.pollInterval == 0 {
		s.pollInterval = ec2PollInterval
	}
	if s.reconcile == nil {
		s.reconcile = func(ctx context.Context) error {
			instances, err := s.getEC2State(ctx)
			if err != nil {
				return err
			}
			s.mu.Lock()
			defer s.mu.Unlock()

			// Remove from pool instances that are not available on EC2.
			for id := range s.pool {
				if instances[id] == nil {
					delete(s.pool, id)
				}
			}
			// Add instances on EC2 that are not in the pool.
			for id, inst := range instances {
				if _, ok := s.pool[id]; !ok {
					baseurl := fmt.Sprintf("https://%s:9000/v1/", *inst.PublicDnsName)
					clnt, err := client.New(baseurl, s.c.HTTPClient, nil)
					if err != nil {
						s.c.Log.Errorf("client %s: %v", baseurl, err)
						continue
					}
					// Add instance to the pool.
					s.pool[*inst.InstanceId] = reflowletPool{inst, clnt}
				}
			}
			s.c.SetPools(vals(s.pool))
			return nil
		}
	}
	s.pool = make(map[string]reflowletPool)
	s.sync = make(chan struct{})
}

// InstanceTypeCounts returns number of instances of each instance type present in the cluster pool.
func (s *state) InstanceTypeCounts() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	instanceTypes := make(map[string]int)
	for _, instance := range s.pool {
		instanceTypes[*instance.inst.InstanceType]++
	}
	return instanceTypes
}

// InstancesCount returns total number of instances (across all instance types) present in the cluster pool.
func (s *state) InstancesCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pool)
}

// Sync reconciles immediately and waits till its complete.
func (s *state) Sync() {
	s.smu.Lock()
	defer s.smu.Unlock()
	s.sync <- struct{}{}
	<-s.sync
}

// Maintain periodically reconciles local state with EC2.
func (s *state) Maintain(ctx context.Context) {
	tick := time.NewTicker(s.pollInterval)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			if err := s.reconcile(ctx); err != nil {
				s.c.Log.Errorf("maintain: %v", err)
			}
		case <-s.sync:
			if err := s.reconcile(ctx); err != nil {
				s.c.Log.Errorf("maintain: %v", err)
			}
			// Notify that sync is done.
			s.sync <- struct{}{}
		case <-ctx.Done():
			return
		}
	}
}

func (s *state) getEC2State(ctx context.Context) (map[string]*reflowletInstance, error) {
	var filters []*ec2.Filter
	for k, v := range s.c.QueryTags() {
		filters = append(filters, &ec2.Filter{
			Name: aws.String("tag:" + k), Values: []*string{aws.String(v)},
		})
	}
	req := &ec2.DescribeInstancesInput{Filters: filters, MaxResults: aws.Int64(1000)}
	instances := make(map[string]*reflowletInstance)
	for req != nil {
		ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
		resp, err := s.c.EC2.DescribeInstancesWithContext(ctx2, req)
		cancel()
		if err != nil {
			return nil, err
		}
		for _, resv := range resp.Reservations {
			for _, inst := range resv.Instances {
				switch *inst.State.Name {
				case "shutting-down", "terminated", "stopping", "stopped":
				default:
					instances[*inst.InstanceId] = newReflowletInstance(inst)
				}
			}
		}
		if resp.NextToken != nil {
			req.NextToken = resp.NextToken
		} else {
			req = nil
		}
	}
	return instances, nil
}

func vals(m map[string]reflowletPool) []pool.Pool {
	pools := make([]pool.Pool, len(m))
	i := 0
	for _, p := range m {
		pools[i] = p.pool
		i++
	}
	return pools
}
