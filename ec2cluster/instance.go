// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

//go:generate go run ../cmd/ec2instances/main.go instances

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"text/template"
	"time"
	"unicode"

	"github.com/grailbio/base/retry"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/digest"
	"github.com/grailbio/base/status"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	bootc "github.com/grailbio/reflow/bootstrap/client"
	"github.com/grailbio/reflow/bootstrap/common"
	"github.com/grailbio/reflow/ec2cluster/instances"
	"github.com/grailbio/reflow/errors"
	infra2 "github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/internal/ecrauth"
	"github.com/grailbio/reflow/internal/execimage"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/pool"
	poolc "github.com/grailbio/reflow/pool/client"
	"gopkg.in/yaml.v2"
)

// memoryDiscount is the amount of memory that's reserved by the
// reflowlet and EC2 together. Ideally we'd be able to be more
// precise about this (e.g., extract actual guarantees about
// available memory from AWS), but this doesn't seem possible at this
// time.
//
// We reserve 5% for the Reflowlet, and the EC2 overhead appears to
// be a little shy of 2%.
const memoryDiscount = 0.05 + 0.02

var (
	// bootstrapArgs is the arguments passed to the bootstrap image
	bootstrapArgs = []string{"-config", "/etc/reflowconfig"}
	// reflowletArgs is the arguments passed to the reflow binary to run a reflowlet
	reflowletArgs = append(bootstrapArgs, "serve", "-ec2cluster")
)

const (
	// ebsThroughputPremiumCost defines the higher premium in USD dollars
	// we are willing to pay for an instance with at least ebsThroughputBenefitPct more EBS throughput
	ebsThroughputPremiumCost = 0.03

	// ebsThroughputPremiumPct defines the higher premium (as a percentage)
	// we are willing to pay for at least ebsThroughputBenefitPct increased EBS throughput
	// when choosing instance types.
	ebsThroughputPremiumPct = 15.0

	// ebsThroughputBenefitPct is the percentage higher EBS throughput we require
	// to justify paying the premium.
	ebsThroughputBenefitPct = 50.0

	// gp2MaxThroughputMinSizeGiB is the minimum disk size of a gp2 EBS volume
	// for maximum throughput.
	// 334GiB is the smallest disk size that yields maximum throughput, as per
	// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSVolumeTypes.html
	gp2MaxThroughputMinSizeGiB = 334

	// gp2PerEBSThresholdGiB is the disk size beyond which we would instead prefer
	// to have EBS volumes of size gp2MaxThroughputMinSizeGiB to optimize for throughput.
	gp2PerEBSThresholdGiB = 200
)

// the smallest acceptable disk sizes (GiB) per EBS volume type.
var minDiskSizes = map[string]uint64{
	// EBS does not allow you to create ST1 volumes smaller than 500GiB.
	"st1": 500,
	"gp2": 1,
}

// instanceConfig represents a instance configuration.
type instanceConfig struct {
	// Type is the EC2 instance type to be launched.
	Type string

	// EBSOptimized is true if we should request an EBS optimized instance.
	EBSOptimized bool
	// EBSThroughput is the max throughput for the EBS optimized instance.
	EBSThroughput float64
	// Resources holds the Reflow resources that are presented by this configuration.
	// It does not include disk sizes; they are dynamic.
	Resources reflow.Resources
	// Price is the on-demand price for this instance type in fractional dollars, in available regions.
	Price map[string]float64
	// SpotOk tells whether spot is supported for this instance type.
	SpotOk bool
	// NVMe specifies whether EBS is exposed as NVMe devices.
	NVMe bool
}

var (
	instanceTypes = map[string]instanceConfig{}
	localDigest   digest.Digest
	digestOnce    once.Task
	reflowletOnce once.Task
	reflowletFile reflow.File
)

func init() {
	for _, typ := range instances.Types {
		instanceTypes[typ.Name] = instanceConfig{
			Type:          typ.Name,
			EBSOptimized:  typ.EBSOptimized,
			EBSThroughput: typ.EBSThroughput,
			Price:         typ.Price,
			Resources: reflow.Resources{
				"cpu": float64(typ.VCPU),
				"mem": (1 - memoryDiscount) * typ.Memory * 1024 * 1024 * 1024,
			},
			// According to Amazon, "t2" instances are the only current-generation
			// instances not supported by spot.
			SpotOk: typ.Generation == "current" && !strings.HasPrefix(typ.Name, "t2."),
			NVMe:   typ.NVMe,
		}
		for key, ok := range typ.CPUFeatures {
			if !ok {
				continue
			}
			// Allocate one feature per VCPU.
			instanceTypes[typ.Name].Resources[key] = float64(typ.VCPU)
		}
	}
}

// instanceState stores everything we know about EC2 instances,
// and implements instance type selection according to runtime
// criteria.
type instanceState struct {
	configs   []instanceConfig
	sleepTime time.Duration
	region    string

	mu          sync.Mutex
	unavailable map[string]time.Time
}

func newInstanceState(configs []instanceConfig, sleep time.Duration, region string) *instanceState {
	s := &instanceState{
		configs:     make([]instanceConfig, len(configs)),
		unavailable: make(map[string]time.Time),
		sleepTime:   sleep,
		region:      region,
	}
	copy(s.configs, configs)
	sort.Slice(s.configs, func(i, j int) bool {
		return s.configs[j].Resources.ScaledDistance(nil) < s.configs[i].Resources.ScaledDistance(nil)
	})
	return s
}

// Unavailable marks the given instance config as busy.
func (s *instanceState) Unavailable(config instanceConfig) {
	s.mu.Lock()
	s.unavailable[config.Type] = time.Now()
	s.mu.Unlock()
}

// Available tells whether the provided resources are potentially
// available as an EC2 instance.
func (s *instanceState) Available(need reflow.Resources) bool {
	for _, config := range s.configs {
		if config.Resources.Available(need) {
			return true
		}
	}
	return false
}

// Largest returns the "largest" instance type from the current configuration.
func (s *instanceState) Largest() instanceConfig {
	return s.configs[0]
}

// MaxAvailable returns the "largest" instance type that has at least
// the required resources and is also believed to be currently
// available. Spot restricts instances to those that may be launched
// via EC2 spot market. MaxAvailable uses (Resources).ScoredDistance
// to determine the largest instance type.
func (s *instanceState) MaxAvailable(need reflow.Resources, spot bool) (instanceConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		best     instanceConfig
		distance = -math.MaxFloat64
	)
	for _, config := range s.configs {
		if time.Since(s.unavailable[config.Type]) < s.sleepTime || (spot && !config.SpotOk) {
			continue
		}
		if !config.Resources.Available(need) {
			continue
		}
		if d := config.Resources.ScaledDistance(need); d > distance {
			distance = d
			best = config
		}
	}
	return best, best.Resources.Available(need)
}

// MinAvailable returns the cheapest instance type that has at least
// the required resources and is also believed to be currently
// available. Spot restricts instances to those that may be launched
// via EC2 spot market.
func (s *instanceState) MinAvailable(need reflow.Resources, spot bool) (instanceConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		price     float64
		best      instanceConfig
		bestPrice = math.MaxFloat64
		found, ok bool
		viable    []instanceConfig
	)
	for _, config := range s.configs {
		if time.Since(s.unavailable[config.Type]) < s.sleepTime || (spot && !config.SpotOk) {
			continue
		}
		if !config.Resources.Available(need) {
			continue
		}
		if price, ok = config.Price[s.region]; !ok {
			continue
		}
		viable = append(viable, config)
		if price < bestPrice {
			bestPrice = price
			best = config
		}
	}
	// Choose a higher cost but better EBS throughput instance type if applicable.
	for _, config := range viable {
		price = config.Price[s.region]
		// Prefer a reasonably more expensive one with higher EBS throughput
		if !found &&
			(price < bestPrice+ebsThroughputPremiumCost ||
				price < bestPrice*(1.0+ebsThroughputPremiumPct/100)) &&
			config.EBSThroughput > best.EBSThroughput*(1.0+ebsThroughputBenefitPct/100) {
			bestPrice = price
			best = config
			found = true
		}
		// Prefer a cheaper one with same EBS throughput.
		if found && price < bestPrice && config.EBSThroughput >= best.EBSThroughput {
			bestPrice = price
			best = config
		}
	}
	return best, best.Resources.Available(need)
}

func (s *instanceState) Type(typ string) (instanceConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.unavailable[typ]) < s.sleepTime {
		return instanceConfig{}, false
	}
	for _, config := range s.configs {
		if config.Type == typ {
			return config, true
		}
	}
	return instanceConfig{}, false
}

// instance represents a concrete instance; it is launched from an instanceConfig
// and additional parameters.
type instance struct {
	HTTPClient      *http.Client
	Config          instanceConfig
	ReflowConfig    infra.Config
	Log             *log.Logger
	Authenticator   ecrauth.Interface
	EC2             ec2iface.EC2API
	InstanceTags    map[string]string
	Labels          pool.Labels
	Spot            bool
	Subnet          string
	InstanceProfile string
	SecurityGroup   string
	Region          string
	BootstrapImage  string
	Price           float64
	EBSType         string
	EBSSize         uint64
	NEBS            int
	AMI             string
	KeyName         string
	SpotProbeDepth  int
	SshKey          string
	Immortal        bool
	CloudConfig     cloudConfig
	Task            *status.Task

	userData string
	err      error
	ec2inst  *ec2.Instance
}

type reflowletInstance struct {
	ec2.Instance

	// Version of the reflowlet instance running on EC2 (populated on instance startup)
	Version string
	// Digest of the executable running on the reflowlet instance
	Digest string
}

func newReflowletInstance(inst *ec2.Instance) *reflowletInstance {
	ri := &reflowletInstance{Instance: *inst}
	ri.update()
	return ri
}

func (i *reflowletInstance) update() {
	for _, tag := range i.Instance.Tags {
		if *tag.Key == "reflowlet:version" {
			i.Version = *tag.Value
		}
		if *tag.Key == "reflowlet:digest" {
			i.Digest = *tag.Value
		}
		if i.Digest != "" && i.Version != "" {
			break
		}
	}
}

// Err returns any error that occurred while launching the instance.
func (i *instance) Err() error {
	return i.err
}

// Instance returns the EC2 instance metadata returned by a successful launch.
func (i *instance) Instance() *reflowletInstance {
	return newReflowletInstance(i.ec2inst)
}

// Go launches an instance, and returns when it fails or the context is done.
// On success (i.Err() == nil), the returned instance is in running state.
// Launch status is reported to the instance's task, if any.
func (i *instance) Go(ctx context.Context) {
	i.configureEBS()
	const maxTries = 10
	type stateT int
	const (
		// Perform capacity check for EC2 spot.
		stateCapacity stateT = iota
		// Launch the instance via EC2.
		stateLaunch
		// Tag the instance
		stateTag
		// Wait for the instance to enter running state.
		stateWaitInstance
		// Describe the instance via EC2 to get the DNS name.
		stateDescribeDns
		// Wait for the bootstrap to become live (and metadata to become available).
		stateWaitBootstrap
		// Install the reflowlet image.
		stateInstallImage
		// Wait for reflowlet to become live (and metadata to become available).
		stateWaitReflowlet
		// Describe the instance via EC2 to get an updated version tag.
		stateDescribeTags

		stateDone
	)
	var (
		state       stateT
		id          string
		dns         string
		n           int
		retryPolicy = retry.MaxTries(retry.Backoff(5*time.Second, 30*time.Second, 1.75), maxTries)
	)
	spotProbeDepth := i.SpotProbeDepth
	// TODO(marius): propagate context to the underlying AWS calls
	for state < stateDone && ctx.Err() == nil {
		switch state {
		case stateCapacity:
			if !i.Spot || spotProbeDepth == 0 {
				break
			}
			i.Task.Printf("probing for EC2 capacity (depth=%d)", spotProbeDepth)
			var ok bool
			ok, i.err = i.ec2HasCapacity(ctx, spotProbeDepth)
			if i.err == nil && !ok {
				i.err = errors.E(errors.Unavailable, errors.New("ec2 capacity is likely exhausted"))
			}
			// If we are hitting instance limits, try smaller depth immediately.
			if i.err == errInstanceLimitExceeded && spotProbeDepth > 1 {
				spotProbeDepth /= 2
				i.err = nil
				continue
			}
		case stateLaunch:
			i.Task.Print("launching EC2 instance")
			id, i.err = i.launch(ctx)
			if i.err != nil {
				i.Task.Printf("launch error: %v", i.err)
				i.Log.Errorf("instance launch error: %v", i.err)
			} else {
				spot := ""
				if i.Spot {
					spot = "spot "
				}
				i.Task.Title(id)
				i.Task.Print("launched")
				i.Log.Debugf("launched %sinstance %v: %s%s", spot, id, i.Config.Type, i.Config.Resources)
			}
		case stateTag:
			vids, err := getVolumeIds(i.EC2, id)
			if err != nil {
				i.Log.Errorf("get attached volumes %s: %v", id, err)
			}
			_, i.err = i.EC2.CreateTags(&ec2.CreateTagsInput{
				Resources: append([]*string{aws.String(id)}, aws.StringSlice(vids)...), Tags: i.getTags()})
		case stateWaitInstance:
			i.Task.Print("waiting for instance to become ready")
			i.err = i.EC2.WaitUntilInstanceRunning(&ec2.DescribeInstancesInput{
				InstanceIds: []*string{aws.String(id)},
			})
		case stateDescribeDns:
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			i.ec2inst, i.err = describeInstance(ctx, i.EC2, id)
			cancel()
			if i.err == nil {
				if i.ec2inst.PublicDnsName == nil || *i.ec2inst.PublicDnsName == "" {
					i.err = errors.Errorf("ec2.describeinstances %v: no public DNS name", id)
				} else {
					dns = *i.ec2inst.PublicDnsName
				}
			}
		case stateWaitBootstrap:
			i.Task.Print("waiting for bootstrap to become available")
			var c *bootc.Client
			c, i.err = bootc.New(fmt.Sprintf("https://%s:9000/v1/", dns), i.HTTPClient, nil)
			if i.err != nil {
				i.err = errors.E(errors.Fatal, i.err)
				break
			}
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			i.err = c.Status(ctx)
			cancel()
			if i.err != nil && strings.HasSuffix(i.err.Error(), "connection refused") {
				i.err = errors.E(errors.Temporary, i.err)
			}
		case stateInstallImage:
			i.Task.Print("installing reflowlet image")
			clnt, err := bootc.New(fmt.Sprintf("https://%s:9000/v1/", *i.ec2inst.PublicDnsName), i.HTTPClient, nil)
			if err != nil {
				i.err = errors.E(errors.Fatal, err)
				break
			}
			var repo reflow.Repository
			err = i.ReflowConfig.Instance(&repo)
			if err != nil {
				i.err = errors.E(errors.Fatal, err)
				break
			}
			ctx2, cancel := context.WithTimeout(ctx, 5*time.Minute)
			err = getReflowletFile(ctx2, repo, i.Log)
			cancel()
			if err != nil {
				i.err = errors.E(errors.Fatal, err)
				break
			}
			ctx2, cancel = context.WithTimeout(ctx, 1*time.Minute)
			reflowletimage := common.Image{
				Path: reflowletFile.Source,
				Args: reflowletArgs,
				Name: "reflowlet",
			}
			i.Log.Debugf("installing reflowlet image %v", reflowletimage)
			// Once the reflowlet image (a reflow binary) is installed on the instance,
			// the instance goes from being a bootstrap instance to a reflowlet instance.
			err = clnt.InstallImage(ctx2, reflowletimage)
			cancel()
			if err != nil && !errors.Is(errors.Net, err) {
				i.err = errors.E(errors.Fatal, err)
				break
			}
		case stateWaitReflowlet:
			i.Task.Print("waiting for reflowlet to become available")
			var c *poolc.Client
			c, i.err = poolc.New(fmt.Sprintf("https://%s:9000/v1/", dns), i.HTTPClient, nil)
			if i.err != nil {
				i.err = errors.E(errors.Fatal, i.err)
				break
			}
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, i.err = c.Config(ctx)
			cancel()
			if i.err != nil && strings.HasSuffix(i.err.Error(), "connection refused") {
				i.err = errors.E(errors.Temporary, i.err)
			}
		case stateDescribeTags:
			i.Task.Print("waiting for reflowlet version tag")
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			i.ec2inst, i.err = describeInstance(ctx, i.EC2, id)
			cancel()
			if i.err != nil {
				i.err = errors.E(errors.Temporary, "%s: describe instance: %v", id)
				break
			}
			ri := i.Instance()
			if ri.Version == "" {
				i.err = errors.E(errors.Temporary, "version tag unavailable")
			}
		default:
			panic("unknown state")
		}
		if i.err == nil {
			n = 0
			state++
			continue
		}
		if awserr, ok := i.err.(awserr.Error); ok {
			switch awserr.Code() {
			// According to EC2 API docs, these codes indicate
			// capacity issues.
			//
			// http://docs.aws.amazon.com/AWSEC2/latest/APIReference/errors-overview.html
			//
			// TODO(marius): add a separate package for interpreting AWS errors.
			case "InsufficientCapacity", "InsufficientInstanceCapacity", "InsufficientHostCapacity", "InsufficientReservedInstanceCapacity", "InstanceLimitExceeded", "Unsupported":
				i.err = errors.E(errors.Unavailable, awserr)
			}
		}
		switch {
		case i.err == nil:
		case errors.Is(errors.Fatal, i.err):
			i.Log.Errorf("instance %v fatal: %v", id, i.err)
			return
		case errors.Is(errors.Unavailable, i.err):
			i.Log.Errorf("instance %v unavailable: %v", id, i.err)
			// Return these immediately because our caller may be able to handle
			// them by selecting a different instance type.
			return
		case !errors.Recover(i.err).Timeout() && !errors.Recover(i.err).Temporary():
			var what string
			switch state {
			case stateCapacity:
				what = "checking capacity"
			case stateLaunch:
				what = "launching instance"
			case stateTag:
				what = "tagging instance"
			case stateWaitInstance:
				what = "waiting for instance"
			case stateDescribeDns:
				what = "describing instance (dns)"
			case stateWaitBootstrap:
				what = "waiting for the bootstrap to load"
			case stateInstallImage:
				what = "installing reflowlet image"
			case stateWaitReflowlet:
				what = "waiting for the reflowlet to load"
			case stateDescribeTags:
				what = "describing instance (version)"
			}
			i.Log.Errorf("error while %s: %v", what, i.err)
		}
		if err := retry.Wait(ctx, retryPolicy, n); err != nil {
			if state == stateWaitReflowlet && errors.Is(errors.TooManyTries, err) {
				// treat reflowlet timeouts as possibly problematic instance types.
				i.err = errors.E(errors.Unavailable, errors.Errorf("reflowlet on %s not available: %v", *i.ec2inst.InstanceId, err))
			}
			break
		}
		n++
		i.Log.Debugf("%s recoverable error (%d/%d): %v", id, n, maxTries, i.err)
	}
	if i.err != nil {
		i.Task.Print(i.err)
		return
	}
	i.err = ctx.Err()
	if i.err == nil {
		i.Task.Print("instance ready")
	}
}

func describeInstance(ctx context.Context, EC2 ec2iface.EC2API, id string) (*ec2.Instance, error) {
	resp, err := EC2.DescribeInstancesWithContext(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []*string{aws.String(id)},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Reservations) != 1 || len(resp.Reservations[0].Instances) != 1 {
		return nil, errors.Errorf("ec2.describeinstances %v: invalid output", id)
	}
	return resp.Reservations[0].Instances[0], nil
}

func getReflowletFile(ctx context.Context, repo reflow.Repository, log *log.Logger) error {
	return reflowletOnce.Do(func() error {
		if !hasEmbedded() {
			return execimage.ErrNoEmbeddedImage
		}
		localDigest, err := imageDigest()
		if err != nil {
			return err
		}
		if reflowletFile, err = repo.Stat(ctx, localDigest); err == nil {
			return nil
		}
		// Image doesn't exist in repo, so upload it.
		r, err := execimage.EmbeddedLinuxImage()
		if err != nil {
			return err
		}
		defer r.Close()
		log.Debugf("uploading reflow image (%s) to repo", localDigest.Short())
		repoDigest, err := repo.Put(ctx, r)
		if err != nil {
			return err
		}
		if repoDigest != localDigest {
			return errors.New("digests mismatch")
		}
		reflowletFile, err = repo.Stat(ctx, localDigest)
		return err
	})
}

func imageDigest() (digest.Digest, error) {
	err := digestOnce.Do(func() error {
		var err error
		r, err := execimage.EmbeddedLinuxImage()
		if err != nil {
			return err
		}
		localDigest, err = execimage.Digest(r)
		defer r.Close()
		return err
	})
	return localDigest, err
}

func hasEmbedded() bool {
	_, err := imageDigest()
	return err == nil
}

// getVolumeIds gets the IDs of the volumes currently attached (or attaching) to the given EC2 instance.
func getVolumeIds(api ec2iface.EC2API, instanceId string) ([]string, error) {
	req := &ec2.DescribeVolumesInput{
		Filters: []*ec2.Filter{
			{Name: aws.String("attachment.instance-id"), Values: aws.StringSlice([]string{instanceId})},
		},
	}
	resp, err := api.DescribeVolumes(req)
	if err != nil {
		return nil, err
	}
	var vids []string
	for _, v := range resp.Volumes {
		for _, a := range v.Attachments {
			vid, state := aws.StringValue(v.VolumeId), aws.StringValue(a.State)
			if aws.StringValue(a.InstanceId) == instanceId &&
				(state == ec2.AttachmentStatusAttached || state == ec2.AttachmentStatusAttaching) {
				vids = append(vids, vid)
			}
		}
	}
	return vids, nil
}

// getTags gets the EC2 tags for the instance.
func (i *instance) getTags() (tags []*ec2.Tag) {
	for k, v := range i.InstanceTags {
		tags = append(tags, &ec2.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	for k, v := range i.Labels {
		tags = append(tags, &ec2.Tag{Key: aws.String(k), Value: aws.String(v)})
	}
	return
}

// configureEBS configures the EBS volume size and number for optimal performance.
func (i *instance) configureEBS() {
	if i.NEBS < 1 {
		i.NEBS = 1
	}
	if min, ok := minDiskSizes[i.EBSType]; ok {
		perEBS := i.EBSSize / uint64(i.NEBS)
		if i.EBSType == "gp2" && perEBS > gp2PerEBSThresholdGiB {
			min = gp2MaxThroughputMinSizeGiB
		}
		if perEBS < min {
			perEBS = min
		}
		nmin := 1 + int(i.EBSSize/perEBS)
		if i.NEBS > nmin {
			i.NEBS = nmin
		}
		i.EBSSize = perEBS * uint64(i.NEBS)
	}
}

func (i *instance) launch(ctx context.Context) (string, error) {
	// First we need to construct the cloud-config that's passed to
	// our instances via EC2's user-data mechanism.
	var c cloudConfig

	if i.SshKey == "" {
		i.Log.Debugf("instance launch: missing public SSH key")
	} else {
		c.SshAuthorizedKeys = []string{i.SshKey}
	}

	// /etc/ecrlogin contains the login command for ECR.
	ecrFile := CloudFile{
		Path:        "/etc/ecrlogin",
		Permissions: "0644",
		Owner:       "root",
	}
	var err error
	ecrFile.Content, err = ecrauth.Login(context.TODO(), i.Authenticator)
	if err != nil {
		return "", err
	}
	c.AppendFile(ecrFile)

	// /etc/reflowconfig contains the (YAML) marshaled configuration file
	b, err := i.ReflowConfig.Marshal(true)
	if err != nil {
		return "", err
	}
	// The remote side does not need a cluster implementation.
	keys := make(infra.Keys)
	err = yaml.Unmarshal(b, &keys)
	if err != nil {
		return "", err
	}
	delete(keys, infra2.Cluster)
	b, err = yaml.Marshal(keys)
	if err != nil {
		return "", err
	}
	c.AppendFile(CloudFile{
		Path:        "/etc/systemd/system.conf",
		Permissions: "0644",
		Owner:       "root",
		Content: `
		[Manager]
		DefaultLimitNOFILE=65536
		`,
	})
	c.AppendFile(CloudFile{
		Path:        "/etc/reflowconfig",
		Permissions: "0644",
		Owner:       "root",
		Content:     string(b),
	})

	// Write the bootstrapping script. It fetches the binary and runs it.
	c.AppendFile(CloudFile{
		Permissions: "0755",
		Path:        "/opt/bin/bootstrap",
		Owner:       "root",
		Content: tmpl(`
		#!/bin/bash
		set -e
		bin=/tmp/reflowbootstrap
		echo "fetching: {{.binary}}"
		curl -s {{.binary}} >$bin
		chmod +x $bin
		export V23_CREDENTIALS=/opt/.v23
		export V23_CREDENTIALS_NO_LOCK=1
		export V23_CREDENTIALS_NO_AGENT=1
		echo "starting: $bin {{.bootstrapArgs}}"
		$bin {{.bootstrapArgs}} || true
        sleep 5
        exit 1
		`, args{"binary": i.BootstrapImage, "bootstrapArgs": strings.Join(bootstrapArgs, " ")}),
	})

	// Turn off CoreOS services that would restart or otherwise disrupt
	// the instances.
	c.CoreOS.Update.RebootStrategy = "off"
	c.AppendUnit(CloudUnit{Name: "update-engine.service", Command: "stop"})
	c.AppendUnit(CloudUnit{Name: "locksmithd.service", Command: "stop"})

	// Configure the disks.
	lvmGroupName := "data"
	deviceName := fmt.Sprintf("%s_group/%s_vol", lvmGroupName, lvmGroupName)
	if i.NEBS < 1 {
		i.NEBS = 1
	}
	devices := make([]string, i.NEBS)
	for idx := range devices {
		if i.Config.NVMe {
			devices[idx] = fmt.Sprintf("nvme%dn1", idx+1)
		} else {
			devices[idx] = fmt.Sprintf("xvd%c", 'b'+idx)
		}
	}
	c.AppendUnit(CloudUnit{
		Name:    fmt.Sprintf("format-%s.service", lvmGroupName),
		Command: "start",
		Content: tmpl(`
			[Unit]
			Description=Format /dev/{{.name}}_group/{{.name}}_vol (after setting up LVM RAID0)
			After={{range $_, $name :=  .devices}}dev-{{$name}}.device {{end}}
			Requires={{range $_, $name := .devices}}dev-{{$name}}.device {{end}}
			[Service]
			Type=oneshot
			RemainAfterExit=yes
			ExecStartPre=/usr/sbin/pvcreate {{range $_, $name := .devices}}/dev/{{$name}} {{end}}
			ExecStartPre=/usr/sbin/vgcreate {{.name}}_group {{range $_, $name := .devices}}/dev/{{$name}} {{end}}
			ExecStartPre=/usr/sbin/lvcreate -l 100%%VG --stripes {{.devices|len}} --stripesize 256 -n {{.name}}_vol {{.name}}_group
			ExecStart=-/usr/sbin/mkfs.ext4 /dev/{{.name}}_group/{{.name}}_vol
		`, args{"devices": devices, "name": lvmGroupName}),
	})
	c.AppendUnit(CloudUnit{
		Name:    "mnt-data.mount",
		Command: "start",
		Content: tmpl(`
			[Unit]
			Description=device /dev/{{.name}} on path /mnt/data
			{{if .mortal}}
			OnFailure=poweroff.target
			OnFailureJobMode=replace-irreversibly
			{{end}}
			[Mount]
			What=/dev/{{.name}}
			Where=/mnt/data
			Type=ext4
			Options=data=writeback
		`, args{"mortal": !i.Immortal, "name": deviceName}),
	})

	c.AppendFile(CloudFile{
		Path:        "/etc/journald-cloudwatch-logs.conf",
		Permissions: "0644",
		Owner:       "root",
		Content: `log_group = "reflow/reflowlet"
fields = ["_HOSTNAME", "PRIORITY", "MESSAGE"]
queue_poll_duration_ms = 1000
queue_flush_log_ms = 5000
field_length = 1024
`,
	})

	c.AppendUnit(CloudUnit{
		Name:    "journald-cloudwatch-logs.service",
		Enable:  true,
		Command: "start",
		Content: tmpl(`
		[Unit]
		Description=journald-cloudwatch-logs
		Wants=basic.target
		After=basic.target network.target
		[Service]
		LogLevelMax=5
		Type=simple
		ExecStartPre=/usr/bin/sh -c "/usr/bin/echo 'log_stream = \"'$(curl http://169.254.169.254/latest/meta-data/public-hostname)'\"' | /usr/bin/cat - /etc/journald-cloudwatch-logs.conf > /tmp/journald-cloudwatch-logs.conf"
		ExecStartPre=/usr/bin/wget https://github.com/advantageous/systemd-cloud-watch/releases/download/v0.2.1/systemd-cloud-watch_linux -O /tmp/systemd-cloud-watch_linux
		ExecStartPre=/usr/bin/chmod +x /tmp/systemd-cloud-watch_linux
		ExecStart=/tmp/systemd-cloud-watch_linux /tmp/journald-cloudwatch-logs.conf
		Restart=on-failure
		RestartSec=30s
		`, args{}),
	})

	var profile, akey, secret, token string
	if i.InstanceProfile != "" {
		profile = fmt.Sprintf("-a %s", i.InstanceProfile)
	} else {
		var creds *credentials.Credentials
		err := i.ReflowConfig.Instance(&creds)
		if err == nil {
			if c, err := creds.Get(); err == nil {
				akey = fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", c.AccessKeyID)
				secret = fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", c.SecretAccessKey)
				token = fmt.Sprintf("AWS_SESSION_TOKEN=%s", c.SessionToken)
			}
		}
	}
	if akey != "" || profile != "" {
		c.AppendUnit(CloudUnit{
			Name:    "aws-xray.service",
			Enable:  true,
			Command: "start",
			Content: tmpl(`
			[Unit]
			Description=xray
			Requires=network.target
			After=network.target
			[Service]
			Environment="{{.aws_access_key_id}}"
			Environment="{{.aws_secret_access_key}}"
			Environment="{{.aws_session_token}}"
			Type=simple
			ExecStartPre=/usr/bin/wget https://s3.dualstack.us-east-2.amazonaws.com/aws-xray-assets.us-east-2/xray-daemon/aws-xray-daemon-linux-2.x.zip
			ExecStartPre=/usr/bin/unzip aws-xray-daemon-linux-2.x.zip -d /tmp
			ExecStart=/tmp/xray {{.profile}} -l debug`, args{"profile": profile, "aws_access_key_id": akey, "aws_secret_access_key": secret, "aws_session_token": token})})
	}

	// We merge the user's cloud config before appending the bootstrap unit
	// so that system units can be run before the bootstrap.
	c.Merge(&i.CloudConfig)

	c.AppendUnit(CloudUnit{
		Name:    "reflowlet.service",
		Enable:  true,
		Command: "start",
		Content: tmpl(`
			[Unit]
			Description=reflowlet
			Requires=network.target
			After=network.target
			{{if .mortal}}
			OnFailure=poweroff.target
			OnFailureJobMode=replace-irreversibly
			{{end}}
			[Service]
			OOMScoreAdjust=-1000
			Type=oneshot
			ExecStart=/opt/bin/bootstrap
		`, args{"mortal": !i.Immortal}),
	})
	b, err = c.Marshal()
	if err != nil {
		return "", err
	}
	// Compress file so that we are below the 16KB limit for user data.
	var gb bytes.Buffer
	b64e := base64.NewEncoder(base64.StdEncoding, &gb)
	ctr := byteCounter{writer: b64e}
	gw := gzip.NewWriter(&ctr)
	_, err = gw.Write(b)
	if err != nil {
		return "", err
	}
	err = gw.Close()
	if err != nil {
		return "", err
	}
	err = b64e.Close()
	if err != nil {
		return "", err
	}
	if ctr.count > 16<<10 {
		return "", errors.New(fmt.Sprintf("size of userdata > 16384: %v ", ctr.count))
	}
	i.userData = gb.String()
	if i.Spot {
		return i.ec2RunSpotInstance(ctx)
	}
	return i.ec2RunInstance()
}

type byteCounter struct {
	count  int64
	writer io.Writer
}

func (b *byteCounter) Write(p []byte) (n int, err error) {
	n, err = b.writer.Write(p)
	if err != nil {
		return
	}
	b.count += int64(n)
	return
}

func (i *instance) ec2RunSpotInstance(ctx context.Context) (string, error) {
	i.Log.Debugf("generating ec2 spot instance request for instance type %v", i.Config.Type)
	// First make a spot instance request.
	params := &ec2.RequestSpotInstancesInput{
		ValidUntil: aws.Time(time.Now().Add(time.Minute)),
		SpotPrice:  aws.String(fmt.Sprintf("%.3f", i.Price)),

		LaunchSpecification: &ec2.RequestSpotLaunchSpecification{
			ImageId:             aws.String(i.AMI),
			EbsOptimized:        aws.Bool(i.Config.EBSOptimized),
			InstanceType:        aws.String(i.Config.Type),
			SubnetId:            aws.String(i.Subnet),
			BlockDeviceMappings: i.ebsDeviceMappings(),
			KeyName:             nonemptyString(i.KeyName),
			UserData:            aws.String(i.userData),

			IamInstanceProfile: &ec2.IamInstanceProfileSpecification{
				Arn: aws.String(i.InstanceProfile),
			},
			SecurityGroupIds: []*string{aws.String(i.SecurityGroup)},
		},
	}
	i.Task.Printf("requesting spot instances with bid of %s", *params.SpotPrice)
	resp, err := i.EC2.RequestSpotInstances(params)
	if err != nil {
		return "", err
	}
	if n := len(resp.SpotInstanceRequests); n != 1 {
		return "", errors.Errorf("ec2.requestspotinstances: got %v entries, want 1", n)
	}
	reqid := aws.StringValue(resp.SpotInstanceRequests[0].SpotInstanceRequestId)
	if reqid == "" {
		return "", errors.Errorf("ec2.requestspotinstances: empty request id")
	}
	i.Task.Printf("awaiting fulfillment of spot request %s", reqid)
	i.Log.Debugf("waiting for spot fullfillment for instance type %v: %s", i.Config.Type, reqid)
	// Also set a timeout context in case the AWS API is stuck.
	toctx, cancel := context.WithTimeout(ctx, time.Minute+10*time.Second)
	defer cancel()
	if err := i.ec2WaitForSpotFulfillment(toctx, reqid); err != nil {
		// If we're not fulfilled by our deadline, we consider spot instances unavailable.
		// Since we've given up, cleanup the request.
		msg := i.ec2CleanupSpotRequest(ctx, reqid)
		// Boot this up to the caller so they can pick a different instance types.
		return "", errors.E(errors.Unavailable, fmt.Errorf("spot request %s cleanup:\n%s\ndue to: %v", reqid, msg, err))
	}
	id, state, err := i.ec2SpotRequestStatus(ctx, reqid)
	if err != nil {
		return "", err
	}
	fyi := fmt.Sprintf("ID: %s, state: %s", id, state)
	i.Task.Printf("spot request %s fulfilled (%s)", reqid, fyi)
	i.Log.Debugf("ec2 spot request %s fulfilled (%s)", reqid, fyi)
	return id, nil
}

func (i *instance) ec2SpotRequestStatus(ctx context.Context, spotID string) (id, state string, err error) {
	var out *ec2.DescribeSpotInstanceRequestsOutput
	out, err = i.EC2.DescribeSpotInstanceRequests(&ec2.DescribeSpotInstanceRequestsInput{
		SpotInstanceRequestIds: []*string{aws.String(spotID)},
	})
	if err != nil {
		return
	}
	if n := len(out.SpotInstanceRequests); n != 1 {
		err = errors.Errorf("ec2.describespotinstancerequests: got %v entries, want 1", n)
		return
	}
	id = aws.StringValue(out.SpotInstanceRequests[0].InstanceId)
	state = aws.StringValue(out.SpotInstanceRequests[0].State)
	if id == "" {
		err = errors.Errorf("ec2.describespotinstancerequests: missing instance ID")
		return
	}
	return
}

// ec2CleanupSpotRequest attempts to clean up a spot request by cancelling it
// and terminating associated EC2 instance (if any).
// Returns a text summary of what happened appropriate for logging.
func (i *instance) ec2CleanupSpotRequest(ctx context.Context, spotID string) string {
	var b strings.Builder
	iid, state, rerr := i.ec2SpotRequestStatus(ctx, spotID)
	b.WriteString(fmt.Sprintf("spot request %s status: ", spotID))
	if rerr == nil {
		b.WriteString(fmt.Sprintf("%s (instance %s)\n", state, iid))
		b.WriteString(fmt.Sprintf("cancel spot request %s: ", spotID))
		if state, cerr := i.ec2CancelSpotRequest(ctx, spotID); cerr != nil {
			b.WriteString(fmt.Sprintf("%v\n", cerr))
		} else {
			b.WriteString(fmt.Sprintf("%s\n", state))
		}
		if iid != "" {
			req := &ec2.TerminateInstancesInput{InstanceIds: aws.StringSlice([]string{iid})}
			if resp, terr := i.EC2.TerminateInstancesWithContext(ctx, req); terr != nil {
				b.WriteString(fmt.Sprintf("terminate instance %s: %v\n", iid, terr))
			} else {
				for _, ti := range resp.TerminatingInstances {
					b.WriteString(fmt.Sprintf("terminate instance %s: %s -> %s\n", iid, *ti.PreviousState.Name, *ti.CurrentState.Name))
				}
			}
		}
	} else {
		b.WriteString(fmt.Sprintf("%v", rerr))
	}
	return b.String()
}

// ec2CancelSpotRequest cancels the spot request spotID and returns its state (or error if applicable)
func (i *instance) ec2CancelSpotRequest(ctx context.Context, spotID string) (string, error) {
	out, err := i.EC2.CancelSpotInstanceRequests(&ec2.CancelSpotInstanceRequestsInput{
		SpotInstanceRequestIds: []*string{aws.String(spotID)},
	})
	if err != nil {
		return "", err
	}
	if n := len(out.CancelledSpotInstanceRequests); n != 1 {
		return "", errors.Errorf("ec2.cancelspotinstancerequests: got %v entries, want 1", n)
	}
	return aws.StringValue(out.CancelledSpotInstanceRequests[0].State), err
}

// ec2WaitForSpotFulfillment waits until the spot request spotID has been fulfilled.
// It differs from (*ec2.EC2).WaitUntilSpotInstanceRequestFulfilledWithContext
// in that it request-cancelled-and-instance-running as a success.
func (i *instance) ec2WaitForSpotFulfillment(ctx context.Context, spotID string) error {
	w := request.Waiter{
		Name:        "ec2WaitForSpotFulfillment",
		MaxAttempts: 40,                                            // default from SDK
		Delay:       request.ConstantWaiterDelay(15 * time.Second), // default from SDK
		Acceptors: []request.WaiterAcceptor{
			{
				State:   request.SuccessWaiterState,
				Matcher: request.PathAllWaiterMatch, Argument: "SpotInstanceRequests[].Status.Code",
				Expected: "fulfilled",
			},
			{
				State:   request.SuccessWaiterState,
				Matcher: request.PathAllWaiterMatch, Argument: "SpotInstanceRequests[].Status.Code",
				Expected: "request-canceled-and-instance-running",
			},
			{
				State:   request.FailureWaiterState,
				Matcher: request.PathAnyWaiterMatch, Argument: "SpotInstanceRequests[].Status.Code",
				Expected: "schedule-expired",
			},
			{
				State:   request.FailureWaiterState,
				Matcher: request.PathAnyWaiterMatch, Argument: "SpotInstanceRequests[].Status.Code",
				Expected: "canceled-before-fulfillment",
			},
			{
				State:   request.FailureWaiterState,
				Matcher: request.PathAnyWaiterMatch, Argument: "SpotInstanceRequests[].Status.Code",
				Expected: "bad-parameters",
			},
			{
				State:   request.FailureWaiterState,
				Matcher: request.PathAnyWaiterMatch, Argument: "SpotInstanceRequests[].Status.Code",
				Expected: "system-error",
			},
		},
		NewRequest: func(opts []request.Option) (*request.Request, error) {
			req, _ := i.EC2.DescribeSpotInstanceRequestsRequest(&ec2.DescribeSpotInstanceRequestsInput{
				SpotInstanceRequestIds: []*string{aws.String(spotID)},
			})
			req.SetContext(ctx)
			req.ApplyOptions(opts...)
			return req, nil
		},
	}
	return w.WaitWithContext(ctx)
}

var errInstanceLimitExceeded = errors.New("InstanceLimitExceeded")

func (i *instance) ec2HasCapacity(ctx context.Context, n int) (bool, error) {
	params := &ec2.RunInstancesInput{
		DryRun:       aws.Bool(true),
		MinCount:     aws.Int64(int64(n)),
		MaxCount:     aws.Int64(int64(n)),
		ImageId:      aws.String(i.AMI),
		InstanceType: aws.String(i.Config.Type),
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	_, err := i.EC2.RunInstancesWithContext(ctx, params)
	if err == nil {
		return false, errors.New("did not expect successful response")
	} else if awserr, ok := err.(awserr.Error); ok {
		switch awserr.Code() {
		case "DryRunOperation":
			return true, nil
		case "RequestCanceled":
			// Apparently AWS's Go SDK will return an AWS error even for
			// context errors. In this case, we treat a timeout as a negative
			// answer.
			return false, nil
		case "InstanceLimitExceeded":
			return false, errInstanceLimitExceeded
		}
		return false, awserr
	} else if err == context.DeadlineExceeded {
		// We'll take an API timeout as a negative answer: this seems to
		// the case empirically.
		return false, nil
	} else if err := ctx.Err(); err != nil {
		return false, err
	}
	return false, fmt.Errorf("expected awserr.Error or context error, got %T", err)
}

func (i *instance) ec2TerminateInstance() {
	if i.ec2inst == nil {
		return
	}
	i.Task.Print("terminating...")
	req := &ec2.TerminateInstancesInput{
		InstanceIds: aws.StringSlice([]string{*i.ec2inst.InstanceId}),
	}
	resp, err := i.EC2.TerminateInstancesWithContext(context.Background(), req)
	if err != nil {
		i.Task.Print(err)
		return
	}
	for _, ti := range resp.TerminatingInstances {
		i.Task.Printf("%s -> %s", *ti.PreviousState.Name, *ti.CurrentState.Name)
	}
}

func (i *instance) ec2RunInstance() (string, error) {
	params := &ec2.RunInstancesInput{
		ImageId:               aws.String(i.AMI),
		MaxCount:              aws.Int64(int64(1)),
		MinCount:              aws.Int64(int64(1)),
		BlockDeviceMappings:   i.ebsDeviceMappings(),
		ClientToken:           aws.String(newID()),
		DisableApiTermination: aws.Bool(false),
		DryRun:                aws.Bool(false),
		EbsOptimized:          aws.Bool(i.Config.EBSOptimized),
		IamInstanceProfile: &ec2.IamInstanceProfileSpecification{
			Arn: aws.String(i.InstanceProfile),
		},
		InstanceInitiatedShutdownBehavior: aws.String("terminate"),
		InstanceType:                      aws.String(i.Config.Type),
		Monitoring: &ec2.RunInstancesMonitoringEnabled{
			Enabled: aws.Bool(true), // Required
		},
		KeyName:          nonemptyString(i.KeyName),
		UserData:         aws.String(i.userData),
		SecurityGroupIds: []*string{aws.String(i.SecurityGroup)},
		SubnetId:         aws.String(i.Subnet),
	}
	i.Log.Debugf("EC2RunInstances %v", params)
	resv, err := i.EC2.RunInstances(params)
	if err != nil {
		return "", err
	}
	if n := len(resv.Instances); n != 1 {
		return "", fmt.Errorf("expected 1 instance; got %d", n)
	}
	return *resv.Instances[0].InstanceId, nil
}

// ebsDeviceMappings returns the set of device mappings requested by
// this instance. When i.NEBS > 1, it requests multiple devices which
// are then RAIDed together. We assume that the first mapping,
// device xvda is reserved as a system device.
func (i *instance) ebsDeviceMappings() []*ec2.BlockDeviceMapping {
	mappings := []*ec2.BlockDeviceMapping{
		{
			// The root device for the OS, Docker images, etc.
			DeviceName: aws.String("/dev/xvda"),
			Ebs: &ec2.EbsBlockDevice{
				DeleteOnTermination: aws.Bool(true),
				VolumeSize:          aws.Int64(200),
				VolumeType:          aws.String("gp2"),
			},
		},
	}
	for idx := 0; idx < i.NEBS; idx++ {
		mappings = append(mappings, &ec2.BlockDeviceMapping{
			DeviceName: aws.String(fmt.Sprintf("/dev/xvd%c", 'b'+idx)),
			Ebs: &ec2.EbsBlockDevice{
				DeleteOnTermination: aws.Bool(true),
				VolumeSize:          aws.Int64(int64(i.EBSSize) / int64(i.NEBS)),
				VolumeType:          aws.String(i.EBSType),
			},
		})
	}
	return mappings
}

func newID() string {
	var b [8]byte
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", b[:])
}

// nonemptyString returns nil if s is empty, or else the pointer to s.
func nonemptyString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type args map[string]interface{}

// tmpl renders the template text, after first stripping common
// (whitespace) prefixes from text.
func tmpl(text string, args interface{}) string {
	lines := strings.Split(text, "\n")
	for len(lines) > 0 && strings.TrimSpace(lines[0]) == "" {
		lines = lines[1:]
	}
	for len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) == "" {
		lines = lines[:len(lines)-1]
	}
	var p int
	if len(lines) > 0 {
		p = strings.IndexFunc(lines[0], func(r rune) bool { return !unicode.IsSpace(r) })
		if p < 0 {
			p = 0
		}
	}
	for i, line := range lines {
		lines[i] = line[p:]
		if strings.TrimSpace(line[:p]) != "" {
			panic(fmt.Sprintf("nonspace prefix in %q", line))
		}
	}
	text = strings.Join(lines, "\n")
	t := template.Must(template.New("ec2template").Parse(text))
	var b bytes.Buffer
	if err := t.Execute(&b, args); err != nil {
		panic(err)
	}
	return b.String()
}
