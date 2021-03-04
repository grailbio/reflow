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
	"net/http"
	"strings"
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
	localDigest   digest.Digest
	localSize     int64
	digestOnce    once.Task
	reflowletOnce once.Task
	reflowletFile reflow.File
)

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
	BootstrapExpiry time.Duration
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

// Instance returns the EC2 instance metadata returned by a successful launch.
func (i *instance) Instance() *reflowletInstance {
	return newReflowletInstance(i.ec2inst)
}

func (i *instance) ManagedInstance() ManagedInstance {
	var id string
	if i.err == nil && i.ec2inst != nil {
		id = *i.ec2inst.InstanceId
	}
	return InstanceSpec{i.Config.Type, i.Config.Resources}.Instance(id)
}

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

func (s stateT) String() string {
	var what string
	switch s {
	case stateCapacity:
		what = "probing for EC2 capacity"
	case stateLaunch:
		what = "launching EC2 instance"
	case stateTag:
		what = "tagging instance and EBS volumes"
	case stateWaitInstance:
		what = "waiting for instance to become ready"
	case stateDescribeDns:
		what = "describing instance (dns)"
	case stateWaitBootstrap:
		what = "waiting for the bootstrap to load"
	case stateInstallImage:
		what = "installing reflowlet image"
	case stateWaitReflowlet:
		what = "waiting for the reflowlet to load"
	case stateDescribeTags:
		what = "waiting for reflowlet version tag"
	case stateDone:
		what = "instance ready"
	}
	return what
}

// Go launches an instance, and returns when it fails or the context is done.
// On success (i.Err() == nil), the returned instance is in running state.
// Launch status is reported to the instance's task, if any.
func (i *instance) Go(ctx context.Context) {
	i.configureEBS()
	const maxTries = 10
	var (
		state       stateT
		id          string
		dns         string
		n           int
		retryPolicy = retry.MaxTries(retry.Backoff(5*time.Second, 30*time.Second, 1.75), maxTries)
	)
	spotProbeDepth := i.SpotProbeDepth

	defer func() {
		// At exit, we terminate a successfully provisioned but un-viable instance.
		if id != "" && state > stateLaunch && state < stateWaitReflowlet {
			i.Log.Debugf("terminating non-reflowlet EC2 instance: %s (state: %s)", id, state)
			ec2TerminateInstance(i.EC2, id)
		}
	}()
	for state < stateDone && ctx.Err() == nil {
		switch state {
		case stateCapacity:
			if !i.Spot || spotProbeDepth == 0 {
				break
			}
			i.Task.Printf("%s (depth=%d)", state, spotProbeDepth)
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
			i.Task.Print(state.String())
			id, i.err = i.launch(ctx)
			if i.err != nil {
				i.Task.Printf("launch error: %v", i.err)
				i.Log.Errorf("instance launch error: %v", i.err)
			} else {
				i.Task.Title(id)
				i.print(id, "launched")
			}
		case stateTag:
			i.print(id, state.String())
			vids, err := getVolumeIds(ctx, i.EC2, id)
			if err != nil {
				i.Log.Errorf("get attached volumes %s: %v", id, err)
			}
			_, i.err = i.EC2.CreateTags(&ec2.CreateTagsInput{
				Resources: append([]*string{aws.String(id)}, aws.StringSlice(vids)...), Tags: i.getTags()})
		case stateWaitInstance:
			i.print(id, state.String())
			i.err = i.EC2.WaitUntilInstanceRunning(&ec2.DescribeInstancesInput{
				InstanceIds: []*string{aws.String(id)},
			})
		case stateDescribeDns:
			i.print(id, state.String())
			i.ec2inst, i.err = describeInstance(ctx, i.EC2, id)
			if i.err == nil {
				if i.ec2inst.PublicDnsName == nil || *i.ec2inst.PublicDnsName == "" {
					i.err = errors.Errorf("ec2.describeinstances %v: no public DNS name", id)
				} else {
					dns = *i.ec2inst.PublicDnsName
				}
			}
			spot := ""
			if i.Spot {
				spot = "spot "
			}
			i.Log.Debugf("launched %sinstance %v (%s): %s%s", spot, id, dns, i.Config.Type, i.Config.Resources)
		case stateWaitBootstrap:
			i.print(id, "confirming instance still running")
			if err := ec2StillRunning(ctx, i.EC2, id); err != nil {
				// Instance not running anymore
				i.err = errors.E("wait bootstrap instance running", errors.Fatal, err)
				break
			}
			i.print(id, state.String())
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
			i.print(id, state.String())
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
			i.print(id, "confirming instance still running")
			if err := ec2StillRunning(ctx, i.EC2, id); err != nil {
				// Instance not running anymore
				i.err = errors.E("wait bootstrap instance running", errors.Fatal, err)
				break
			}
			i.print(id, state.String())
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
			i.print(id, state.String())
			i.ec2inst, i.err = describeInstance(ctx, i.EC2, id)
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
			i.Log.Errorf("instance %v: %v", id, i.err)
			return
		case errors.Is(errors.Unavailable, i.err):
			i.Log.Errorf("instance %v: %v", id, i.err)
			// Return these immediately because our caller may be able to handle
			// them by selecting a different instance type.
			return
		case !errors.Recover(i.err).Timeout() && !errors.Recover(i.err).Temporary():
			i.Log.Errorf("error while %s: %v", state, i.err)
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
		i.print(id, fmt.Sprintf("%v", i.err))
		return
	}
	i.err = ctx.Err()
	if i.err == nil {
		i.print(id, "instance ready")
	}
}

func (i *instance) print(id, msg string) {
	msg = fmt.Sprintf("[%s] %s", id, msg)
	i.Log.Debug(msg)
	i.Task.Print(msg)
}

func describeInstance(ctx context.Context, EC2 ec2iface.EC2API, id string) (*ec2.Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
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
		d, sz, err := imageDigestAndSize()
		if err != nil {
			return err
		}
		reflowletFile, err = repo.Stat(ctx, d)
		switch {
		case err != nil:
		case !reflowletFile.ContentHash.IsZero() && reflowletFile.ContentHash == d:
			return nil
		// TODO(swami/pboyapalli): The size check shouldn't be needed (since the ContentHash check is superior)
		// but for some reason, the SHA values aren't getting set on the object, so we need to investigate why.
		case reflowletFile.Size == sz:
			return nil
		}
		// Image doesn't exist in repo (or doesn't match the local image's digest or size), so upload it.
		r, err := execimage.EmbeddedLinuxImage()
		if err != nil {
			return err
		}
		defer r.Close()
		log.Debugf("uploading reflow image (%s) to repo", d.Short())
		repoDigest, err := repo.Put(ctx, r)
		if err != nil {
			return err
		}
		if repoDigest != d {
			return errors.New("digests mismatch")
		}
		reflowletFile, err = repo.Stat(ctx, d)
		return err
	})
}

func imageDigestAndSize() (digest.Digest, int64, error) {
	err := digestOnce.Do(func() error {
		var err error
		r, err := execimage.EmbeddedLinuxImage()
		if err != nil {
			return err
		}
		localDigest, localSize, err = execimage.DigestAndSize(r)
		defer r.Close()
		return err
	})
	return localDigest, localSize, err
}

func hasEmbedded() bool {
	_, _, err := imageDigestAndSize()
	return err == nil
}

// getVolumeIds gets the IDs of the volumes currently attached (or attaching) to the given EC2 instance.
func getVolumeIds(ctx context.Context, api ec2iface.EC2API, instanceId string) ([]string, error) {
	req := &ec2.DescribeVolumesInput{
		Filters: []*ec2.Filter{
			{Name: aws.String("attachment.instance-id"), Values: aws.StringSlice([]string{instanceId})},
		},
	}
	resp, err := api.DescribeVolumesWithContext(ctx, req)
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

func (i *instance) bootstrapExpiryArgs() []string {
	// We don't want to set too small of an expiry
	const minExpiry = 30 * time.Second
	expiry := i.BootstrapExpiry
	if expiry < minExpiry {
		i.Log.Debugf("overriding bootstrap expiry %s with minimum %s", expiry, minExpiry)
		expiry = minExpiry
	}
	return []string{"-expiry", fmt.Sprintf("%s", expiry)}
}

func ec2StillRunning(ctx context.Context, api ec2iface.EC2API, id string) error {
	const (
		attempts = 5
		delay    = 2 * time.Second
	)
	ctx, cancel := context.WithTimeout(ctx, (attempts+1)*delay)
	defer cancel()
	return api.WaitUntilInstanceRunningWithContext(
		ctx, &ec2.DescribeInstancesInput{InstanceIds: []*string{aws.String(id)}},
		request.WithWaiterMaxAttempts(attempts),
		request.WithWaiterDelay(request.ConstantWaiterDelay(delay)))
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
		`, args{
			"binary":        i.BootstrapImage,
			"bootstrapArgs": strings.Join(append(bootstrapArgs, i.bootstrapExpiryArgs()...), " "),
		}),
	})

	// Turn off CoreOS services that would restart or otherwise disrupt the instances.
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
	waitErr := i.ec2WaitForSpotFulfillment(toctx, reqid)
	var id, state string
	switch {
	case waitErr == nil:
		id, state, err = ec2SpotRequestStatus(ctx, i.EC2, reqid)
		if err == nil {
			break
		}
		// Here, we were supposedly able to fulfil the request, but were unable to read the spot request status
		// which means we don't have a usable spot instance ID.
		fallthrough
	default:
		// We were unable to fulfill by our deadline, or we were unable to get the instance ID after (supposed) fulfillment.
		// Since we consider the spot instance unavailable, cleanup the request.
		msg := ec2CleanupSpotRequest(context.Background(), i.EC2, reqid)
		i.Log.Debugf("ec2 spot request %s failed\n%s", reqid, msg)
		// Boot this up to the caller so they can pick a different instance types.
		if err != nil {
			msg = fmt.Sprintf("%s\ndue to: %v", msg, err)
		}
		return "", errors.E(errors.Unavailable, fmt.Errorf("spot request %s cleanup:\n%s", reqid, msg))
	}
	i.print(id, fmt.Sprintf("ec2 request (spot) %s fulfilled, state: %s", reqid, state))
	return id, nil
}

func ec2SpotRequestStatus(ctx context.Context, api ec2iface.EC2API, spotID string) (id, state string, err error) {
	var out *ec2.DescribeSpotInstanceRequestsOutput
	out, err = api.DescribeSpotInstanceRequestsWithContext(ctx, &ec2.DescribeSpotInstanceRequestsInput{
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
func ec2CleanupSpotRequest(ctx context.Context, api ec2iface.EC2API, spotID string) string {
	var b strings.Builder
	iid, state, rerr := ec2SpotRequestStatus(ctx, api, spotID)
	b.WriteString(fmt.Sprintf("spot request %s status: ", spotID))
	if rerr != nil {
		b.WriteString(fmt.Sprintf("%v", rerr))
	}
	if state != ec2.SpotInstanceStateCancelled {
		b.WriteString(fmt.Sprintf("%s (instance %s)\n", state, iid))
		b.WriteString(fmt.Sprintf("cancel spot request %s: ", spotID))
		if state, cerr := ec2CancelSpotRequest(ctx, api, spotID); cerr != nil {
			b.WriteString(fmt.Sprintf("%v\n", cerr))
		} else {
			b.WriteString(fmt.Sprintf("%s\n", state))
		}
	}
	if iid != "" {
		req := &ec2.TerminateInstancesInput{InstanceIds: aws.StringSlice([]string{iid})}
		if resp, terr := api.TerminateInstancesWithContext(ctx, req); terr != nil {
			b.WriteString(fmt.Sprintf("terminate instance %s: %v\n", iid, terr))
		} else {
			for _, ti := range resp.TerminatingInstances {
				b.WriteString(fmt.Sprintf("terminate instance %s: %s -> %s\n", iid, *ti.PreviousState.Name, *ti.CurrentState.Name))
			}
		}
	}
	return b.String()
}

// ec2CancelSpotRequest cancels the spot request spotID and returns its state (or error if applicable)
func ec2CancelSpotRequest(ctx context.Context, api ec2iface.EC2API, spotID string) (string, error) {
	out, err := api.CancelSpotInstanceRequestsWithContext(ctx, &ec2.CancelSpotInstanceRequestsInput{
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

func ec2TerminateInstance(api ec2iface.EC2API, id string) {
	req := &ec2.TerminateInstancesInput{InstanceIds: aws.StringSlice([]string{id})}
	resp, err := api.TerminateInstances(req)
	if err != nil {
		log.Errorf("terminating instance %s: %v", id, err)
		return
	}
	for _, ti := range resp.TerminatingInstances {
		log.Printf("%s: %s -> %s", *ti.InstanceId, *ti.PreviousState.Name, *ti.CurrentState.Name)
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
	id, state := *resv.Instances[0].InstanceId, *resv.Instances[0].State
	i.print(id, fmt.Sprintf("ec2 request fulfilled, state: %s", state))
	return id, nil
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
