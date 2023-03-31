// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package volume implements support for maintaining (EBS) volumes on an EC2 instance
// by watching the disk usage of the underlying disk device and resizing the EBS volumes
// whenever necessary based on provided parameters.
package volume

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/grailbio/base/cloud/ec2util"
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

// Volume is an abstraction of an EC2 instance's EBS Volume(s) and supports volume specific operations.
type Volume interface {
	Device

	// GetSize returns the total size of the volume.
	GetSize(ctx context.Context) (size data.Size, err error)

	// ReadyToModify returns whether this volume is ready to be modified (if desired).
	// If the return value for ready is false, then reason may contain a reason why it's not ready.
	ReadyToModify(ctx context.Context) (ready bool, reason string, err error)

	// SetSize sets this volume's size to the specified amount.
	SetSize(ctx context.Context, newSize data.Size) error

	// GetVolumeIds returns the volume IDs.
	GetVolumeIds() []string
}

const maxRetries = 10

var (
	defaultRetrier = retry.MaxRetries(retry.Backoff(5*time.Second, 30*time.Second, 1.5), maxRetries)
	minMaxByType   = map[string]struct{ min, max data.Size }{
		ec2.VolumeTypeGp2: {data.GiB, 16 * data.TiB},
		ec2.VolumeTypeGp3: {data.GiB, 16 * data.TiB},
		ec2.VolumeTypeIo1: {4 * data.GiB, 16 * data.TiB},
	}
)

// ebsLvmVolume implements the Volume interface for a disk device which is backed by
// one or more EBS volumes that are setup as an LVM group.
type ebsLvmVolume struct {
	device

	ebsVolIds  []string
	ebsVolType string

	ec2 ec2iface.EC2API
	log *log.Logger

	retrier retry.Policy
}

// NewEbsLvmVolume returns a Volume backed by EC2 EBS volumes grouped together in an LVM device.
// Enforces that all backing EBS volumes are of equal size and of the same type.
// Size modifications are done such that the size is distributed equally across all volumes.
func NewEbsLvmVolume(sess *session.Session, log *log.Logger, path string) (Volume, error) {
	device, err := deviceFor(path)
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(string(device), "/dev/mapper/") {
		return nil, errors.E(errors.NotSupported, fmt.Sprintf("not an lvm device: %s", device))
	}
	physDevices, err := physicalDevices(string(device))
	if err != nil {
		return nil, err
	}
	if len(physDevices) == 0 {
		return nil, fmt.Errorf("found no physical devices for: %s", device)
	}
	log.Printf("physical devices for device (%s): %s", device, strings.Join(physDevices, ", "))

	iid, err := ec2util.GetInstanceIdentityDocument(sess)
	if err != nil {
		return nil, err
	}
	svc := ec2.New(sess)
	var ebsVolIds []string
	// We assume that all devices are either nvme or not by looking at just the first one.
	// This is ok because based on EC2 instance type, (all) EBS volumes are either exposed as nvme or not.
	if strings.HasPrefix(physDevices[0], "/dev/nvme") {
		ebsVolIds, err = findNvmeVolumes(physDevices)
	} else {
		ebsVolIds, err = getVolumeIds(svc, iid.InstanceID, physDevices)
	}
	if err != nil {
		return nil, err
	}
	v := &ebsLvmVolume{device: device, ebsVolIds: ebsVolIds, ec2: svc, log: log, retrier: defaultRetrier}
	err = v.init()
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (v *ebsLvmVolume) init() error {
	req := &ec2.DescribeVolumesInput{VolumeIds: aws.StringSlice(v.ebsVolIds)}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	resp, err := v.ec2.DescribeVolumesWithContext(ctx, req)
	cancel()
	if err != nil {
		return err
	}
	if n, nv := len(v.ebsVolIds), len(resp.Volumes); n != nv {
		return fmt.Errorf("unexpected number of volumes: %d (!= %d)", nv, n)
	}
	vid0 := aws.StringValue(resp.Volumes[0].VolumeId)
	sz0 := data.GiB * data.Size(aws.Int64Value(resp.Volumes[0].Size))
	typ0 := aws.StringValue(resp.Volumes[0].VolumeType)
	for _, v := range resp.Volumes[1:] {
		vid, sz, typ := aws.StringValue(v.VolumeId), data.GiB*data.Size(aws.Int64Value(v.Size)), aws.StringValue(v.VolumeType)
		if sz != sz0 {
			return fmt.Errorf("unequal volume size %s (%s) vs %s (%s)", vid, sz, vid0, sz0)
		}
		if typ != typ0 {
			return fmt.Errorf("mismatched volume types %s (%s) vs %s (%s)", vid, typ, vid0, typ0)
		}
	}
	v.ebsVolType = typ0
	return nil
}

// GetSize returns the sum of the size of all EBS volumes for the current instance.
func (v *ebsLvmVolume) GetSize(ctx context.Context) (data.Size, error) {
	req := &ec2.DescribeVolumesInput{VolumeIds: aws.StringSlice(v.ebsVolIds)}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	resp, err := v.ec2.DescribeVolumesWithContext(ctx, req)
	cancel()
	if err != nil {
		return 0, err
	}
	if n, nv := len(v.ebsVolIds), len(resp.Volumes); n != nv {
		return 0, fmt.Errorf("unexpected number of volumes: %d (!= %d)", nv, n)
	}
	var size data.Size
	for _, v := range resp.Volumes {
		size += data.GiB * data.Size(aws.Int64Value(v.Size))
	}
	return size, nil
}

// ReadyToModify checks to see if there is a still active
// volume modification involving the given volumeId.
func (v *ebsLvmVolume) ReadyToModify(ctx context.Context) (bool, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	mstate, err := v.getModificationState(ctx)
	cancel()
	if err != nil {
		return false, "", err
	}
	ready := true
	var reasons []string
	for vid, state := range mstate {
		switch state {
		case "modifying", "optimizing":
			ready = false
			reasons = append(reasons, fmt.Sprintf("%s (%s)", vid, state))
		}
	}
	return ready, strings.Join(reasons, ", "), nil
}

// SetSize sets this volume's size to the given amount specified (ceiled to GiB).
// Only supports increasing the size and will return an error if a reduction is attempted.
func (v *ebsLvmVolume) SetSize(ctx context.Context, newSize data.Size) error {
	currSize, err := v.GetSize(ctx)
	if err != nil {
		return err
	}
	if newSize == currSize {
		return nil
	}
	if newSize < currSize {
		return errors.E(errors.NotSupported, fmt.Sprintf("reducing size %s -> %s", currSize, newSize))
	}
	perVolSize := data.Size(int64(newSize) / int64(len(v.ebsVolIds)))
	limits := minMaxByType[v.ebsVolType]
	if perVolSize < limits.min {
		perVolSize = limits.min
	}
	if perVolSize > limits.max {
		perVolSize = limits.max
	}
	perVolSizeGiB := int64(math.Ceil(perVolSize.Count(data.GiB)))
	// As per AWS, the size change takes affect when the EBS volume is in the 'optimizing' state.
	// So loop until we can confirm that each volume is in the 'optimizing' (or just in case 'completed' state).
	type stateT int
	const (
		// Modify volumes (if any)
		stateModify stateT = iota
		// Get the modification status for all volumes
		stateGetModificationStatus
		// Check the modification status for all volumes
		stateCheckModificationStatus
		stateDone
	)

	var (
		state    stateT
		stateStr string
		retries  int
		mstates  map[string]string
		modVols  = v.ebsVolIds
	)
	for state < stateDone {
		err = nil
		switch state {
		case stateModify:
			stateStr = fmt.Sprintf("modifying volumes (%s)", strings.Join(modVols, ", "))
			errs := make([]error, len(modVols))
			_ = traverse.Each(len(modVols), func(idx int) error {
				req := &ec2.ModifyVolumeInput{
					VolumeId: aws.String(modVols[idx]),
					Size:     aws.Int64(perVolSizeGiB),
				}
				ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, errs[idx] = v.ec2.ModifyVolumeWithContext(ctx2, req)
				cancel()
				return nil
			})
			var failed []string
			for idx, err := range errs {
				if err != nil {
					failed = append(failed, modVols[idx])
				}
			}
			modVols = failed
			if len(failed) > 0 {
				err = fmt.Errorf("failed to modify (%s): %v", strings.Join(failed, ", "), errs)
			}
		case stateGetModificationStatus:
			stateStr = fmt.Sprintf("get modification status (%s)", strings.Join(v.ebsVolIds, ", "))
			ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
			mstates, err = v.getModificationState(ctx2)
			cancel()
		case stateCheckModificationStatus:
			stateStr = fmt.Sprintf("checking modification status (%v)", mstates)
			var reasons []string
			for v, s := range mstates {
				switch s {
				case "optimizing", "completed":
					// do nothing, ie, leave done as true
				// As per AWS: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/monitoring-volume-modifications.html
				// Rarely, a transient AWS fault can result in a failed state. This is not an indication of volume health;
				// it merely indicates that the modification to the volume failed. If this occurs, retry the volume modification.
				case "failed":
					modVols = append(modVols, v)
					reasons = append(reasons, fmt.Sprintf("volume %s: %s", v, s))
				case "modifying":
					state = stateGetModificationStatus
					reasons = append(reasons, fmt.Sprintf("volume %s: %s", v, s))
				}
			}
			if len(reasons) > 0 {
				err = fmt.Errorf("not ready: %s", strings.Join(reasons, ", "))
			}
		}
		if err == nil {
			v.log.Printf("%s: success", stateStr)
			retries = 0
			state++
			continue
		}
		v.log.Errorf("%s: %v", stateStr, err)
		if len(modVols) > 0 {
			state = stateModify
		}
		if err = retry.Wait(ctx, v.retrier, retries); err != nil {
			break
		}
		retries++
	}
	if state < stateDone {
		return fmt.Errorf("unknown volume status after setting size to %s: %v", newSize, err)
	}
	return nil
}

// GetVolumeIds returns the EBS volume IDs.
func (v *ebsLvmVolume) GetVolumeIds() []string {
	return v.ebsVolIds
}

// getModificationState returns the current modification state for each underlying EBS volume.
// Apart from the valid values described below, we add a (default) 'unmodified' state.
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_VolumeModification.html
// Current state of modification. Modification state is null for unmodified volumes.
// Valid Values: modifying | optimizing | completed | failed
func (v *ebsLvmVolume) getModificationState(ctx context.Context) (map[string]string, error) {
	mstate := make(map[string]string, len(v.ebsVolIds))
	for _, vid := range v.ebsVolIds {
		mstate[vid] = "unmodified"
	}
	req := &ec2.DescribeVolumesModificationsInput{VolumeIds: aws.StringSlice(v.ebsVolIds)}
	resp, err := v.ec2.DescribeVolumesModificationsWithContext(ctx, req)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok && awsErr.Code() == "InvalidVolumeModification.NotFound" {
			return mstate, nil
		}
		return nil, err
	}
	for _, vm := range resp.VolumesModifications {
		if vm.ModificationState == nil {
			continue
		}
		mstate[aws.StringValue(vm.VolumeId)] = strings.ToLower(aws.StringValue(vm.ModificationState))
	}
	return mstate, nil
}

// getVolumeIds gets the volume IDs of the given set of devices (if) attached to the given instance.
func getVolumeIds(api ec2iface.EC2API, instanceId string, devices []string) ([]string, error) {
	req := &ec2.DescribeVolumesInput{
		Filters: []*ec2.Filter{
			{Name: aws.String("attachment.instance-id"), Values: aws.StringSlice([]string{instanceId})},
			{Name: aws.String("attachment.device"), Values: aws.StringSlice(devices)},
		},
	}
	resp, err := api.DescribeVolumes(req)
	if err != nil {
		return nil, err
	}
	byDevice := make(map[string]string, len(devices))
	for _, v := range resp.Volumes {
		for _, a := range v.Attachments {
			vid, dev, state := aws.StringValue(v.VolumeId), aws.StringValue(a.Device), aws.StringValue(a.State)
			if aws.StringValue(a.InstanceId) == instanceId && state == ec2.AttachmentStatusAttached {
				byDevice[dev] = vid
			}
		}
	}
	var missing, vids []string
	for _, d := range devices {
		if vid := byDevice[d]; vid == "" {
			missing = append(missing, d)
		} else {
			vids = append(vids, vid)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, fmt.Errorf("cannot determine volume id for devices: %s", strings.Join(missing, ", "))
	}
	return vids, nil
}

// This looks for the volume id matching the nvme device under /dev/disk/by-id/
// For example, vol-00a123fe298580278 creates a symlink at /dev/disk/by-id/nvme-Amazon_Elastic_Block_Store_vol00a123fe298580278
// /dev/disk/by-id:
// lrwxrwxrwx. 1 root root 13 Mar 20 17:10 nvme-Amazon_Elastic_Block_Store_vol00a123fe298580278 -> ../../nvme1n1
// The passed in devices look like "/dev/nvme1n1".
func findNvmeVolumes(devices []string) ([]string, error) {
	const devDir = "/dev/disk/by-id"
	dir, err := os.Open(devDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dir.Close() }()
	fileInfoList, err := dir.Readdir(0)
	if err != nil {
		return nil, err
	}
	byDevice := make(map[string]string)
	for _, fileInfo := range fileInfoList {
		if fileInfo.Mode()&os.ModeSymlink != os.ModeSymlink {
			continue
		}
		name := fileInfo.Name()
		resolved, err := filepath.EvalSymlinks(filepath.Join(devDir, name))
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(name, "nvme-Amazon_Elastic_Block_Store_vol") {
			id := strings.TrimPrefix(name, "nvme-Amazon_Elastic_Block_Store_vol")
			byDevice[resolved] = "vol-" + id
		}
	}
	volIds, missing := make([]string, 0, len(devices)), make([]string, 0, len(devices))
	for _, dev := range devices {
		vid := byDevice[dev]
		if vid == "" {
			missing = append(missing, dev)
		} else {
			volIds = append(volIds, vid)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return nil, errors.E(errors.Fatal, fmt.Sprintf("cannot determine volume id for devices: %s", strings.Join(missing, ", ")))
	}
	sort.Strings(volIds)
	return volIds, nil
}
