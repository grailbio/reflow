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

// Volume is an abstraction of a disk device backed by an EC2 instance's EBS volumes.
type Volume interface {
	Device

	// EBSSize returns the sum of the size of all EBS volumes.
	EBSSize(ctx context.Context) (size data.Size, err error)

	// ResizeEBS resizes the EBS volumes such that their sizes add up to newSize.
	ResizeEBS(ctx context.Context, newSize data.Size) error

	// EBSIds returns the IDs of all EBS volumes.
	EBSIds() []string
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

// EBSSize returns the sum of the size of all EBS volumes.
func (v *ebsLvmVolume) EBSSize(ctx context.Context) (data.Size, error) {
	sizeById, err := v.getSizeById(ctx)
	if err != nil {
		return 0, err
	}
	var size data.Size
	for _, s := range sizeById {
		size += s
	}
	return size, nil
}

// getSizeById returns a map of EBS volume ID to size.
func (v *ebsLvmVolume) getSizeById(ctx context.Context) (map[string]data.Size, error) {
	req := &ec2.DescribeVolumesInput{VolumeIds: aws.StringSlice(v.ebsVolIds)}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	resp, err := v.ec2.DescribeVolumesWithContext(ctx, req)
	cancel()
	if err != nil {
		return nil, err
	}
	if n, nv := len(v.ebsVolIds), len(resp.Volumes); n != nv {
		return nil, fmt.Errorf("unexpected number of volumes: %d (!= %d)", nv, n)
	}
	sizeById := make(map[string]data.Size, len(resp.Volumes))
	for _, v := range resp.Volumes {
		sizeById[aws.StringValue(v.VolumeId)] = data.GiB * data.Size(aws.Int64Value(v.Size))
	}
	return sizeById, nil
}

// ResizeEBS resizes the EBS volumes such that their sizes (ceiled to GiB) add up to newSize.
// Size reductions are not supported.
func (v *ebsLvmVolume) ResizeEBS(ctx context.Context, newSize data.Size) error {
	// Determine new size per EBS volume.
	perVolSize := data.Size(int64(newSize) / int64(len(v.ebsVolIds)))
	limits := minMaxByType[v.ebsVolType]
	if perVolSize < limits.min {
		perVolSize = limits.min
	}
	if perVolSize > limits.max {
		perVolSize = limits.max
	}
	perVolSizeGiB := numGiB(perVolSize)

	// Determine which EBS volumes need to be modified.
	sizeById, err := v.getSizeById(ctx)
	if err != nil {
		return err
	}
	idsToModify := make([]string, 0, len(sizeById))
	for id, size := range sizeById {
		sizeGiB := numGiB(size)
		if sizeGiB >= perVolSizeGiB {
			v.log.Printf("skipping ebs volume %s, size (%d GiB) >= target size (%d GiB)", id, sizeGiB, perVolSizeGiB)
			continue
		}
		idsToModify = append(idsToModify, id)
	}
	if len(idsToModify) == 0 {
		v.log.Printf("no ebs volume needs to be modified")
		return nil
	}

	// Determine if EBS volumes are ready to be modified.
	stateById, err := v.getModificationStateById(ctx, idsToModify)
	if err != nil {
		return err
	}
	var notReadyVols []string
	for id, state := range stateById {
		switch state {
		case "modifying", "optimizing":
			notReadyVols = append(notReadyVols, fmt.Sprintf("%s (%s)", id, state))
		}
	}
	if len(notReadyVols) > 0 {
		return fmt.Errorf("cannot modify ebs volumes before previous modification completes: %s", strings.Join(notReadyVols, ","))
	}

	// Modify EBS volumes.
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
		state          stateT
		stateStr       string
		modifiedIds    []string
		modifiedStates map[string]string
	)
	retriesByState := make(map[stateT]int)
	for state < stateDone {
		err = nil
		switch state {
		case stateModify:
			stateStr = fmt.Sprintf("modifying volumes (%s)", strings.Join(idsToModify, ", "))
			errs := make([]error, len(idsToModify))
			_ = traverse.Each(len(idsToModify), func(idx int) error {
				req := &ec2.ModifyVolumeInput{
					VolumeId: aws.String(idsToModify[idx]),
					Size:     aws.Int64(perVolSizeGiB),
				}
				ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, errs[idx] = v.ec2.ModifyVolumeWithContext(ctx2, req)
				cancel()
				return nil
			})
			modifiedIds = nil
			var failed []string
			for idx, err := range errs {
				if err != nil {
					failed = append(failed, idsToModify[idx])
				} else {
					modifiedIds = append(modifiedIds, idsToModify[idx])
				}
			}
			idsToModify = failed
			if len(failed) > 0 {
				err = fmt.Errorf("failed to modify (%s): %v", strings.Join(failed, ", "), errs)
			}
		case stateGetModificationStatus:
			stateStr = fmt.Sprintf("get modification status (%s)", strings.Join(modifiedIds, ", "))
			modifiedStates, err = v.getModificationStateById(ctx, modifiedIds)
		case stateCheckModificationStatus:
			stateStr = fmt.Sprintf("checking modification status (%v)", modifiedStates)
			var reasons []string
			for v, s := range modifiedStates {
				switch s {
				case "optimizing", "completed":
					// do nothing, ie, leave done as true
				// As per AWS: https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/monitoring-volume-modifications.html
				// Rarely, a transient AWS fault can result in a failed state. This is not an indication of volume health;
				// it merely indicates that the modification to the volume failed. If this occurs, retry the volume modification.
				case "failed":
					idsToModify = append(idsToModify, v)
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
			retriesByState[state] = 0
			state++
			continue
		}
		v.log.Errorf("%s: %v", stateStr, err)
		if err = retry.Wait(ctx, v.retrier, retriesByState[state]); err != nil {
			break
		}
		retriesByState[state]++
		if len(idsToModify) > 0 {
			state = stateModify
		}
	}
	if state < stateDone {
		return fmt.Errorf("unknown volume status after setting size to %s: %v", newSize, err)
	}
	return nil
}

// EBSIds returns the EBS volume IDs.
func (v *ebsLvmVolume) EBSIds() []string {
	return v.ebsVolIds
}

// getModificationStateById returns the current modification state for the input EBS volumes.
// Apart from the valid values described below, we add a (default) 'unmodified' state.
// https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_VolumeModification.html
// Current state of modification. Modification state is null for unmodified volumes.
// Valid Values: modifying | optimizing | completed | failed
func (v *ebsLvmVolume) getModificationStateById(ctx context.Context, volIds []string) (map[string]string, error) {
	mstate := make(map[string]string, len(volIds))
	for _, vid := range volIds {
		mstate[vid] = "unmodified"
	}
	req := &ec2.DescribeVolumesModificationsInput{VolumeIds: aws.StringSlice(volIds)}
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	resp, err := v.ec2.DescribeVolumesModificationsWithContext(ctx2, req)
	cancel()
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

// numGiB returns the number of GiB in size.
func numGiB(size data.Size) int64 {
	return int64(math.Ceil(size.Count(data.GiB)))
}
