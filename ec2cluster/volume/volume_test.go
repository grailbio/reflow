// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow/log"
)

var errTest = fmt.Errorf("test error")

func descVolsFn(m map[string]int64, err error) func([]string) (*ec2.DescribeVolumesOutput, error) {
	var out *ec2.DescribeVolumesOutput
	outIds := make([]string, 0, len(m))
	if err == nil {
		var vols []*ec2.Volume
		for k, v := range m {
			vols = append(vols, &ec2.Volume{VolumeId: aws.String(k), Size: aws.Int64(v)})
			outIds = append(outIds, k)
		}
		out = &ec2.DescribeVolumesOutput{Volumes: vols}
	}
	return func(volIds []string) (*ec2.DescribeVolumesOutput, error) {
		sort.Strings(volIds)
		sort.Strings(outIds)
		if !reflect.DeepEqual(volIds, outIds) {
			return nil, fmt.Errorf("DescribeVolumes called with %v, want %v", volIds, outIds)
		}
		return out, err
	}
}

func TestEBSSize(t *testing.T) {
	for _, tt := range []struct {
		vols  []string
		fn    func([]string) (*ec2.DescribeVolumesOutput, error)
		wsize data.Size
		werr  bool
	}{
		{[]string{"volA", "volB"}, descVolsFn(nil, errTest), 0, true},
		{[]string{"volA"}, descVolsFn(map[string]int64{"volA": 10, "volB": 20}, nil), 0, true},
		{[]string{"volA", "volB"}, descVolsFn(map[string]int64{"volA": 10, "volB": 20}, nil), 30 * data.GiB, false},
	} {
		v := &ebsLvmVolume{ebsVolIds: tt.vols, log: log.Std, ec2: &mockEC2Client{descVolsFn: tt.fn}}
		size, err := v.EBSSize(context.Background())
		if gotE := err != nil; gotE != tt.werr {
			t.Errorf("got error: %v, want error: %t", err, tt.werr)
		}
		if tt.werr {
			continue
		}
		if got, want := size, tt.wsize; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}

func volModsFn(outs []*ec2.DescribeVolumesModificationsOutput, errs []error) func([]string) (*ec2.DescribeVolumesModificationsOutput, error) {
	var idx int
	return func(volIds []string) (*ec2.DescribeVolumesModificationsOutput, error) {
		var (
			out *ec2.DescribeVolumesModificationsOutput
			err error
		)
		if outs != nil {
			if idx < len(outs) {
				out = outs[idx]
			} else {
				out = outs[len(outs)-1]
			}
			outIds := make([]string, 0, len(out.VolumesModifications))
			for _, mod := range out.VolumesModifications {
				outIds = append(outIds, aws.StringValue(mod.VolumeId))
			}
			sort.Strings(volIds)
			sort.Strings(outIds)
			if !reflect.DeepEqual(volIds, outIds) {
				out = nil
				return nil, fmt.Errorf("DescribeVolumeModifications called with %v, want %v", volIds, outIds)
			}
		}
		if errs != nil {
			if idx < len(errs) {
				err = errs[idx]
			} else {
				err = errs[len(errs)-1]
			}
		}
		idx++
		return out, err
	}
}

func volModsOut(m map[string]string) *ec2.DescribeVolumesModificationsOutput {
	var vms []*ec2.VolumeModification
	for k, v := range m {
		ms := aws.String(v)
		if v == "" {
			ms = nil
		}
		vms = append(vms, &ec2.VolumeModification{VolumeId: aws.String(k), ModificationState: ms})
	}
	return &ec2.DescribeVolumesModificationsOutput{VolumesModifications: vms}
}

func modVolsFn(m map[string][]error) func(string) (*ec2.ModifyVolumeOutput, error) {
	var idxMu sync.Mutex
	idxs := make(map[string]int, len(m))
	for k := range m {
		idxs[k] = 0
	}
	return func(vid string) (out *ec2.ModifyVolumeOutput, err error) {
		idxMu.Lock()
		defer idxMu.Unlock()
		idx, errs := idxs[vid], m[vid]
		if errs != nil {
			if idx > 0 && idx >= len(errs) {
				idx = len(errs) - 1
			}
			err = errs[idx]
		}
		idxs[vid] = idx + 1
		return
	}
}

func TestResizeEBS(t *testing.T) {
	retries := 5
	for _, tt := range []struct {
		name       string
		vols       []string
		newSz      data.Size
		descVol    func([]string) (*ec2.DescribeVolumesOutput, error)
		descVolMod func([]string) (*ec2.DescribeVolumesModificationsOutput, error)
		modVol     func(volId string) (*ec2.ModifyVolumeOutput, error)
		werr       bool
		nModVols   int
	}{
		{
			name:    "fail to describe volumes",
			vols:    []string{"volA", "volB"},
			newSz:   10 * data.GiB,
			descVol: descVolsFn(nil, errTest),
			werr:    true,
		},
		{
			name:    "sum of volumes' size >= newSz",
			vols:    []string{"volA", "volB"},
			newSz:   10 * data.GiB,
			descVol: descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil),
			werr:    false,
		},
		{
			name:       "fail to describe volume modification status",
			vols:       []string{"volA", "volB"},
			newSz:      50 * data.GiB,
			descVol:    descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil),
			descVolMod: volModsFn(nil, []error{errTest}),
			werr:       true,
		},
		{
			name:    "volume is not ready to be modified",
			vols:    []string{"volA", "volB"},
			newSz:   50 * data.GiB,
			descVol: descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil),
			descVolMod: volModsFn(
				[]*ec2.DescribeVolumesModificationsOutput{
					volModsOut(map[string]string{"volA": "completed", "volB": "modifying"}),
				},
				nil,
			),
			werr: true,
		},
		{
			name:    "volume modification request fails",
			vols:    []string{"volA", "volB"},
			newSz:   50 * data.GiB,
			descVol: descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil),
			descVolMod: volModsFn(
				[]*ec2.DescribeVolumesModificationsOutput{
					volModsOut(map[string]string{"volA": "completed", "volB": "completed"}),
				},
				nil,
			),
			modVol: modVolsFn(
				map[string][]error{
					"volA": {errTest},
					"volB": {nil},
				},
			),
			werr:     true,
			nModVols: 2 + retries,
		},
		{
			name:    "volume modification requests succeed but volume fails to modify",
			vols:    []string{"volA", "volB"},
			newSz:   50 * data.GiB,
			descVol: descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil),
			descVolMod: volModsFn(
				[]*ec2.DescribeVolumesModificationsOutput{
					volModsOut(map[string]string{"volA": "completed", "volB": "completed"}),
					volModsOut(map[string]string{"volA": "optimizing", "volB": "failed"}),
				},
				nil,
			),
			modVol: modVolsFn(
				map[string][]error{
					"volA": {nil},
					"volB": {nil},
				},
			),
			werr:     true,
			nModVols: 2 + retries,
		},
		{
			name:    "all volumes resize successfully",
			vols:    []string{"volA", "volB"},
			newSz:   50 * data.GiB,
			descVol: descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil),
			descVolMod: volModsFn(
				[]*ec2.DescribeVolumesModificationsOutput{
					volModsOut(map[string]string{"volA": "completed", "volB": "completed"}),
					volModsOut(map[string]string{"volA": "optimizing", "volB": "optimizing"}),
				},
				nil,
			),
			modVol: modVolsFn(
				map[string][]error{
					"volA": {nil},
					"volB": {nil},
				},
			),
			werr:     false,
			nModVols: 2,
		},
		{
			name:    "all volumes resize successfully after checking modification status 3 times",
			vols:    []string{"volA", "volB"},
			newSz:   50 * data.GiB,
			descVol: descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil),
			descVolMod: volModsFn(
				[]*ec2.DescribeVolumesModificationsOutput{
					volModsOut(map[string]string{"volA": "completed", "volB": "completed"}),
					volModsOut(map[string]string{"volA": "modifying", "volB": "modifying"}),
					volModsOut(map[string]string{"volA": "modifying", "volB": "modifying"}),
					volModsOut(map[string]string{"volA": "optimizing", "volB": "optimizing"}),
				},
				nil,
			),
			modVol: modVolsFn(
				map[string][]error{
					"volA": {nil},
					"volB": {nil},
				},
			),
			werr:     false,
			nModVols: 2,
		},
		{
			name:    "both resizes succeed on the second try, but failures are detected at different points in time",
			vols:    []string{"volA", "volB"},
			newSz:   50 * data.GiB,
			descVol: descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil),
			descVolMod: volModsFn(
				[]*ec2.DescribeVolumesModificationsOutput{
					volModsOut(map[string]string{"volA": "completed", "volB": "completed"}),
					volModsOut(map[string]string{"volA": "modifying", "volB": "failed"}),
					volModsOut(map[string]string{"volA": "failed", "volB": "modifying"}),
					volModsOut(map[string]string{"volA": "optimizing", "volB": "optimizing"}),
				},
				nil,
			),
			modVol: modVolsFn(
				map[string][]error{
					"volA": {nil},
					"volB": {nil},
				},
			),
			werr:     false,
			nModVols: 4,
		},
		{
			name:    "volA doesn't need to be resized, volB resizes successfully",
			vols:    []string{"volA", "volB"},
			newSz:   50 * data.GiB,
			descVol: descVolsFn(map[string]int64{"volA": 5, "volB": 25}, nil),
			descVolMod: volModsFn(
				[]*ec2.DescribeVolumesModificationsOutput{
					volModsOut(map[string]string{"volA": "completed"}),
					volModsOut(map[string]string{"volA": "optimizing"}),
				},
				nil,
			),
			modVol: modVolsFn(
				map[string][]error{
					"volA": {nil},
				},
			),
			werr:     false,
			nModVols: 1,
		},
	} {
		c := &mockEC2Client{descVolsFn: tt.descVol, descVolsModsFn: tt.descVolMod, modVolsFn: tt.modVol}
		v := &ebsLvmVolume{ebsVolIds: tt.vols, log: log.Std, ebsVolType: ec2.VolumeTypeGp3,
			ec2:     c,
			retrier: retry.MaxRetries(retry.Backoff(10*time.Millisecond, 20*time.Millisecond, 1.5), retries),
		}
		err := v.ResizeEBS(context.Background(), tt.newSz)
		if gotE := err != nil; gotE != tt.werr {
			t.Errorf("%s: error: got: %v, want: %t", tt.name, err, tt.werr)
		}
		if got, want := c.nModVols, tt.nModVols; got != want {
			t.Errorf("%s: nModVols: got %d, want: %d", tt.name, got, want)
		}
	}
}
