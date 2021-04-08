// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/data"
	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow/log"
)

var errTest = fmt.Errorf("test error")

func descVolsFn(m map[string]int64, err error) func() (*ec2.DescribeVolumesOutput, error) {
	var out *ec2.DescribeVolumesOutput
	if err == nil {
		var vols []*ec2.Volume
		for k, v := range m {
			vols = append(vols, &ec2.Volume{VolumeId: aws.String(k), Size: aws.Int64(v)})
		}
		out = &ec2.DescribeVolumesOutput{Volumes: vols}
	}
	return func() (*ec2.DescribeVolumesOutput, error) {
		return out, err
	}
}

func TestGetSize(t *testing.T) {
	for _, tt := range []struct {
		vols  []string
		fn    func() (*ec2.DescribeVolumesOutput, error)
		wsize data.Size
		werr  bool
	}{
		{[]string{"volA", "volB"}, descVolsFn(nil, errTest), 0, true},
		{[]string{"volA"}, descVolsFn(map[string]int64{"volA": 10, "volB": 20}, nil), 0, true},
		{[]string{"volA", "volB"}, descVolsFn(map[string]int64{"volA": 10, "volB": 20}, nil), 30 * data.GiB, false},
	} {
		v := &ebsLvmVolume{ebsVolIds: tt.vols, log: log.Std, ec2: &mockEC2Client{descVolsFn: tt.fn}}
		size, err := v.GetSize(context.Background())
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

func volModsFn(outs []*ec2.DescribeVolumesModificationsOutput, errs []error) func() (*ec2.DescribeVolumesModificationsOutput, error) {
	var idx int
	return func() (*ec2.DescribeVolumesModificationsOutput, error) {
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

func TestReadyToModify(t *testing.T) {
	for _, tt := range []struct {
		vols    []string
		fn      func() (*ec2.DescribeVolumesModificationsOutput, error)
		wready  bool
		wreason string
		werr    error
	}{
		{[]string{"volA", "volB"}, volModsFn(nil, []error{errTest}), false, "", errTest},
		{
			[]string{"volA", "volB"},
			volModsFn(nil, []error{awserr.New("InvalidVolumeModification.NotFound", "", nil)}),
			true, "", nil,
		},
		{
			[]string{"volA", "volB"},
			volModsFn([]*ec2.DescribeVolumesModificationsOutput{volModsOut(map[string]string{"volA": "", "volB": "completed"})}, nil),
			true, "", nil,
		},
		{
			[]string{"volA", "volB"},
			volModsFn([]*ec2.DescribeVolumesModificationsOutput{volModsOut(map[string]string{"volA": "", "volB": "failed"})}, nil),
			true, "", nil,
		},
		{
			[]string{"volA", "volB"},
			volModsFn([]*ec2.DescribeVolumesModificationsOutput{volModsOut(map[string]string{"volA": "", "volB": "modifying"})}, nil),
			false, "volB (modifying)", nil,
		},
		{
			[]string{"volA", "volB"},
			volModsFn([]*ec2.DescribeVolumesModificationsOutput{volModsOut(map[string]string{"volA": "optimizing", "volB": "completed"})}, nil),
			false, "volA (optimizing)", nil,
		},
	} {
		v := &ebsLvmVolume{ebsVolIds: tt.vols, log: log.Std, ec2: &mockEC2Client{descVolsModsFn: tt.fn}}
		gready, greason, gerr := v.ReadyToModify(context.Background())
		if got, want := gerr, tt.werr; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if tt.werr != nil {
			continue
		}
		if got, want := gready, tt.wready; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := greason, tt.wreason; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
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

func TestSetSize(t *testing.T) {
	for _, tt := range []struct {
		vols  []string
		newSz data.Size
		dvmfn func() (*ec2.DescribeVolumesModificationsOutput, error)
		mvfn  func(volId string) (*ec2.ModifyVolumeOutput, error)
		dvfn  func() (*ec2.DescribeVolumesOutput, error)
		werr  bool
	}{
		{[]string{"volA", "volB"}, 10 * data.GiB, nil, nil, descVolsFn(nil, errTest), true},
		{[]string{"volA", "volB"}, 10 * data.GiB, nil, nil, descVolsFn(map[string]int64{"volA": 5, "volB": 5}, nil), false},
		{
			[]string{"volA", "volB"}, 10 * data.GiB,
			volModsFn(nil, []error{errTest}),
			modVolsFn(map[string][]error{"volA": {errTest}}),
			descVolsFn(map[string]int64{"volA": 2, "volB": 2}, nil),
			true,
		},
		{
			[]string{"volA", "volB"}, 10 * data.GiB,
			volModsFn([]*ec2.DescribeVolumesModificationsOutput{
				nil,
				volModsOut(map[string]string{"volA": "", "volB": "failed"}),
				volModsOut(map[string]string{"volA": "modifying", "volB": "modifying"}),
				volModsOut(map[string]string{"volA": "completed", "volB": "optimizing"}),
			}, []error{errTest, nil}),
			modVolsFn(map[string][]error{"volA": {errTest, nil}}),
			descVolsFn(map[string]int64{"volA": 2, "volB": 2}, nil),
			false,
		},
	} {
		v := &ebsLvmVolume{ebsVolIds: tt.vols, log: log.Std,
			ec2:     &mockEC2Client{descVolsModsFn: tt.dvmfn, modVolsFn: tt.mvfn, descVolsFn: tt.dvfn},
			retrier: retry.MaxRetries(retry.Backoff(10*time.Millisecond, 20*time.Millisecond, 1.5), 5),
		}
		err := v.SetSize(context.Background(), tt.newSz)
		if gotE := err != nil; gotE != tt.werr {
			t.Errorf("got error: %v, want error: %t", err, tt.werr)
		}
	}
}
