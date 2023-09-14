// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
)

var testWatcherParams = infra.VolumeWatcher{
	LowThresholdPct:       50.0,
	HighThresholdPct:      75.0,
	WatcherSleepDuration:  100 * time.Millisecond,
	ResizeSleepDuration:   50 * time.Millisecond,
	FastThresholdDuration: 50 * time.Millisecond,
	FastIncreaseFactor:    10,
	SlowIncreaseFactor:    5,
}

type transientErr struct {
	err     error
	numLeft int
}

func (e *transientErr) Error() error {
	if e.numLeft > 0 {
		e.numLeft--
		return e.err
	}
	return nil
}

// testVolume implements Volume.
type testVolume struct {
	// EBS.
	ebsSize              data.Size
	nEBSSize, nResizeEBS int
	resizeEBSErr         transientErr
	// Device.
	fsSize                             data.Size
	fsUsage                            float64
	nFSSize, nFSUsage, nResizeFS       int
	fsSizeErr, fsUsageErr, resizeFSErr transientErr
}

func (v *testVolume) EBSSize(ctx context.Context) (size data.Size, err error) {
	v.nEBSSize++
	return v.ebsSize, nil
}

func (v *testVolume) ResizeEBS(ctx context.Context, newSize data.Size) error {
	v.nResizeEBS++
	if err := v.resizeEBSErr.Error(); err != nil {
		return err
	}
	v.ebsSize = newSize
	return nil
}

func (v *testVolume) EBSIds() []string {
	return []string{"vol-testvolume1"}
}

// FSSize implements Device.
func (v *testVolume) FSSize() (data.Size, error) {
	v.nFSSize++
	if err := v.fsSizeErr.Error(); err != nil {
		return 0, err
	}
	return v.fsSize, nil
}

// FSUsage implements Device.
func (v *testVolume) FSUsage() (float64, error) {
	v.nFSUsage++
	if err := v.fsUsageErr.Error(); err != nil {
		return -1.0, err
	}
	return v.fsUsage, nil
}

// ResizeFS implements Device.
func (v *testVolume) ResizeFS() error {
	v.nResizeFS++
	if err := v.resizeFSErr.Error(); err != nil {
		return err
	}
	oldSize := v.fsSize
	newSize := v.ebsSize
	resizeFactor := newSize.Bytes() / oldSize.Bytes()

	v.fsSize = v.ebsSize
	v.fsUsage = math.Round(v.fsUsage / float64(resizeFactor))
	return nil
}

func TestWatcher_initError(t *testing.T) {
	vol := testVolume{fsSizeErr: transientErr{errTest, 1}}
	if _, err := NewWatcher(&vol, testWatcherParams, log.Std); err == nil {
		t.Errorf("got no error, want error")
	}
	vol = testVolume{fsUsageErr: transientErr{errTest, 1}}
	if _, err := NewWatcher(&vol, testWatcherParams, log.Std); err == nil {
		t.Errorf("got no error, want error")
	}
}

func TestWatcher(t *testing.T) {
	for _, tt := range []struct {
		name string
		vol  *testVolume
		// EBS.
		finalEBSSize data.Size
		nEBSSize     int
		nResizeEBS   int
		// Device.
		nFSSize     int
		nFSUsageMin int
		nResizeFS   int
	}{
		{
			name: "no resize",
			vol: &testVolume{
				ebsSize: data.GiB,
				fsUsage: 40.0,
			},
			finalEBSSize: data.GiB,
			nFSSize:      2,
			nFSUsageMin:  10,
		},
		{
			name: "EBS resize succeeds on the third attempt",
			vol: &testVolume{
				ebsSize:      data.GiB,
				fsSize:       data.GiB,
				fsUsage:      80.0,
				resizeEBSErr: transientErr{fmt.Errorf("volume not ready"), 2},
			},
			finalEBSSize: data.Size(testWatcherParams.SlowIncreaseFactor) * data.GiB,
			nEBSSize:     4,
			nResizeEBS:   3,
			nFSSize:      3,
			nFSUsageMin:  7,
			nResizeFS:    1,
		},
		{
			name: "filesystem resize succeeds on third attempt",
			vol: &testVolume{
				ebsSize:     data.GiB,
				fsSize:      data.GiB,
				fsUsage:     80.0,
				resizeFSErr: transientErr{errTest, 2},
			},
			finalEBSSize: data.Size(testWatcherParams.SlowIncreaseFactor) * data.GiB,
			nEBSSize:     2,
			nResizeEBS:   1,
			nFSSize:      3,
			nFSUsageMin:  7,
			nResizeFS:    3,
		},
		{
			name: "EBS and filesystem resizes succeed on first attempt",
			vol: &testVolume{
				ebsSize: data.GiB,
				fsSize:  data.GiB,
				fsUsage: 80.0,
			},
			finalEBSSize: data.Size(testWatcherParams.SlowIncreaseFactor) * data.GiB,
			nEBSSize:     2,
			nResizeEBS:   1,
			nFSSize:      3,
			nFSUsageMin:  7,
			nResizeFS:    1,
		},
	} {
		w, err := NewWatcher(tt.vol, testWatcherParams, log.Std)
		if err != nil {
			t.Error(err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		go w.Watch(ctx)
		// Wait till watch routine is done.
		<-ctx.Done()
		cancel()
		if got, want := tt.vol.ebsSize, tt.finalEBSSize; got != want {
			t.Errorf("%s: ebsSize: got %d, want %d", tt.name, got, want)
		}
		if got, want := tt.vol.nEBSSize, tt.nEBSSize; got != want {
			t.Errorf("%s: nEBSSize: got %d, want %d", tt.name, got, want)
		}
		if got, want := tt.vol.nResizeEBS, tt.nResizeEBS; got != want {
			t.Errorf("%s: nResizeEBS: got %d, want %d", tt.name, got, want)
		}
		if got, wantMin := tt.vol.nFSUsage, tt.nFSUsageMin; got < wantMin {
			t.Errorf("%s: nFSUsage: got %d, want >= %d", tt.name, got, wantMin)
		}
		if got, want := tt.vol.nFSSize, tt.nFSSize; got != want {
			t.Errorf("%s: nFSSize: got %d, want %d", tt.name, got, want)
		}
		if got, want := tt.vol.nResizeFS, tt.nResizeFS; got != want {
			t.Errorf("%s: nResizeFS: got %d, want %d", tt.name, got, want)
		}
	}
}
