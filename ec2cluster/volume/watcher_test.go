// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"context"
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

type testVolume struct {
	size                              data.Size
	ready                             bool
	reason                            string
	duPct                             float64
	sizeErr, setSizeErr, readyErr     error
	duErr, rfsErr                     error
	nGetSize, nSetSize                int
	nReadyToModify, nUsage, nResizeFS int
}

func (v *testVolume) GetSize(ctx context.Context) (size data.Size, err error) {
	v.nGetSize++
	if v.sizeErr != nil {
		return -1, v.sizeErr
	}
	return v.size, nil
}

func (v *testVolume) ReadyToModify(ctx context.Context) (ready bool, reason string, err error) {
	v.nReadyToModify++
	if v.readyErr != nil {
		return false, "", v.readyErr
	}
	return v.ready, v.reason, nil
}

func (v *testVolume) SetSize(ctx context.Context, newSize data.Size) error {
	v.nSetSize++
	if v.setSizeErr != nil {
		return v.setSizeErr
	}
	v.size = newSize
	v.duPct = 10.0
	return nil
}

func (v *testVolume) Usage() (float64, error) {
	v.nUsage++
	if v.duErr != nil {
		return -1.0, v.duErr
	}
	return v.duPct, nil
}

func (v *testVolume) ResizeFS() error {
	v.nResizeFS++
	return v.rfsErr
}

func (v *testVolume) GetVolumeIds() []string {
	return []string{"vol-testvolume1"}
}

func TestWatcher_initError(t *testing.T) {
	if _, err := NewWatcher(&testVolume{sizeErr: errTest}, testWatcherParams, log.Std); err == nil {
		t.Errorf("got no error, want error")
	}
	if _, err := NewWatcher(&testVolume{size: data.GiB, duErr: errTest}, testWatcherParams, log.Std); err == nil {
		t.Errorf("got no error, want error")
	}
}

func TestWatcher(t *testing.T) {
	for _, tt := range []struct {
		v                        *testVolume
		vSz                      data.Size
		nGetSizeMin, nSetSizeMin int
		nModifyMin, nUsageMin    int
		nResizeFSMin             int
	}{
		{
			&testVolume{size: data.GiB, duPct: 40.0},
			data.GiB, 2, 0, 0, 10, 0,
		},
		{
			&testVolume{size: data.GiB, duPct: 80.0, ready: false, reason: "test"},
			data.GiB, 2, 0, 15, 3, 0,
		},
		{
			&testVolume{size: data.GiB, duPct: 80.0, ready: true, rfsErr: errTest},
			data.Size(testWatcherParams.SlowIncreaseFactor) * data.GiB, 4, 1, 1, 3, 10,
		},
		{
			&testVolume{size: data.GiB, duPct: 80.0, ready: true},
			data.Size(testWatcherParams.SlowIncreaseFactor) * data.GiB, 5, 1, 1, 10, 1,
		},
	} {
		w, err := NewWatcher(tt.v, testWatcherParams, log.Std)
		if err != nil {
			t.Error(err)
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		go w.Watch(ctx)
		// Wait till watch routine is done
		<-ctx.Done()
		cancel()
		if got, want := tt.v.size, tt.vSz; got != want {
			t.Errorf("size: got %v, want %v", got, want)
		}
		if got, want := tt.v.nGetSize, tt.nGetSizeMin; got < want {
			t.Errorf("nGetSize: got %v, want >= %v", got, want)
		}
		if got, want := tt.v.nSetSize, tt.nSetSizeMin; got != want {
			t.Errorf("nSetSize: got %v, want %v", got, want)
		}
		if got, want := tt.v.nReadyToModify, tt.nModifyMin; got < want {
			t.Errorf("nReadyToModify: got %v, want >= %v", got, want)
		}
		if got, want := tt.v.nUsage, tt.nUsageMin; got < want {
			t.Errorf("nUsage: got %v, want >= %v", got, want)
		}
		if got, want := tt.v.nResizeFS, tt.nResizeFSMin; got < want {
			t.Errorf("nResizeFS: got %v, want >= %v", got, want)
		}
	}
}
