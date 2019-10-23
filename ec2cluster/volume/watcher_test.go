// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
)

var (
	errTest           = fmt.Errorf("test error")
	testWatcherParams = infra.VolumeWatcher{
		LowThresholdPct:       50.0,
		HighThresholdPct:      75.0,
		WatcherSleepDuration:  100 * time.Millisecond,
		ResizeSleepDuration:   50 * time.Millisecond,
		FastThresholdDuration: 50 * time.Millisecond,
	}
)

type testVolume struct {
	size                          data.Size
	ready                         bool
	reason                        string
	duPct                         float64
	sizeErr, setSizeErr, readyErr error
	duErr, rfsErr                 error
	ngs, nss, nm, ndu, nrfs       int
}

func (v *testVolume) GetSize(ctx context.Context) (size data.Size, err error) {
	v.ngs++
	if v.sizeErr != nil {
		return -1, v.sizeErr
	}
	return v.size, nil
}

func (v *testVolume) ReadyToModify(ctx context.Context) (ready bool, reason string, err error) {
	v.nm++
	if v.readyErr != nil {
		return false, "", v.readyErr
	}
	return v.ready, v.reason, nil
}

func (v *testVolume) SetSize(ctx context.Context, newSize data.Size) error {
	v.nss++
	if v.setSizeErr != nil {
		return v.setSizeErr
	}
	v.size = newSize
	v.duPct = 10.0
	return nil
}

func (v *testVolume) Usage() (float64, error) {
	v.ndu++
	if v.duErr != nil {
		return -1.0, v.duErr
	}
	return v.duPct, nil
}

func (v *testVolume) ResizeFS() error {
	v.nrfs++
	return v.rfsErr
}

func TestWatcher_initError(t *testing.T) {
	if _, err := NewWatcher(context.Background(), log.Std, &testVolume{sizeErr: errTest}, testWatcherParams); err == nil {
		t.Errorf("got no error, want error")
	}
	if _, err := NewWatcher(context.Background(), log.Std, &testVolume{size: data.GiB, duErr: errTest}, testWatcherParams); err == nil {
		t.Errorf("got no error, want error")
	}
}

func TestWatcher(t *testing.T) {
	for _, tt := range []struct {
		v *testVolume
		// wants
		vSz                     data.Size
		watcherSz               data.Size
		ngs, nss, nm, ndu, nrfs int
	}{
		{
			&testVolume{size: data.GiB, duPct: 40.0},
			data.GiB, data.GiB, 3, 0, 0, 3, 0,
		},
		{
			&testVolume{size: data.GiB, duPct: 80.0, ready: false, reason: "test"},
			data.GiB, data.GiB, 2, 0, 3, 2, 0,
		},
		{
			&testVolume{size: data.GiB, duPct: 80.0, ready: true, rfsErr: errTest},
			2 * data.GiB, data.GiB, 3, 1, 1, 2, 2,
		},
		{
			&testVolume{size: data.GiB, duPct: 80.0, ready: true},
			2 * data.GiB, 2 * data.GiB, 4, 1, 1, 2, 1,
		},
	} {
		w, err := NewWatcher(context.Background(), log.Std, tt.v, testWatcherParams)
		if err != nil {
			t.Error(err)
			continue
		}
		ctx, _ := context.WithDeadline(context.Background(), time.Now().Add(275*time.Millisecond))
		go w.Watch(ctx)
		// Wait till watch routine is done
		<-ctx.Done()
		if got, want := tt.v.size, tt.vSz; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.v.ngs, tt.ngs; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.v.nss, tt.nss; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.v.nm, tt.nm; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.v.ndu, tt.ndu; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := tt.v.nrfs, tt.nrfs; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
