// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"context"
	"fmt"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow/infra"
	"github.com/grailbio/reflow/log"
)

// Watcher has the capability of watching a volume and resizing it if and when necessary
// based on configured set of parameters.
type Watcher struct {
	v   Volume
	w   infra.VolumeWatcher
	log *log.Logger
}

// NewWatcher creates a new watcher
func NewWatcher(v Volume, w infra.VolumeWatcher, log *log.Logger) (*Watcher, error) {
	if _, err := v.GetSize(context.Background()); err != nil {
		return nil, fmt.Errorf("get volume size: %v", err)
	}
	if _, err := v.Usage(); err != nil {
		return nil, err
	}
	return &Watcher{v: v, w: w, log: log}, nil
}

// Watch watches the underlying volume and resizes it whenever necessary.
// Watch will not return until the provided context is done.
func (w *Watcher) Watch(ctx context.Context) {
	type stateT int
	const (
		// Watch the volume
		stateWatch stateT = iota
		// Resize the volume
		stateResizeVolume
		// Resize the filesystem
		stateResizeFS
	)

	var (
		state                  stateT
		oldSize                data.Size
		lastBelowThresholdTime = time.Now()
		iter                   = time.NewTicker(w.w.WatcherSleepDuration)
	)

	sz, err := w.v.GetSize(ctx)
	if err != nil {
		w.log.Error(err)
	}
	pct, err := w.v.Usage()
	if err != nil {
		w.log.Error(err)
	}
	w.log.Printf("started watching %s (volume size: %s, used: %.2f%%)", w.v.GetVolumeIds(), sz, pct)

	for {
		select {
		case <-ctx.Done():
			w.log.Debugf("exiting")
			return
		case <-iter.C:
		}

		switch state {
		case stateWatch:
			pct, err = w.v.Usage()
			if err != nil {
				w.log.Error(err)
				break
			}
			prefix := fmt.Sprintf("watching (volume size: %s, used: %.2f%%)", sz, pct)
			if pct < w.w.LowThresholdPct {
				lastBelowThresholdTime = time.Now()
				w.log.Printf("%s: below low threshold (%.2f%% < %.2f%%)", prefix, pct, w.w.LowThresholdPct)
				break
			}
			if pct < w.w.HighThresholdPct {
				ago := time.Since(lastBelowThresholdTime).Round(time.Second)
				w.log.Printf("%s: below high threshold (%.2f%% < %.2f%%) and was below low threshold %s ago",
					prefix, pct, w.w.HighThresholdPct, ago)
				break
			}
			// Above high threshold
			ago := time.Since(lastBelowThresholdTime).Round(time.Second)
			w.log.Printf("%s: above high threshold (%.2f%% > %.2f%%) and was below low threshold %s ago",
				prefix, pct, w.w.HighThresholdPct, ago)
			// Now we are going to resize, so reduce the wait duration
			// so that we keep re-attempting quickly in case of failures.
			iter.Stop()
			iter = time.NewTicker(w.w.ResizeSleepDuration)
			state = stateResizeVolume
		case stateResizeVolume:
			w.log.Printf("resizing volume")
			ready, reason, err := w.v.ReadyToModify(ctx)
			if err != nil {
				w.log.Errorf("ReadyToModify: %v", err)
				break
			}
			if !ready {
				w.log.Printf("not ready modify volume due to: %s", reason)
				break
			}
			w.log.Printf("volume ready to modify")
			// Determine factor by which to increase disk size.
			incFactor := w.w.SlowIncreaseFactor
			if dur := time.Now().Sub(lastBelowThresholdTime); dur < w.w.FastThresholdDuration {
				incFactor = w.w.FastIncreaseFactor
			}
			w.log.Printf("attempting size increase by %dX", incFactor)
			currSize, err := w.v.GetSize(ctx)
			oldSize = currSize
			if err != nil {
				w.log.Error(err)
				break
			}
			newSize := currSize * data.Size(incFactor)
			if err := w.v.SetSize(ctx, newSize); err != nil {
				w.log.Errorf("failed to change size (%s -> %s): %v", currSize, newSize, err)
				// In case of failure, check size again, instead of being stuck trying repeatedly.
				state = stateWatch
				break
			}
			sz, err = w.v.GetSize(ctx)
			if err != nil {
				w.log.Errorf("failed to get volume size after resizing, will continue to resize FS: %v", err)
			} else {
				w.log.Printf("changed volume size %s -> %s", currSize, sz)
			}
			state = stateResizeFS
		case stateResizeFS:
			w.log.Printf("resizing filesystem")
			if err := w.v.ResizeFS(); err != nil {
				w.log.Errorf("resize filesystem: %v", err)
				break
			}
			sz, err = w.v.GetSize(ctx)
			if err != nil {
				w.log.Errorf("cannot get volume size (after resizeFS): %v", err)
			}
			w.log.Printf("successfully increased filesystem size %s -> %s", oldSize, sz)
			iter.Stop()
			iter = time.NewTicker(w.w.WatcherSleepDuration)
			state = stateWatch
		}
	}
}
