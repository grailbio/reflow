// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package volume

import (
	"context"
	"fmt"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow/log"
)

// DefaultWatcherParams are a default set of volume watcher parameters which will double the disk size
// if the disk usage >75%, or quadruple if the usage went from <55% to >75% within 24 hours.
var DefaultWatcherParams = WatcherParams{
	LowThresholdPct:       55.0,
	HighThresholdPct:      75.0,
	WatcherSleepDuration:  10 * time.Minute,
	ResizeSleepDuration:   10 * time.Second,
	FastThresholdDuration: 24 * time.Hour,
}

// WatcherParams represents the set of parameters that govern the behavior of a volume watcher.
// Every WatcherSleepDuration, the watcher will check the disk usage and keep track of the
// last time at which the usage was below the LowThresholdPct. If the disk usage goes
// above HighThresholdPct, then a resize is triggered.  The volume size will be increased to
// 2x the current size unless the time taken to go from below LowThresholdPct to above HighThresholdPct
// was within FastThresholdDuration, in which case the size will be increased to 4x the current size.
// Once the underlying volume is resized, a filesystem resize will be attempted every ResizeSleepDuration
// until success.
// TODO(swami):  Provide this using infra package so that it can be modified in reflow config.
type WatcherParams struct {
	// LowThresholdPct defines how full the filesystem needs to be
	// to trigger the low threshold.
	LowThresholdPct float64

	// HighThresholdPct defines how full the filesystem needs to be
	// to trigger the high threshold.
	// The time it takes for the filesystem to fill from low threshold to high threshold
	// selects between a lower (time is longer) or higher (time is shorter)
	// amount of extra space to get added to the volume.
	HighThresholdPct float64

	// WatcherSleepDuration is the frequency with which to check
	// whether disk are full over threshold and need resizing
	WatcherSleepDuration time.Duration

	// ResizeSleepDuration is the frequency with which to attempt
	// resizing the volume and filesystem once we've hit above HighThresholdPct
	ResizeSleepDuration time.Duration

	// FastThresholdDuration is the time duration within which if the disk usage
	// went from below LowThresholdPct to above HighThresholdPct, then
	// we quadruple the disk size (otherwise we just double)
	FastThresholdDuration time.Duration
}

type Watcher struct {
	vol Volume
	wp  WatcherParams
	log *log.Logger
}

// NewWatcher creates a new watcher
func NewWatcher(ctx context.Context, log *log.Logger, v Volume, wp WatcherParams) (Watcher, error) {
	if _, err := v.GetSize(ctx); err != nil {
		return Watcher{}, fmt.Errorf("get volume size: %v", err)
	}
	if _, err := v.Usage(); err != nil {
		return Watcher{}, err
	}
	return Watcher{vol: v, wp: wp, log: log}, nil
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
		vSize                  data.Size
		lastBelowThresholdTime = time.Now()
		iterWait               = w.wp.WatcherSleepDuration
	)

	for {
		select {
		case <-ctx.Done():
			w.log.Debugf("exiting")
			return
		case <-time.After(iterWait):
		}

		switch state {
		case stateWatch:
			sz, err := w.vol.GetSize(ctx)
			if err != nil {
				w.log.Error(err)
				break
			}
			vSize = sz
			pct, err := w.vol.Usage()
			if err != nil {
				w.log.Error(err)
				break
			}
			prefix := fmt.Sprintf("watching (volume size: %s, used: %.2f%%)", sz, pct)
			if pct < w.wp.LowThresholdPct {
				lastBelowThresholdTime = time.Now()
				w.log.Printf("%s: below low threshold (%.2f%% < %.2f%%)", prefix, pct, w.wp.LowThresholdPct)
				break
			}
			if pct < w.wp.HighThresholdPct {
				ago := time.Since(lastBelowThresholdTime).Round(time.Second)
				w.log.Printf("%s: below high threshold (%.2f%% < %.2f%%) and was below low threshold %s ago",
					prefix, pct, w.wp.HighThresholdPct, ago)
				break
			}
			// Above high threshold
			ago := time.Since(lastBelowThresholdTime).Round(time.Second)
			w.log.Printf("%s: above high threshold (%.2f%% > %.2f%%) and was below low threshold %s ago",
				prefix, pct, w.wp.HighThresholdPct, ago)
			// Now we are going to resize, so reduce the wait duration to 1 minute
			// so that we keep re-attempting quickly in case of failures.
			iterWait = w.wp.ResizeSleepDuration
			state = stateResizeVolume
		case stateResizeVolume:
			w.log.Printf("resizing volume")
			ready, reason, err := w.vol.ReadyToModify(ctx)
			if err != nil {
				w.log.Errorf("ReadyToModify: %v", err)
				break
			}
			if !ready {
				w.log.Printf("not ready modify volume due to: %s", reason)
				break
			}
			w.log.Printf("volume ready to modify")
			// Either double or quadruple (if filling fast) the size
			incFactor := 2
			if dur := time.Now().Sub(lastBelowThresholdTime); dur < w.wp.FastThresholdDuration {
				incFactor *= 2
			}
			w.log.Printf("attempting size increase by %dX", incFactor)
			currSize, err := w.vol.GetSize(ctx)
			if err != nil {
				w.log.Error(err)
				break
			}
			newSize := currSize * data.Size(incFactor)
			if err := w.vol.SetSize(ctx, newSize); err != nil {
				w.log.Errorf("failed to change size (%s -> %s): %v", currSize, newSize, err)
				// In case of failure, check size again, instead of being stuck trying repeatedly.
				state = stateWatch
				break
			}
			w.log.Printf("changed volume size %s -> %s", currSize, newSize)
			state = stateResizeFS
		case stateResizeFS:
			w.log.Printf("resizing filesystem")
			if err := w.vol.ResizeFS(); err != nil {
				w.log.Errorf("resize filesystem: %v", err)
				break
			}
			sz, err := w.vol.GetSize(ctx)
			if err != nil {
				w.log.Errorf("cannot get volume size (after resizeFS): %v", err)
			}
			w.log.Printf("successfully increased filesystem size %s -> %s", vSize, sz)
			iterWait = w.wp.WatcherSleepDuration
			state = stateWatch
		}
	}
}
