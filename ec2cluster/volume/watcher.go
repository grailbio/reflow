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

// NewWatcher creates a new watcher.
func NewWatcher(v Volume, w infra.VolumeWatcher, log *log.Logger) (*Watcher, error) {
	if _, err := v.FSSize(); err != nil {
		return nil, err
	}
	if _, err := v.FSUsage(); err != nil {
		return nil, err
	}
	return &Watcher{v: v, w: w, log: log}, nil
}

// Watch watches the underlying volume and resizes it whenever necessary.
// Watch will not return until the provided context is done.
func (w *Watcher) Watch(ctx context.Context) {
	type stateT int
	const (
		// Watch the volume.
		stateWatch stateT = iota
		// Resize the EBS volumes.
		stateResizeEBSVolumes
		// Resize the filesystem.
		stateResizeFS
	)

	var (
		state                  stateT
		lastBelowThresholdTime = time.Now()
		iter                   = time.NewTicker(w.w.WatcherSleepDuration)
	)

	fsSize, err := w.v.FSSize()
	if err != nil {
		w.log.Error(err)
	}
	fsUsage, err := w.v.FSUsage()
	if err != nil {
		w.log.Error(err)
	}
	w.log.Printf("started watching volume backed by %s (size: %s, used: %.2f%%)", w.v.EBSIds(), fsSize, fsUsage)

	for {
		select {
		case <-ctx.Done():
			w.log.Debugf("exiting")
			return
		case <-iter.C:
		}

		switch state {
		case stateWatch:
			fsUsage, err = w.v.FSUsage()
			if err != nil {
				w.log.Error(err)
				break
			}
			prefix := fmt.Sprintf("watching volume (size: %s, used: %.2f%%)", fsSize, fsUsage)
			if fsUsage < w.w.LowThresholdPct {
				lastBelowThresholdTime = time.Now()
				w.log.Printf("%s: below low threshold (%.2f%% < %.2f%%)", prefix, fsUsage, w.w.LowThresholdPct)
				break
			}
			if fsUsage < w.w.HighThresholdPct {
				ago := time.Since(lastBelowThresholdTime).Round(time.Second)
				w.log.Printf("%s: below high threshold (%.2f%% < %.2f%%) and was below low threshold %s ago",
					prefix, fsUsage, w.w.HighThresholdPct, ago)
				break
			}
			// Above high threshold
			ago := time.Since(lastBelowThresholdTime).Round(time.Second)
			w.log.Printf("%s: above high threshold (%.2f%% > %.2f%%) and was below low threshold %s ago",
				prefix, fsUsage, w.w.HighThresholdPct, ago)
			// Now we are going to resize, so reduce the wait duration
			// so that we keep re-attempting quickly in case of failures.
			iter.Stop()
			iter = time.NewTicker(w.w.ResizeSleepDuration)
			state = stateResizeEBSVolumes
		case stateResizeEBSVolumes:
			w.log.Printf("resizing volume")
			// Determine factor by which to increase disk size based on filesystem (not EBS) size. This will handle
			// partial failures gracefully (e.g. if the previous resize failed only for a subset of volumes).
			incFactor := w.w.SlowIncreaseFactor
			if dur := time.Now().Sub(lastBelowThresholdTime); dur < w.w.FastThresholdDuration {
				incFactor = w.w.FastIncreaseFactor
			}
			newSize := fsSize * data.Size(incFactor)
			w.log.Printf("attempting size increase by %dX", incFactor)

			// Resize EBS volumes.
			ebsSize, err := w.v.EBSSize(ctx)
			if err != nil {
				w.log.Errorf("failed to get size of ebs volumes before resize: %s", err)
				break
			}
			if err := w.v.ResizeEBS(ctx, newSize); err != nil {
				w.log.Errorf("failed to change ebs volumes size (%s -> %s): %v", ebsSize, newSize, err)
				// In case of failure, check size again, instead of being stuck trying repeatedly.
				// Do not attempt to resize the filesystem even if a subset of volumes resized successfully. In RAID0,
				// the amount of space used in each volume is limited to the size of the smallest volume.
				state = stateWatch
				break
			}
			newEbsSize, err := w.v.EBSSize(ctx)
			if err != nil {
				w.log.Errorf("failed to get size of ebs volumes after resize, will continue to resize FS: %v", err)
			} else {
				w.log.Printf("changed ebs volumes size %s -> %s", ebsSize, newEbsSize)
			}
			state = stateResizeFS
		case stateResizeFS:
			w.log.Printf("resizing filesystem")
			if err := w.v.ResizeFS(); err != nil {
				w.log.Errorf("resize filesystem: %v", err)
				break
			}
			newFSSize, err := w.v.FSSize()
			if err != nil {
				w.log.Errorf("failed to get filesystem size after resize: %v", err)
			}
			w.log.Printf("successfully increased filesystem size %s -> %s", fsSize, newFSSize)
			iter.Stop()
			iter = time.NewTicker(w.w.WatcherSleepDuration)
			fsSize = newFSSize
			state = stateWatch
		}
	}
}
