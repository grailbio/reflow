// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/assoc"
	"github.com/grailbio/reflow/liveset/bloomlive"
	"github.com/willf/bloom"
)

// If the value does not exist in repository, an error is returned.
func unmarshal(ctx context.Context, repo reflow.Repository, k digest.Digest, v interface{}) error {
	rc, err := repo.Get(ctx, k)
	if err != nil {
		return err
	}
	defer rc.Close()
	return json.NewDecoder(rc).Decode(v)
}

func (c *Cmd) collect(ctx context.Context, args ...string) {
	flags := flag.NewFlagSet("collect", flag.ExitOnError)
	thresholdFlag := flags.String("threshold", "", "cache entries older than this threshold will be collected")
	dryRunFlag := flags.Bool("dry-run", true, "when true, reports on what would have been collected without actually removing anything from the cache")
	help := `Collect performs garbage collection of the reflow cache,
	removing entries that have not been accessed more recently than the
	provided threshold date.`

	c.Parse(flags, args, help, "collect [-threshold date]")
	threshold, err := time.Parse("2006-01-02", *thresholdFlag)
	if err != nil {
		flags.Usage()
	}

	a, err := c.Config.Assoc()
	if err != nil {
		c.Fatal(err)
	}
	r, err := c.Config.Repository()
	if err != nil {
		c.Fatal(err)
	}

	// Use an estimate of the item count in the assoc to create our bloom filters
	count, err := a.Count(ctx)
	c.Log.Debugf("Finding liveset for cache with %d associations and threshold: %v", count, threshold)

	keyFilter := bloom.NewWithEstimates(uint(count), .000001)
	valueFilter := bloom.NewWithEstimates(uint(count)*10, .000001)

	// The mapping handler will be call from multiple threads
	var resultsLock sync.Mutex
	itemsScannedCount := int64(0)
	liveItemCount := int64(0)
	liveObjectsInFilesets := int64(0)

	start := time.Now()
	err = a.Scan(ctx, assoc.MappingHandlerFunc(func(k, v digest.Digest, lastAccessTime time.Time) {
		var s reflow.Fileset
		live := lastAccessTime.After(threshold)
		if live {
			err = unmarshal(ctx, r, v, &s)
			if err != nil {
				// If we can't build up the liveset we want to bail now
				c.Fatal(fmt.Errorf("error parsing fileset %v (%v)", k, err))
			}
		}
		// The repository checking happens outside the results lock for better performance
		resultsLock.Lock()
		defer resultsLock.Unlock()
		if live {
			for _, f := range s.Files() {
				liveObjectsInFilesets++
				valueFilter.Add(f.ID.Bytes())
			}
			keyFilter.Add(k.Bytes())
			valueFilter.Add(v.Bytes())
			liveItemCount++
		}
		itemsScannedCount++
		if itemsScannedCount%10000 == 0 {
			c.Log.Debugf("Scanned item %d in association", itemsScannedCount)
		}
	}))
	// Bail if anything went wrong since we're about to garbage collect based on these livesets
	if err != nil {
		c.Fatal(err)
	}

	// Some debugging information
	c.Log.Debugf("Time to scan associations %s", time.Since(start))
	c.Log.Printf("Scanned %d associations, found %d live associations and %d live objects",
		itemsScannedCount, liveItemCount, liveObjectsInFilesets)

	// Garbage collect the repository using the values liveset
	if err = r.CollectWithThreshold(ctx, bloomlive.New(valueFilter), threshold, *dryRunFlag); err != nil {
		c.Fatal(err)
	}

	// Garbage collect the association using the keys liveset
	if err = a.CollectWithThreshold(ctx, bloomlive.New(keyFilter), threshold, *dryRunFlag); err != nil {
		c.Fatal(err)
	}
}
