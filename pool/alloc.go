// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package pool

import (
	"context"
	"fmt"
	"time"

	"github.com/grailbio/base/retry"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
)

const (
	keepaliveInterval    = 2 * time.Minute
	keepaliveTimeout     = 10 * time.Second
	keepaliveMaxInterval = 5 * time.Minute
	keepaliveTries       = 5
	ivOffset             = 30 * time.Second
)

// KeepaliveRetryInitialWaitInterval is the initial duration to wait before
// retrying if a keepalive attempt fails on an alloc (with a retryable failure)
var KeepaliveRetryInitialWaitInterval = 2 * time.Second

// Non-fatal keepalive failures will be retried using this policy. The policy is
// configured such that the last retry will occur within the policy's max duration.
// With a=KeepaliveRetryInitialWaitInterval, b=backoffFactor (1.5), n=keepaliveTries,
// ivOffset should be such that: sum_{i=0 .. n-1} a*b^i < ivOffset
var keepaliveRetryPolicy retry.Policy = retry.MaxTries(retry.Backoff(KeepaliveRetryInitialWaitInterval, ivOffset, 1.5), keepaliveTries)

// Alloc represent a resource allocation attached to a single
// executor, a reservation of resources on a single node.
type Alloc interface {
	reflow.Executor

	// Pool returns the pool from which the alloc is reserved.
	Pool() Pool

	// ID returns the ID of alloc in the pool. The format of the ID is opaque.
	ID() string

	// Keepalive maintains the lease of this Alloc. It must be called again
	// before the expiration of the returned duration. The user may also
	// request a maintenance interval. This is just a hint and may not be
	// respected by the Alloc.
	Keepalive(ctx context.Context, interval time.Duration) (time.Duration, error)

	// Inspect returns Alloc metadata.
	Inspect(ctx context.Context) (AllocInspect, error)

	// Free frees the alloc. Pending tasks are killed but its Repository
	// is not collected. Some implementations may implement "zombie"
	// allocs so that they can be inspected after Free is called.
	Free(ctx context.Context) error
}

// Labels represents a set of metadata labels for a run.
type Labels map[string]string

// Add returns a copy of Labels l with an added key and value.
func (l Labels) Add(k, v string) Labels {
	m := l.Copy()
	m[k] = v
	return m
}

// Copy returns a copy of l.
func (l Labels) Copy() Labels {
	m := make(Labels)
	for k, v := range l {
		m[k] = v
	}
	return m
}

// AllocMeta contains Alloc requester metadata.
type AllocMeta struct {
	Want   reflow.Resources
	Owner  string
	Labels Labels
}

// AllocInspect contains Alloc metadata.
type AllocInspect struct {
	ID            string
	Resources     reflow.Resources
	Meta          AllocMeta
	Created       time.Time
	LastKeepalive time.Time
	Expires       time.Time
}

// keepalive returns the interval to the next keepalive.
func keepalive(ctx context.Context, alloc Alloc) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, keepaliveTimeout)
	defer cancel()
	return alloc.Keepalive(ctx, keepaliveInterval)
}

// Keepalive maintains the lease on alloc until it expires (e.g., by
// calling Free), or until the passed-in context is cancelled.
// Keepalive retries errors by exponential backoffs with a fixed
// configuration.
func Keepalive(ctx context.Context, log *log.Logger, alloc Alloc) error {
	log = log.Tee(nil, fmt.Sprintf("keepalive %s: ", alloc.ID()))
	for {
		var (
			iv   time.Duration
			err  error
			last time.Time
		)
		for retries := 0; ; retries++ {
			if !last.IsZero() && time.Since(last) > iv {
				log.Errorf("failed to maintain keepalive within interval %s", iv)
			}
			iv, err = keepalive(ctx, alloc)
			if err == nil || errors.Is(errors.Fatal, err) {
				break
			}
			// Context errors indicate that our caller has given up.
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			// We blindly retry other (non-Fatal) errors.
			log.Errorf("try %d/%d failed to maintain keepalive: %s", retries, keepaliveTries, err)
			if rerr := retry.Wait(ctx, keepaliveRetryPolicy, retries); rerr != nil {
				return rerr
			}
		}
		if err != nil {
			return err
		}
		last = time.Now()
		// Renew the keepalive a little bit before the last one expires.
		iv -= ivOffset
		if iv < 0*time.Second {
			continue
		}
		if iv > keepaliveMaxInterval {
			iv = keepaliveMaxInterval
		}
		select {
		case <-time.After(iv):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
