package ec2cluster

import (
	"context"
	"sync"
	"time"

	"github.com/grailbio/base/sync/once"
)

// capacityFunc must return if there is enough capacity to bring up `depth` number of instances
// of given instance type or an error (if any).  If the error is `errInstanceLimitExceeded` then
// the spot prober will attempt at a smaller depth to see if there's enough capacity at that depth.
// Any other error is considered a valid response (and will be cached by the spot prober).
type capacityFunc func(ctx context.Context, instanceType string, depth int) (bool, error)

// spotProber probes if spot instances of a given instance type are available.
// It internally caches the result of probing (done using capacityFunc),
// ie the depth at which each instance type was deemed to have capacity at the time of a successful probe.
// For subsequent calls, no probing is done unless the number of available instances are exhausted or the ttl expires.
// Errors returned by capacityFunc (other than `errInstanceLimitExceeded`) are treated
// as a valid response meaning no available capacity.
type spotProber struct {
	// capacityFunc is the capacity determining function to use.
	// See godoc of capacityFunc for details.
	capacityFunc capacityFunc
	maxDepth     int
	ttl          time.Duration

	mu         sync.Mutex
	typeProber once.Map
	typeLimit  sync.Map
}

type spotValue struct {
	limit      int
	expiration time.Time
}

// NewSpotProber returns a spot prober which uses the given capacityFunc to determine capacity,
// the given maxProbeDepth as the depth to start probing at and uses the ttl as expiration of
// previously cached probing result.
func NewSpotProber(capacityFunc capacityFunc, maxProbeDepth int, ttl time.Duration) *spotProber {
	if maxProbeDepth == 0 {
		maxProbeDepth = 5 // Use a default minimum if not specified.
	}
	return &spotProber{capacityFunc: capacityFunc, maxDepth: maxProbeDepth, ttl: ttl}
}

// hasProbedCapacity returns whether the internal cache has a probed capacity value for
// the given instance type and whether further probing should be done.
func (p *spotProber) hasProbedCapacity(instanceType string) (hasCapacity, needProbe bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	needProbe = true
	v, ok := p.typeLimit.Load(instanceType)
	if !ok {
		return
	}
	sv := v.(spotValue)
	if time.Now().After(sv.expiration) {
		p.typeLimit.Delete(instanceType)
		p.typeProber.Forget(instanceType)
		return
	}
	needProbe = false
	if sv.limit > 0 {
		hasCapacity = true
		sv.limit = sv.limit - 1
		if sv.limit > 0 {
			p.typeLimit.Store(instanceType, sv)
		} else {
			p.typeLimit.Delete(instanceType)
			p.typeProber.Forget(instanceType)
		}
	}
	return
}

// HasCapacity returns whether there is capacity for the given instanceType at the time
// and an error (if any occurred at the time of probing)
func (p *spotProber) HasCapacity(ctx context.Context, instanceType string) (bool, error) {
	for {
		ok, needProbe := p.hasProbedCapacity(instanceType)
		if !needProbe {
			return ok, nil
		}
		err := p.typeProber.Do(instanceType, func() error {
			var (
				ok    bool
				err   error
				depth = p.maxDepth
			)
			for depth > 0 {
				ok, err = p.capacityFunc(ctx, instanceType, depth)
				if err == nil && ok {
					sv := spotValue{limit: depth, expiration: time.Now().Add(p.ttl)}
					p.typeLimit.Store(instanceType, sv)
					return nil
				}
				if err != nil && err != errInstanceLimitExceeded {
					break
				}
				depth /= 2
			}
			// TODO(swami): Perhaps don't always cache responses with transient errors ?
			sv := spotValue{limit: 0, expiration: time.Now().Add(p.ttl)}
			p.typeLimit.Store(instanceType, sv)
			return err
		})
		if err != nil {
			return false, err
		}
	}
}
