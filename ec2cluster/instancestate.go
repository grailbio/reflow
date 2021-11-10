// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	sa "github.com/grailbio/base/cloud/spotadvisor"
	"github.com/grailbio/base/sync/once"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/ec2cluster/instances"
)

var (
	instanceTypes     = map[string]instanceConfig{}
	allInstancesState *instanceState
	initOnce          once.Task
)

func init() {
	for _, typ := range instances.Types {
		instanceTypes[typ.Name] = instanceConfig{
			Type:          typ.Name,
			EBSOptimized:  typ.EBSOptimized,
			EBSThroughput: typ.EBSThroughput,
			Price:         typ.Price,
			Resources: reflow.Resources{
				"cpu": float64(typ.VCPU),
				// We don't set the memory here, because we will do it later based on verification status.
			},
			// According to Amazon, "t2" instances are the only current-generation
			// instances not supported by spot.
			SpotOk: typ.Generation == "current" && !strings.HasPrefix(typ.Name, "t2."),
			NVMe:   typ.NVMe,
		}
		for key, ok := range typ.CPUFeatures {
			if !ok {
				continue
			}
			// Allocate one feature per VCPU.
			instanceTypes[typ.Name].Resources[key] = float64(typ.VCPU)
		}
	}

	if err := initOnce.Do(func() error {
		var configs []instanceConfig
		for _, config := range instanceTypes {
			configs = append(configs, config)
		}
		if len(configs) == 0 {
			return fmt.Errorf("no configured instance types")
		}
		allInstancesState = newInstanceState(configs, time.Millisecond, "us-west-2", nil)
		return nil
	}); err != nil {
		panic(err)
	}
}

// advisor is an interface for interacting with EC2 instance data. Currently, it
// is only implemented by spotadvisor.SpotAdvisor.
type advisor interface {
	GetMaxInterruptProbability(sa.OsType, sa.AwsRegion, sa.InstanceType) (sa.InterruptProbability, error)
}

// instanceState stores everything we know about EC2 instances,
// and implements instance type selection according to runtime
// criteria.
type instanceState struct {
	configs   []instanceConfig
	sleepTime time.Duration
	region    string
	// cheapestIndex points to the index in 'configs' of the cheapest instance config.
	cheapestIndex int
	// advisor is optional, if provided it will be used to help determine available instances.
	advisor advisor

	mu          sync.Mutex
	unavailable map[string]time.Time
}

func newInstanceState(configs []instanceConfig, sleep time.Duration, region string, adv advisor) *instanceState {
	s := &instanceState{
		configs:     make([]instanceConfig, len(configs)),
		unavailable: make(map[string]time.Time),
		sleepTime:   sleep,
		region:      region,
		advisor:     adv,
	}
	copy(s.configs, configs)
	sort.Slice(s.configs, func(i, j int) bool {
		return s.configs[j].Resources.ScaledDistance(nil) < s.configs[i].Resources.ScaledDistance(nil)
	})
	for i, cfg := range s.configs {
		cheapestCfg := s.configs[s.cheapestIndex]
		if price, cheapest := cfg.Price[region], cheapestCfg.Price[region]; price < cheapest {
			s.cheapestIndex = i
		}
		// Update the resources based on memory sampled during verification
		verifiedStatus := instances.VerifiedByRegion[region][cfg.Type]
		cfg.Resources["mem"] = float64(verifiedStatus.ExpectedMemoryBytes())
	}
	return s
}

// Unavailable marks the given instance config as busy.
func (s *instanceState) Unavailable(config instanceConfig) {
	s.mu.Lock()
	s.unavailable[config.Type] = time.Now()
	s.mu.Unlock()
}

// Available tells whether the provided resources are potentially
// available as an EC2 instance.
func (s *instanceState) Available(need reflow.Resources) bool {
	for _, config := range s.configs {
		if config.Resources.Available(need) {
			return true
		}
	}
	return false
}

// Largest returns the "largest" instance type from the current configuration.
func (s *instanceState) Largest() instanceConfig {
	return s.configs[0]
}

// Cheapest returns the "cheapest" instance type from the current configuration.
func (s *instanceState) Cheapest() instanceConfig {
	return s.configs[s.cheapestIndex]
}

// By default, we try to select spot instances with < 10% probability of being
// interrupted, but we will resort to a higher threshold if necessary. An
// analysis of the cost to fulfill various resource requirements at different
// interrupt probability thresholds showed a clear pattern of diminishing returns
// for thresholds higher than 10%. There are very few cases where we can get a
// better price by having a higher threshold, and in those cases we would have to
// go to a significantly higher threshold to see the cost benefit. If we were to
// use a 5% threshold, we would be paying about 20% more than at the 10%
// thresold, but would only see a marginal improvement in interrupt probability.
// So, 10% was chosen as the default. More details can be found in SYSINFRA-621.
const desiredInterruptProb = sa.LessThanTenPct

// MaxAvailable returns the "largest" instance type that has at least the
// required resources and is also believed to be currently available. Spot
// restricts instances to those that may be launched via EC2 spot market and
// tries to minimize interrupt probability. MaxAvailable uses
// (Resources).ScoredDistance to determine the largest instance type.
func (s *instanceState) MaxAvailable(need reflow.Resources, spot bool) (instanceConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		best     instanceConfig
		distance = -math.MaxFloat64
		found    bool
	)
	for prob := desiredInterruptProb; prob <= sa.Any; prob++ {
		for _, config := range s.configs {
			if time.Since(s.unavailable[config.Type]) < s.sleepTime || (spot && !config.SpotOk) {
				continue
			}
			if !config.Resources.Available(need) {
				continue
			}
			if spot && !s.withinThreshold(config, prob) {
				continue
			}
			if d := config.Resources.ScaledDistance(need); d > distance {
				distance = d
				best = config
				found = true
			}
		}
		if found {
			break
		}
	}
	return best, best.Resources.Available(need)
}

// MinAvailable returns the cheapest instance type that has at least the required
// resources, is believed to be currently available and is less expensive than
// maxPrice. Spot restricts instances to those that may be launched via EC2 spot
// market and tries to minimize interrupt probability.
func (s *instanceState) MinAvailable(need reflow.Resources, spot bool, maxPrice float64) (instanceConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var (
		price     float64
		best      instanceConfig
		bestPrice = math.MaxFloat64
		found, ok bool
		viable    []instanceConfig
	)
	for prob := desiredInterruptProb; prob <= sa.Any; prob++ {
		viable = []instanceConfig{}
		for _, config := range s.configs {
			if time.Since(s.unavailable[config.Type]) < s.sleepTime || (spot && !config.SpotOk) {
				continue
			}
			if !config.Resources.Available(need) {
				continue
			}
			if price, ok = config.Price[s.region]; !ok {
				continue
			}
			if price > maxPrice {
				continue
			}
			if spot && !s.withinThreshold(config, prob) {
				continue
			}
			viable = append(viable, config)
			if price < bestPrice {
				bestPrice = price
				best = config
			}
		}
		if len(viable) > 0 {
			break
		}
	}

	// Sort by instance type name, since some instance types have the exact same cost.
	sort.Slice(viable, func(i, j int) bool { return viable[i].Type < viable[j].Type })

	// Choose a higher cost but better EBS throughput instance type if applicable.
	for _, config := range viable {
		price = config.Price[s.region]
		// Prefer a reasonably more expensive one with higher EBS throughput
		if !found &&
			(price < bestPrice+ebsThroughputPremiumCost ||
				price < bestPrice*(1.0+ebsThroughputPremiumPct/100)) &&
			config.EBSThroughput > best.EBSThroughput*(1.0+ebsThroughputBenefitPct/100) {
			bestPrice = price
			best = config
			found = true
		}
		// Prefer a cheaper one with same EBS throughput.
		if found && price < bestPrice && config.EBSThroughput >= best.EBSThroughput {
			bestPrice = price
			best = config
		}
	}
	return best, best.Resources.Available(need)
}

func (s *instanceState) Type(typ string) (instanceConfig, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Since(s.unavailable[typ]) < s.sleepTime {
		return instanceConfig{}, false
	}
	for _, config := range s.configs {
		if config.Type == typ {
			return config, true
		}
	}
	return instanceConfig{}, false
}

func (s *instanceState) withinThreshold(config instanceConfig, desiredProb sa.InterruptProbability) bool {
	if s.advisor != nil {
		prob, err := s.advisor.GetMaxInterruptProbability(sa.Linux, sa.AwsRegion(s.region), sa.InstanceType(config.Type))
		if err == nil && prob > desiredProb {
			return false
		}
	}
	return true
}

// InstanceType returns the instance type (and the amount of resources it provides)
// which is most appropriate for the needed resources.
// `spot` determines whether we should consider instance types that are available
// as spot instances or not.
func InstanceType(need reflow.Resources, spot bool, maxPrice float64) (string, reflow.Resources) {
	config, _ := allInstancesState.MinAvailable(need, spot, maxPrice)
	return config.Type, config.Resources
}

// OnDemandPrice returns the on-demand hourly price of the given instance type in the given region.
func OnDemandPrice(typ, region string) (hourlyPriceUsd float64) {
	config, _ := allInstancesState.Type(typ)
	return config.Price[region]
}
