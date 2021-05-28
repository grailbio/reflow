// Copyright 2020 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package ec2cluster

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/log"
	"golang.org/x/time/rate"
)

// testConfig implements InstanceSpec to simulate an instance configuration.
type testConfig struct {
	typ   string
	price float64
	r     reflow.Resources
}

// testManagedCluster implements ManagedCluster for testing purposes.
type testManagedCluster struct {
	cheapestInstancePriceUSD float64

	mu            sync.Mutex
	nextId        int
	activeConfigs map[string]testConfig
	allConfigs    map[string]testConfig
	live          map[string]ManagedInstance
}

// newCluster creates a new testManagedCluster
func newCluster(configs []testConfig) *testManagedCluster {
	rand.Seed(time.Now().Unix())
	mActive, mAll := make(map[string]testConfig, len(configs)), make(map[string]testConfig, len(configs))
	for _, c := range configs {
		mActive[c.typ] = c
		mAll[c.typ] = c
	}
	cluster := &testManagedCluster{activeConfigs: mActive, allConfigs: mAll, live: make(map[string]ManagedInstance)}
	cluster.resetCheapest()
	return cluster
}

// resetCheapest resets the cheapest price.  Must be called while 'c.mu' is held.
func (c *testManagedCluster) resetCheapest() {
	cheapest := 1000.0
	for _, c := range c.activeConfigs {
		if c.price < cheapest {
			cheapest = c.price
		}
	}
	c.cheapestInstancePriceUSD = cheapest
}

// addConfig adds another config which can be used for launching instances.
func (c *testManagedCluster) addConfig(tc testConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.activeConfigs[tc.typ] = tc
	c.allConfigs[tc.typ] = tc
	c.resetCheapest()
}

// deleteConfig marks the given testConfig as inactive. It will no longer be used to launch instances
// but is retained for cost accounting.
func (c *testManagedCluster) deleteConfig(typ string) {
	delete(c.activeConfigs, typ)
	c.resetCheapest()
}

// clearLive clears all live instances from the cluster
func (c *testManagedCluster) clearLive() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.live) > 0 {
		c.live = make(map[string]ManagedInstance)
	}
}

func (c *testManagedCluster) Available(need reflow.Resources, maxPrice float64) (InstanceSpec, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var min testConfig
	for _, config := range c.activeConfigs {
		if config.r.Available(need) {
			if (min.typ == "" || min.price > config.price) && config.price <= maxPrice {
				min = config
			}
		}
	}
	if min.typ == "" {
		return InstanceSpec{}, false
	}
	return InstanceSpec{min.typ, min.r}, true
}

func (c *testManagedCluster) Launch(ctx context.Context, spec InstanceSpec) ManagedInstance {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 5% of the time, simulate an inability to launch
	if rand.Intn(100) < 5 {
		return spec.Instance("")
	}
	config := c.activeConfigs[spec.Type]
	iid := fmt.Sprintf("instance-%s-%d", config.typ, c.nextId)
	c.nextId++
	i := spec.Instance(iid)

	// 5% of the time, simulate a launched instance which never gets recognized
	// when the cluster state is 'Refresh'ed.
	if rand.Intn(100) >= 5 {
		c.live[iid] = i
	}
	// Simulate a small delay in launching an instance
	time.Sleep(5 * time.Millisecond)
	return i
}

func (c *testManagedCluster) Refresh(ctx context.Context) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Simulate a delay in time taken to refresh
	time.Sleep(10 * time.Millisecond)
	m := make(map[string]string, len(c.live))
	for id, inst := range c.live {
		m[id] = inst.Type
	}
	return m, nil
}

func (c *testManagedCluster) Notify(waiting, pending reflow.Resources) { /* do nothing */ }

func (c *testManagedCluster) InstancePriceUSD(typ string) float64 {
	return c.allConfigs[typ].price
}

func (c *testManagedCluster) CheapestInstancePriceUSD() float64 {
	return c.cheapestInstancePriceUSD
}

func TestManagerStart(t *testing.T) {
	var ec2Is []*ec2.Instance
	for _, state := range []string{"terminated", "shutting-down", "running"} {
		i, _ := create("i-"+state, state, "", "")
		ec2Is = append(ec2Is, i)
	}
	dio := &ec2.DescribeInstancesOutput{Reservations: []*ec2.Reservation{{Instances: ec2Is}}}
	var configs []instanceConfig
	for _, config := range instanceTypes {
		configs = append(configs, config)
	}
	c := &Cluster{
		EC2:            &mockEC2Client{output: dio},
		stats:          newStats(),
		pools:          make(map[string]reflowletPool),
		instanceState:  newInstanceState(configs, time.Minute, "us-west-2"),
		refreshLimiter: rate.NewLimiter(rate.Every(time.Millisecond), 1),
	}
	m := &Manager{cluster: c, refreshInterval: time.Millisecond}
	m.Start()
	defer m.Shutdown()
	checkState(t, c, "i-running")
}

func TestManagerBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	c := newCluster([]testConfig{
		{"type-a", 0.25, reflow.Resources{"cpu": 2, "mem": 3 * float64(data.GiB)}},
	})
	m := NewManager(c, 250, 5, log.Std)
	m.refreshInterval = 50 * time.Millisecond
	m.Start()
	defer m.Shutdown()

	// req should be met
	<-m.Allocate(ctx, reflow.Requirements{Min: reflow.Resources{"cpu": 1, "mem": 2 * float64(data.GiB)}})

	// req won't be met
	second := m.Allocate(ctx, reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 4 * float64(data.GiB)}})
	assertManager(t, m, 0.25, 0)
	// add an config which'll satisfy the new requirement
	c.addConfig(testConfig{"type-b", 0.5, reflow.Resources{"cpu": 2, "mem": 6 * float64(data.GiB)}})

	// req should be met
	<-second
	assertManager(t, m, 0.75, 0)
}

func assertManager(t *testing.T, m *Manager, hCost float64, nPending int) {
	t.Helper()
	if got, want := m.hourlyCostUSD(), hCost; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := m.nPending(), nPending; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestManagerBasicWide(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := newCluster([]testConfig{
		{"type-a", 0.25, reflow.Resources{"cpu": 2, "mem": 4 * float64(data.GiB)}},
	})
	m := NewManager(c, 5, 5, log.Std)
	m.refreshInterval = 50 * time.Millisecond
	m.launchTimeout = 500 * time.Millisecond
	m.Start()
	defer m.Shutdown()

	// req should be met
	var req reflow.Requirements
	req.AddParallel(reflow.Resources{"cpu": 1, "mem": 2 * float64(data.GiB)})
	req.AddParallel(reflow.Resources{"cpu": 1, "mem": 2 * float64(data.GiB)})
	<-m.Allocate(ctx, req)
	assertManager(t, m, 0.25, 0)

	req = reflow.Requirements{}
	req.AddParallel(reflow.Resources{"cpu": 1, "mem": 4 * float64(data.GiB)})
	req.AddParallel(reflow.Resources{"cpu": 2, "mem": 2 * float64(data.GiB)})
	req.AddParallel(reflow.Resources{"cpu": 6, "mem": 20 * float64(data.GiB)})

	// req won't be met
	second := m.Allocate(ctx, req)
	assertManager(t, m, 0.25, 0)
	// add an config which'll satisfy the new requirement
	c.addConfig(testConfig{"type-b", 0.5, reflow.Resources{"cpu": 18, "mem": 60 * float64(data.GiB)}})
	c.mu.Lock()
	c.deleteConfig("type-a")
	c.mu.Unlock()

	<-second
	assertManager(t, m, 0.75, 0)

	counts := make(map[string]int)
	for _, inst := range c.live {
		counts[inst.Type] = counts[inst.Type] + 1
	}
	if got, want := counts["type-a"], 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := counts["type-b"], 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestManagerMultipleAllocsSmall(t *testing.T) {
	testCluster(t, newCluster([]testConfig{
		{"type-a", 0.25, reflow.Resources{"cpu": 1, "mem": 2 * float64(data.GiB)}},
	}), 100, 2, 20)
}

func TestManagerMultipleAllocsLarge(t *testing.T) {
	testCluster(t, newCluster([]testConfig{
		{"type-b", 0.5, reflow.Resources{"cpu": 3, "mem": 6 * float64(data.GiB)}},
	}), 500, 2, 300)
}

func TestManagerMultipleAllocsExpensive(t *testing.T) {
	testCluster(t, newCluster([]testConfig{
		{"type-a", 0.25, reflow.Resources{"cpu": 1, "mem": 2 * float64(data.GiB)}},
	}), 2, 5, 10)
}

func TestManagerMultipleAllocsEnormous(t *testing.T) {
	testCluster(t, newCluster([]testConfig{
		{"type-b", 0.5, reflow.Resources{"cpu": 3, "mem": 6 * float64(data.GiB)}},
		{"type-c", 2.5, reflow.Resources{"cpu": 20, "mem": 100 * float64(data.GiB)}},
	}), 200, 20, 2000)
}

func testCluster(t *testing.T, c *testManagedCluster, maxHourlyCostUSD float64, maxPendingInstances int, numAllocs int) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	m := NewManager(c, maxHourlyCostUSD, maxPendingInstances, log.Std)
	m.refreshInterval = 50 * time.Millisecond
	m.launchTimeout = 500 * time.Millisecond
	m.drainTimeout = 0 // time.Millisecond
	m.Start()
	defer m.Shutdown()

	maxInstanceCost := -1.0
	for _, config := range c.activeConfigs {
		if config.price > maxInstanceCost {
			maxInstanceCost = config.price
		}
	}
	if maxInstanceCost == -1.0 {
		t.Fatal("failed to update maxInstanceCost")
	}

	// Goroutine to keep checking if max pending and max live are within limits
	// Also if the manager hits maxHourlyCost, clear the pool to allow for
	// further allocation and clearing through all the requirements.
	go func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()

		for {
			if np := m.nPending(); np > maxPendingInstances {
				t.Errorf("max pending size %d > max %d", np, maxPendingInstances)
			}
			if hCost := m.hourlyCostUSD(); hCost+maxInstanceCost >= maxHourlyCostUSD {
				if hCost > maxHourlyCostUSD {
					t.Errorf("pool cost %0.2f > max %0.2f", hCost, maxHourlyCostUSD)
				}
				if m.nPending() == 0 {
					// Clear the pool to make further progress
					c.clearLive()
					// Force a sync so that cluster manager picks up that the instances
					// are all gone immediately.
					m.forceSync()
				}
			}
			select {
			case <-tick.C:
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
	var wg sync.WaitGroup
	var numDone int32
	for i := 0; i < numAllocs; i++ {
		// request an allocation
		w := m.Allocate(ctx, reflow.Requirements{Min: reflow.Resources{"cpu": 1, "mem": 2 * float64(data.GiB)}})
		wg.Add(1)
		// trigger a goroutine to wait for it
		go func() {
			defer wg.Done()
			start := time.Now()
			select {
			case <-ctx.Done():
				n, np, hCost := m.nPool(), m.nPending(), m.hourlyCostUSD()
				t.Errorf("not allocated after %dms (npool %d, npending %d, hourlycost %0.2f)", time.Since(start).Milliseconds(), n, np, hCost)
			case <-w:
				atomic.AddInt32(&numDone, 1)
			}
		}()
	}
	wg.Wait()
	if got, want := int(atomic.LoadInt32(&numDone)), numAllocs; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
