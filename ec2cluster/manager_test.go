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
)

// testConfig implements InstanceSpec to simulate an instance configuration.
type testConfig struct {
	typ   string
	price float64
	r     reflow.Resources
}

// testManagedCluster implements ManagedCluster for testing purposes.
type testManagedCluster struct {
	mu      sync.Mutex
	nextId  int
	configs map[string]testConfig
	live    map[string]ManagedInstance
}

// newCluster creates a new
func newCluster(configs []testConfig) *testManagedCluster {
	rand.Seed(time.Now().Unix())
	m := make(map[string]testConfig, len(configs))
	for _, c := range configs {
		m[c.typ] = c
	}
	return &testManagedCluster{configs: m, live: make(map[string]ManagedInstance)}
}

// addConfig adds another config which can be used for launching instances.
func (c *testManagedCluster) addConfig(tc testConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.configs[tc.typ] = tc
}

// clearLive clears all live instances from the cluster
func (c *testManagedCluster) clearLive() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.live) > 0 {
		c.live = make(map[string]ManagedInstance)
	}
}

func (c *testManagedCluster) Available(need reflow.Resources) (InstanceSpec, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var min testConfig
	for _, config := range c.configs {
		if config.r.Available(need) {
			if min.typ == "" || min.price > config.price {
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
	config := c.configs[spec.Type]
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

func (c *testManagedCluster) Refresh(ctx context.Context) (map[string]bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Simulate a delay in time taken to refresh
	time.Sleep(10 * time.Millisecond)
	m := make(map[string]bool, len(c.live))
	for id := range c.live {
		m[id] = true
	}
	return m, nil
}

func (c *testManagedCluster) Notify(waiting, pending reflow.Resources) { /* do nothing */ }

func TestManagerStart(t *testing.T) {
	var ec2Is []*ec2.Instance
	for _, state := range []string{"terminated", "shutting-down", "running"} {
		i, _ := create("i-"+state, state, "", "")
		ec2Is = append(ec2Is, i)
	}
	dio := &ec2.DescribeInstancesOutput{Reservations: []*ec2.Reservation{{Instances: ec2Is}}}
	c := &Cluster{EC2: &mockEC2Client{output: dio}, stats: newStats(), pools: make(map[string]reflowletPool)}
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
	m := NewManager(c, 2, 2, log.Std)
	m.refreshInterval = 50 * time.Millisecond
	m.Start()
	defer m.Shutdown()

	// req should be met
	<-m.Allocate(ctx, reflow.Requirements{Min: reflow.Resources{"cpu": 1, "mem": 2 * float64(data.GiB)}})

	// req won't be met
	second := m.Allocate(ctx, reflow.Requirements{Min: reflow.Resources{"cpu": 2, "mem": 4 * float64(data.GiB)}})
	assertManager(t, m, 1, 0)
	// add an config which'll satisfy the new requirement
	c.addConfig(testConfig{"type-b", 0.5, reflow.Resources{"cpu": 2, "mem": 6 * float64(data.GiB)}})

	// req should be met
	<-second
	assertManager(t, m, 2, 0)
}

func assertManager(t *testing.T, m *Manager, nPool, nPending int) {
	t.Helper()
	if got, want := m.nPool(), nPool; got != want {
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
	assertManager(t, m, 1, 0)

	req = reflow.Requirements{}
	req.AddParallel(reflow.Resources{"cpu": 1, "mem": 4 * float64(data.GiB)})
	req.AddParallel(reflow.Resources{"cpu": 2, "mem": 2 * float64(data.GiB)})
	req.AddParallel(reflow.Resources{"cpu": 6, "mem": 20 * float64(data.GiB)})

	// req won't be met
	second := m.Allocate(ctx, req)
	assertManager(t, m, 1, 0)
	// add an config which'll satisfy the new requirement
	c.addConfig(testConfig{"type-b", 0.5, reflow.Resources{"cpu": 18, "mem": 60 * float64(data.GiB)}})
	c.mu.Lock()
	delete(c.configs, "type-a")
	c.mu.Unlock()

	<-second
	assertManager(t, m, 2, 0)

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

// TestManagerMultipleAllocsSmall seems to be flaky (See T43494)
// TODO(swami):  Uncomment after addressing flakiness
//func TestManagerMultipleAllocsSmall(t *testing.T) {
//	testCluster(t, newCluster([]testConfig{
//		{"type-a", 0.25, reflow.Resources{"cpu": 1, "mem": 2 * float64(data.GiB)}},
//	}), 20, 4, 2)
//}

func TestManagerMultipleAllocsLarge(t *testing.T) {
	testCluster(t, newCluster([]testConfig{
		{"type-b", 0.5, reflow.Resources{"cpu": 3, "mem": 6 * float64(data.GiB)}},
	}), 300, 20, 5)
}

func TestManagerMultipleAllocsEnormous(t *testing.T) {
	testCluster(t, newCluster([]testConfig{
		{"type-b", 0.5, reflow.Resources{"cpu": 3, "mem": 6 * float64(data.GiB)}},
		{"type-c", 2.5, reflow.Resources{"cpu": 20, "mem": 100 * float64(data.GiB)}},
	}), 2000, 100, 20)
}

func testCluster(t *testing.T, c *testManagedCluster, numAllocs, maxInstances, maxPending int) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	m := NewManager(c, maxInstances, maxPending, log.Std)
	m.refreshInterval = 50 * time.Millisecond
	m.launchTimeout = 500 * time.Millisecond
	m.Start()
	defer m.Shutdown()

	// Goroutine to keep checking if max pending and max live are within limits
	// Also if the manager hits maxInstances, clear the pool to allow for
	// further allocation and clearing through all the requirements.
	go func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		for {
			if np := m.nPending(); np > maxPending {
				t.Errorf("max pending size %d > max %d", np, maxPending)
			}
			if n := m.nPool(); n >= maxInstances {
				if n > maxInstances {
					t.Errorf("pool size %d > max %d", n, maxInstances)
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
				n, np := m.nPool(), m.nPending()
				t.Errorf("not allocated after %dms (npool %d, npending %d)", time.Since(start).Milliseconds(), n, np)
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
