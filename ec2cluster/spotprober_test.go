package ec2cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grailbio/base/traverse"
)

var someErr = fmt.Errorf("some error")

type testCapacityFunc struct {
	mu          sync.Mutex
	calls       map[string]int
	onceAt2Done bool
}

func (f *testCapacityFunc) nCalls(instanceType string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[instanceType]
}

func (f *testCapacityFunc) hasCapacity(ctx context.Context, instanceType string, depth int) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = make(map[string]int)
	}
	f.calls[instanceType] += 1
	switch instanceType {
	case "never":
		return false, nil
	case "always":
		return true, nil
	case "ok_at_5":
		return depth <= 5, nil
	case "ok_at_2":
		return depth <= 2, nil
	case "once_at_2":
		if depth > 2 || f.onceAt2Done {
			return false, nil
		}
		f.onceAt2Done = true
		return true, nil
	case "error_at_2":
		if depth > 2 {
			return false, nil
		}
		return false, someErr
	case "error":
		return false, someErr
	}
	return false, fmt.Errorf("unknown instance type: %s", instanceType)
}

func TestSpotProber_HasCapacity(t *testing.T) {
	ttl := 100 * time.Millisecond
	f := &testCapacityFunc{}
	p := NewSpotProber(f.hasCapacity, 10, ttl)
	for i, tt := range []struct {
		sleep        time.Duration
		instanceType string
		want         bool
		wantE        error
		wantCalls    int
	}{
		{0, "error", false, someErr, 1},
		{0, "error_at_2", false, someErr, 3},
		{0, "never", false, nil, 4},
		{0, "ok_at_2", true, nil, 3},
		{0, "ok_at_5", true, nil, 2},
		{0, "always", true, nil, 1},

		{0, "error", false, nil /* previous result is cached and unexpired */, 1},
		{0, "error_at_2", false, nil /* previous result is cached and unexpired */, 3},

		{0, "never", false, nil, 4},
		{0, "ok_at_2", true, nil, 3},
		{0, "ok_at_5", true, nil, 2},
		{0, "always", true, nil, 1},

		{ttl + 1*time.Millisecond, "error", false, someErr, 2},
		{0, "error_at_2", false, someErr, 6},
		{0, "never", false, nil, 8},
		{0, "ok_at_2", true, nil, 6},
		{0, "ok_at_5", true, nil, 4},
		{0, "always", true, nil, 2},
	} {
		time.Sleep(tt.sleep)
		got, gotE := p.HasCapacity(context.Background(), tt.instanceType)
		if gotE != tt.wantE {
			t.Errorf("[%d] got error %v, want error %v", i, gotE, tt.wantE)
		}
		if got != tt.want {
			t.Errorf("[%d] got %v, want %v", i, got, tt.want)
		}
		if gotCalls := f.nCalls(tt.instanceType); gotCalls != tt.wantCalls {
			t.Errorf("[%d] got %v, want %v", i, gotCalls, tt.wantCalls)
		}
	}
}

func TestSpotProber_HasCapacityConcurrent(t *testing.T) {
	ttl := 100 * time.Millisecond
	f := &testCapacityFunc{}
	p := NewSpotProber(f.hasCapacity, 10, ttl)
	for _, tt := range []struct {
		instanceType          string
		n, wantOKs, wantCalls int
	}{
		{"always", 5, 5, 1},
		{"ok_at_2", 5, 5, 9},
		{"ok_at_5", 10, 10, 4},
		{"once_at_2", 5, 2, 3 /* for a successful probe */ + 4 /* for a failed probe */},
	} {
		var okN int32
		if err := traverse.Each(tt.n, func(i int) error {
			ok, err := p.HasCapacity(context.Background(), tt.instanceType)
			if ok {
				atomic.AddInt32(&okN, 1)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if got, want := int(atomic.LoadInt32(&okN)), tt.wantOKs; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := f.nCalls(tt.instanceType), tt.wantCalls; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
	}
}
