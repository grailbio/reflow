package local

import (
	"context"
	"testing"
	"time"

	"github.com/grailbio/reflow/log"
)

func newFakeAlloc(p *Pool, id string, ka time.Duration) *alloc {
	a := &alloc{
		Executor: &Executor{Log: log.Std},
		id:       id,
		p:        p,
		created:  time.Now(),
		expires:  time.Now().Add(ka),
	}
	p.allocs[id] = a
	return a
}

func TestStopIfIdleFor(t *testing.T) {
	if stopped, _ := (&Pool{}).StopIfIdleFor(time.Second); !stopped {
		t.Fatal("idle pool must be stopped")
	}
	p := &Pool{Log: log.Std}
	p.allocs = map[string]*alloc{}
	a1 := newFakeAlloc(p, "alloc1", 100*time.Millisecond)
	stopped, _ := p.StopIfIdleFor(time.Second)
	if stopped {
		t.Fatal("busy pool must not be stopped")
	}
	_, _ = a1.Keepalive(context.Background(), time.Minute)
	stopped, tte := p.StopIfIdleFor(time.Second)
	if stopped {
		t.Fatal("busy pool must not be stopped")
	}
	if tte > 60*time.Second || tte < 59*time.Second {
		t.Fatalf("got %s, want ~1min", tte)
	}
	a2 := newFakeAlloc(p, "alloc2", 100*time.Millisecond)
	_, _ = a2.Keepalive(context.Background(), 5*time.Minute)
	stopped, tte = p.StopIfIdleFor(time.Second)
	if stopped {
		t.Fatal("busy pool must not be stopped")
	}
	if tte > 300*time.Second || tte < 299*time.Second {
		t.Fatalf("got %s, want ~5min", tte)
	}
	p.allocs = map[string]*alloc{} // clear all allocs
	if stopped, _ := p.StopIfIdleFor(time.Second); !stopped {
		t.Fatal("idle pool must be stopped")
	}
}
