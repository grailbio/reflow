package local

import (
	"context"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/pool"
	"github.com/grailbio/reflow/taskdb"
)

type mockTaskDB struct {
	taskdb.TaskDB
	rid      digest.Digest
	r        reflow.Resources
	kaid     digest.Digest
	kaN      int
	endId    digest.Digest
	endTimeN int
}

func (db *mockTaskDB) SetResources(ctx context.Context, id digest.Digest, resources reflow.Resources) error {
	db.rid = id
	db.r = resources
	return nil
}

func (db *mockTaskDB) KeepIDAlive(ctx context.Context, id digest.Digest, keepalive time.Time) error {
	db.kaid = id
	db.kaN += 1
	return nil
}

func (db *mockTaskDB) SetEndTime(ctx context.Context, id digest.Digest, end time.Time) (err error) {
	db.endId = id
	db.endTimeN += 1
	return
}

func TestMaintainTaskDBRow(t *testing.T) {
	tdb := &mockTaskDB{}
	r := reflow.Resources{"mem": 10, "cpu": 1, "disk": 20}
	id := reflow.NewStringDigest("testpool")
	p := Pool{ResourcePool: pool.NewResourcePool(nil, nil), TaskDBPoolId: id, TaskDB: tdb}
	p.Init(r, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	done := make(chan struct{})
	go func() {
		p.MaintainTaskDBRow(ctx)
		done <- struct{}{}
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-done
	if got, want := tdb.r, r; !got.Equal(want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tdb.kaN, 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tdb.endTimeN, 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tdb.rid, id.Digest(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tdb.kaid, id.Digest(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := tdb.endId, id.Digest(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
