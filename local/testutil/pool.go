// Package testutil provides utilities for testing code that involves pools.
package testutil

import (
	"context"
	"io/ioutil"
	"reflect"
	"testing"
	"time"

	dockerclient "github.com/docker/engine-api/client"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/local"
	"github.com/grailbio/reflow/pool"
	"grail.com/testutil"
)

const bashImage = "yikaus/alpine-bash"

// NewDockerClientOrSkip returns a local Docker client. The test is
// marked as skipped if there is no local Docker instance.
func NewDockerClientOrSkip(t *testing.T) *dockerclient.Client {
	client, err := dockerclient.NewClient(
		"unix:///var/run/docker.sock", "1.22", /*client.DefaultVersion*/
		nil, map[string]string{"user-agent": "reflow"})
	if err != nil {
		t.Skip("error instantiating docker client:", err)
	}
	return client
}

// NewTestPoolOrSkip returns a new local Pool useful for testing. The
// test is marked as skipped if the pool cannot be constructed
// because there is no local Docker instance.
func NewTestPoolOrSkip(t *testing.T) (*local.Pool, func()) {
	// We put this in /tmp because it's one of the default locations
	// that are bindable from Docker for Mac.
	dir, cleanup := testutil.TempDir(t, "/tmp", "reflowtest")
	p := &local.Pool{
		Client: NewDockerClientOrSkip(t),
		Dir:    dir,
	}
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	return p, cleanup
}

// TestPool exercises the pool p on the testing instance t.
func TestPool(t *testing.T, p pool.Pool) {
	ctx := context.Background()
	offers, err := p.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(offers), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	// We accept half the memory and disk; we use 0 CPUs.
	o := offers[0]
	r := o.Available()
	orig := r
	r.CPU = 0
	r.Memory /= 2
	r.Disk /= 2
	alloc, err := o.Accept(ctx, pool.AllocMeta{r, "test", nil})
	if err != nil {
		t.Fatal(err)
	}
	offers, err = p.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(offers), 1; got != want {
		t.Fatalf("got %v, want %v", got, want)
	}
	o = offers[0]
	if got, want := o.Available().Memory, (orig.Memory - orig.Memory/2); got != want {
		t.Fatalf("got %v, want %v", got, want)
	}

	id := reflow.Digester.FromString("alloctest")
	exec, err := alloc.Put(ctx, id, reflow.ExecConfig{
		Type:  "exec",
		Image: bashImage,
		Cmd:   "echo logthis; echo foobar > $out",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Give it some time to fetch the image, etc.
	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()
	err = exec.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	res, err := exec.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	origres := res

	// Now we force expiry to see that we can grab everything.
	// We grab a new alloc, and check that our old alloc died;
	// there should now be zero offers.
	intv := 1 * time.Nanosecond
	d, err := alloc.Keepalive(ctx, intv)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := d, intv; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	time.Sleep(d)
	offers, err = p.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(offers), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	o = offers[0]
	if got, want := o.Available(), orig; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	alloc1, err := o.Accept(ctx, pool.AllocMeta{o.Available(), "test", nil})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := alloc1.Resources(), o.Available(); got != want {
		t.Errorf("got %v, want %v", got, want)
	}

	_, err = alloc.Keepalive(ctx, intv)
	if !errors.Match(errors.NotExist, err) {
		t.Fatalf("expected NotExist error, got %v", err)
	}
	// Look it up again to get its zombie.
	alloc, err = p.Alloc(ctx, alloc.ID())
	if err != nil {
		t.Fatal(err)
	}
	exec, err = alloc.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	res, err = exec.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := res, origres; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	rc, err := exec.Logs(ctx, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), "logthis\n"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	// We shouldn't have any offers now.
	offers, err = p.Offers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(offers), 0; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
