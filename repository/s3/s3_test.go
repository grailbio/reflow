package s3

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/grailbio/reflow"
	"grail.com/testutil/s3test"
)

const bucket = "test"

func TestRepository(t *testing.T) {
	ctx := context.Background()
	client := s3test.NewClient(t, bucket)
	r := &Repository{Client: client, Bucket: bucket}
	const content = "hello, world"
	id, err := r.Put(ctx, bytes.NewReader([]byte(content)))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := id, reflow.Digester.FromString(content); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	file, err := r.Stat(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := file, (reflow.File{ID: id, Size: int64(len(content))}); got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	rc, err := r.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := ioutil.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), content; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestPutGetFile(t *testing.T) {
	ctx := context.Background()
	client := s3test.NewClient(t, bucket)
	r := &Repository{Client: client, Bucket: bucket}
	const content = "another piece of content"
	file := reflow.File{
		ID:   reflow.Digester.FromString(content),
		Size: int64(len(content)),
	}
	if err := r.PutFile(ctx, file, bytes.NewReader([]byte(content))); err != nil {
		t.Fatal(err)
	}
	f, err := ioutil.TempFile("", "s3test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	n, err := r.GetFile(ctx, file.ID, f)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := n, file.Size; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(b), content; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
