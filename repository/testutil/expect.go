package testutil

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"reflect"
	"testing"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/errors"
)

//go:generate stringer -type=CallType

// CallType indicates the type of repository call.
type CallType int

// The concrete types of repository calls.
const (
	CallGet CallType = iota
	CallPut
	CallWriteTo
	CallReadFrom
)

// RepositoryCall describes a single call to a Repository: its
// expected arguments, and a reply.
type RepositoryCall struct {
	T            CallType
	ArgID        digest.Digest
	ArgURL       url.URL
	ArgBytes     []byte
	ArgReadError error

	ReplyID         digest.Digest
	ReplyErr        error
	ReplyReadCloser io.ReadCloser
}

// ExpectRepository is a Repository implementation used for
// testing; it takes a script of expected calls and replies. Violations
// are reported to the testing.T instance.
type ExpectRepository struct {
	*testing.T                  // the testing instance to use
	RepoURL    *url.URL         // the repository URL
	calls      []RepositoryCall // the script of repository calls
}

// NewExpectRepository creates a new testing repository.
func NewExpectRepository(t *testing.T, rawurl string) *ExpectRepository {
	u, err := url.Parse(rawurl)
	if err != nil {
		t.Fatal(err)
	}
	return &ExpectRepository{T: t, RepoURL: u}
}

// Expect adds a call to the repository's script.
func (r *ExpectRepository) Expect(call RepositoryCall) {
	r.calls = append(r.calls, call)
}

// Complete should be called when testing is finished; it verifies
// that the entire script has been exhausted.
func (r *ExpectRepository) Complete() error {
	if n := len(r.calls); n != 0 {
		return fmt.Errorf("finished with %d calls remaining", n)
	}
	return nil
}

func (r *ExpectRepository) call(c *RepositoryCall) error {
	if len(r.calls) == 0 {
		return fmt.Errorf("unexpected call %v", c)
	}
	expect := r.calls[0]
	r.calls = r.calls[1:]
	if got, want := c.T, expect.T; got != want {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	if got, want := c.ArgID, expect.ArgID; got != want {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	if got, want := c.ArgURL, expect.ArgURL; got != want {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	if got, want := c.ArgBytes, expect.ArgBytes; !reflect.DeepEqual(got, want) {
		return fmt.Errorf("got %v, want %v", got, want)
	}
	*c = expect
	return nil
}

// Stat always returns a NotExist error.
func (r *ExpectRepository) Stat(_ context.Context, id digest.Digest) (reflow.File, error) {
	return reflow.File{}, errors.E("stat", id, errors.NotExist)
}

// Get implements the repository's Get call.
func (r *ExpectRepository) Get(_ context.Context, id digest.Digest) (io.ReadCloser, error) {
	call := RepositoryCall{T: CallGet, ArgID: id}
	if err := r.call(&call); err != nil {
		return nil, err
	}
	return call.ReplyReadCloser, call.ReplyErr
}

// Put implements the repository's Put call.
func (r *ExpectRepository) Put(_ context.Context, body io.Reader) (digest.Digest, error) {
	call := RepositoryCall{T: CallPut}
	call.ArgBytes, call.ArgReadError = ioutil.ReadAll(body)
	if err := r.call(&call); err != nil {
		return digest.Digest{}, err
	}
	return call.ReplyID, call.ReplyErr
}

// WriteTo implements the repository's WriteTo call.
func (r *ExpectRepository) WriteTo(_ context.Context, id digest.Digest, u *url.URL) error {
	call := RepositoryCall{T: CallWriteTo, ArgID: id, ArgURL: *u}
	if err := r.call(&call); err != nil {
		return err
	}
	return call.ReplyErr
}

// ReadFrom implements the repository's ReadFrom call.
func (r *ExpectRepository) ReadFrom(_ context.Context, id digest.Digest, u *url.URL) error {
	call := RepositoryCall{T: CallReadFrom, ArgID: id, ArgURL: *u}
	if err := r.call(&call); err != nil {
		return err
	}
	return call.ReplyErr
}

// Collect is not supported for the expect repository.
func (*ExpectRepository) Collect(context.Context, reflow.Liveset) error {
	return errors.E("collect", errors.NotSupported)
}

// URL returns the repository's URL.
func (r *ExpectRepository) URL() *url.URL { return r.RepoURL }
