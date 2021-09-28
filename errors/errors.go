// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package errors provides a standard error definition for use in
// Reflow. Each error is assigned a class of error (kind) and an
// operation with optional arguments. Errors may be chained, and thus
// can be used to annotate upstream errors.
//
// Errors may be serialized to- and deserialized from JSON, and thus
// shipped over network services.
//
// Package errors provides functions Errorf and New as convenience
// constructors, so that users need import only one error package.
//
// The API was inspired by package upspin.io/errors.
package errors

import (
	"bytes"
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"

	"github.com/grailbio/base/digest"
)

// Separator is inserted between chained errors while rendering.
// The default value (":\n\t") is intended for interactive tools. A
// server can set this to a different value to be more log friendly.
var Separator = ":\n\t"

// Kind denotes the type of the error. The error's kind is used to
// render the error message and also for interpretation.
type Kind int

const (
	// Other denotes an unknown error.
	Other Kind = iota
	// Canceled denotes a cancellation error.
	Canceled
	// Timeout denotes a timeout error.
	Timeout
	// Temporary denotes a transient error.
	Temporary
	// NotExist denotes an error originating from a nonexistant resource.
	NotExist
	// NotAllowed denotes a permissions error.
	NotAllowed
	// NotSupported indicates the operation was not supported.
	NotSupported
	// TooManyTries indicates that the operation was retried too many times.
	TooManyTries
	// Integrity denotes an integrity check error.
	Integrity
	// ResourcesExhausted indicates that there were insufficient resources.
	ResourcesExhausted
	// Eval denotes an evaluation error.
	Eval
	// Unavailable denotes that a resource is temporarily unavailable.
	Unavailable
	// Fatal denotes an unrecoverable error.
	Fatal
	// Invalid indicates an invalid state or data.
	Invalid
	// Net indicates a networking error.
	Net
	// Precondition indicates that a precondition was not met.
	Precondition
	// OOM indicates a out-of-memory error.
	OOM

	maxKind
)

// String renders a human-readable description of kind k.
func (k Kind) String() string {
	switch k {
	default:
		return "unknown error"
	case Canceled:
		return "canceled"
	case Timeout:
		return "timeout"
	case Temporary:
		return "temporary"
	case NotExist:
		return "resource does not exist"
	case NotAllowed:
		return "access denied"
	case NotSupported:
		return "operation not supported"
	case TooManyTries:
		return "too many tries"
	case ResourcesExhausted:
		return "resources exhausted"
	case Eval:
		return "evaluation error"
	case Integrity:
		return "integrity error"
	case Unavailable:
		return "unavailable"
	case Fatal:
		return "fatal"
	case Invalid:
		return "invalid"
	case Net:
		return "network error"
	case Precondition:
		return "precondition was not met"
	case OOM:
		return "OOM error"
	}
}

var kind2string = [maxKind]string{
	Other:              "Other",
	Canceled:           "Canceled",
	Timeout:            "Timeout",
	Temporary:          "Temporary",
	NotExist:           "NotExist",
	NotAllowed:         "NotAllowed",
	NotSupported:       "NotSupported",
	TooManyTries:       "TooManyTries",
	ResourcesExhausted: "ResourcesExhausted",
	Eval:               "Eval",
	Integrity:          "Integrity",
	Unavailable:        "Unavailable",
	Fatal:              "Fatal",
	Invalid:            "Invalid",
	Net:                "Net",
	Precondition:       "Precondition",
	OOM:                "OOM",
}

var string2kind = map[string]Kind{
	"Other":              Other,
	"Canceled":           Canceled,
	"Timeout":            Timeout,
	"Temporary":          Temporary,
	"NotExist":           NotExist,
	"NotAllowed":         NotAllowed,
	"NotSupported":       NotSupported,
	"TooManyTries":       TooManyTries,
	"ResourcesExhausted": ResourcesExhausted,
	"Eval":               Eval,
	"Integrity":          Integrity,
	"Unavailable":        Unavailable,
	"Fatal":              Fatal,
	"Invalid":            Invalid,
	"Net":                Net,
	"Precondition":       Precondition,
	"OOM":                OOM,
}

// Error defines a Reflow error. It is used to indicate an error
// associated with an operation (and arguments), and may wrap another
// error.
//
// Errors should be constructed by errors.E.
type Error struct {
	// Kind is the error's type.
	Kind Kind
	// Op is a one-word description of the operation that errored.
	Op string
	// Arg is an (optional) list of arguments to the operation.
	Arg []string
	// Err is this error's underlying error: this error is caused
	// by Err.
	Err error
}

type timeout interface {
	Timeout() bool
}

type temporary interface {
	Temporary() bool
}

// E is used to construct errors. E constructs errors from a set of
// arguments; each of which must be one of the following types:
//
//	string
//		The first string argument is taken as the error's Op; subsequent
//		arguments are taken as the error's Arg.
//	digest.Digest
//		Taken as an Arg.
//	Kind
//		Taken as the error's Kind.
//	error
//		Taken as the error's underlying error.
//
// If a Kind is provided, there is no further processing. If not, and
// an underlying error is provided, E attempts to interpret it as
// follows: (1) If the underlying  error is another *Error, and there
// is no Kind argument, the Kind is inherited from the *Error. (2) If
// the underlying error has method Timeout() bool, it is invoked, and
// if it returns true, the error's kind is set to Timeout. (3) If the
// underlying error has method Temporary() bool, it is invoked, and
// if it returns true, the error's kind is set to Temporary. (4) If
// the underyling error is context.Canceled, the error's kind is set
// to Canceled. (5) If the underlying error is an os.IsNotExist
// error, the error's kind is set to NotExist.
func E(args ...interface{}) error {
	if len(args) == 0 {
		panic("no args")
	}
	e := new(Error)
	for _, arg := range args {
		switch arg := arg.(type) {
		case string:
			if e.Op == "" {
				e.Op = arg
			} else {
				e.Arg = append(e.Arg, arg)
			}
		case digest.Digest:
			e.Arg = append(e.Arg, arg.String())
		case Kind:
			e.Kind = arg
		case *Error:
			copy := *arg
			e.Err = &copy
		case error:
			e.Err = arg
		// or: alloc, exec independently.
		// and, URI for other things?
		default:
			_, file, line, _ := runtime.Caller(1)
			e.Arg = append(e.Arg, fmt.Sprintf("illegal (%T %v from %s:%d)", arg, arg, filepath.Base(file), line))
		}
	}
	if e.Err == nil {
		return e
	}
	switch prev := e.Err.(type) {
	case *Error:
		if prev.Kind == e.Kind {
			e.Kind = prev.Kind
			prev.Kind = Other
		} else if e.Kind == Other {
			e.Kind = prev.Kind
			prev.Kind = Other
		}
		if prev.Op == "" && prev.Kind == Other {
			e.Err = prev.Err
		}
	default:
		if e.Kind != Other {
			break
		}
		if err, ok := e.Err.(temporary); ok && err.Temporary() {
			e.Kind = Temporary
		}
		if err, ok := e.Err.(timeout); ok && err.Timeout() {
			e.Kind = Timeout
		}
		if e.Err == context.Canceled {
			e.Kind = Canceled
		}
		if os.IsNotExist(e.Err) {
			e.Kind = NotExist
		}
	}
	return e
}

func pad(b *bytes.Buffer, s string) {
	if b.Len() == 0 {
		return
	}
	b.WriteString(s)
}

// Error renders this error and its chain of underlying errors,
// separated by Separator.
func (e *Error) Error() string {
	return e.ErrorSeparator(Separator)
}

// ErrorSeparator renders this errors and its chain of underlying
// errors, separated by sep.
func (e *Error) ErrorSeparator(sep string) string {
	if e == nil {
		return "<nil>"
	}
	b := new(bytes.Buffer)
	if e.Op != "" {
		b.WriteString(e.Op)
		for i := range e.Arg {
			b.WriteString(" " + e.Arg[i])
		}
	}
	if e.Kind != Other {
		pad(b, ": ")
		b.WriteString(e.Kind.String())
	}
	if e.Err != nil {
		if err, ok := e.Err.(*Error); ok {
			pad(b, sep)
			b.WriteString(err.ErrorSeparator(sep))
		} else {
			pad(b, ": ")
			b.WriteString(e.Err.Error())
		}
	}
	return b.String()
}

// Timeout tells whether this error is a timeout error.
func (e *Error) Timeout() bool {
	return e.Kind == Timeout
}

// Temporary tells whether this error is temporary.
func (e *Error) Temporary() bool {
	return e.Kind == Temporary || e.Kind == Unavailable
}

// Errorf is an alternate spelling of fmt.Errorf.
var Errorf = fmt.Errorf

// New is an alternate spelling of errors.New.
var New = goerrors.New

// Recover recovers any error into an *Error. If the passed-in err is
// already an *Error or nil, it is simply returned; otherwise it is wrapped.
func Recover(err error) *Error {
	if err == nil {
		return nil
	}
	if err, ok := err.(*Error); ok {
		return err
	}
	return E(err).(*Error)
}

// Copy creates a shallow copy of Error e.
func (e *Error) Copy() *Error {
	f := new(Error)
	*f = *e
	return f
}

// HTTPStatus indicates the HTTP status that should be presented
// in conjunction with this error.
func (e *Error) HTTPStatus() int {
	switch e.Kind {
	case NotExist:
		return 404 // Not Found
	case NotAllowed:
		return 405 // Method Not Allowed
	case Temporary:
		return 503 // Service Unavailable
	default:
		return 500 //  Internal Server Error
	}
}

type jsonError struct {
	Op    string
	Arg   []string
	Kind  string
	Cause *jsonError `json:",omitempty"`
	Error string
}

func (j *jsonError) toError() error {
	if j == nil {
		return nil
	}
	if j.Error != "" {
		return New(j.Error)
	}
	var args []interface{}
	args = append(args, j.Op)
	for _, arg := range j.Arg {
		args = append(args, arg)
	}
	args = append(args, string2kind[j.Kind])
	if j.Cause != nil {
		args = append(args, j.Cause.toError())
	}
	return E(args...)
}

func toJSON(err error) *jsonError {
	switch e := err.(type) {
	case *Error:
		j := &jsonError{
			Op:   e.Op,
			Arg:  e.Arg,
			Kind: kind2string[e.Kind],
		}
		if e.Err != nil {
			j.Cause = toJSON(e.Err)
		}
		return j
	default:
		return &jsonError{Error: err.Error()}
	}
}

// MarshalJSON implements JSON marshalling for Error.
func (e *Error) MarshalJSON() ([]byte, error) {
	return json.Marshal(toJSON(e))
}

// UnmarshalJSON implements JSON unmarshalling for Error.
func (e *Error) UnmarshalJSON(b []byte) error {
	var ej jsonError
	if err := json.Unmarshal(b, &ej); err != nil {
		return err
	}
	e2, ok := ej.toError().(*Error)
	if !ok {
		return Errorf("expected *Error, got %T", e2)
	}
	*e = *e2
	return nil
}

// Match compares err1 with err2. If err1 has type Kind, Match
// reports whether err2's Kind is the same, otherwise, Match checks
// that every nonempty field in err1 has the same value in err2. If
// err1 is an *Error with a non-nil Err field, Match recurs to check
// that the two errors chain of underlying errors also match.
func Match(err1, err2 error) bool {
	if err1 == err2 {
		return true
	}
	e2 := Recover(err2)
	e1, ok := err1.(*Error)
	if !ok {
		return false
	}
	if e1.Op != "" && e2.Op != e1.Op {
		return false
	}
	if len(e1.Arg) != len(e2.Arg) {
		return false
	}
	for i := range e1.Arg {
		if e1.Arg[i] != e2.Arg[i] {
			return false
		}
	}
	if e1.Kind != Other && e2.Kind != e1.Kind {
		return false
	}
	if e1.Err != nil {
		if _, ok := e1.Err.(*Error); ok {
			return Match(e1.Err, e2.Err)
		}
		if e2.Err == nil || e2.Err.Error() != e1.Err.Error() {
			return false
		}
	}
	return true
}

// Is tells whether an error has a specified kind, except for the
// indeterminate kind Other. In the case an error has kind Other, the
// chain is traversed until a non-Other error is encountered.
func Is(kind Kind, err error) bool {
	if err == nil {
		return false
	}
	return is(kind, Recover(err))
}

func is(kind Kind, e *Error) bool {
	if e.Kind != Other {
		return e.Kind == kind
	}
	if e.Err != nil {
		if e2, ok := e.Err.(*Error); ok {
			return is(kind, e2)
		}
	}
	return false
}

// Transient tells whether error err is likely transient, and thus may
// be usefully retried. The passed in error must not be nil.
func Transient(err error) bool {
	switch Recover(err).Kind {
	case Timeout, Temporary, TooManyTries, Unavailable:
		return true
	default:
		return false
	}
}

// Restartable determines if the provided error is restartable.
// Restartable errors include transient errors and network errors.
// The passed in error must not be nil.
func Restartable(err error) bool {
	return Transient(err) || Is(Net, err)
}

// NonRetryable tells whether error err is likely fatal, and thus not
// useful to retry. The passed in error must not be nil.
// Note that NonRetryable != !Restartable; these functions are not the
// inverse of each other. They each report on the errors they know to
// be either non-retryable or restartable, but there exist errors which
// don't belong to either category.
func NonRetryable(err error) bool {
	switch Recover(err).Kind {
	case NotSupported, Invalid, NotExist, Fatal:
		return true
	default:
		return false
	}
}

// retryableErrorKey is used to store and access retryable error kinds in a context.
// Intentionally not exported, use WithRetryableKinds and GetRetryableKinds instead.
type retryableErrorKey struct{}

// WithRetryableKinds returns a child context of ctx with the given error kinds and
// any pre-existing ones. WithRetryableKinds should be used rather than directly
// setting the value to avoid overwriting any previously set retryable errors.
func WithRetryableKinds(ctx context.Context, ks ...Kind) context.Context {
	set, ok := ctx.Value(retryableErrorKey{}).(map[Kind]bool)
	if !ok {
		set = make(map[Kind]bool)
	}
	for _, k := range ks {
		set[k] = true
	}
	return context.WithValue(ctx, retryableErrorKey{}, set)
}

// GetRetryableKinds returns a slice of any retryable error kinds stored in ctx.
func GetRetryableKinds(ctx context.Context) []Kind {
	kinds, ok := ctx.Value(retryableErrorKey{}).(map[Kind]bool)
	if !ok {
		return []Kind{}
	}
	return getKeys(kinds)
}

func getKeys(m map[Kind]bool) (out []Kind) {
	for k, _ := range m {
		out = append(out, k)
	}
	return out
}

// Multi represents a composite error which combines multiple (non-nil) errors.
// Multi is useful in cases where there is a need to (concurrently) combine multiple (non-critical) errors into one.
type Multi struct {
	mu     sync.Mutex
	errors []error
}

// Add adds the given set of errors (ignoring nil errors).
// Add can be called concurrently.
func (e *Multi) Add(errs ...error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, err := range errs {
		if err == nil {
			continue
		}
		e.errors = append(e.errors, err)
	}
}

// Combined returns an error which combines all the underlying errors.
func (e *Multi) Combined() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch len(e.errors) {
	case 0:
		return nil
	case 1:
		return e.errors[0]
	}
	errStrs := make([]string, len(e.errors))
	for i, err := range e.errors {
		errStrs[i] = fmt.Sprintf("[%s]", err.Error())
	}
	sort.Strings(errStrs)
	return fmt.Errorf("[%d errs]: %s", len(e.errors), strings.Join(errStrs, ", "))
}
