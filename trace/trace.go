// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// Package trace provides a tracing system for Reflow events.
// Following Dapper [1], trace events are named by a span. Spans are
// coordinates in a tree of events, and each span is associated with
// a logical timeline (e.g., an exec, a cache lookup, a run, etc.).
// Traces thus form a tree of timelines, where the operation
// represented by a single timeline is dependent on all of its child
// timelines.
//
// A span's ID is the 3-tuple
//
// 	parent ID, ID, span kind
//
// The parent ID is the ID of the span's parent. (The ID 0 is
// reserved for the root span.) The ID is a unique ID to the span
// itself, and the span's kind tells what kind of timeline the span
// represents (e.g., a cache lookup, or a command execution).
//
// Additionally, each event is associated with a timestamp and an
// event type.
//
// Tracing metadata is propagated through Go's context mechanism:
// each operation that creates a new span is given a context that
// represents that span. Package functions are provided to emit trace
// events to the current span, as defined a context.
//
// [1] https://research.google.com/pubs/pub36356.html
package trace

import (
	"context"
	"net/http"
	"time"

	"github.com/grailbio/base/digest"
)

// Kind is the type of spans.
type Kind int

const (
	// Run is the span type for a Reflow run.
	Run Kind = iota
	// Exec is the span type for a single exec.
	Exec
	// Cache is the span type for cache operations.
	Cache
	// Transfer is the span type for transfer operations.
	Transfer
	// AllocReq is the span type for resource allocation requests.
	AllocReq
	// AllocLifespan is the span type for a single alloc's lifespan.
	AllocLifespan
)

//go:generate stringer -type=Kind

var nopFunc = func() {}

// Start traces the beginning of a span of the indicated kind, name and id.
// Name is an abbreviated info about the span. Name may be used by trace implementations to
// display on a UI. Start returns a new context for this span. The returned context should be
// used to create child spans.  Notes on the context will be associated with fresh span/or annotations
// on the current span (implementation dependent). Calling done() ends the span.
func Start(ctx context.Context, kind Kind, id digest.Digest, name string) (outctx context.Context, done func()) {
	if !On(ctx) {
		return ctx, nopFunc
	}
	t := tracer(ctx)
	ctx, _ = t.Emit(ctx, Event{Time: time.Now(), SpanKind: kind, Id: id, Name: name, Kind: StartEvent})
	return ctx, func() {
		t.Emit(ctx, Event{Time: time.Now(), SpanKind: kind, Id: id, Name: name, Kind: EndEvent})
	}
}

// Note emits the provided key and value as a trace event associated with the span of the provided context.
func Note(ctx context.Context, key string, value interface{}) {
	if !On(ctx) {
		return
	}
	Emit(ctx, Event{
		Time:  time.Now(),
		Kind:  NoteEvent,
		Key:   key,
		Value: value,
	})
}

// ReadHTTPContext restores the trace context from HTTP headers.
func ReadHTTPContext(ctx context.Context, h http.Header) context.Context {
	if !On(ctx) {
		return ctx
	}
	t := tracer(ctx)
	return t.ReadHTTPContext(ctx, h)
}

// WriteHTTPContext saves the current trace context to HTTP headers.
func WriteHTTPContext(ctx context.Context, h *http.Header) {
	if !On(ctx) {
		return
	}
	t := tracer(ctx)
	t.WriteHTTPContext(ctx, h)
}

// CopyTraceContext copies the trace context from src to dst.
func CopyTraceContext(src, dst context.Context) context.Context {
	if !On(src) {
		return dst
	}
	t := tracer(src)
	dst = t.CopyTraceContext(src, dst)
	return dst
}

// URL returns the url of the current trace.
func URL(ctx context.Context) string {
	if !On(ctx) {
		return ""
	}
	t := tracer(ctx)
	return t.URL(ctx)
}

// Flush should be called at least once, when no more events
// will be emitted, to guarantee that traces are persisted.
func Flush(ctx context.Context) {
	if On(ctx) {
		t := tracer(ctx)
		t.Flush()
	}
}
