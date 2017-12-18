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
)

var nopFunc = func() {}

// Span stores the parent-child tuple of IDs that define a trace
// span. The zero Span struct is the root span.
type Span struct {
	Parent, Id digest.Digest
	Kind       Kind
}

// Start traces the beginning of a span of the indicated kind, and
// with the given ID. Start returns a new context for this span:
// Notes on the context will be associated with fresh span; new spans
// become children of this span.
func Start(ctx context.Context, kind Kind, id digest.Digest) (outctx context.Context, done func()) {
	if !On(ctx) {
		return ctx, nopFunc
	}
	// This is ok: the root span is the zero value.
	span, _ := ctx.Value(spanKey).(Span)
	span.Parent = span.Id
	span.Id = id
	span.Kind = kind
	t := tracer(ctx)
	t.Emit(Event{Time: time.Now(), Span: span, Kind: StartEvent})
	return context.WithValue(ctx, spanKey, span), func() {
		t.Emit(Event{Time: time.Now(), Span: span, Kind: EndEvent})
	}
}

// Note emits the provided key and value as a trace event associated
// with the span of the provided context.
func Note(ctx context.Context, key string, value interface{}) {
	if !On(ctx) {
		return
	}
	span, _ := ctx.Value(spanKey).(Span)
	Emit(ctx, Event{
		Time:  time.Now(),
		Span:  span,
		Kind:  NoteEvent,
		Key:   key,
		Value: value,
	})
}
