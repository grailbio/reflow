// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package trace

import (
	"context"
	"net/http"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/infra"
)

// EventKind is the type of trace event.
type EventKind int

const (
	// StartEvent is the start of a trace span.
	StartEvent EventKind = iota
	// EndEvent is the end of a trace span.
	EndEvent
	// NoteEvent is an annotation on the current span.
	NoteEvent
)

// Event stores a single trace event. Each event must have at least a
// timestamp and an event kind. Other arguments depend on the event kind.
type Event struct {
	// Time is the timestamp of the event, generated at the source of
	// that event.
	Time time.Time
	// Kind is the type of event.
	Kind EventKind
	// Key stores the key for NoteEvents.
	Key string
	// Value stores the value for NoteEvents.
	Value interface{}
	// The following fields are set for events of type StartEvent and EndEvent.
	// Id of the span this event belongs to.
	Id digest.Digest
	// Name of the span this belongs to.
	Name string
	// Kind of span.
	SpanKind Kind
}

// Tracer is a sink for trace events. Tracer implementations should
// not block: they are called synchronously.
type Tracer interface {
	// Emit is called to emit a new event to the tracer.
	// The returned context should be used to create children spans.
	Emit(context.Context, Event) (context.Context, error)
	// WriteHTTPContext saves the current trace context to http header.
	WriteHTTPContext(context.Context, *http.Header)
	// ReadHTTPContext restores the trace context from http header.
	ReadHTTPContext(context.Context, http.Header) context.Context
	// CopyTraceContext copies trace specific metadata from src to dst.
	CopyTraceContext(src context.Context, dst context.Context) context.Context
	// URL returns the trace URL for the trace associated with ctx.
	URL(context.Context) string
	// Flush should be called at least once, when no more events
	// will be emitted, to guarantee traces that are persisted.
	Flush()
}

// NopTracer is default tracer that does nothing.
var NopTracer Tracer = nopTracer{}

type nopTracer struct{}

func init() {
	infra.Register("noptracer", new(nopTracer))
}

// Init implements infra.Provider and gets called when an instance of nopTracer
// is created with infra.Config.Instance(...)
func (nopTracer) Init() error {
	return nil
}

// Help implements infra.Provider
func (nopTracer) Help() string {
	return "nopTracer does nothing, use it when you don't want any tracing"
}

func (nopTracer) Flush() {
	return
}

func (nopTracer) Emit(ctx context.Context, e Event) (context.Context, error) {
	return ctx, nil
}

func (nopTracer) WriteHTTPContext(context.Context, *http.Header) {
	return
}

func (nopTracer) ReadHTTPContext(ctx context.Context, h http.Header) context.Context {
	return ctx
}

func (nopTracer) CopyTraceContext(src context.Context, dst context.Context) context.Context {
	return dst
}

func (nopTracer) URL(context.Context) string {
	return "none (since nopTracer is in use)"
}

// WithTracer returns a context that emits trace events to the
// provided tracer.
func WithTracer(ctx context.Context, tracer Tracer) context.Context {
	if tracer == nil {
		return ctx
	}
	return context.WithValue(ctx, tracerKey, tracer)

}

// On returns true if there is a current tracer associated with the
// provided context.
func On(ctx context.Context) bool {
	_, ok := ctx.Value(tracerKey).(Tracer)
	return ok
}

func tracer(ctx context.Context) Tracer {
	return ctx.Value(tracerKey).(Tracer)
}

// Emit emits a raw event on the tracer affiliated with the provided context.
func Emit(ctx context.Context, event Event) (context.Context, error) {
	return ctx.Value(tracerKey).(Tracer).Emit(ctx, event)
}
