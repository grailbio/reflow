// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package trace

import (
	"context"
	"time"
)

// EventKind is the type of trace event.
type EventKind int

const (
	// StartEvent is the start of a trace span.
	StartEvent EventKind = iota
	// EndEvent is the end of a trace span.
	EndEvent
	// NoteEvent is a note on the current span.
	NoteEvent
)

// Event stores a single trace event. Each event must have at least a
// timestamp, span, and an event kind. Other arguments depend on the
// event kind.
type Event struct {
	// Time is the timestamp of the event, generated at the source of
	// that event.
	Time time.Time
	// Span is the span to which this event belongs.
	Span Span
	// Kind is the type of event.
	Kind EventKind
	// Key stores the key for NoteEvents.
	Key string
	// Value stores the value for NoteEvents.
	Value interface{}
}

// Tracers are sinks for trace events. Tracer implementations should
// not block: they are called synchronously.
type Tracer interface {
	// Emit is called to emit a new event to the tracer.
	Emit(Event) error
}

// WithTracer returns a context that emits trace events to the
// provided tracer.
func WithTracer(ctx context.Context, tracer Tracer) context.Context {
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
func Emit(ctx context.Context, event Event) error {
	return ctx.Value(tracerKey).(Tracer).Emit(event)
}
