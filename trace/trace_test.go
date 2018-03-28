// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package trace_test

import (
	"context"
	"crypto"
	_ "crypto/sha256"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/reflow/trace"
)

var digester = digest.Digester(crypto.SHA256)

type chanTracer chan trace.Event

func (c chanTracer) Emit(ctx context.Context, e trace.Event) (context.Context, error) {
	c <- e
	return ctx, nil
}

func (c chanTracer) WriteHTTPContext(ctx context.Context, h *http.Header)               {}
func (c chanTracer) ReadHTTPContext(ctx context.Context, h http.Header) context.Context { return ctx }
func (c chanTracer) CopyTraceContext(src, dst context.Context) context.Context {
	return dst
}
func (c chanTracer) URL(context.Context) string { return "" }

func id(i int) digest.Digest {
	return digester.FromString(strconv.Itoa(i))
}

func TestTrace(t *testing.T) {
	now := time.Now()
	tracer := make(chanTracer, 1024)
	ctx := trace.WithTracer(context.Background(), tracer)
	ctx1, done1 := trace.Start(ctx, trace.Run, id(1), "1")
	trace.Note(ctx1, "hello", "world")
	ctx2, done2 := trace.Start(ctx1, trace.Exec, id(2), "2")
	trace.Note(ctx2, "exec", "blah")
	trace.Note(ctx1, "exec", "1")
	done2()
	done1()

	expect := []trace.Event{
		{Kind: trace.StartEvent, SpanKind: trace.Run, Id: id(1), Name: "1"},
		{Kind: trace.NoteEvent, Key: "hello", Value: "world"},
		{Kind: trace.StartEvent, SpanKind: trace.Exec, Id: id(2), Name: "2"},
		{Kind: trace.NoteEvent, Key: "exec", Value: "blah"},
		{Kind: trace.NoteEvent, Key: "exec", Value: "1"},
		{Kind: trace.EndEvent, SpanKind: trace.Exec, Id: id(2), Name: "2"},
		{Kind: trace.EndEvent, SpanKind: trace.Run, Id: id(1), Name: "1"},
	}

	for _, ex := range expect {
		var ev trace.Event
		select {
		case ev = <-tracer:
		default:
			t.Fatalf("failed to receive expected event %v", ex)
		}
		if ev.Time.Before(now) {
			t.Errorf("bad timestamp: got %v, expected time later or equal to %v", ev.Time, now)
		}
		now = ev.Time
		if got, want := ev.Kind, ex.Kind; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := ev.SpanKind, ex.SpanKind; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := ev.Key, ex.Key; got != want {
			t.Errorf("got %v, want %v", got, want)
		}
		if got, want := ev.Value, ex.Value; !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	}
drain:
	for {
		select {
		case ev := <-tracer:
			t.Errorf("received excess event %v", ev)
		default:
			break drain
		}
	}
}
