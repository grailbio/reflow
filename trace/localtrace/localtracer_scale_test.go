// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

//go:build !race
// +build !race

// due to the build constraints above, tests in this file are skipped when running
// with race detection, because it's limited to 8128 simultaneously alive goroutines

package localtrace

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/trace"
)

// TestLocalTracerContention stress tests concurrent uses of the localtracer to
// highlight regressions in mutex contention.
func TestLocalTracerContention(t *testing.T) {
	_, lt, err := getTestRunIdAndLocalTracer()
	if err != nil {
		t.Fatal(err)
	}

	// each invocation of traceFunc will emit a complete trace: a start event, up to
	// 5 note events, and an end event.
	traceFunc := func() {
		name := fmt.Sprintf("name%d", rand.Int())
		id := reflow.Digester.FromString(name)

		startCtx, _ := lt.Emit(context.Background(), trace.Event{
			Time:     time.Now(),
			Kind:     trace.StartEvent,
			Id:       id,
			Name:     name,
			SpanKind: trace.Exec,
		})

		// emit up to 5 note events
		_ = traverse.Each(rand.Intn(5), func(i int) error {
			_, _ = lt.Emit(startCtx, trace.Event{
				Time:  time.Now(),
				Kind:  trace.NoteEvent,
				Key:   fmt.Sprintf("noteEvent%d", i),
				Value: i,
			})
			return nil
		})

		_, _ = lt.Emit(startCtx, trace.Event{
			Time:     time.Now(),
			Kind:     trace.EndEvent,
			Id:       id,
			Name:     name,
			SpanKind: trace.Exec,
		})
	}

	// The testcases progressively increase the concurrency and check for the elapsed
	// time. We want to ensure that the increase in time is mostly linear, within a
	// margin of error.
	testcases := []struct {
		name         string
		concurrency  int
		wantDuration time.Duration // note: if test is run with go's race detector, it will take longer
	}{
		{
			"10,000 concurrent traces",
			10000,
			300 * time.Millisecond,
		},
		{
			"100,000 concurrent traces",
			100000,
			3000 * time.Millisecond,
		},
	}

	// we can allow a fairly high margin of error to reduce flakiness since we just
	// want to ensure elapsed time doesn't grow exponentially
	const moe = 1.25

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			startTime := time.Now()
			err = traverse.Each(tc.concurrency, func(_ int) error {
				traceFunc()
				return nil
			})
			if err != nil {
				t.Errorf("error emitting concurrent traces: %s", err)
			}
			elapsedTime := time.Now().Sub(startTime)
			if float64(elapsedTime/tc.wantDuration) > float64(moe) {
				t.Errorf("took %v, expected less than %v", elapsedTime, tc.wantDuration)
			}
		})
	}
}
