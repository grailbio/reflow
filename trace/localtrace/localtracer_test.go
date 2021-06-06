package localtrace

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/grailbio/base/traverse"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
)

func TestLocalTracerInfra(t *testing.T) {
	config, err := getTestConfig()
	if err != nil {
		t.Fatal(err)
	}
	var tracer trace.Tracer
	config.Must(&tracer)
	_, ok := tracer.(*LocalTracer)
	if !ok {
		t.Fatalf("%v is not a LocalTracer", reflect.TypeOf(t))
	}
}

func TestLocalTracerEmit(t *testing.T) {
	startTime := time.Now()
	endTime := startTime.Add(time.Minute)
	durationMicroSeconds := time.Minute.Microseconds()
	flowName := "TestFlow"
	flowId := reflow.Digester.FromString(flowName)
	noteKey, noteValue := "testNoteKey", "testNoteValue"
	want := Event{
		Pid:  1,
		Tid:  0,
		Ts:   startTime.UnixNano() / 1000,
		Ph:   "X",
		Dur:  durationMicroSeconds,
		Name: flowName,
		Cat:  trace.Exec.String(),
		Args: map[string]interface{}{
			"beginTime": startTime.Format(time.RFC850),
			"endTime":   endTime.Format(time.RFC850),
			noteKey:     noteValue,
		},
	}

	_, lt, err := getTestRunIdAndLocalTracer()
	if err != nil {
		t.Fatal(err)
	}

	// start event
	startCtx, err := lt.Emit(context.Background(), trace.Event{
		Time:     startTime,
		Kind:     trace.StartEvent,
		Id:       flowId,
		Name:     flowName,
		SpanKind: trace.Exec,
	})
	if err != nil {
		t.Error(err)
	}
	if _, err = eventKey.getEvent(startCtx); err != nil {
		t.Error("expected startCtx to contain an Event value")
	}

	// note event
	noteCtx, err := lt.Emit(startCtx, trace.Event{
		Time:     startTime,
		Kind:     trace.NoteEvent,
		Key:      noteKey,
		Value:    noteValue,
		Id:       flowId,
		Name:     flowName,
		SpanKind: trace.Exec,
	})
	if err != nil {
		t.Error(err)
	}
	if _, err = eventKey.getEvent(noteCtx); err != nil {
		t.Error("expected noteCtx to contain an Event value")
	}

	// end event
	endContext, err := lt.Emit(noteCtx, trace.Event{
		Time:     endTime,
		Kind:     trace.EndEvent,
		Id:       flowId,
		Name:     flowName,
		SpanKind: trace.Exec,
	})
	if err != nil {
		t.Error(err)
	}
	if endContext != nil {
		t.Errorf("wanted nil ctx after emitting EndEvent, got: %s", endContext)
	}

	if got, want := len(lt.trace.Events), 1; got != want {
		t.Fatalf("wanted %d trace event(s), got %d", want, got)
	}

	got := lt.trace.Events[0]
	got.Tid = 0 // tid will be random so reset it before comparing with what we want
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("\nwant %v\ngot %v", want, got)
	}
}

// If running with `go test`, make sure to include the `-race` flag to highlight any regressions
// e.g. `go test -v -race -run TestLocalTracerConcurrent`
func TestLocalTracerConcurrentEmitPids(t *testing.T) {
	const numRoutines = 5
	testcases := []struct {
		name     string
		rootCtx  context.Context
		spanKind trace.Kind
		wantPids [numRoutines]int // make sure these are sorted so that the comparison works
	}{
		{
			name:     "exec ctx has pid",
			rootCtx:  context.WithValue(context.Background(), pidKey, 99),
			spanKind: trace.Exec,
			wantPids: [numRoutines]int{99, 99, 99, 99, 99},
		},
		{
			name:     "exec ctx without pid",
			rootCtx:  context.Background(),
			spanKind: trace.Exec,
			wantPids: [numRoutines]int{1, 2, 3, 4, 5},
		},
		{
			name:     "run ctx without pid",
			rootCtx:  context.Background(),
			spanKind: trace.Run,
			wantPids: [numRoutines]int{0, 0, 0, 0, 0},
		},
		{
			name:     "allocreq ctx with pid",
			rootCtx:  context.WithValue(context.Background(), pidKey, 88),
			spanKind: trace.AllocReq,
			wantPids: [numRoutines]int{0, 0, 0, 0, 0},
		},
	}

	for _, tc := range testcases {
		tc := tc
		_, lt, err := getTestRunIdAndLocalTracer()
		if err != nil {
			t.Fatal(err)
		}
		t.Run(tc.name, func(t *testing.T) {
			rand.Seed(time.Now().Unix())
			var wg sync.WaitGroup
			for i := 0; i < numRoutines; i++ {
				i := i
				wg.Add(1)
				go func() {
					name := fmt.Sprintf("goroutine%d exec", i)
					id := reflow.Digester.FromString(name)
					startCtx, _ := lt.Emit(tc.rootCtx, trace.Event{
						Time:     time.Now(),
						Kind:     trace.StartEvent,
						Id:       id,
						Name:     name,
						SpanKind: tc.spanKind,
					})
					time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)
					wg.Add(1)
					go func() { // async emit a NoteEvent while also emitting a EndEvent
						_, _ = lt.Emit(startCtx, trace.Event{
							Time:  time.Now(),
							Kind:  trace.NoteEvent,
							Key:   fmt.Sprintf("noteEvent%d", i),
							Value: i,
						})
						wg.Done()
					}()
					_, _ = lt.Emit(startCtx, trace.Event{
						Time:     time.Now(),
						Kind:     trace.EndEvent,
						Id:       id,
						Name:     name,
						SpanKind: tc.spanKind,
					})
					wg.Done()
				}()
			}
			wg.Wait()

			var gotPids []int
			for _, e := range lt.trace.Events {
				gotPids = append(gotPids, e.Pid)
			}
			if got, want := len(gotPids), len(tc.wantPids); got != want {
				t.Errorf("got %d emitted events, wanted %d", got, want)
			}
			sort.Ints(gotPids)
			for i, wantPid := range tc.wantPids {
				if gotPid := gotPids[i]; gotPid != wantPid {
					t.Errorf("got pid %d, want %d", gotPid, wantPid)
				}
			}
		})
	}
}

// If running with `go test`, make sure to include the `-race` flag to highlight any regressions
// e.g. `go test -v -race -run TestLocalTracerConcurrent`
func TestLocalTracerConcurrentNoteEvents(t *testing.T) {
	const numConcurrentNotes = 500
	name := "concurrent note events"
	id := reflow.Digester.FromString(name)

	_, lt, err := getTestRunIdAndLocalTracer()
	if err != nil {
		t.Fatal(err)
	}

	startCtx, _ := lt.Emit(context.Background(), trace.Event{
		Time:     time.Now(),
		Kind:     trace.StartEvent,
		Id:       id,
		Name:     name,
		SpanKind: trace.Exec,
	})

	var wg sync.WaitGroup
	for i := 0; i < numConcurrentNotes; i++ {
		i := i
		wg.Add(1)
		go func() {
			_, _ = lt.Emit(startCtx, trace.Event{
				Time:  time.Now(),
				Kind:  trace.NoteEvent,
				Key:   "noteEventSameKey",
				Value: i,
			})
			_, _ = lt.Emit(startCtx, trace.Event{
				Time:  time.Now(),
				Kind:  trace.NoteEvent,
				Key:   fmt.Sprintf("noteEvent%d", i),
				Value: i,
			})
			wg.Done()
		}()
	}
	wg.Wait()

	_, _ = lt.Emit(startCtx, trace.Event{
		Time:     time.Now(),
		Kind:     trace.EndEvent,
		Id:       id,
		Name:     name,
		SpanKind: trace.Exec,
	})

	if got, want := len(lt.trace.Events), 1; got != want {
		t.Errorf("got %d emitted events, wanted %d", got, want)
	}

	// +3 because we have: 1 for StartEvent, 1 for EndEvent, 1 for NoteEvent with "noteEventSameKey"
	if got, want := len(lt.trace.Events[0].Args), numConcurrentNotes+3; got != want {
		t.Errorf("got %d attributes on the emitted event, wanted %d (start time, end time, and %d notes)", got, want, numConcurrentNotes)
	}
}

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

	// The testcases progressively increase the concurrency and check for the
	// elapsed time. We want to ensure that the increase in time is at most linear.
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
			if elapsedTime > tc.wantDuration {
				t.Errorf("took %v, expected less than %v", elapsedTime, tc.wantDuration)
			}
		})
	}
}

func TestLocalTracerFlushEncodeDecode(t *testing.T) {
	testTraceFilepath := "/tmp/TestLocalTracerFlushEncodeDecode.trace"
	lt, err := New(testTraceFilepath)
	if err != nil {
		t.Error(err)
	}

	name := "TestEvent"
	id := reflow.Digester.FromString(name)
	startCtx, _ := lt.Emit(context.Background(), trace.Event{
		Time:     time.Now(),
		Kind:     trace.StartEvent,
		Id:       id,
		Name:     name,
		SpanKind: trace.Exec,
	})
	_, _ = lt.Emit(startCtx, trace.Event{
		Time:     time.Now(),
		Kind:     trace.EndEvent,
		Id:       id,
		Name:     name,
		SpanKind: trace.Exec,
	})

	lt.Flush()

	f, err := os.Open(testTraceFilepath)
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()
	if err != nil {
		t.Error(err)
	}

	gotLt := LocalTracer{}
	if err = gotLt.trace.Decode(f); err != nil {
		t.Error(err)
	}
	if len(gotLt.trace.Events) != 1 {
		t.Fatalf("expected 1 event from trace file, but got: %d", len(gotLt.trace.Events))
	}
	if got := gotLt.trace.Events[0].Name; got != name {
		t.Errorf("got: %s, want: %s", got, name)
	}
}

func TestLocalTracerCopyTraceContext(t *testing.T) {
	_, lt, err := getTestRunIdAndLocalTracer()
	if err != nil {
		t.Fatal(err)
	}

	src := context.WithValue(context.Background(), eventKey, Event{})
	dst := context.Background()

	dst = lt.CopyTraceContext(src, dst)
	if _, err := eventKey.getEvent(dst); err != nil {
		t.Errorf("expected dst to have copied value from src\n%s\n%s", dst, src)
	}
}

func TestLocalTracerURL(t *testing.T) {
	runId, lt, err := getTestRunIdAndLocalTracer()
	if err != nil {
		t.Fatal(err)
	}

	wantSuffix := fmt.Sprintf("%s.trace", runId.Hex())
	if got := lt.URL(context.Background()); !strings.HasSuffix(got, wantSuffix) {
		t.Fatalf("\ngot:\t\t\t%s\nwant suffix:\t%s", got, wantSuffix)
	}
}

func getTestConfig() (infra.Config, error) {
	var schema = infra.Schema{
		"runid":  new(taskdb.RunID),
		"tracer": new(trace.Tracer),
	}
	return schema.Make(infra.Keys{
		"runid":  "runid",
		"tracer": "localtracer",
	})

}

func getTestRunIdAndLocalTracer() (*taskdb.RunID, *LocalTracer, error) {
	config, err := getTestConfig()
	if err != nil {
		return nil, nil, err
	}
	var runId *taskdb.RunID
	config.Must(&runId)
	var tracer trace.Tracer
	config.Must(&tracer)
	return runId, tracer.(*LocalTracer), nil
}

func TestNew(t *testing.T) {
	want := &LocalTracer{
		rwmu:          sync.RWMutex{},
		tidMap:        sync.Map{},
		trace:         T{},
		tracefilepath: "",
	}
	tests := []struct {
		name          string
		tracefilepath string
		wantErr       bool
	}{
		{
			name:          "valid filepath",
			tracefilepath: "/tmp/foo.trace",
			wantErr:       false,
		},
		{
			name:          "nonexistent dir",
			tracefilepath: "/tmp/nonexistent-dir/foo.trace",
			wantErr:       true,
		},
		{
			name:          "path is a dir not file",
			tracefilepath: "/tmp",
			wantErr:       true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(tt.tracefilepath)
			if tt.wantErr != (err != nil) {
				t.Fatalf("wantErr: %v, got: %v", tt.wantErr, err)
			}
			if !tt.wantErr {
				want.tracefilepath = tt.tracefilepath
				if !reflect.DeepEqual(want, got) {
					t.Fatalf("want: %v, got: %v", want, got)
				}
			}
		})
	}
}
