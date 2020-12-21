package localtrace

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

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
	endTime := time.Now().Add(time.Minute)
	durationMicroSeconds := endTime.Sub(startTime).Microseconds()
	flowName := "TestFlow"
	flowId := reflow.Digester.FromString(flowName)
	noteKey, noteValue := "testNoteKey", "testNoteValue"
	wantFinalEvent := Event{
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
	if got := lt.trace.Events[0]; !reflect.DeepEqual(got, wantFinalEvent) {
		t.Fatalf("\nwant %v\ngot  %v", wantFinalEvent, got)
	}
}

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
