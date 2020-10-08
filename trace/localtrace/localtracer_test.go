package localtrace

import (
	"context"
	"crypto"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/infra"
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
	flowId := digest.New(crypto.SHA256, []byte(flowName))
	noteKey, noteValue := "testNoteKey", "testNoteValue"
	wantFinalEvent := Event{
		Pid:  0,
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

	config, err := getTestConfig()
	if err != nil {
		t.Fatal(err)
	}
	var tracer trace.Tracer
	config.Must(&tracer)
	lt := tracer.(*LocalTracer)

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

func TestLocalTracerCopyTraceContext(t *testing.T) {
	config, err := getTestConfig()
	if err != nil {
		t.Fatal(err)
	}
	var runid *taskdb.RunID
	config.Must(&runid)
	var tracer trace.Tracer
	config.Must(&tracer)
	lt := tracer.(*LocalTracer)

	src := context.WithValue(context.Background(), eventKey, Event{})
	dst := context.Background()

	dst = lt.CopyTraceContext(src, dst)
	if _, err := eventKey.getEvent(dst); err != nil {
		t.Errorf("expected dst to have copied value from src\n%s\n%s", dst, src)
	}
}

func TestLocalTracerURL(t *testing.T) {
	config, err := getTestConfig()
	if err != nil {
		t.Fatal(err)
	}
	var runid *taskdb.RunID
	config.Must(&runid)
	var tracer trace.Tracer
	config.Must(&tracer)
	lt := tracer.(*LocalTracer)

	wantSuffix := fmt.Sprintf("%s.trace", runid.Hex())
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
