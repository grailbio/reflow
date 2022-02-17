package localtrace

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grailbio/base/digest"
	"github.com/grailbio/infra"
	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/taskdb"
	"github.com/grailbio/reflow/trace"
)

func init() {
	infra.Register("localtracer", new(LocalTracer))
}

type LocalTracer struct {
	rwmu       sync.RWMutex
	pidCounter int32
	tidMap     sync.Map
	trace      T

	tracefilepath string
}

// Init implements infra.Provider and gets called when an instance of LocalTracer
// is created with infra.Config.Instance(...)
func (lt *LocalTracer) Init(runID *taskdb.RunID) error {
	base, err := reflow.Runbase(digest.Digest(*runID))
	if err != nil {
		return err
	}
	lt.tracefilepath = base + ".trace"
	return nil
}

// New returns a new LocalTracer instance. This function is intended for cases
// where Reflow is used as a library and the user would like to specify the
// path of the output trace file.
func New(path string) (*LocalTracer, error) {
	// Validate path by trying to create it
	if f, err := os.Create(path); err != nil {
		return nil, err
	} else {
		f.Close()
	}
	return &LocalTracer{
		tracefilepath: path,
	}, nil
}

// Help implements infra.Provider
func (lt *LocalTracer) Help() string {
	return "configure a local tracer to write traces to ~/.reflow/runs, viewable with chrome://tracing"
}

type key int

const (
	eventKey key = iota
	pidKey
)

func (k key) getEvent(ctx context.Context) (Event, error) {
	if event, ok := ctx.Value(k).(Event); ok {
		return event, nil
	}
	return Event{}, fmt.Errorf("no event found for key: %d", k)
}

// getPid encapsulates the logic which determines the "pid" for a given event.
// This isn't a real "pid", we're just using this field in the chrome tracing
// format to give us nested spans that work well for our use case. Specifically,
// Run and AllocReq spans will just default to 0, so that they are displayed
// together at the top of the visualization. For all other span kinds, we attempt
// to retrieve the pid from the ctx or, if it's not there, increment to get a
// unique pid. With this implementation, we can get a fresh pid for each unique
// alloc, and as long as that alloc's ctx is used to create spans for tasks on
// that alloc, the trace visualization will group them all together.
// Important: getPid is safe for concurrent use.
func (lt *LocalTracer) getPid(ctx context.Context, e trace.Event) (context.Context, int) {
	switch {
	case e.SpanKind == trace.Run || e.SpanKind == trace.AllocReq:
		return ctx, 0
	case ctx.Value(pidKey) != nil:
		// if this ctx already has an associated pid, return it
		return ctx, ctx.Value(pidKey).(int)
	default:
		// otherwise generate a new unique pid, store it and return it along with the updated ctx
		pid := int(atomic.AddInt32(&lt.pidCounter, 1))
		return context.WithValue(ctx, pidKey, pid), pid
	}
}

// getTid encapsulates the logic for determining the "tid" based on a given event
// ID. This isn't a real thread ID, we're just using this field in the chrome
// tracing format to group together different spans that belong together. One way
// this is used is to group together different steps of a single task (load,
// exec, unload, etc.) into a single row in the trace visualization.
// Important: getTid is safe for concurrent use.
func (lt *LocalTracer) getTid(id string) int {
	tid, ok := lt.tidMap.Load(id)
	if !ok {
		tid = rand.Int()
		lt.tidMap.Store(id, tid)
	}
	return tid.(int)
}

// Flush writes the completed trace events to a file at lt.tracefilepath
// and can be called concurrently.
func (lt *LocalTracer) Flush() {
	lt.rwmu.RLock()
	defer lt.rwmu.RUnlock()
	// If the file already exists, os.Create will truncate it to zero. This is okay
	// because lt.trace contains all previously emitted events and we rewrite them.
	if tracefile, err := os.Create(lt.tracefilepath); err == nil {
		defer tracefile.Close()
		_ = lt.trace.Encode(tracefile)
	}
}

// Emit emits a trace event and implements the trace.Tracer interface. This
// should never be used directly, instead use trace.Start and trace.Note.
func (lt *LocalTracer) Emit(ctx context.Context, e trace.Event) (context.Context, error) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	switch e.Kind {
	case trace.StartEvent:
		var pid int
		ctx, pid = lt.getPid(ctx, e)
		tid := lt.getTid(e.Id.Short())
		event := Event{
			Pid:  pid,
			Tid:  tid,
			Ts:   e.Time.UnixNano() / 1000, // Ts has to be in microseconds
			Ph:   "X",                      // X indicates a "complete" event in the Chrome tracing format. "Dur" will be filled in later on EndEvent
			Name: e.Name,
			Cat:  e.SpanKind.String(),
			Args: map[string]interface{}{
				"beginTime": e.Time.Format(time.RFC850),
			},
		}
		// store the StartEvent in the ctx so that we can update it on subsequent
		// NoteEvents or complete it when the EndEvent is received
		return context.WithValue(ctx, eventKey, event), nil
	case trace.EndEvent:
		if event, err := eventKey.getEvent(ctx); err == nil {
			lt.rwmu.Lock()
			// convert to microseconds to ensure common units before calculating duration
			event.Dur = (e.Time.UnixNano() / 1000) - event.Ts
			event.Args["endTime"] = e.Time.Format(time.RFC850)
			lt.trace.Events = append(lt.trace.Events, event)
			lt.rwmu.Unlock()
		}
		// don't return a context for EndEvent; it shouldn't be used
		return nil, nil
	case trace.NoteEvent:
		if event, err := eventKey.getEvent(ctx); err == nil {
			// storing the key/val note in the duration event's args will
			// display them in the trace viewer
			lt.rwmu.Lock()
			event.Args[e.Key] = e.Value
			lt.rwmu.Unlock()
		}
		// return the same context; if it contained an event, it will have been updated
		return ctx, nil
	default:
		panic("unsupported trace event kind")
	}
}

// WriteHTTPContext is not implemented for LocalTracer, this stub implements the
// trace.Tracer interface.
func (lt *LocalTracer) WriteHTTPContext(ctx context.Context, header *http.Header) {
	panic("LocalTracer.WriteHTTPContext not implemented")
}

// ReadHTTPContext is not implemented for LocalTracer, this stub implements the
// trace.Tracer interface.
func (lt *LocalTracer) ReadHTTPContext(ctx context.Context, header http.Header) context.Context {
	panic("LocalTracer.ReadHTTPContext not implemented")
}

// CopyTraceContext copies the trace context from src to dst and implements the
// trace.Tracer interface. Do not use directly, instead use trace.CopyTraceContext.
func (lt *LocalTracer) CopyTraceContext(src context.Context, dst context.Context) context.Context {
	event, _ := eventKey.getEvent(src)
	// okay to ignore the error, event will be nil
	return context.WithValue(dst, eventKey, event)
}

// URL returns the location of the output trace file and implements the
// trace.Tracer interface. Do not use directly, instead use trace.URL.
func (lt *LocalTracer) URL(_ context.Context) string {
	return lt.tracefilepath
}

// Event is an event in the Chrome tracing format. The fields are mirrored
// exactly from: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
// Copied from: github.com/grailbio/bigslice/internal/trace/trace.go
type Event struct {
	Pid  int                    `json:"pid"`
	Tid  int                    `json:"tid"`
	Ts   int64                  `json:"ts"`
	Ph   string                 `json:"ph"`
	Dur  int64                  `json:"dur,omitempty"`
	Name string                 `json:"name"`
	Cat  string                 `json:"cat,omitempty"`
	Args map[string]interface{} `json:"args"`
}

// T represents the JSON object format in the Chrome tracing format. For more
// details, see: https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview
// Copied from: github.com/grailbio/bigslice/internal/trace/trace.go
type T struct {
	Events []Event `json:"traceEvents"`
}

// Encode JSON encodes t into outfile.
func (t *T) Encode(w io.Writer) error {
	return json.NewEncoder(w).Encode(t)
}

// Decode decodes the JSON object format read from r into t. Call this with a t
// zero value.
func (t *T) Decode(r io.Reader) error {
	return json.NewDecoder(r).Decode(t)
}
