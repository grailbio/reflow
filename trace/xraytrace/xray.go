package xraytrace

import (
	"context"
	"fmt"
	"net/http"
	"regexp"

	"github.com/aws/aws-xray-sdk-go/header"
	"github.com/aws/aws-xray-sdk-go/strategy/sampling"
	"github.com/aws/aws-xray-sdk-go/xray"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/trace"
)

func init() {
	b := []byte(`{
		"version": 1,
		"default": {
			"fixed_target": 1,
			"rate": 1.0
		},
		"rules": [ ]
	}`)
	l, err := sampling.NewLocalizedStrategyFromJSONBytes(b)
	if err != nil {
		log.Debugf("parse xray strategy: %s", err)
		l, _ = sampling.NewLocalizedStrategy()
	}
	addr := "127.0.0.1:2000"
	err = xray.Configure(xray.Config{
		// TODO(pgopal) - Make the daemon address configurable
		DaemonAddr:       addr,    // default
		LogLevel:         "error", // default
		SamplingStrategy: l,
	})
	if err != nil {
		log.Debug("xray: ", err)
	}
}

// Tracer is the reflow tracer implementation for xray.
type tracer struct{}

// Xray is the xray tracer.
var Xray trace.Tracer = tracer{}

const xrayHttpHeaderName = "x-aws-xray-trace"

// WriteHTTPContext writes the trace context to the HTTP header.
func (tracer) WriteHTTPContext(ctx context.Context, h *http.Header) {
	seg := xray.GetSegment(ctx)
	if seg == nil {
		return
	}
	h.Add(xrayHttpHeaderName, seg.DownstreamHeader().String())
}

// ReadHTTPContext reads the trace context from HTTP headers and returns a new context with the trace context.
func (tracer) ReadHTTPContext(ctx context.Context, h http.Header) context.Context {
	str := h.Get(xrayHttpHeaderName)
	if str == "" {
		return ctx
	}
	xrayheader := header.FromString(str)
	if xrayheader == nil {
		return ctx
	}
	ctx, _ = xray.BeginFacadeSegment(ctx, "", xrayheader)
	return ctx
}

// CopyTraceContext copies the trace context from src to dst.
func (tracer) CopyTraceContext(src, dst context.Context) context.Context {
	return context.WithValue(dst, xray.ContextKey, xray.GetSegment(src))
}

// Emit emits a trace event.
func (tracer) Emit(ctx context.Context, e trace.Event) (context.Context, error) {
	switch e.Kind {
	case trace.StartEvent:
		var seg *xray.Segment
		// Segment name has some restrictions:
		// https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html#api-segmentdocuments-fields
		r := regexp.MustCompile(`[^a-zA-Z0-9_.:/%&#=+\-@ ]+`)
		name := r.ReplaceAllString(e.Name, " ")
		if len(name) > 200 {
			name = name[:197] + "..."
		}
		switch e.SpanKind {
		case trace.Run:
			ctx, seg = xray.BeginSegment(ctx, name)
		default:
			parent := xray.GetSegment(ctx)
			if parent != nil {
				h := parent.DownstreamHeader()
				ctx, seg = xray.NewSegmentFromHeader(ctx, name, h)
			} else {
				ctx, seg = xray.BeginSegment(ctx, name)
			}
		}
		seg.AddAnnotation("id", e.Id.String())
		seg.AddAnnotation("kind", e.SpanKind.String())
		seg.AddAnnotation("name", e.Name)
		return ctx, nil
	case trace.EndEvent:
		seg := xray.GetSegment(ctx)
		if seg == nil {
			return ctx, fmt.Errorf("event not found %+v", e.Id)
		}
		seg.Close(nil)
		return ctx, nil
	case trace.NoteEvent:
		s := xray.GetSegment(ctx)
		if s == nil {
			return ctx, fmt.Errorf("no current segment")
		}
		err := s.AddAnnotation(e.Key, e.Value)
		return ctx, err
	}
	return ctx, nil
}

// URL returns the trace URL.
func (tracer) URL(ctx context.Context) string {
	id := xray.TraceID(ctx)
	var url string
	if id != "" {
		url = "https://console.aws.amazon.com/xray/home#/traces/" + id
	}
	return url
}
