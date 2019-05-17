package xraytrace

import (
	"github.com/grailbio/reflow/trace"

	"reflect"
	"testing"

	"github.com/grailbio/infra"
)

func TestXrayTracerInfra(t *testing.T) {
	var schema = infra.Schema{
		"tracer": new(trace.Tracer),
	}
	config, err := schema.Make(infra.Keys{
		"tracer": "github.com/grailbio/reflow/trace/xraytrace.Tracer",
	})
	if err != nil {
		t.Fatal(err)
	}
	var tracer trace.Tracer
	config.Must(&tracer)
	_, ok := tracer.(*Tracer)
	if !ok {
		t.Fatalf("%v is not an xraytrace", reflect.TypeOf(t))
	}
}
