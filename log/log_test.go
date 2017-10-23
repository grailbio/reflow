package log_test

import (
	"reflect"
	"testing"

	"github.com/grailbio/reflow/log"
)

type outputBuffer struct {
	messages []string
}

func (o *outputBuffer) Output(calldepth int, s string) error {
	o.messages = append(o.messages, s)
	return nil
}

func TestLogger(t *testing.T) {
	var b1, b2 outputBuffer
	l1 := log.New(&b1, log.InfoLevel)
	l2 := l1.Tee(&b2, "two: ")
	l1.Printf("hello, world")
	l2.Error("error")

	if got, want := b1.messages, ([]string{"hello, world", "two: error"}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := b2.messages, ([]string{"error"}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestLevels(t *testing.T) {
	var b outputBuffer
	l := log.New(&b, log.ErrorLevel)
	l.Print("this message should be dropped")
	l.Debug("this too")
	l.Error("i should see this message")
	l.Error("and this")
	if got, want := b.messages, ([]string{"i should see this message", "and this"}); !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

	for _, level := range []log.Level{log.InfoLevel, log.DebugLevel} {
		if l.At(level) {
			t.Errorf("logger at %v", level)
		}
	}
	if !l.At(log.ErrorLevel) {
		t.Error("not at ErrorLevel")
	}
}

func TestMultiOutputter(t *testing.T) {
	var b1, b2 outputBuffer
	l := log.New(log.MultiOutputter(&b1, &b2), log.InfoLevel)
	l.Printf("m")
	want := []string{"m"}
	if got := b1.messages; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := b2.messages; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}
