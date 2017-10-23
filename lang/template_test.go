package lang

import (
	"reflect"
	"testing"
)

func TestTemplate(t *testing.T) {
	tmpl, err := newTemplate(`hello%there {{foobar}}, {{ok}}`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := tmpl.Idents, []string{"foobar", "ok"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	s := tmpl.Format("blah", "bloop")
	if got, want := s, "hello%%there blah, bloop"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
