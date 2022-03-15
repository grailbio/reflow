package values

import (
	"reflect"
	"testing"

	"github.com/grailbio/reflow"
)

var (
	f0 = reflow.File{}
	f1 = reflow.File{ID: reflow.Digester.FromString("f1 contents"), Size: 1}
	f2 = reflow.File{ID: reflow.Digester.FromString("f2 contents"), Size: 2}
	f3 = reflow.File{ID: reflow.Digester.FromString("f3 contents"), Size: 3}
	f4 = reflow.File{ID: reflow.Digester.FromString("f4 contents"), Size: 4}
	f5 = reflow.File{ID: reflow.Digester.FromString("f5 contents"), Size: 5}
)

func TestMutableDir(t *testing.T) {
	contents := map[string]reflow.File{"a": f1, "b": f2, "d": f3, "c": f4}
	var md MutableDir
	for k, v := range contents {
		md.Set(k, v)
	}
	var d Dir
	d.AddContents(contents)
	if got, want := md.Dir(), d; !Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSumDir(t *testing.T) {
	var d1, d2 Dir
	d1.AddContents(map[string]reflow.File{"c": f4, "a": f1, "b": f2})
	d2.AddContents(map[string]reflow.File{"d": f3, "c": f5})
	if got, want := d1.SortedKeys(), []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := d2.SortedKeys(), []string{"c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	d := SumDir(d1, d2)

	if got, want := d.SortedKeys(), []string{"a", "b", "c", "d"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	for _, tt := range []struct {
		key  string
		want reflow.File
	}{
		{"a", f1}, {"b", f2}, {"c", f5}, {"d", f3}, {"e", f0},
	} {
		if got, _ := d.Lookup(tt.key); got != tt.want {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}

	d = SumDir(d2, d1)
	for _, tt := range []struct {
		key  string
		want reflow.File
	}{
		{"a", f1}, {"b", f2}, {"c", f4}, {"d", f3}, {"e", f0},
	} {
		if got, _ := d.Lookup(tt.key); got != tt.want {
			t.Errorf("got %v, want %v", got, tt.want)
		}
	}
}

func TestEqualDir(t *testing.T) {
	var d1, d2, d3 Dir
	d1.AddContents(map[string]reflow.File{"c": f4, "a": f1, "b": f2})
	d2.AddContents(map[string]reflow.File{"d": f3, "c": f5})
	d3.AddContents(map[string]reflow.File{"d": f3, "c": f5, "a": f1, "b": f2})

	if got, want := d1.Equal(d2), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := d1.Equal(d3), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	d := SumDir(d1, d2) // must equal d3.
	if got, want := d.Equal(d3), true; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	d = SumDir(d2, d1)
	if got, want := d.Equal(d3), false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
