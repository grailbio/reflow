package values

import (
	"testing"

	"github.com/grailbio/reflow/types"
)

func TestDigest(t *testing.T) {
	typ := types.Map(
		types.String,
		types.Struct(
			&types.Field{"field1", types.Int},
			&types.Field{"field2", types.String}))
	m := make(Map)
	m["hello"] = Struct{
		"field1": NewInt(123),
		"field2": T("hello world"),
	}
	m["world"] = Struct{
		"field1": NewInt(321),
		"field2": T("foo bar"),
	}
	d, _ := Digester.Parse("sha256:c1c3e68de6ccf619538b5810a4feaeac5049505b7719ad67321f62d0c63f52a9")
	if got, want := Digest(m, typ), d; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}
