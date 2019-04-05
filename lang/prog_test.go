// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package lang

import (
	"bytes"
	"os"
	"testing"

	types2 "github.com/grailbio/reflow/types"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/flow"
	op "github.com/grailbio/reflow/test/flow"
)

func newProgramOrError(t *testing.T, file string) *Program {
	t.Helper()
	var buf bytes.Buffer
	p := &Program{Errors: &buf, File: file}
	f, err := os.Open(p.File)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := p.ParseAndTypecheck(f); err != nil {
		t.Fatalf("error %v: %s", err, buf.String())
	}
	if mainType := p.ModuleType().FieldMap()["Main"]; mainType != types2.Fileset {
		t.Fatalf("expected Main to be %v, got %v ", types2.Fileset, mainType)
	}
	return p
}

func v1(f *flow.Flow) *flow.Flow {
	return f.Canonicalize(flow.Config{HashV1: true})
}

func TestProgInclude(t *testing.T) {
	p := newProgramOrError(t, "testdata/main.reflow")
	want := v1(&flow.Flow{
		Op: flow.Merge,
		Deps: []*flow.Flow{
			{
				Op:  flow.Extern,
				URL: mustURL("s3://blah"),
				Deps: []*flow.Flow{{
					Op:    flow.Exec,
					Cmd:   "\n\techo %s >$out\n",
					Image: "ubuntu",
					Deps: []*flow.Flow{{
						Op:  flow.Intern,
						URL: mustURL("file://blah"),
					}},
				}},
			},
		},
	})
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestProgParams(t *testing.T) {
	p := newProgramOrError(t, "testdata/param.reflow")
	flags := p.Flags()
	inputFlag := flags.Lookup("input")
	if inputFlag == nil {
		t.Fatalf("missing input flag")
	}
	if got, want := inputFlag.Usage, "the input directory"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	outputFlag := flags.Lookup("output")
	if outputFlag == nil {
		t.Fatalf("missing output flag")
	}
	if got, want := outputFlag.Usage, "the output directory"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	inputFlag.Value.Set("file://input")
	outputFlag.Value.Set("file://output")
	want := v1(&flow.Flow{
		Op: flow.Merge,
		Deps: []*flow.Flow{
			{
				Op:  flow.Extern,
				URL: mustURL("file://output"),
				Deps: []*flow.Flow{{
					Op:  flow.Intern,
					URL: mustURL("file://input"),
				}},
			},
		},
	})
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestIntern(t *testing.T) {
	p := newProgramOrError(t, "testdata/intern.reflow")
	p.Args = []string{"s3://arg1", "s3://arg2"}
	want := v1(op.Merge(op.Extern("s3://output",
		op.Pullup(
			op.Merge(op.Intern("s3://arg1"),
				op.Intern("s3://arg2")),
			op.Intern("s3://input")))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestInternRewrite(t *testing.T) {
	p := newProgramOrError(t, "testdata/internrewrite.reflow")
	p.Args = []string{"s3://arg1", "s3://arg2"}
	want := v1(op.Merge(op.Extern("s3://output",
		op.Pullup(
			op.Merge(op.Intern("s3://arg1"),
				op.Intern("s3://arg2")),
			op.Collect(`\.`, "theinput", op.Intern("s3://bucket/input"))))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestInternList(t *testing.T) {
	p := newProgramOrError(t, "testdata/internlist.reflow")
	want := v1(op.Merge(op.Extern("s3://output",
		op.Pullup(
			op.Collect(`^`, "dir1/", op.Intern("s3://bucket1/dir1/")),
			op.Collect(`^`, "dir2/", op.Intern("s3://bucket2/dir2/")),
			op.Collect(`\.`, "file3", op.Intern("s3f://bucket3/file3"))))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}

	p = newProgramOrError(t, "testdata/internlist2.reflow")
	want = v1(op.Merge(op.Extern("s3://output",
		op.Pullup(op.Collect(`\.`, "file", op.Intern("s3f://bucket3/file"))))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}

}

func TestInternListRewrite(t *testing.T) {
	p := newProgramOrError(t, "testdata/internlistrewrite.reflow")
	want := v1(op.Merge(op.Extern("s3://output",
		op.Pullup(
			op.Collect(`^`, "xyz/", op.Intern("s3://bucket1/dir1/")),
			op.Collect(`^`, "dir2/", op.Intern("s3://bucket2/dir2/")),
			op.Collect(`\.`, "thefile", op.Intern("s3f://bucket3/file3"))))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestEscape(t *testing.T) {
	p := newProgramOrError(t, "testdata/escape.reflow")
	want := v1(op.Merge(op.Extern("s3://output",
		op.Exec("x", `
	echo %%
	%s
`, reflow.Resources{}, op.Intern("s3://input")))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestHash(t *testing.T) {
	p := newProgramOrError(t, "testdata/hash.reflow")
	for v := p.Eval().Visitor(); v.Walk(); v.Visit() {
		if v.Config.HashV1 {
			t.Errorf("expected !hashv1 for %v", v)
		}
	}
}
