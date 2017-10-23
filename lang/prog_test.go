package lang

import (
	"bytes"
	"os"
	"testing"

	"github.com/grailbio/reflow"
	"github.com/grailbio/reflow/test/flow"
)

func newProgramOrError(t *testing.T, file string) *Program {
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
	return p
}

func v1(f *reflow.Flow) *reflow.Flow {
	return f.Canonicalize(reflow.Config{HashV1: true})
}

func TestProgInclude(t *testing.T) {
	p := newProgramOrError(t, "testdata/main.reflow")
	want := v1(&reflow.Flow{
		Op: reflow.OpMerge,
		Deps: []*reflow.Flow{
			{
				Op:  reflow.OpExtern,
				URL: mustURL("s3://blah"),
				Deps: []*reflow.Flow{{
					Op:    reflow.OpExec,
					Cmd:   "\n\techo %s >$out\n",
					Image: "ubuntu",
					Deps: []*reflow.Flow{{
						Op:  reflow.OpIntern,
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
	want := v1(&reflow.Flow{
		Op: reflow.OpMerge,
		Deps: []*reflow.Flow{
			{
				Op:  reflow.OpExtern,
				URL: mustURL("file://output"),
				Deps: []*reflow.Flow{{
					Op:  reflow.OpIntern,
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
	want := v1(flow.Merge(flow.Extern("s3://output",
		flow.Pullup(
			flow.Merge(flow.Intern("s3://arg1"), flow.Intern("s3://arg2")),
			flow.Intern("s3://input")))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestInternRewrite(t *testing.T) {
	p := newProgramOrError(t, "testdata/internrewrite.reflow")
	p.Args = []string{"s3://arg1", "s3://arg2"}
	want := v1(flow.Merge(flow.Extern("s3://output",
		flow.Pullup(
			flow.Merge(flow.Intern("s3://arg1"), flow.Intern("s3://arg2")),
			flow.Collect(`\.`, "theinput", flow.Intern("s3://bucket/input"))))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestInternList(t *testing.T) {
	p := newProgramOrError(t, "testdata/internlist.reflow")
	want := v1(flow.Merge(flow.Extern("s3://output",
		flow.Pullup(
			flow.Collect(`^`, "dir1/", flow.Intern("s3://bucket1/dir1/")),
			flow.Collect(`^`, "dir2/", flow.Intern("s3://bucket2/dir2/")),
			flow.Collect(`\.`, "file3", flow.Intern("s3f://bucket3/file3"))))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}

	p = newProgramOrError(t, "testdata/internlist2.reflow")
	want = v1(flow.Merge(flow.Extern("s3://output",
		flow.Pullup(flow.Collect(`\.`, "file", flow.Intern("s3f://bucket3/file"))))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}

}

func TestInternListRewrite(t *testing.T) {
	p := newProgramOrError(t, "testdata/internlistrewrite.reflow")
	want := v1(flow.Merge(flow.Extern("s3://output",
		flow.Pullup(
			flow.Collect(`^`, "xyz/", flow.Intern("s3://bucket1/dir1/")),
			flow.Collect(`^`, "dir2/", flow.Intern("s3://bucket2/dir2/")),
			flow.Collect(`\.`, "thefile", flow.Intern("s3f://bucket3/file3"))))))
	if got := p.Eval(); got.Digest() != want.Digest() {
		t.Errorf("got %v, want %v", got.DebugString(), want.DebugString())
	}
}

func TestEscape(t *testing.T) {
	p := newProgramOrError(t, "testdata/escape.reflow")
	want := v1(flow.Merge(flow.Extern("s3://output",
		flow.Exec("x", `
	echo %%
	%s
`, reflow.Resources{}, flow.Intern("s3://input")))))
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
