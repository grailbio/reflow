// Copyright 2019 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package tool

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/grailbio/testutil"
)

func TestBatch(t *testing.T) {
	dir, cleanup := testutil.TempDir(t, "", "")
	defer cleanup()
	var (
		cmd Cmd
		ctx = context.Background()
	)
	saveDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(saveDir); err != nil {
			t.Fatal(err)
		}
	}()
	if err := os.Chdir("testdata"); err != nil {
		t.Fatal(err)
	}
	filename := filepath.Join(dir, "batch.rf")
	cmd.genbatch(ctx, "-o", filename)
	p, err := ioutil.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(p), `val Main = [
	make("test.rf",
		b := true,
		d := dir("s3://grail-marius/1.dir/"),
		f := file("s3://grail-marius/1.file"),
		fl := 1.1,
		i := 1,
		s := "a",
	).Main,
	make("test.rf",
		b := false,
		d := dir("s3://grail-marius/2.dir/"),
		f := file("s3://grail-marius/2.file"),
		fl := 2.2,
		i := 2,
		s := "b",
	).Main,
]
`; !strings.HasSuffix(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}

}
