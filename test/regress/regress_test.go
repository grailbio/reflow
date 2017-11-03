// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit regress

package regress

import (
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestRegress performs regression checking, and requires AWS credentials for file transfers.
func TestRegress(t *testing.T) {
	const reflow = "./test.reflow"
	cmd := exec.Command("go", "build", "-o", reflow, "github.com/grailbio/reflow/cmd/reflow")
	if err := cmd.Run(); err != nil {
		t.Fatalf("go build: %s", err)
	}
	defer os.Remove(reflow)
	infos, err := ioutil.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range infos {
		if filepath.Ext(info.Name()) != ".rf" {
			continue
		}
		cmd := exec.Command(reflow, "run", "-local", filepath.Join("testdata", info.Name()))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("%s: %s\n%s", info.Name(), err, string(out))
		}
	}
}
