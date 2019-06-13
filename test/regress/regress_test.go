// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit regress

package regress

import (
	"flag"
	"io/ioutil"
	"os/exec"
	"path/filepath"
	"testing"
)

var binary = flag.String("regress_test.binary", "", "reflow binary to use for the test")

// TestRegress performs regression checking, and requires AWS credentials for file transfers.
func TestRegress(t *testing.T) {
	if *binary == "" {
		t.Fatalf("usage: go test -regress_test.binary </path/to/binary>")
	}
	infos, err := ioutil.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	for _, info := range infos {
		if filepath.Ext(info.Name()) != ".rf" {
			continue
		}
		cmd := exec.Command(*binary, "run", "-local", filepath.Join("testdata", info.Name()))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Errorf("%s: %s\n%s", info.Name(), err, string(out))
		}
	}
}
