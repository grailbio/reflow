// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

// +build !unit regress

package regress

import (
	"flag"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

var (
	binary   = flag.String("regress_test.binary", "", "reflow binary to use for the test")
	localDir = flag.String("regress_test.localdir", "", "local dir to use for the test")
)

// TestRegress performs regression checking, and requires AWS credentials for file transfers.
func TestRegress(t *testing.T) {
	if *binary == "" {
		const reflow = "./test.reflow"
		cmd := exec.Command("go", "build", "-o", reflow, "github.com/grailbio/reflow/cmd/reflow")
		if err := cmd.Run(); err != nil {
			t.Fatalf("go build: %s", err)
		}
		defer os.Remove(reflow)
		*binary = reflow
	}
	tests, err := ioutil.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-log", "debug", "run", "-local"}
	if "" != *localDir {
		args = append(args, "-localdir", *localDir)
	}
	for _, test := range tests {
		t.Run(test.Name(), func(t *testing.T) {
			if filepath.Ext(test.Name()) != ".rf" || filepath.Base(test.Name()) == "generate.rf" {
				return
			}
			t.Parallel()
			testargs := append(args, filepath.Join("testdata", test.Name()))
			// Run using (reflow) cache.
			cmd := exec.Command(*binary, testargs...)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("%s: %s\n%s", test.Name(), err, string(out))
			}

			// Run without (reflow) cache.
			cmd = exec.Command(*binary, append([]string{"-cache", "off"}, testargs...)...)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Errorf("%s (-cache=off): %s\n%s", test.Name(), err, string(out))
			}
		})
	}
}
