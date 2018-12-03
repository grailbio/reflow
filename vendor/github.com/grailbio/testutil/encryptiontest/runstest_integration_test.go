// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// +build !unit

package encryptiontest_test

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/testutil"
	"github.com/grailbio/testutil/encryptiontest"
	"v.io/x/lib/gosh"
)

func dieharder(t *testing.T, sh *gosh.Shell, name string, data []byte) (string, bool) {
	// Tests 15 and 101 represent the dieharder runs test and the STS runs test.
	// -g 200 means 'read data from stdin'.
	for _, tst := range []string{"15", "101"} {
		cl := []string{"dieharder", "-d", tst, "-g", "200"}
		cmd := sh.Cmd(cl[0], cl[1:]...)
		cmd.SetStdinReader(bytes.NewReader(data))
		output := cmd.CombinedOutput()
		if !strings.Contains(output, "PASSED") || strings.Contains(output, "FAILED") {
			return output, false
		}
	}
	return "", true
}

func TestData(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	sh := gosh.NewShell(t)
	defer testutil.NoCleanupOnError(t, sh.Cleanup)

	// Test that dieharder and our runstest agree.
	for _, tc := range reliableGenerators {
		then := time.Now()
		fmt.Fprintf(os.Stderr, "TestData: %v\n", tc.name)
		dataSize := 4 * 20000000 // dieharder wants 20M 4-byte (int) datapoints.

		var dieharderResult, runstestResult bool
		significance := encryptiontest.OnePercent
		dieharderOutput := ""

		fmt.Fprintf(os.Stderr, "TestData: %v @ %v\n", tc.name, significance)
		if !tc.fixed {
			_, _, _, dieharderResult = encryptiontest.RunAtSignificanceLevel(significance,
				func(s encryptiontest.Significance) bool {
					data := tc.generator(dataSize)
					out, result := dieharder(t, sh, tc.name, data)
					dieharderOutput += out
					return result
				})
			_, _, _, runstestResult = encryptiontest.RunAtSignificanceLevel(significance,
				func(s encryptiontest.Significance) bool {
					data := tc.generator(dataSize)
					return encryptiontest.IsRandom(data, s)
				})
		} else {
			data := tc.generator(dataSize)
			dieharderOutput, dieharderResult = dieharder(t, sh, tc.name, data)
			runstestResult = encryptiontest.IsRandom(data, significance)
		}

		if dieharderResult != runstestResult {
			t.Errorf("%v: dieharder and runstest disagree, dieharder passed %v, runstest pass %v, significance: %v", tc.name, dieharderResult, runstestResult, significance)
			if got, want := dieharderResult, tc.random; got != want {
				t.Logf("%v: dieharder unexpected result: got %v, want %v:\n%v\n", tc.name, got, want, dieharderOutput)
			}
			if got, want := runstestResult, tc.random; got != want {
				t.Logf("%v: runstest unexpected result: got %v, want %v", tc.name, got, want)
			}
		}

		fmt.Fprintf(os.Stderr, "TestData: %v, done: %v\n", tc.name, time.Now().Sub(then))
	}
}
