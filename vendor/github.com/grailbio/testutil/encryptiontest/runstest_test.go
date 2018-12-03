// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package encryptiontest_test

import (
	"strings"
	"testing"

	"github.com/grailbio/testutil/encryptiontest"
)

func TestRuns(t *testing.T) {
	for i, tc := range []struct {
		in                 []byte
		median             byte
		above, below, runs int
	}{
		{[]byte{0, 0}, 0, 0, 0, 1},
		{[]byte{0, 1}, 0, 1, 0, 1},
		{[]byte{0, 129}, 64, 1, 1, 2},
		{[]byte{0, 129, 128}, 128, 1, 1, 2},
		{[]byte{0, 128, 127, 129}, 127, 2, 1, 2},
	} {
		median := encryptiontest.Median(tc.in)
		if got, want := median, tc.median; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		above, below, runs := encryptiontest.MedianRuns(tc.in)
		if got, want := above, tc.above; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		if got, want := below, tc.below; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
		if got, want := runs, tc.runs; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}
}

func TestRunStatistic(t *testing.T) {
	for i, tc := range []struct {
		n1, n2 int
		want   float64
	}{
		{10, 11, 11.476190476190476},
		{18, 22, 20.8},
	} {
		if got, want := encryptiontest.ExpectedRuns(tc.n1, tc.n2), tc.want; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}

	for i, tc := range []struct {
		n1, n2 int
		want   float64
	}{
		{10, 11, 4.963718820861678},
		{18, 22, 9.544615384615385},
	} {
		if got, want := encryptiontest.Variance(tc.n1, tc.n2), tc.want; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}

	for i, tc := range []struct {
		runs, n1, n2 int
		want         float64
	}{
		{11, 10, 10, 0},
		{20, 18, 22, 0.25894693252097933},
	} {
		if got, want := encryptiontest.RunsTestStatistic(tc.runs, tc.n1, tc.n2), tc.want; got != want {
			t.Errorf("%d: got %v, want %v", i, got, want)
		}
	}
}

func TestRunAt(t *testing.T) {
	fr := 0
	failAt := 7
	failed, expected, iterations, success := encryptiontest.RunAtSignificanceLevel(
		encryptiontest.OnePercent, func(_ encryptiontest.Significance) bool { fr++; return fr > failAt })

	if got, want := failed, failAt; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := expected, 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := iterations, 200; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := success, false; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestIsRandom(t *testing.T) {
	t.Skipf("Reenable after T1997 is fixed")
	for _, tc := range reliableGenerators {
		for _, significance := range []encryptiontest.Significance{
			encryptiontest.FivePercent,
			encryptiontest.OnePercent,
			encryptiontest.PointTwoPercent,
		} {
			attempts := 0
			flakyAttempts := 2
			for {
				var failed, expectedFailures, iterations int
				var israndom bool
				if !tc.fixed {
					failed, expectedFailures, iterations, israndom = encryptiontest.RunAtSignificanceLevel(significance,
						func(s encryptiontest.Significance) bool {
							data := tc.generator(encryptiontest.MinDataSize)
							return encryptiontest.IsRandom(data, s)
						})
				} else {
					iterations = 1
					data := tc.generator(encryptiontest.MinDataSize)
					israndom = encryptiontest.IsRandom(data, significance)
				}

				t.Logf("%v", tc.name)
				t.Logf("%v", strings.Repeat("=", len(tc.name)))
				t.Logf("level, iterations: %v, %v", significance, iterations)
				t.Logf("  failed/expected: %v/%v", failed, expectedFailures)
				t.Logf("        is random: %t", israndom)
				if got, want := israndom, tc.random; got != want {
					// We allow a 'flaky' test to fail some number of times with
					// a single success counting as success. These tests are not
					// 'flaky' in the software sense, rather they are statistical
					// in nature and will naturally fail in some cases. The
					// correct statistical approach is to monitor the failures
					// and make sure that they fit the expected distribution.
					// However, for now, we'll just stop the test failing when
					// the stats don't work out!
					if !tc.flaky || attempts >= flakyAttempts {
						t.Errorf("%v: FAIL: got %v, want %v", tc.name, got, want)
					}
					attempts++
				} else {
					break
				}
			}
		}
	}
}
