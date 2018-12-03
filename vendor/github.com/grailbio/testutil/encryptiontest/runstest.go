// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package encryptiontest

import (
	"fmt"
	"math"
	"sort"
)

// Median returns the median value of the supplied byte slice.
func Median(data []byte) byte {
	sorted := make([]byte, len(data))
	copy(sorted, data)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	middle := len(sorted) / 2
	result := int(sorted[middle])
	if len(sorted)%2 == 0 {
		result = (result + int(sorted[middle-1])) / 2
	}
	return byte(result & 0xff)
}

// MedianRuns returns the number of runs in the supplied byte slice where a run
// is defined as a sequence of consecutive positive (> median) or negative
// (< median) values. Median values are ignored.
func MedianRuns(data []byte) (above, below, runs int) {
	if len(data) == 0 {
		panic("zero length slice")
	}
	median := Median(data)
	runs = 1
	hasprev := false
	prev := false
	for _, d := range data {
		if d == median {
			continue
		}
		switch {
		case d < median:
			below++
		case d > median:
			above++
		}
		if hasprev && (prev != (d < median)) {
			runs++
		}
		hasprev = true
		prev = d < median
	}
	return
}

// ExpectedRuns returns the approximation for the number of expected runs when
// n1 and n2 are the number of positive/negative values in a sample.
// expeted runs = 2 * n1 * n2 / n1 + n2
// n1 and n2 must be >= 10.
func ExpectedRuns(n1, n2 int) float64 {
	if n1 < 10 || n2 < 10 {
		panic("n1 and n2 must be >= 10")
	}
	return (float64(2*n1*n2) / float64(n1+n2)) + 1
}

// Variance returns the approximation to the standard deviation
// of the number of runs where n1 and n2 are the number of positive/negative
// values in a sample.
// variance =  2 * n1 * n2 * (2 * n1 * n2 − n1 − n2)
//             -------------------------------------
//                  (n1 + n2)^2 * (n1 + n2 − 1)
// n1 and n2 must be >= 10
func Variance(n1, n2 int) float64 {
	if n1 < 10 || n2 < 10 {
		panic("n1 and n2 must be >= 10")
	}
	n1f := float64(n1)
	n2f := float64(n2)
	numerator := 2 * n1f * n2f * (2*n1f*n2f - n1f - n2f)
	denominator := (n1f + n2f) * (n1f + n2f) * (n1f + n2f - 1)
	return numerator / denominator
}

// RunsTestStatistic calculates the 'runs test' stastistic as per:
// http://www.itl.nist.gov/div898/handbook/eda/section3/eda35d.htm
// and other sources.
func RunsTestStatistic(runs, n1, n2 int) float64 {
	expected := ExpectedRuns(n1, n2)
	stdev := math.Sqrt(Variance(n1, n2))
	return math.Abs((float64(runs) - expected) / stdev)
}

var pZp = map[Significance]float64{
	0.999: +3.090,
	0.995: +2.576,
	0.990: +2.326,
	0.975: +1.960,
	0.950: +1.645,
	0.900: +1.282,
}

// Significance represents the statistical significance level acceptable for
// a given test. Note that the significance level represents the chance of a
// false positive, so choosing the .2% level implies a .2% change of a false
// positive.
type Significance float64

const (
	// PointTwoPercent represents a 0.2% significance level.
	PointTwoPercent Significance = 0.999
	// OnePercent represents a 1% significance level.
	OnePercent Significance = 0.995
	// FivePercent represents a 5% significance level.
	FivePercent Significance = 0.975
)

func (s Significance) String() string {
	switch s {
	case PointTwoPercent:
		return "0.2%"
	case OnePercent:
		return "1.0%"
	case FivePercent:
		return "5.0%"
	default:
		panic("unreachable")
		return ""
	}
}

// IsTestStatisticRandom returns true if the suipplied test statistic value
// meets the criteria defined in
// http://www.itl.nist.gov/div898/handbook/eda/section3/eda35d.htm
// for being random at the requested significance level.
// Also see: http://influentialpoints.com/Training/runs_tests-principles-properties-assumptions.htm
// and note the assumptions:
// - In its usual form, the test is not sensitive to departures
//   from randomness for run lengths of two.
// - The runs test has the wrong type error rate if used to evaluate the
//   independence of errors in time-series regression models.
func IsTestStatisticRandom(t float64, significance Significance) bool {
	if t < 0 {
		t = -t
	}
	return t <= pZp[significance]
}

// IsRandom uses the runstest to determine if the supplied data is random
// at the requested significance level. Note, that, the test is expected to
// fail (at the rate implied by the sigfnificance level) even if the data is
// random. Hence any user of this test should take that into account; the
// RunAtSignificanceLevel function in this package is provided as a convenience
// for doing so.
func IsRandom(buf []byte, significance Significance) bool {
	above, below, runs := MedianRuns(buf)
	tstat := RunsTestStatistic(runs, above, below)
	return IsTestStatisticRandom(tstat, significance)
}

// MinDataSize is a suggested minimum data size for the runstest to be
// meaingfully and reliably used.
const MinDataSize = 20000

// RunAtSignificanceLevel runs the supplied function sufficient times to
// measure the false positive rate for the requested significance level. The
// supplied function should return the success of the test being measured.
// returns true if the function meets the requested significance level.
func RunAtSignificanceLevel(significance Significance, fn func(s Significance) bool) (failed int, expectedFailures int, iterations int, success bool) {
	switch significance {
	case PointTwoPercent:
		iterations = 500
		expectedFailures = 1
	case OnePercent:
		iterations = 200
		expectedFailures = 2
	case FivePercent:
		iterations = 200
		expectedFailures = 10
	default:
		panic(fmt.Sprintf("unrecognised significance level: %v", significance))
		return
	}
	for i := 0; i < iterations; i++ {
		if !fn(significance) {
			failed++
		}
	}
	return failed, expectedFailures, iterations, failed <= expectedFailures
}
