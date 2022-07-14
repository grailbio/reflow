// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package monitoring

import (
	"fmt"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow/log"
	dto "github.com/prometheus/client_model/go"
)

type oomDetector struct {
	*timeSeries
}

const (
	periodicity = 5 * time.Second

	// memAvailableBytesPrecision is the minimum change in memAvailableBytes that we track.
	memAvailableBytesPrecision float64 = 50 << 20 // 50MiB

	// memAvailableBytesThresholdForOom is the memAvailableBytes threshold,
	// which if reached within a time period, it is considered to have caused a possible OOM.
	memAvailableBytesThresholdForOom = 100 * data.MiB
)

func NewNodeOomDetector(nodeMetricsUrl string, log *log.Logger) *oomDetector {
	m := &oomDetector{timeSeries: newTimeSeries(nodeMetricsUrl, periodicity, []string{memAvailableBytes, memTotalBytes}, log)}
	m.setupConsumerFn()
	return m
}

func (m *oomDetector) setupConsumerFn() {
	consumerFn := m.consumerFn
	m.consumerFn = func(t time.Time, mfs []dto.MetricFamily) {
		for _, mf := range mfs {
			if mf.GetName() != memAvailableBytes || len(mf.GetMetric()) != 1 {
				continue
			}
			metric := mf.GetMetric()[0]
			v, ok := singleValue(mf.GetType(), metric)
			if !ok {
				continue
			}
			v = memAvailableBytesPrecision * float64(1+int64(v/memAvailableBytesPrecision))
			// memAvailableBytes is of type Gauge.
			metric.Gauge.Value = &v
		}
		consumerFn(t, mfs)
	}
}

// Oom implements OomDetector (see reflow/local/oom.go)
func (m *oomDetector) Oom(pid int, start, end time.Time) (bool, string) {
	if pid != -1 {
		return false, ""
	}
	tvs, err := m.query(memTotalBytes, start, end)
	if err != nil {
		m.log.Errorf("oomDetector query %s: %v", memTotalBytes, err)
	}
	if l := len(tvs); l == 0 {
		m.log.Errorf("oomDetector query %s: no values found", memTotalBytes)
		return false, ""
	}
	totBytes := tvs[0]
	tvs, err = m.query(memAvailableBytes, start, end)
	if err != nil {
		m.log.Errorf("oomDetector query %s: %v", memAvailableBytes, err)
	} else {
		minAvail := min(tvs, totBytes)
		minAvailSz, totSz := data.Size(minAvail), data.Size(totBytes)
		if minAvailSz < memAvailableBytesThresholdForOom {
			return true, fmt.Sprintf("possible OOM within period [%s, %s]: min %s: %s (<%s, total: %s)",
				start.Format(time.RFC3339), end.Format(time.RFC3339), memAvailableBytes, minAvailSz, memAvailableBytesThresholdForOom, totSz)
		}
	}
	return false, ""
}
