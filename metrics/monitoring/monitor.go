// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package monitoring

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

type MetricsMonitor interface {
	// Go starts monitoring of metrics using the given context.
	// Typically Go would never return until context cancellation and hence should be run in a goroutine.
	Go(ctx context.Context)
}

// monitor periodically (every p duration) fetches prometheus metrics using fetcherFn and pushes them to consumerFn.
type monitor struct {
	url string
	p   time.Duration
	log *log.Logger

	fetcherFn  func(ctx context.Context) (time.Time, []dto.MetricFamily, error)
	consumerFn func(t time.Time, mfs []dto.MetricFamily)
}

// Go starts monitoring of prometheus metrics and returns only upon ctx cancellation.
func (m *monitor) Go(ctx context.Context) {
	if m.fetcherFn == nil {
		panic("monitor.fetcherFn must be set")
	}
	if m.consumerFn == nil {
		panic("monitor.consumerFn must be set")
	}
	iter := time.NewTicker(m.p)
	for {
		nctx, cancel := context.WithTimeout(ctx, time.Second)
		if t, mfs, err := m.fetcherFn(nctx); err == nil {
			m.consumerFn(t, mfs)
		} else {
			m.log.Debugf("pressure monitor fetchUrl %s: %v", m.url, err)
		}
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-iter.C:
		}
	}
}

// NewMetricsLogger periodically (every p duration) fetches prometheus metrics from the given url and
// logs them to the given logger. If restrict is provided, only the metrics specified will be logged.
func NewMetricsLogger(url string, p time.Duration, restrict map[string]bool, log *log.Logger) *monitor {
	if url == "" {
		panic("no url specified")
	}
	f := &urlFetcher{url}
	return &monitor{url: url, p: p, log: log, fetcherFn: f.fetchUrl,
		consumerFn: func(t time.Time, mfs []dto.MetricFamily) {
			for _, mf := range mfs {
				if n := mf.GetName(); len(restrict) > 0 && !restrict[n] {
					continue
				}
				if s := toString(mf); s != "" {
					log.Printf("%s: %s  (%s)", mf.GetName(), s, mf.GetHelp())
				}
			}
			log.Printf("dump %s completed (%s)", url, time.Now().Sub(t))
		},
	}
}

type periodValue struct {
	first, last time.Time
	v           float64
}

func (tv periodValue) String() string {
	return fmt.Sprintf("(%s, %s): %v", tv.first.Format(time.RFC3339), tv.last.Format(time.RFC3339), tv.v)
}

// timeSeries maintains a time series of values for each "tracked" metric as a series of periodValues.
// This means the time series "condensed" to only track changes in values. ie, we keep track of the time-period
// (if applicable) for which a particular value was seen and whenever it changes, we record a new periodValue.
// Note that this is only efficient in the following cases:
// - if the metrics being tracked change infrequently (eg: node_pressure_memory_stalled_seconds_total)
//   or never (eg: node_memory_MemTotal_bytes)
// - the underlying monitor.consumerFn is modified to round values to some meaningful precision
//   (eg: node_memory_MemAvailable_bytes can be rounded to the nearest 50MiB, to ignore minor changes)
type timeSeries struct {
	*monitor

	mu         sync.Mutex
	timeSeries map[string][]*periodValue
}

// newTimeSeries returns a timeSeries monitor which periodically (every p duration) monitors prometheus
// metrics (restricted to the given names), from the given URL and maintains their time series.
func newTimeSeries(url string, p time.Duration, names []string, log *log.Logger) *timeSeries {
	m := timeSeries{timeSeries: make(map[string][]*periodValue)}
	for _, name := range names {
		m.timeSeries[name] = []*periodValue{}
	}
	f := &urlFetcher{url}
	m.monitor = &monitor{url: url, p: p, log: log, fetcherFn: f.fetchUrl, consumerFn: m.update}
	return &m
}

// query returns the list of values for the metric named name, within the given time period [start, end].
// Query returns results based on the intersection of the given time period [start, end]
// and the recorded time period [t_1, t_n].
func (m *timeSeries) query(name string, start, end time.Time) (tvs []float64, err error) {
	if !start.Before(end) {
		err = fmt.Errorf("query %s: start %s must be before end %s", name, start, end)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	changes, ok := m.timeSeries[name]
	if !ok {
		err = fmt.Errorf("metric %s not tracked", name)
		return
	}
	startIndex := sort.Search(len(changes), func(i int) bool { return !changes[i].first.Before(start) })
	if n := len(changes); startIndex == n && changes[n-1].last.Before(start) {
		// start is past all the recorded changes.
		return
	}
	if startIndex > 0 && changes[startIndex-1].last.After(start) {
		startIndex--
	}
	endIndex := sort.Search(len(changes), func(i int) bool { return changes[i].last.After(end) })
	if n := len(changes); n > 0 && endIndex == n {
		endIndex--
	}
	if endIndex >= startIndex {
		for i := startIndex; i <= endIndex; i++ {
			tvs = append(tvs, changes[i].v)
		}
	}
	return
}

// update updates the time series with the metrics seen at time t.
func (m *timeSeries) update(t time.Time, mfs []dto.MetricFamily) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, mf := range mfs {
		name := mf.GetName()
		if m.timeSeries[name] == nil {
			// metric not tracked
			continue
		}
		if len(mf.GetMetric()) != 1 {
			// Only single-valued metrics are supported.
			continue
		}
		v, ok := singleValue(mf.GetType(), mf.GetMetric()[0])
		if !ok {
			continue
		}
		changes, ok := m.timeSeries[name]
		if !ok || len(changes) == 0 {
			changes = append(changes, &periodValue{first: t, last: t, v: v})
		} else if last := changes[len(changes)-1]; last.v != v {
			last.last = t
			changes = append(changes, &periodValue{first: t, last: t, v: v})
		} else {
			last.last = t
		}
		// Since we sample periodically, this should always be sorted,
		// but just in case of any races, we sort anyway.
		sort.Slice(changes, func(i, j int) bool { return changes[i].first.Before(changes[j].first) })
		m.timeSeries[name] = changes
	}
}

type urlFetcher struct {
	url string
}

// fetchUrl retrieves metrics from the provided URL and returns MetricFamily proto messages.
func (f *urlFetcher) fetchUrl(ctx context.Context) (time.Time, []dto.MetricFamily, error) {
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, "GET", f.url, nil)
	if err != nil {
		return start, nil, errors.E("GET", f.url, err)
	}
	req.Header.Add("Accept", string(expfmt.FmtProtoDelim))
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return start, nil, errors.E("GET", f.url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return start, nil, errors.E("GET", f.url, resp.Status)
	}
	var (
		dec  = expfmt.NewDecoder(resp.Body, expfmt.FmtProtoDelim)
		mfs  []dto.MetricFamily
		done bool
	)
	defer func() { _ = resp.Body.Close() }()
	for !done {
		var mf dto.MetricFamily
		err := dec.Decode(&mf)
		switch {
		case err == nil:
			mfs = append(mfs, mf)
		case err == io.EOF:
			done = true
		default:
			return start, mfs, errors.E("parse", "pbutil.ReadDelimited", err)
		}
	}
	return start, mfs, nil
}

func toString(mf dto.MetricFamily) string {
	var b bytes.Buffer
	sep := ""
	for _, m := range mf.GetMetric() {
		var prefix string
		if l := labelString(m); l != "" {
			prefix = fmt.Sprintf("(%s)=", l)
		}
		v, ok := singleValue(mf.GetType(), m)
		if !ok {
			continue
		}
		if strings.Contains(mf.GetName(), "_bytes") {
			// kind of a hack to make bytes show up in human-readable form.
			_, _ = fmt.Fprintf(&b, "%s%s%s", sep, prefix, data.Size(v))
		} else {
			_, _ = fmt.Fprintf(&b, "%s%s%.4f", sep, prefix, v)
		}
		sep = ", "
	}
	return b.String()
}

func labelString(m *dto.Metric) string {
	var b bytes.Buffer
	sep := ""
	for _, lp := range m.GetLabel() {
		_, _ = fmt.Fprint(&b, sep)
		_, _ = fmt.Fprintf(&b, "%s=%s", lp.GetName(), lp.GetValue())
		sep = ","
	}
	return b.String()
}

// singleValue returns the value of the given metric (if supported).
// singleValue will return false for unsupported metric types.
func singleValue(typ dto.MetricType, m *dto.Metric) (v float64, ok bool) {
	switch typ {
	case dto.MetricType_GAUGE:
		v, ok = m.GetGauge().GetValue(), true
	case dto.MetricType_COUNTER:
		v, ok = m.GetCounter().GetValue(), true
	}
	return
}

func diff(vs []float64) (diff float64) {
	if len(vs) > 0 {
		diff = vs[len(vs)-1] - vs[0]
	}
	return
}
