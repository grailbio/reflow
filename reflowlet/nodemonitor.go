// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package reflowlet

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/grailbio/base/data"
	"github.com/grailbio/reflow/errors"
	"github.com/grailbio/reflow/log"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

var NodeMetricsNames = map[string]bool{
	"node_disk_read_bytes_total":       true, // Per Device: The total number of bytes read successfully.
	"node_disk_reads_completed_total":  true, // Per Device: The total number of reads completed successfully.
	"node_disk_writes_completed_total": true, // Per Device: The total number of writes completed successfully.
	"node_disk_written_bytes_total":    true, // Per Device: The total number of bytes written successfully.

	"node_filefd_allocated": true, // File descriptor statistics: allocated.
	"node_filefd_maximum":   true, // File descriptor statistics: maximum.

	"node_load1":  true, // CPU: 1m load average.
	"node_load5":  true, // CPU: 5m load average.
	"node_load15": true, // CPU: 15m load average.

	"node_memory_MemAvailable_bytes": true, // Memory information field MemAvailable_bytes.
	"node_memory_MemFree_bytes":      true, // Memory information field MemFree_bytes.
	"node_memory_MemTotal_bytes":     true, // Memory information field MemTotal_bytes.

	"node_pressure_cpu_waiting_seconds_total":    true, // Total time in seconds that processes have waited for CPU time
	"node_pressure_io_stalled_seconds_total":     true, // Total time in seconds no process could make progress due to IO congestion
	"node_pressure_io_waiting_seconds_total":     true, // Total time in seconds that processes have waited due to IO congestion
	"node_pressure_memory_stalled_seconds_total": true, // Total time in seconds no process could make progress due to memory congestion
	"node_pressure_memory_waiting_seconds_total": true, // Total time in seconds that processes have waited for memory

	"node_vmstat_oom_kill": true,
}

// fetch retrieves metrics from the provided URL and returns MetricFamily proto messages.
func fetch(ctx context.Context, url string) ([]dto.MetricFamily, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, errors.E("GET", url, err)
	}
	req.Header.Add("Accept", string(expfmt.FmtProtoDelim))
	client := http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.E("GET", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.E("GET", url, resp.Status)
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
			return mfs, errors.E("parse", "pbutil.ReadDelimited", err)
		}
	}
	return mfs, nil
}

// DumpMetrics dumps node metrics to the given logger.
// If restrict is specified, then only metric names specified in the map are logged.
func DumpMetrics(ctx context.Context, url string, log *log.Logger, restrict map[string]bool) {
	if url == "" {
		return
	}
	start := time.Now()
	mfs, err := fetch(ctx, url)
	if err != nil && ctx.Err() == nil {
		// Only log the error, since we can get values even if there was a subsequent error.
		log.Error(err)
	}
	var skipped []string
	for _, mf := range mfs {
		select {
		case <-ctx.Done():
			log.Printf("dump %s interrupted: %v", url, ctx.Err())
			return
		default:
		}
		if n := mf.GetName(); len(restrict) > 0 && !restrict[n] {
			if strings.HasPrefix(n, "node_") {
				skipped = append(skipped, n)
			}
			continue
		}
		if s := oneLineString(mf); s != "" {
			log.Printf("%s", s)
		}
	}
	sort.Strings(skipped)

	var msgSuffix string
	if len(skipped) > 0 {
		msgSuffix = fmt.Sprintf(" skipped metrics: %s", strings.Join(skipped, ", "))
	}
	log.Printf("dump %s completed (%s)%s", url, time.Now().Sub(start), msgSuffix)
}

func oneLineString(mf dto.MetricFamily) string {
	var b bytes.Buffer
	_, _ = fmt.Fprintf(&b, "%s: ", mf.GetName())
	sep := ""
	for _, m := range mf.GetMetric() {
		_, _ = fmt.Fprint(&b, sep)
		var (
			prefix string
			v      float64
		)
		switch mf.GetType() {
		case dto.MetricType_GAUGE:
			v = m.GetGauge().GetValue()
		case dto.MetricType_COUNTER:
			v = m.GetCounter().GetValue()
			if l := labelString(m); l != "" {
				prefix = fmt.Sprintf("(%s)=", l)
			}
		default:
			// Currently not supported.
			return ""
		}
		if strings.Contains(mf.GetName(), "_bytes") {
			// kind of a hack to make bytes show up in human-readable form.
			_, _ = fmt.Fprintf(&b, "%s%s", prefix, data.Size(v))
		} else {
			_, _ = fmt.Fprintf(&b, "%s%.4f", prefix, v)
		}
		sep = ", "
	}
	_, _ = fmt.Fprintf(&b, "  (%s)", mf.GetHelp())
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
