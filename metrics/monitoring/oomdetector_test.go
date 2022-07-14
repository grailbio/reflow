// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package monitoring

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

func TestNodeOomDetector(t *testing.T) {
	m := &oomDetector{timeSeries: newTimeSeries("", time.Millisecond,
		[]string{memAvailableBytes, memTotalBytes}, nil)}
	m.setupConsumerFn()
	start := time.Now()
	memScale := float64(10 << 20) // Use a 10Mb scale.
	memTotal := float64(1 << 30)  // 1GiB total memory.
	f := &testFetcher{start: start, p: 10 * time.Second, mType: dto.MetricType_GAUGE,
		values: []map[string]float64{
			{memAvailableBytes: 80 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 20 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 2 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 3 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 4 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 1 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 31 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 32 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 33 * memScale, memTotalBytes: memTotal},
			{memAvailableBytes: 34 * memScale, memTotalBytes: memTotal},
		},
	}
	m.fetcherFn = f.fetch

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	m.Go(ctx)
	cancel()

	for i, tt := range []struct {
		st, et     time.Time
		wantOom    bool
		wantSuffix string
	}{
		// Tests for outside the recorderd time range
		{start.Add(-100 * time.Second), start.Add(-40 * time.Second), false, ""},
		{start.Add(200 * time.Second), start.Add(240 * time.Second), false, ""},
		{start.Add(time.Second), start.Add(19 * time.Second), false, ""},
		// Tests for memAvailableBytes-based oom determination
		{start.Add(1 * time.Second), start.Add(19 * time.Second), false, ""},
		{start.Add(20 * time.Second), start.Add(35 * time.Second), true, "min node_memory_MemAvailable_bytes: 50.0MiB (<100.0MiB, total: 1.0GiB)"},
		{start.Add(40 * time.Second), start.Add(67 * time.Second), true, "min node_memory_MemAvailable_bytes: 50.0MiB (<100.0MiB, total: 1.0GiB)"},
	} {
		gotOom, gotStr := m.Oom(-1, tt.st, tt.et)
		if got, want := gotOom, tt.wantOom; got != want {
			t.Errorf("[%d] (%s, %s) got %v, want %v", i, tt.st.Format(time.RFC3339), tt.et.Format(time.RFC3339), got, want)
		}
		if tt.wantOom && tt.wantSuffix == "" {
			panic(fmt.Sprintf("[%d] test suffix not defined", i))
		}
		if !strings.HasSuffix(gotStr, tt.wantSuffix) {
			t.Errorf("[%d] (%s, %s) got %s, missing suffix %s", i, tt.st.Format(time.RFC3339), tt.et.Format(time.RFC3339), gotStr, tt.wantSuffix)
		}
		if oom, _ := m.Oom(rand.Intn(100), tt.st, tt.et); oom {
			t.Errorf("[%d] (%s, %s) got true, must be false when non -1 pid is given", i, tt.st.Format(time.RFC3339), tt.et.Format(time.RFC3339))
		}
	}
}
