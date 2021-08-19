package monitoring

import (
	"context"
	"fmt"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

type testFetcher struct {
	start  time.Time
	p      time.Duration
	values []map[string]float64
	mType  dto.MetricType

	i int
}

func (t *testFetcher) fetch(_ context.Context) (ts time.Time, mfs []dto.MetricFamily, err error) {
	if t.i == len(t.values) {
		err = fmt.Errorf("testFetcher, values exhausted")
		return
	}
	ts = t.start.Add(time.Duration(t.i) * t.p)
	for name, v := range t.values[t.i] {
		name, v := name, v
		mf := dto.MetricFamily{
			Name: &name,
			Type: &t.mType,
		}
		switch t.mType {
		case dto.MetricType_COUNTER:
			mf.Metric = []*dto.Metric{{Counter: &dto.Counter{Value: &v}}}
		case dto.MetricType_GAUGE:
			mf.Metric = []*dto.Metric{{Gauge: &dto.Gauge{Value: &v}}}
		}
		mfs = append(mfs, mf)
	}
	t.i++
	return
}

func TestTimeSeries(t *testing.T) {
	const nameA, nameB, nameC = "name_a", "name_b", "untracked"
	m := newTimeSeries("", time.Millisecond, []string{nameA, nameB}, nil)
	start := time.Now()
	f := &testFetcher{start: start, p: 10 * time.Second, mType: dto.MetricType_COUNTER,
		values: []map[string]float64{
			{nameA: 0.0, nameB: 0.0, nameC: 0.0},
			{nameA: 4.0, nameB: 0.0},
			{nameA: 4.0, nameB: 0.0},
			{nameA: 4.0, nameB: 1.0, nameC: 1.0},
			{nameA: 6.0, nameB: 1.0},
			{nameA: 6.0, nameB: 2.0, nameC: 1.0},
			{nameA: 6.0, nameB: 2.0},
		},
	}
	m.fetcherFn = f.fetch
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	m.Go(ctx)
	cancel()

	if _, err := m.query(nameC, start, start); err == nil {
		t.Error("got no error, want error")
	}
	if _, err := m.query(nameA, start.Add(time.Second), start); err == nil {
		t.Error("got no error, want error")
	}
	for i, tt := range []struct {
		name     string
		st, et   time.Time
		wantDiff float64
	}{
		// Tests for outside the recorded time range
		{nameA, start.Add(-100 * time.Second), start.Add(-40 * time.Second), 0.0},
		{nameA, start.Add(100 * time.Second), start.Add(140 * time.Second), 0.0},
		// Tests for within the recorded time range, with coinciding timestamps.
		{nameA, start, start.Add(5 * time.Second), 0.0},
		{nameA, start, start.Add(10 * time.Second), 4.0},
		{nameA, start, start.Add(25 * time.Second), 4.0},
		{nameA, start, start.Add(40 * time.Second), 6.0},
		{nameA, start, start.Add(100 * time.Second), 6.0},
		// Tests for within the recorded time range, without coinciding timestamps.
		{nameA, start.Add(time.Second), start.Add(5 * time.Second), 0.0},
		{nameA, start.Add(time.Second), start.Add(10 * time.Second), 4.0},
		{nameA, start.Add(time.Second), start.Add(25 * time.Second), 4.0},
		{nameA, start.Add(time.Second), start.Add(40 * time.Second), 6.0},
		{nameA, start.Add(time.Second), start.Add(100 * time.Second), 6.0},
		// Tests for outside the recorded time range
		{nameB, start.Add(-10 * time.Second), start.Add(-2 * time.Second), 0.0},
		{nameB, start.Add(80 * time.Second), start.Add(120 * time.Second), 0.0},
		// Tests for within the recorded time range, with coinciding timestamps.
		{nameB, start, start.Add(20 * time.Second), 0.0},
		{nameB, start, start.Add(30 * time.Second), 1.0},
		{nameB, start, start.Add(50 * time.Second), 2.0},
		// Tests for within the recorded time range, without coinciding timestamps.
		{nameB, start.Add(10 * time.Second), start.Add(20 * time.Second), 0.0},
		{nameB, start.Add(10 * time.Second), start.Add(30 * time.Second), 1.0},
		{nameB, start.Add(10 * time.Second), start.Add(50 * time.Second), 2.0},
	} {
		tvs, err := m.query(tt.name, tt.st, tt.et)
		if err != nil {
			t.Errorf("[%d] %v", i, err)
			continue
		}
		if got, want := diff(tvs), tt.wantDiff; got != want {
			t.Errorf("[%d] (%s, %s) got %v, want %v", i, tt.st.Format(time.RFC3339), tt.et.Format(time.RFC3339), got, want)
		}
	}
}
