// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics

import (
	"context"
	"fmt"

	rfcontext "github.com/grailbio/reflow/context"
)

// Gauge wraps prometheus.Gauge. Gauges can be set to arbitrary values.
type Gauge interface {
	// Set updates the value of the gauge
	Set(float64)
	// Inc increments the Gauge by 1. Use Add to increment it by arbitrary
	// values.
	Inc()
	// Dec decrements the Gauge by 1. Use Sub to decrement it by arbitrary
	// values.
	Dec()
	// Add adds the given value to the Gauge. (The value can be negative,
	// resulting in a decrease of the Gauge.)
	Add(float64)
	// Sub subtracts the given value from the Gauge. (The value can be
	// negative, resulting in an increase of the Gauge.)
	Sub(float64)
}

// Counter wraps prometheus.Counter. Counters can only increase in value.
type Counter interface {
	// Inc adds one to the counter
	Inc()
	// Add adds the given value to the counter. It panics if the value is <
	// 0.
	Add(float64)
}

// Histogram wraps prometheus.Histogram. Histograms record observations of events and discretize
// them into preconfigured buckets.
type Histogram interface {
	// Observe adds a sample observation to the histogram
	Observe(float64)
}

type labelSet []string

type gaugeOpts struct {
	Labels labelSet
	Help   string
}

type counterOpts struct {
	Labels labelSet
	Help   string
}

type histogramOpts struct {
	Labels  labelSet
	Help    string
	Buckets []float64
}

// mustCompleteLabels confirms that all of the labels in the given labelSet are satisfied by labels.
func mustCompleteLabels(labelSet labelSet, labels map[string]string) bool {
	if len(labels) != len(labelSet) {
		return false
	}
	for _, label := range labelSet {
		if _, ok := labels[label]; !ok {
			return false
		}
	}
	return true
}

// getGauge inspects the given context and returns the requested Gauge if metrics are enabled on the context.
func getGauge(ctx context.Context, name string, labels map[string]string) Gauge {
	if !On(ctx) {
		return &nopGauge{}
	}
	if opts, ok := Gauges[name]; !ok {
		msg := fmt.Sprintf("attempted to get undeclared gauge %s", name)
		panic(msg)
	} else {
		if !mustCompleteLabels(opts.Labels, labels) {
			msg := fmt.Sprintf("attempted to get gauge %s with invalid labels, expected %v but got %v",
				name, opts.Labels, labels)
			panic(msg)
		}
	}
	return metricsClient(ctx).GetGauge(name, labels)
}

// getCounter inspects the given context and returns the requested Counter if metrics are enabled on the context.
func getCounter(ctx context.Context, name string, labels map[string]string) Counter {
	if !On(ctx) {
		return &nopCounter{}
	}
	if opts, ok := Counters[name]; !ok {
		msg := fmt.Sprintf("attempted to get undeclared counter %s", name)
		panic(msg)
	} else {
		if !mustCompleteLabels(opts.Labels, labels) {
			err := fmt.Sprintf("attempted to set counter %s with invalid labels, expected %v but got %v",
				name, opts.Labels, labels)
			panic(err)
		}
	}
	return metricsClient(ctx).GetCounter(name, labels)
}

// getHistogram inspects the given context and returns the requested Histogram if metrics are enabled on the context.
func getHistogram(ctx context.Context, name string, labels map[string]string) Histogram {
	if !On(ctx) {
		return &nopHistogram{}
	}
	if opts, ok := Histograms[name]; !ok {
		msg := fmt.Sprintf("attempted to get undeclared histogram %s", name)
		panic(msg)
	} else {
		if !mustCompleteLabels(opts.Labels, labels) {
			err := fmt.Sprintf("attempted to set histogram %s with invalid labels, expected %v but got %v",
				name, opts.Labels, labels)
			panic(err)
		}
	}
	return metricsClient(ctx).GetHistogram(name, labels)
}

// Client is a sink for metrics.
type Client interface {
	GetGauge(name string, labels map[string]string) Gauge
	GetCounter(name string, labels map[string]string) Counter
	GetHistogram(name string, labels map[string]string) Histogram
}

// NopClient is default metrics client that does nothing.
var NopClient Client = &nopClient{}

// WithClient returns a context that emits trace events to the
// provided Client.
func WithClient(ctx context.Context, client Client) context.Context {
	if client == nil {
		return ctx
	}
	return context.WithValue(ctx, rfcontext.MetricsClientKey, client)
}

// On returns true if there is a current Client associated with the
// provided context.
func On(ctx context.Context) bool {
	_, ok := ctx.Value(rfcontext.MetricsClientKey).(Client)
	return ok
}

func metricsClient(ctx context.Context) Client {
	return ctx.Value(rfcontext.MetricsClientKey).(Client)
}
