// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package metrics

import "github.com/grailbio/infra"

type nopGauge struct{}

func (n *nopGauge) Set(float64) {}

func (n *nopGauge) Inc() {}

func (n *nopGauge) Dec() {}

func (n *nopGauge) Add(float64) {}

func (n *nopGauge) Sub(float64) {}

type nopCounter struct{}

func (n *nopCounter) Inc() {}

func (n *nopCounter) Add(float64) {}

type nopHistogram struct{}

func (n *nopHistogram) Observe(float64) {}

type nopClient struct{}

// GetGauge returns a nopGauge which does nothing.
func (*nopClient) GetGauge(string, map[string]string) Gauge {
	return &nopGauge{}
}

// GetCounter returns a nopCounter which does nothing.
func (*nopClient) GetCounter(string, map[string]string) Counter {
	return &nopCounter{}
}

// GetHistogram returns a nopHistogram which does nothing.
func (*nopClient) GetHistogram(string, map[string]string) Histogram {
	return &nopHistogram{}
}

func init() {
	infra.Register("nopmetrics", new(nopClient))
}

// Init implements infra.Provider and gets called when an instance of nopClient
// is created with infra.Config.Instance(...)
func (nopClient) Init() error {
	return nil
}

// Help implements infra.Provider
func (nopClient) Help() string {
	return "nopClient does nothing, use it when you don't want any metrics"
}
