// Copyright 2021 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache 2.0
// license that can be found in the LICENSE file.

package prometrics

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/grailbio/infra"
	"github.com/grailbio/reflow/log"
	"github.com/grailbio/reflow/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	infra.Register("prometrics", new(client))
}

type client struct {
	// Namespace is given as a prefix to all prometheus metrics.
	Namespace string

	// Port is the port to serve Prometheus metrics.
	Port int

	reg        *prometheus.Registry
	gauges     map[string]*prometheus.GaugeVec
	counters   map[string]*prometheus.CounterVec
	histograms map[string]*prometheus.HistogramVec
}

// String implements infra.Provider.
func (r *client) String() string {
	return fmt.Sprintf("%T,Port=%d", r, r.Port)
}

// Help implements infra.Provider.
func (r *client) Help() string {
	return "configure a prometheus metrics client hosted on the given port"
}

// Init implements infra.Provider.
func (r *client) Init() error {
	r.reg = prometheus.NewRegistry()
	r.gauges = make(map[string]*prometheus.GaugeVec)
	r.counters = make(map[string]*prometheus.CounterVec)
	r.histograms = make(map[string]*prometheus.HistogramVec)
	r.initCollectors()
	go func() {
		log.Printf("hosting prometheus at %d", r.Port)
		handler := promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{})
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", r.Port), handler))
	}()
	return nil
}

// Flags implements infra.Provider.
func (r *client) Flags(flags *flag.FlagSet) {
	flags.IntVar(&r.Port, "port", 9100, "port to serve metrics")
	flags.StringVar(&r.Namespace, "namespace", "reflow", "namespace to prepend to metrics")
}

// initCollectors inspects the counters/gauges/histograms defined on the root metrics implementation
// and initializes their backing stores in the prometheus registry on the new client. It should only
// be called once.
func (r *client) initCollectors() {
	for name, opts := range metrics.Gauges {
		gv := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: r.Namespace,
			Name:      name,
			Help:      opts.Help,
		}, opts.Labels)
		r.gauges[name] = gv
		if err := r.reg.Register(gv); err != nil {
			log.Fatal(err)
		}
	}

	for name, opts := range metrics.Counters {
		cv := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: r.Namespace,
			Name:      name,
			Help:      opts.Help,
		}, opts.Labels)
		r.counters[name] = cv
		if err := r.reg.Register(cv); err != nil {
			log.Fatal(err)
		}
	}

	for name, opts := range metrics.Histograms {
		hv := prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: r.Namespace,
			Name:      name,
			Buckets:   opts.Buckets,
			Help:      opts.Help,
		}, opts.Labels)
		r.histograms[name] = hv
		if err := r.reg.Register(hv); err != nil {
			log.Fatal(err)
		}
	}
}

func (r *client) GetGauge(name string, labels map[string]string) metrics.Gauge {
	cv := r.gauges[name]
	gauge, err := cv.GetMetricWith(labels)
	if err != nil {
		log.Fatal(err)
	}
	return gauge
}

func (r *client) GetCounter(name string, labels map[string]string) metrics.Counter {
	cv := r.counters[name]
	counter, err := cv.GetMetricWith(labels)
	if err != nil {
		log.Fatal(err)
	}
	return counter
}

func (r *client) GetHistogram(name string, labels map[string]string) metrics.Histogram {
	hv := r.histograms[name]
	histogram, err := hv.GetMetricWith(labels)
	if err != nil {
		log.Fatal(err)
	}
	return histogram
}

// NewClient returns a prometheus metrics Client that wraps the existing registry.
func NewClient(reg *prometheus.Registry) metrics.Client {
	r := &client{
		reg:        reg,
		Namespace:  "reflow",
		gauges:     make(map[string]*prometheus.GaugeVec),
		counters:   make(map[string]*prometheus.CounterVec),
		histograms: make(map[string]*prometheus.HistogramVec),
	}
	r.initCollectors()
	return r
}
