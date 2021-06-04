# Reflow Metrics Library

The Reflow metrics library should be used to provide observability into
Reflow. 

## Defining metrics

Metrics are defined by `metrics.json`. Each dictionary in the
`metrics.json` root array will produce a metric, and those dictionaries
are configured by the following fields:

- Name
  - Metric names are written in snake_case and can only contain 
    lowercase alpha characters.

- Type
  - The following types of metrics are available.
    - Counter: counters can only be incremented or reset to zero.
    - Gauge: gauges can be incremented, decremented or set to an
      arbitrary value.
    - Histogram: histograms can used to observe arbitrary values
      (discretized to a set of buckets configured at compilation).

- Help
  - All metrics should be documented with a helpful help message. This
    field can be used by clients to provide documentation at query-time.

- Labels
  - Labels give dimensionality to metrics. Labels are written in 
    snake_case and can only contain lowercase alpha characters.

- Buckets
  - Buckets is a special field available on the "histogram" metric type.
    Observations will be discretized to fit into these buckets, based on
    the implementation of the metrics client.

## Regenerating metrics.go

After updating metrics.json, use the genmetrics utility binary to
generate static the go package functions that are called to provide
observations.

`go run cmd/genmetrics/main.go metrics/metrics.json metrics`
