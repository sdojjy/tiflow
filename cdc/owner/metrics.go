// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package owner

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	changefeedBarrierTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "barrier_ts",
			Help:      "barrier ts of changefeeds",
		}, []string{"namespace", "changefeed"})

	changefeedCheckpointTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts",
			Help:      "checkpoint ts of changefeeds",
		}, []string{"namespace", "changefeed"})
	changefeedCheckpointTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_ts_lag",
			Help:      "checkpoint ts lag of changefeeds in seconds",
		}, []string{"namespace", "changefeed"})

	changefeedCheckpointLagDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "checkpoint_lag_histogram",
			Help:      "checkpoint lag histogram of changefeeds",
			Buckets: []float64{1, 2, 2.5, 2.8,
				3, 3.2, 3.4, 3.6, 3.8,
				4, 4.2, 4.4, 4.6, 4.8,
				5, 5.2, 5.4, 5.6, 5.8,
				6, 6.2, 6.4, 6.6, 6.8,
				7, 7.2, 7.4, 7.6, 7.8,
				8, 8.2, 8.4, 8.6, 8.8,
				10, 14, 20, 40, 80, 160, 320},
		}, []string{"namespace", "changefeed"})

	changefeedResolvedTsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "resolved_ts",
			Help:      "resolved ts of changefeeds",
		}, []string{"namespace", "changefeed"})
	changefeedResolvedTsLagGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "resolved_ts_lag",
			Help:      "resolved ts lag of changefeeds in seconds",
		}, []string{"namespace", "changefeed"})

	changefeedResolvedTsLagDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "resolved_ts_lag_histogram",
			Help:      "resolved_ts lag histogram of changefeeds",
			Buckets: []float64{1, 2, 2.5, 2.8,
				3, 3.2, 3.4, 3.6, 3.8,
				4, 4.2, 4.4, 4.6, 4.8,
				5, 5.2, 5.4, 5.6, 5.8,
				6, 6.2, 6.4, 6.6, 6.8,
				7, 7.2, 7.4, 7.6, 7.8,
				8, 8.2, 8.4, 8.6, 8.8,
				10, 14, 20, 40, 80, 160, 320},
		}, []string{"namespace", "changefeed"})

	ownershipCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "ownership_counter",
			Help:      "The counter of ownership increases every 5 seconds on a owner capture",
		})
	changefeedStatusGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "status",
			Help:      "The status of changefeeds",
		}, []string{"namespace", "changefeed"})
	changefeedTickDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "changefeed_tick_duration",
			Help:      "Bucketed histogram of owner tick changefeed reactor time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		}, []string{"namespace", "changefeed"})
	changefeedCloseDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "changefeed_close_duration",
			Help:      "Bucketed histogram of owner close changefeed reactor time (s).",
			Buckets:   prometheus.ExponentialBuckets(0.01 /* 10 ms */, 2, 18),
		})
	changefeedIgnoredDDLEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "ticdc",
			Subsystem: "owner",
			Name:      "ignored_ddl_event_count",
			Help:      "The total count of ddl events that are ignored in changefeed.",
		}, []string{"namespace", "changefeed"})
)

const (
	// When heavy operations (such as network IO and serialization) take too much time, the program
	// should print a warning log, and if necessary, the timeout should be exposed externally through
	// monitor.
	changefeedLogsWarnDuration = 1 * time.Second
	schedulerLogsWarnDuration  = 1 * time.Second
)

// InitMetrics registers all metrics used in owner
func InitMetrics(registry *prometheus.Registry) {
	registry.MustRegister(changefeedBarrierTsGauge)

	registry.MustRegister(changefeedCheckpointTsGauge)
	registry.MustRegister(changefeedCheckpointTsLagGauge)
	registry.MustRegister(changefeedCheckpointLagDuration)

	registry.MustRegister(changefeedResolvedTsGauge)
	registry.MustRegister(changefeedResolvedTsLagGauge)
	registry.MustRegister(changefeedResolvedTsLagDuration)

	registry.MustRegister(ownershipCounter)
	registry.MustRegister(changefeedStatusGauge)
	registry.MustRegister(changefeedTickDuration)
	registry.MustRegister(changefeedCloseDuration)
	registry.MustRegister(changefeedIgnoredDDLEventCounter)
}
