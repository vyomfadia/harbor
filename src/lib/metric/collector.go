// Copyright Project Harbor Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metric

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
)

// RegisterCollectors register all the common static collector
func RegisterCollectors() {
	prometheus.MustRegister([]prometheus.Collector{
		TotalInFlightGauge,
		TotalReqCnt,
		TotalReqDurSummary,
		TotalProxyReq,
		TotalProxyUpstreamReq,
		QuotaFlushDurSummary,
		QuotaFlushKeysTotal,
		SessionCreatedTotal,
	}...)
}

var (
	// TotalInFlightGauge used to collect total in flight number
	TotalInFlightGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: os.Getenv(NamespaceEnvKey),
			Subsystem: os.Getenv(SubsystemEnvKey),
			Name:      "http_inflight_requests",
			Help:      "The total number of requests",
		},
	)

	// TotalReqCnt used to collect total request counter
	TotalReqCnt = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: os.Getenv(NamespaceEnvKey),
			Subsystem: os.Getenv(SubsystemEnvKey),
			Name:      "http_request_total",
			Help:      "The total number of requests",
		},
		[]string{"method", "code", "operation"},
	)

	// TotalReqDurSummary used to collect total request duration summaries
	TotalReqDurSummary = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace:  os.Getenv(NamespaceEnvKey),
			Subsystem:  os.Getenv(SubsystemEnvKey),
			Name:       "http_request_duration_seconds",
			Help:       "The time duration of the requests",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"method", "operation"})

	// TotalProxyReq used to collect total proxy cache requests
	TotalProxyReq = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: os.Getenv(NamespaceEnvKey),
			Subsystem: os.Getenv(SubsystemEnvKey),
			Name:      "http_registry_proxy_requests_total",
			Help:      "The total number of requests sent to the proxy cache",
		},
		[]string{"project", "method"})

	// TotalProxyUpstreamReq used to collect total requests sent to the upstream
	TotalProxyUpstreamReq = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: os.Getenv(NamespaceEnvKey),
			Subsystem: os.Getenv(SubsystemEnvKey),
			Name:      "http_registry_proxy_upstream_requests_total",
			Help:      "The total number of proxy cache requests that fetched from the upstream registry (cache miss)",
		},
		[]string{"project", "method"})

	// QuotaFlushDurSummary collects the duration of flushing the quota usage to db
	QuotaFlushDurSummary = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Namespace:  os.Getenv(NamespaceEnvKey),
			Subsystem:  os.Getenv(SubsystemEnvKey),
			Name:       "quota_flush_duration_seconds",
			Help:       "The time duration of flushing the quota usage from redis to db",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		})

	// QuotaFlushKeysTotal collects the number of quotas processed when flushing the quota usage
	QuotaFlushKeysTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: os.Getenv(NamespaceEnvKey),
			Subsystem: os.Getenv(SubsystemEnvKey),
			Name:      "quota_flush_keys_total",
			Help:      "The total number of quotas processed when flushing the quota usage from redis to db",
		},
		[]string{"result"})

	// SessionCreatedTotal used to collect the total number of sessions persisted to the session storage
	// SessionCreatedTotal collects the total number of sessions persisted to the session storage
	SessionCreatedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: os.Getenv(NamespaceEnvKey),
			Subsystem: os.Getenv(SubsystemEnvKey),
			Name:      "session_created_total",
			Help:      "The total number of sessions created in the session storage",
		},
	)
)
