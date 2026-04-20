// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// XidProcessingErrorType enumerates the "error_type" label values that can be
// emitted on XidProcessingErrors. Kept here (rather than as untyped string
// literals at the emission site) so startup pre-initialization and runtime
// emission cannot drift apart.
const (
	// XIDParseError is emitted from the local CSV parser when a matched
	// log line cannot be converted to a numeric XID code.
	XIDParseError = "xid_parse_error"

	// Sidecar-path error types. Emitted by pkg/xid/parser/sidecar when
	// talking to the XID analyser service.
	XIDSidecarJSONMarshalError     = "json_marshal_error"
	XIDSidecarRequestCreationError = "request_creation_error"
	XIDSidecarRequestSendingError  = "request_sending_error"
	XIDSidecarHTTPStatusError      = "http_status_error"
	XIDSidecarResponseReadingError = "response_reading_error"
	XIDSidecarResponseDecodingErr  = "response_decoding_error"
)

// allXIDProcessingErrorTypes is the closed set of error_type label values that
// the XID pipeline can emit. Pre-initializing all of them is cheap (one series
// per (error_type, node) pair) and ensures Google Managed Prometheus does not
// swallow the first real sample.
var allXIDProcessingErrorTypes = []string{
	XIDParseError,
	XIDSidecarJSONMarshalError,
	XIDSidecarRequestCreationError,
	XIDSidecarRequestSendingError,
	XIDSidecarHTTPStatusError,
	XIDSidecarResponseReadingError,
	XIDSidecarResponseDecodingErr,
}

var (
	XidCounterMetric = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "syslog_health_monitor_xid_errors",
			Help: "Total number of XID found",
		},
		[]string{"node", "err_code"},
	)

	XidProcessingErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "syslog_health_monitor_xid_processing_errors",
			Help: "Total number of errors encountered during XID processing",
		},
		[]string{"error_type", "node"},
	)

	XidProcessingLatency = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "syslog_health_monitor_xid_processing_latency_seconds",
			Help:    "Histogram of XID processing latency",
			Buckets: prometheus.DefBuckets,
		},
	)
)

// PreInitialize materializes the XID CounterVec series at zero for the local
// node so backends that establish a baseline from the first ingested sample
// (e.g. Google Managed Prometheus) do not drop the first real occurrence.
//
// Pre-initialized series:
//   - XidCounterMetric{node=nodeName, err_code=<each code>}:
//     one entry per XID code in xidCodes. Callers typically pass the keys of
//     the embedded NVIDIA XID catalog loaded via common.LoadErrorResolutionMap.
//     NVL5 XIDs reported with a subcode suffix (e.g. "145.RLW_SRC_TRACK") are
//     NOT pre-initialized because the subcode space depends on the specific
//     interrupt content and is not enumerable at startup.
//   - XidProcessingErrors{error_type=<each>, node=nodeName}:
//     one entry per value in allXIDProcessingErrorTypes.
//
// Calling PreInitialize is idempotent; WithLabelValues(...).Add(0) is a no-op
// on an already-materialized counter.
func PreInitialize(nodeName string, xidCodes []int) {
	if nodeName == "" {
		return
	}

	for _, code := range xidCodes {
		XidCounterMetric.WithLabelValues(nodeName, strconv.Itoa(code)).Add(0)
	}

	for _, errType := range allXIDProcessingErrorTypes {
		XidProcessingErrors.WithLabelValues(errType, nodeName).Add(0)
	}
}
