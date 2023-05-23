// Copyright 2018 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schedule

import "github.com/prometheus/client_golang/prometheus"

var (
	scatterCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "scatter_operators_count",
			Help:      "Counter of region scatter operators.",
		}, []string{"type", "event"})

	scatterDistributionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "scatter_distribution",
			Help:      "Counter of the distribution in scatter.",
		}, []string{"store", "is_leader", "engine"})

	// LabelerEventCounter is a counter of the scheduler labeler system.
	LabelerEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pd",
			Subsystem: "schedule",
			Name:      "labeler_event_counter",
			Help:      "Counter of the scheduler label.",
		}, []string{"type", "event"})
)

func init() {
	prometheus.MustRegister(scatterCounter)
	prometheus.MustRegister(scatterDistributionCounter)
	prometheus.MustRegister(LabelerEventCounter)
}