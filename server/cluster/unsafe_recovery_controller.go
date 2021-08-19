// Copyright 2021 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type UnsafeRecoveryStage int

const (
	Ready UnsafeRecoveryStage = iota
	CollectingClusterInfo
	Recovering
)

type UnsafeRecoveryController struct {
	sync.RWMutex

	RaftCluster           *cluster
	stage                 UnsafeRecoveryStage
	failedStores          map[uint64]bool
	storeReports          map[uint64]*pdpb.StoreReport // Store info proto
	numStoresReported     int
	storeRecoveryPlans    map[uint64]*pdpb.RecoveryPlan // StoreRecoveryPlan proto
	numStoresPlanExecuted int
}

func newUnsafeRecoveryController(cluster *RaftCluster) *UnsafeRecoveryController {
	return &UnsafeRecoveryController{
		cluster:               cluster,
		stage:                 Ready,
		failedStores:          make(map[uint64]bool),
		storeReports:          make(map[uint64]*pdpb.StoreReport),
		numStoresReported:     0,
		storeRecoveryPlans:    make(map[uint64]*pdpb.RecoveryPlan),
		numStoresPlanExecuted: 0,
	}
}

func (u *UnsafeRecoveryController) RemoveFailedStores(failedStores map[uint64]bool) error {
	u.Lock()
	defer u.Unlock()
	if len(failedStores) == 0 {
		return errors.Errorf("No store specified")
	}
	if len(u.failedStores) != 0 {
		return errors.Errorf("Another request is working in progress")
	}
	u.failedStores = failedStores
	for _, s := range u.cluster.GetStores() {
		if s.IsTombstone() || s.IsPhysicallyDestroyed() || failedStores[s.GetID()] {
			continue
		}
		u.storeReports[s.GetID()] = nil
	}
	u.stage = CollectingClusterInfo
	return nil
}

func (u *UnsafeRecoveryController) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	u.Lock()
	defer u.Unlock()
	if len(u.failedStores) == 0 {
		return
	}
	switch u.stage {
	case CollectingClusterInfo:
		if heartbeat.StoreReport.StoreId == 0 {
			// Inform the store to send detailed report in the next heartbeat.
			resp.SendDetailedReportInNextHeartbeat = true
		} else if u.storeReports[heartbeat.StoreReport.StoreId] == nil {
			u.storeReports[heartbeat.StoreReport.StoreId] = heartbeat.StoreReport
			u.numStoresReported++
			if u.numStoresReported == len(u.storeReports) {
				// Info collection is done.
				u.stage = Recovering
				go u.GenerateRecoveryPlan()
			}
		}
	case Recovering:
		if u.storeRecoveryPlans[heartbeat.StoreReport.StoreId] != nil {
			if !u.IsPlanExecuted(heartbeat.StoreReport) {
				// If the plan has not been executed, send it through the heartbeat response.
				resp.Plan = u.storeRecoveryPlans[heartbeat.StoreReport.StoreId]
			} else {
				u.numStoresPlanExecuted++
				if u.numStoresPlanExecuted == len(u.storeRecoveryPlans) {
					// The recovery is finished.
					u.stage = Ready
					u.failedStores = make(map[uint64]bool)
					u.storeReports = make(map[uint64]*pdpb.StoreReport)
					u.numStoresReported = 0
					u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
					u.numStoresPlanExecuted = 0
				}
			}

		}
	}
}

func (u *UnsafeRecoveryController) IsPlanExecuted(report *pdpb.StoreReport) bool {
	return true
}

func (u *UnsafeRecoveryController) GenerateRecoveryPlan() {
	u.Lock()
	defer u.Unlock()
}

func (u *UnsafeRecoveryController) Show() string {
	u.RLock()
	defer u.RUnlock()
	switch u.stage {
	case Ready:
		return "Ready"
	case CollectingClusterInfo:
		return fmt.Sprintf("Collecting cluster info from all alive stores, %d/%d.",
			u.numStoresReported, len(u.storeReports))
	case Recovering:
		return fmt.Sprintf("Recovering, %d/%d", u.numStoresPlanExecuted, len(u.storeRecoveryPlans))
	}
	return "Undefined status"
}

func (u *UnsafeRecoveryController) History() string {
	history := "Current status: " + u.Show()
	history += "\nFailed stores: "
	for storeID, _ := range u.failedStores {
		history += string(storeID) + ","
	}
	history += "\nStore reports: "
	for storeID, report := range u.storeReports {
		history += "\n" + string(storeID)
		if report == nil {
			history += ": not yet reported"
		} else {
			history += ": reported"
		}
	}
	return history
}
