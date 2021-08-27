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
	"strconv"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
)

type unsafeRecoveryStage int

const (
	ready unsafeRecoveryStage = iota
	collectingClusterInfo
	recovering
)

type unsafeRecoveryController struct {
	sync.RWMutex

	cluster               *RaftCluster
	stage                 unsafeRecoveryStage
	failedStores          map[uint64]string
	storeReports          map[uint64]*pdpb.StoreReport // Store info proto
	numStoresReported     int
	storeRecoveryPlans    map[uint64]*pdpb.RecoveryPlan // StoreRecoveryPlan proto
	numStoresPlanExecuted int
}

func newUnsafeRecoveryController(cluster *RaftCluster) *unsafeRecoveryController {
	return &unsafeRecoveryController{
		cluster:               cluster,
		stage:                 ready,
		failedStores:          make(map[uint64]string),
		storeReports:          make(map[uint64]*pdpb.StoreReport),
		numStoresReported:     0,
		storeRecoveryPlans:    make(map[uint64]*pdpb.RecoveryPlan),
		numStoresPlanExecuted: 0,
	}
}

// RemoveFailedStores removes failed stores from the cluster.
func (u *unsafeRecoveryController) RemoveFailedStores(failedStores map[uint64]string) error {
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
		if s.IsTombstone() || s.IsPhysicallyDestroyed() {
			continue
		}
		_, exists := failedStores[s.GetID()]
		if exists {
			continue
		}
		u.storeReports[s.GetID()] = nil
	}
	u.stage = collectingClusterInfo
	return nil
}

// HandleStoreHeartbeat handles the store heartbeat requests and checks whether the stores need to
// send detailed report back.
func (u *unsafeRecoveryController) HandleStoreHeartbeat(heartbeat *pdpb.StoreHeartbeatRequest, resp *pdpb.StoreHeartbeatResponse) {
	u.Lock()
	defer u.Unlock()
	if len(u.failedStores) == 0 {
		return
	}
	switch u.stage {
	case collectingClusterInfo:
		if heartbeat.StoreReport == nil {
			// Inform the store to send detailed report in the next heartbeat.
			resp.SendDetailedReportInNextHeartbeat = true
		} else if u.storeReports[heartbeat.StoreReport.StoreId] == nil {
			u.storeReports[heartbeat.StoreReport.StoreId] = heartbeat.StoreReport
			u.numStoresReported++
			if u.numStoresReported == len(u.storeReports) {
				// Info collection is done.
				u.stage = recovering
				go u.generateRecoveryPlan()
			}
		}
	case recovering:
		if u.storeRecoveryPlans[heartbeat.StoreReport.StoreId] != nil {
			if !u.isPlanExecuted(heartbeat.StoreReport) {
				// If the plan has not been executed, send it through the heartbeat response.
				resp.Plan = u.storeRecoveryPlans[heartbeat.StoreReport.StoreId]
			} else {
				u.numStoresPlanExecuted++
				if u.numStoresPlanExecuted == len(u.storeRecoveryPlans) {
					// The recovery is finished.
					u.stage = ready
					u.failedStores = make(map[uint64]string)
					u.storeReports = make(map[uint64]*pdpb.StoreReport)
					u.numStoresReported = 0
					u.storeRecoveryPlans = make(map[uint64]*pdpb.RecoveryPlan)
					u.numStoresPlanExecuted = 0
				}
			}

		}
	}
}

func (u *unsafeRecoveryController) isPlanExecuted(report *pdpb.StoreReport) bool {
	return true
}

type rangeItem struct {
	key, value []byte
}

func (r rangeItem) Less(than btree.Item) bool {
	return r.key < than.(rangeItem).key
}

func (u *unsafeRecoveryController) generateRecoveryPlan() {
	u.Lock()
	defer u.Unlock()
	regionsNewestReport := make(map[uint64]*pdpb.PeerReport)
	for _, storeReport := range u.storeReports {
		for peerReport := range storeReports.Reports {
			currentReport, ok := regionsNewestReport[peerReport.RegionState.Region.Id]
			if ok && currentReport.RegionState.Region.RegionEpoch.Version < peerReport.RegionState.Region.RegionEpoch.Version {
				// If the newer version of the region's state has already been reported by other stores, ignore the outdated region state.
				continue
			}
			regionsNewestReport[peerReport.RegionState.Region.Id] = peerReport
		}
	}
	validRangesStartToEnd := btree.New(2)
	validRangesEndToStart := btree.New(2)
	leaderlessRegions := make([]*pdpb.PeerReport)
	for region, report := range regionsNewestReport {
		if report.HasLeader {
			validRangesStartToEnd.ReplaceOrInsert(rangeItem{report.RegionState.Region.StartKey, report.RegionState.Region.EndKey})
			validRangesEndToStart.ReplaceOrInsert(rangeItem{report.RegionState.Region.EndKey, report.RegionState.Region.StartKey})
		} else {
			leaderlessRegions = append(leaderlessRegions, report)
		}
	}
	sort.SliceTable(leaderlessRegions, func(i, j int) bool {
		return leaderlessRegions[i].RaftState.LastIndex > leaderlessRegions[j].RaftState.LastIndex
	})
	for leaderlessRegion := range versionReverseSortedLeaderlessRegions {
		startKey := leaderlessRegion.RegionState.Region.StartKey
		endKey := leaderlessRegion.RegionState.Region.EndKey
		coveredStartKeys := make([]rangItem, 0)
		validRangesStartToEnd.AscendRange(rangeItem{startKey, ""}, rangeItem{endKey, ""}, func(item btree.Item) bool {
			coveredStartKeys = append(coveredStartKeys, item.(rangeItem))
		})
		coveredEndKeys := make([]rangeItem, 0)
		validRangesEndToStart.AscendRange(rangeItem{startKey, ""}, rangeItem{endKey, ""}, func(item btree.Item) bool {
			coveredEndKeys = append(coveredEndKeys, item.(rangeItem))
		})
		plan := pdpb.RecoveryPlan
		lastEnd := startKey
		recoveredRangeStartKey := startKey
		recoveredRangeEndKey := endKey
		if len(coveredEndKeys) != 0 && coveredEndKey[0].value < startKey {
			// The first available range that overlaps the leaderless region covers the leaderless region's start key
			//
			// Available ranges  : ... |_________|    |______   ....
			// Leaderless region : ...     |_________________   ....
			//
			//                                   |____|         <- New region
			//                         |_____________________   <- New available range start key
			lastEnd = coveredEndKey[0]
			recoveredRangeStartKey = coveredEndKey[0].value
		}
		for _, coveredStartKey := range coveredStartKeys {
			newRegion := pdpb.Region
			newRegion.StartKey = lastEnd
			newRegion.EndKey = coveredStartKey.key
			plan.ToRegions.add(newRegion)
			lastEnd = coveredStartKey.value
			validRangesStartToEnd.Delete(coveredStartKey)
		}
		for _, coveredEndKey := range coveredEndKeys {
			validrangesEndToStart.Delete(coveredEndKey)
		}
		if lastEnd < endKey {
			// Available ranges  : ... ____|  |____________|      ...
			// Leaderless region : ... _________________________| ...
			//
			//                                              |___| <- Trimmed region
			//                         _________________________| <- New available range end key
			lastNewRegion := pdpb.Region
			lastNewRegion.StartKey = lastEnd
			lastNewRegion.EndKey = endKey
			plan.ToRegions.add(lastNewRegion)
		} else {
			recoveredRangeEndKey = lastEnd
		}
		alreadyCovered := false
		if len(coveredStartKeys) == 0 && len(coveredEndKeys) == 0 {
			validRangesStartToEnd.DescendLessOrEqual(rangeItem{startKey, ""}, func(item btree.Item) bool {
				if item.(rangeItem).value >= endKey {
					alreadyCovered = true
				}
				return false
			})
		}
		if !alreadyCovered {
			validRangesStartToEnd.ReplaceOrInsert(rangeItem{recoveredRangeStartKey, recoveredRangeEndKey})
			validRangesEndToStart.ReplaceOrInsert(rangeItem{recoveredRnageEndKey, recoveredRangeStartKey})
		}
	}
}

// Show returns the current status of ongoing unsafe recover operation.
func (u *unsafeRecoveryController) Show() string {
	u.RLock()
	defer u.RUnlock()
	switch u.stage {
	case ready:
		return "Ready"
	case collectingClusterInfo:
		return fmt.Sprintf("Collecting cluster info from all alive stores, %d/%d.",
			u.numStoresReported, len(u.storeReports))
	case recovering:
		return fmt.Sprintf("Recovering, %d/%d", u.numStoresPlanExecuted, len(u.storeRecoveryPlans))
	}
	return "Undefined status"
}

// History returns the history logs of the current unsafe recover operation.
func (u *unsafeRecoveryController) History() string {
	history := "Current status: " + u.Show()
	history += "\nFailed stores: "
	for storeID := range u.failedStores {
		history += strconv.FormatUint(storeID, 10) + ","
	}
	history += "\nStore reports: "
	for storeID, report := range u.storeReports {
		history += "\n" + strconv.FormatUint(storeID, 10)
		if report == nil {
			history += ": not yet reported"
		} else {
			history += ": reported"
		}
	}
	return history
}
