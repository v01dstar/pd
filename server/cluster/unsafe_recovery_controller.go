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
	// Peers that cannot elect a leader due to store failures.
	leaderlessPeers := make([]*pdpb.PeerReport)
	regionReports := make(map[uint64]*pdpb.PeerReport)
	for _, storeReport := range u.storeReports {
		for _, peerReport := range storeReports.Reports {
			if peerReport.HasLeader {
				existingReport, ok := regionReports[peerReport.RegionState.Region.Id]
				if ok && existingReport.RegionState.Region.RegionEpoch.Version > peerReport.RegionState.Region.RegionEpoch.Version {
					// Only keeps the newest region report.
					continue
				}
				regionReports[peerReport.RegionState.Region.Id] = peerReport
			} else {
				leaderlessPeers = append(leaderlessPeers, peerReport)
			}
		}
	}
	// Uses 2 ordered map to implement the interval map.
	validRangesStartToEnd := btree.New(2)
	validRangesEndToStart := btree.New(2)
	for region, report := range regionReports {
		validRangesStartToEnd.ReplaceOrInsert(rangeItem{report.RegionState.Region.StartKey, report.RegionState.Region.EndKey})
		validRangesEndToStart.ReplaceOrInsert(rangeItem{report.RegionState.Region.EndKey, report.RegionState.Region.StartKey})
	}
	sort.SliceTable(leaderlessPeers, func(i, j int) bool {
		return leaderpeerlessPeers[i].RaftState.LastIndex > leaderlessPeers[j].RaftState.LastIndex
	})
	for _, peer := range leaderlessPeers {
		// Iterates all leaderless peers, if a peer has been fully
		// covered by a valid range (from another region or the same region but another peer), do nothing, while, if a peer can fill some
		// of the gaps between available ranges so far, find all the gaps and ask the peer
		// to create new regions for each of the gap, besides, merge the all the scattered
		// ranges that are connected by filling up these gaps.
		startKey := peer.RegionState.Region.StartKey
		endKey := peer.RegionState.Region.EndKey
		coveredStartKeys := make([]rangItem, 0)
		validRangesStartToEnd.AscendGreaterOrEqual(rangeItem{startKey, ""}, func(item btree.Item) bool {
			if item.(rangeItem).key > endKey {
				return false
			}
			coveredStartKeys = append(coveredStartKeys, item.(rangeItem))
			return true
		})
		coveredEndKeys := make([]rangeItem, 0)
		validRangesEndToStart.AscendGreaterOrEqual(rangeItem{startKey, ""}, func(item btree.Item) bool {
			if item.(rangeItem).key > endKey {
				return false
			}
			coveredEndKeys = append(coveredEndKeys, item.(rangeItem))
			return true
		})
		plan := &pdpb.PeerPlan{} // Stores the target state, empty plan means dropping the peer entirely.
		newContinuousRangeStart := startKey
		newContinuousRangeEnd := endKey
		lastEnd := startKey
		if len(coveredEndKeys) != 0 && coveredEndKey[0].value <= startKey {
			// The first available range that overlaps the leaderless region covers the
			// leaderless region's start key. In this case, the first new region to be
			// created starts with the first covered end key, and the new continuous
			// range starts with the start key of the first overlapping range.
			//
			// Available ranges  : ... |_________|    |______   ....
			// Leaderless region : ...     |_________________   ....
			//
			//                                   |____|         <- First new region
			//                         |_____________________   <- New available range start key
			lastEnd = coveredEndKey[0].key // The end key of the first overlapping range.
			recoveredRangeStartKey = coveredEndKey[0].value
		}
		for _, coveredStartKey := range coveredStartKeys {
			if coveredStartKey.key == startKey {
				continue
			}
			newRegion := &pdpb.Region{}
			newRegion.StartKey = lastEnd
			newRegion.EndKey = coveredStartKey.key
			plan.ToRegions = append(plan.ToRegions, newRegion)
			lastEnd = coveredStartKey.value
			validRangesStartToEnd.Delete(coveredStartKey) // Delete this, since it is going to be merged
		}
		for _, coveredEndKey := range coveredEndKeys {
			validrangesEndToStart.Delete(coveredEndKey) // Delete this, since it is going to be merged
		}
		if lastEnd < endKey {
			// Available ranges  : ... ____|  |____________|      ...
			// Leaderless region : ... _________________________| ...
			//
			//                                              |___| <- The last new region
			//                         _________________________| <- New available range end key
			lastNewRegion := &pdpb.Region{}
			lastNewRegion.StartKey = lastEnd
			lastNewRegion.EndKey = endKey
			plan.ToRegions = append(plan.ToRegions, lastNewRegion)
		} else {
			recoveredRangeEndKey = lastEnd
		}
		fullyCovered := false
		if len(coveredStartKeys) == 0 && len(coveredEndKeys) == 0 {
			validRangesStartToEnd.DescendLessOrEqual(rangeItem{startKey, ""}, func(item btree.Item) bool {
				// Find the nearest smaller start key from all available ranges,
				// see if its end key is greater or equal than the current peer's
				// end key (fully covers the current peer).
				if item.(rangeItem).value >= endKey {
					fullyCovered = true
				}
				return false
			})
		}
		if !fullyCovered {
			validRangesStartToEnd.ReplaceOrInsert(rangeItem{recoveredRangeStartKey, recoveredRangeEndKey})
			validRangesEndToStart.ReplaceOrInsert(rangeItem{recoveredRnageEndKey, recoveredRangeStartKey})
		}
		recoveryPlan, ok := storeRecoveryPlans[peer.StoreId]
		if !ok {
			storeRecoveryPlans[peer.StoreId] = &pdpb.RecoveryPlan{}
			recoveryPlan = storeRecoveryPlans[peer.StoreId]
		}
		recoveryPlan.PeerRecoveryPlan = append(recoveryPlan.PeerRecoveryPlan, plan)
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
