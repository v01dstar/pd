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
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/google/btree"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
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
	targetRegions := make(map[uint64]*metapb.Region)
	toBeRemovedRegions := make(map[uint64]bool)
	storeId := report.StoreId
	for _, create := range u.storeRecoveryPlans[storeId].Creates {
	        targetRegions[create.Id] = create
	}
	for _, update := range u.storeRecoveryPlans[storeId].Updates {
	        targetRegions[update.Id] = update
	}
	for _, del := range u.storeRecoveryPlans[storeId].Deletes {
	        toBeRemovedRegions[del] = true
	}
	numFinished := 0
	for _, peerReport := range report.Reports {
		region := peerReport.RegionState.Region
		if _, ok := toBeRemovedRegions[region.Id]; ok {
			return false
		} else if target, ok := targetRegions[region.Id]; ok {
			if bytes.Equal(target.StartKey, region.StartKey) && bytes.Equal(target.EndKey, region.EndKey) {
				numFinished += 1
			} else {
				return false
			}
		}
	}
	return numFinished == len(targetRegions)

}

type regionItem struct {
	region *metapb.Region
}

func (r regionItem) Less(other btree.Item) bool {
	return bytes.Compare(r.region.StartKey, other.(regionItem).region.StartKey) < 0
}

func (u *unsafeRecoveryController) canElectLeader(region *metapb.Region) bool {
	numFailedPeers := 0
	for _, peer := range region.Peers {
		if _, ok := u.failedStores[peer.StoreId]; ok {
			numFailedPeers += 1
		}
	}
	return numFailedPeers*2 < len(region.Peers)
}

func (u *unsafeRecoveryController) removeFailedStores(region *metapb.Region) bool {
	updated := false
	var newPeerList []*metapb.Peer
	for _, peer := range region.Peers {
		if _, ok := u.failedStores[peer.StoreId]; !ok {
			newPeerList = append(newPeerList, peer)
		} else {
			updated = true
		}
	}
	region.Peers = newPeerList
	return updated
}

type peerStorePair struct {
	peer    *pdpb.PeerReport
	storeId uint64
}

func (u *unsafeRecoveryController) generateRecoveryPlan() {
	u.Lock()
	defer u.Unlock()
	healthyRegions := make(map[uint64]*pdpb.PeerReport)
	var allPeerReports []*peerStorePair
	for storeId, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.Reports {
			allPeerReports = append(allPeerReports, &peerStorePair{peerReport, storeId})
			regionId := peerReport.RegionState.Region.Id
			if existing, ok := healthyRegions[regionId]; ok && existing.RaftState.LastIndex > peerReport.RaftState.LastIndex {
				// Only keeps the newest region report.
				continue
			}
			if u.canElectLeader(peerReport.RegionState.Region) {
				healthyRegions[regionId] = peerReport
			} else {
				delete(healthyRegions, regionId)
			}
		}
	}
	recoveredRanges := btree.New(2)
	for _, report := range healthyRegions {
		recoveredRanges.ReplaceOrInsert(regionItem{report.RegionState.Region})
	}
	sort.SliceStable(allPeerReports, func(i, j int) bool {
		return allPeerReports[i].peer.RegionState.Region.RegionEpoch.Version > allPeerReports[j].peer.RegionState.Region.RegionEpoch.Version
	})
	for _, peerStorePair := range allPeerReports {
		region := peerStorePair.peer.RegionState.Region
		storeId := peerStorePair.storeId
		updated := u.removeFailedStores(region)
		// Iterates all leaderless peers, if a peer has been fully
		// covered by a valid range (from another region or the same region but another peer), do nothing, while, if a peer can fill some
		// of the gaps between available ranges so far, find all the gaps and ask the peer
		// to create new regions for each of the gap, besides, merge the all the scattered
		// ranges that are connected by filling up these gaps.
		overlapRanges := make([]*metapb.Region, 0)
		recoveredRanges.DescendLessOrEqual(regionItem{region}, func(item btree.Item) bool {
			if bytes.Compare(item.(regionItem).region.StartKey, region.StartKey) < 0 {
				// Only adds the first region that has smaller start key to the
				// *potential* overlap regions list.
				overlapRanges = append(overlapRanges, item.(regionItem).region)
			}
			return false
		})

		recoveredRanges.AscendGreaterOrEqual(regionItem{region}, func(item btree.Item) bool {
			if len(region.EndKey) != 0 && bytes.Compare(item.(regionItem).region.StartKey, region.EndKey) > 0 {
				return false
			}
			overlapRanges = append(overlapRanges, item.(regionItem).region)
			return true
		})
		lastEnd := region.StartKey
		reachedTheEnd := false
		var creates []*metapb.Region
		for _, overlapRegion := range overlapRanges {
			if bytes.Compare(lastEnd, overlapRegion.StartKey) < 0 {
				newRegion := proto.Clone(region).(*metapb.Region)
				newRegion.StartKey = lastEnd
				newRegion.EndKey = overlapRegion.StartKey
				newRegion.Id, _ = u.cluster.AllocID()
				creates = append(creates, newRegion)
				recoveredRanges.ReplaceOrInsert(regionItem{newRegion})
				if len(overlapRegion.EndKey) == 0 {
					reachedTheEnd = true
					break
				}
				lastEnd = overlapRegion.EndKey
			} else if len(overlapRegion.EndKey) == 0 {
				reachedTheEnd = true
				break
			} else if bytes.Compare(overlapRegion.EndKey, lastEnd) > 0 {
				lastEnd = overlapRegion.EndKey
			}
		}
		if !reachedTheEnd && (bytes.Compare(lastEnd, region.EndKey) < 0 || len(region.EndKey) == 0) {
			newRegion := proto.Clone(region).(*metapb.Region)
			newRegion.Id, _ = u.cluster.AllocID()
			newRegion.StartKey = lastEnd
			newRegion.EndKey = region.EndKey
			creates = append(creates, newRegion)
			recoveredRanges.ReplaceOrInsert(regionItem{newRegion})
		}
		if len(creates) != 0 {
			storeRecoveryPlan, exists := u.storeRecoveryPlans[storeId]
			if !exists {
				u.storeRecoveryPlans[storeId] = &pdpb.RecoveryPlan{}
				storeRecoveryPlan = u.storeRecoveryPlans[storeId]
			}
			storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, creates...)
		}
		if _, healthy := healthyRegions[region.Id]; healthy {
			if updated {
				storeRecoveryPlan, exists := u.storeRecoveryPlans[storeId]
				if !exists {
					u.storeRecoveryPlans[storeId] = &pdpb.RecoveryPlan{}
					storeRecoveryPlan = u.storeRecoveryPlans[storeId]
				}
				storeRecoveryPlan.Updates = append(storeRecoveryPlan.Updates, region)
			}
		} else {
			storeRecoveryPlan, exists := u.storeRecoveryPlans[storeId]
			if !exists {
				u.storeRecoveryPlans[storeId] = &pdpb.RecoveryPlan{}
				storeRecoveryPlan = u.storeRecoveryPlans[storeId]
			}
			storeRecoveryPlan.Deletes = append(storeRecoveryPlan.Deletes, region.Id)
		}
	}
	lastEnd := []byte("")
	var creates []*metapb.Region
	recoveredRanges.Ascend(func(item btree.Item) bool {
	        region := item.(regionItem).region
		if bytes.Compare(region.StartKey, lastEnd) != 0 {
			newRegion := &metapb.Region{}
			newRegion.StartKey = lastEnd
			newRegion.EndKey = region.StartKey
			newRegion.Id, _ = u.cluster.AllocID()
			creates = append(creates, newRegion)
		}
		lastEnd = region.EndKey
		return true
	})
	if bytes.Compare(lastEnd, []byte("")) != 0 {
		newRegion := &metapb.Region{}
		newRegion.StartKey = lastEnd
		newRegion.Id, _ = u.cluster.AllocID()
		creates = append(creates, newRegion)
	}
	var allStores []uint64
	for storeId, _:= range u.storeReports {
	    allStores = append(allStores, storeId)
	}
	for idx, create := range creates {
	        storeId := allStores[idx % len(allStores)]
		storeRecoveryPlan, exists := u.storeRecoveryPlans[storeId]
		if !exists {
			u.storeRecoveryPlans[storeId] = &pdpb.RecoveryPlan{}
			storeRecoveryPlan = u.storeRecoveryPlans[storeId]
		}
		storeRecoveryPlan.Creates = append(storeRecoveryPlan.Creates, create)
	}
	u.stage = recovering
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
