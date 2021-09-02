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
	"strconv"
	"sync"

	"github.com/gogo/protobuf/proto"
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

type regionItem struct {
	region *metapb.Region
}

func (r regionItem) Less(other btree.Item) bool {
	return bytes.Compare(r.region.StartKey, other.(regionItem).region.StartKey) < 0
}

func (u *unsafeRecoveryController) canElectLeader(region *metapb.Region) {
	numFailedPeers := 0
	for _, peer := range reigon.Peers {
		_, ok := u.failedStores[peer.StoreId]
		if ok {
			numFailedPeers += 1
		}
	}
	return numFailedPeers*2 < len(region.Peers)
}

func (u *unsafeRecoveryController) generateRecoveryPlan() {
	u.Lock()
	defer u.Unlock()
	// Peers that cannot elect a leader due to store failures.
	leaderlessPeers := make([]*pdpb.PeerReport)
	availableRegionReports := make(map[uint64]*metapb.Region)
	for _, storeReport := range u.storeReports {
		for _, peerReport := range storeReports.Reports {
			region := peerReport.RegionState.Region
			if u.canElectLeader(region) {
				existing, ok := availableRegionReports[region.Id]
				if ok && existing.RegionEpoch.ConfVer > region.RegionEpoch.ConfVer && existing.RegionEpoch.Version > region.RegionEpoch.Version {
					// Only keeps the newest region report.
					continue
				}
				availableRegionReports[region.Id] = region
			} else {
				leaderlessPeers = append(leaderlessPeers, peerReport)
			}
		}
	}
	// Uses 2 ordered map to implement the interval map.
	validRegions := btree.New(2)
	for _, region := range regionReports {
		validRegions.ReplaceOrInsert(regionItem{region})
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
		overlapRegions := make([]*metapb.Region, 0)
		validRegions.DescendLessOrEqual(regionItem{&metapb.Region{StartKey: startKey}}, func(item btree.Item) bool {
			if item.(regionItem).region.StartKey != startKey {
				// If the start keys are the same, the region will still be added while we
				// traverse the btree in ascending order below.
				overlapRegions = append(overlapRegions, item.(regionItem).region)
			}
			return false
		})

		validRegions.AscendGreaterOrEqual(regionItem{&metapb.Region{StartKey: startKey}}, func(item btree.Item) bool {
			if item.(regionItem).region.StartKey > endKey {
				return false
			}
			overlapRegions = append(overlapRegions, item.(regionItem).region)
			return true
		})
		newStart := startKey
		if len(overlapRegions) > 0 && overlapRegions[0].StartKey <= startKey && overlapRegions[0].EndKey > startKey {
			newStart = overlapRegions[0].EndKey
		}
		for _, overlapRegion := range overlapRegions {
			if newStart < overlapRegion.StartKey {
				newEnd = overlapRegion.StartKey
			} else if newStart == overlapRegion.StartKey {
				newStart = overlapRegion.EndKey
			}
		}
		peerPlan := &pdpb.PeerPlan{}
		peerPlan.RegionId = peer.RegionState.Region.Id
		if newStart >= endKey {
			// This peer's range has been fully covered, we can drop this peer.
			peerPlan.Drop = true
		} else {
			// Trim this peer's range to fill a hole.
			updatedRegion := proto.Clone(peer.RegionState.Region).(*metapb.Region)
			updatedRegion.StartKey = newStart
			updatedRegion.EndKey = newEnd
			peerPlan.Target = updatedRegion
			validRegions.ReplaceOrInsert(regionItem{updatedRegion})
		}

		storePlan, ok := storeRecoveryPlans[peer.StoreId]
		if !ok {
			storeRecoveryPlans[peer.StoreId] = &pdpb.RecoveryPlan{}
			storePlan = storeRecoveryPlans[peer.StoreId]
		}
		storePlan.PeerPlan = append(storePlan.PeerPlan, peerPlan)
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
