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
	regionReports := make(map[uint64]*metapb.Region)
	storeByRegion := make(map[uint64]uint64)
	for storeId, storeReport := range u.storeReports {
		for _, peerReport := range storeReport.Reports {
			region := peerReport.RegionState.Region
			existing, ok := regionReports[region.Id]
			if ok && existing.RegionEpoch.ConfVer > region.RegionEpoch.ConfVer && existing.RegionEpoch.Version > region.RegionEpoch.Version {
				// Only keeps the newest region report.
				continue
			}
			regionReports[region.Id] = region
			storeByRegion[region.Id] = storeId
		}
	}
	leaderlessRegions := make([]uint64)
	validRegions := btree.New(2)
	for id, region := range regionReports {
		if !u.canElectLeader(region) {
			leaderlessRegions = append(leaderlessRegions, id)
		} else {
			validRegions.ReplaceOrInsert(regionItem{region})
		}
	}
	sort.SliceTable(leaderlessRegions, func(i, j int) bool {
		return regionRerports[leaderpeerlessRegions[i]].RaftState.LastIndex > regionReports[leaderlessRegions[j]].RaftState.LastIndex
	})
	recoveryPlanByRegion := make(map[uint64]*pdpb.PeerPlan)
	for _, regionId := range leaderlessRegions {
		region := regionReports[regionId]
		// Iterates all leaderless peers, if a peer has been fully
		// covered by a valid range (from another region or the same region but another peer), do nothing, while, if a peer can fill some
		// of the gaps between available ranges so far, find all the gaps and ask the peer
		// to create new regions for each of the gap, besides, merge the all the scattered
		// ranges that are connected by filling up these gaps.
		overlapRegions := make([]*metapb.Region, 0)
		validRegions.DescendLessOrEqual(regionItem{region}, func(item btree.Item) bool {
			if item.(regionItem).region.StartKey != reigon.StartKey {
				// Only adds the first region that has smaller start key to the
				// potential overlap regions list, since the potentials overlap
				// regions that have greater or equal start keys are going to be
				// added to the list by the ascending search below.
				overlapRegions = append(overlapRegions, item.(regionItem).region)
			}
			return false
		})

		validRegions.AscendGreaterOrEqual(regionItem{region}, func(item btree.Item) bool {
			if region.EndKey != nil && item.(regionItem).region.StartKey > region.EndKey {
				return false
			}
			overlapRegions = append(overlapRegions, item.(regionItem).region)
			return true
		})
		peerPlan := &pdpb.PeerPlan{}
		peerPlan.RegionId = regionId
		isFirstPiece := true // Reuse the region id for the first piece of the cutted regions.
		lastEnd := region.StartKey
		for _, overlapRegion := range overlapRegions {
			if lastEnd == nil {
				break
			}
			if lastEnd < overlapRegion.StartKey {
				target := proto.Clone(region).(*metapb.Region)
				if isFirstPiece {
					target.Id = regionId
					isFirstPiece = false
				} else {
					newRegionId = u.cluster.AllocID()
					target.Id = newRegionId
				}
				target.StartKey = lastEnd
				target.EndKey = overlapRegion.StartKey
				peerPlan.Targets = append(peerPlan.Targets, target)
				validRegions.ReplaceOrInsert(regionItem{target})
				lastEnd = overlapRegion.EndKey
			} else if overlapRegionEndKey == nil || overlapRegion.EndKey > lastEnd {
				lastEnd = overlapRegion.EndKey
			}
		}
		if lastEnd != nil && (lastEnd < region.EndKey || region.EndKey == nil) {
			target := proto.Clone(region).(*metapb.Region)
			if isFirstPiece {
				target.Id = regionId
				isFirstPiece = false
			} else {
				newRegionId = u.cluster.AllocID()
				target.Id = newRegionId
			}
			target.StartKey = lastEnd
			target.EndKey = region.EndKey
			peerPlan.Targets = append(peerPlan.Targets, target)
			validRegions.ReplaceOrInsert(regionItem{target})
		}
		recoveryPlanByRegion[regionId] = peerPlan
	}
	for storeId, storeReport := range u.storeReports {
		storePlan := &pdpb.RecoveryPlan{}
		for _, peerReport := range storeReport.Reports {
			regionId := peerReport.RegionState.Region.Id
			regionPlan, isLeaderLess := recoveryPlanByRegion[regionId]
			if !isLeaderLess {
				continue
			}
			if storeByRegion[regionId] != storeId {
				// If this peer is not the newest replica chosen for the region,
				// notify the store to drop it by sending a empty peer plan
				peerPlan := &pdpb.PeerPlan{}
				peerPlan.RegionId = regionId
				storePlan.PeerPlan = append(storePlan.PeerPlan, peerPlan)
			} else {
				storePlan.PeerPlan = append(storePlan.PeerPlan, regionPlan)
			}
		}
		u.StoreRecoveryPlans[storeId] = storePlan
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
