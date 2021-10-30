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
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	"github.com/tikv/pd/pkg/mock/mockid"
	"github.com/tikv/pd/server/core"
	"github.com/tikv/pd/server/kv"
)

var _ = Suite(&testUnsafeRecoverSuite{})

type testUnsafeRecoverSuite struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *testUnsafeRecoverSuite) TearDownTest(c *C) {
	s.cancel()
}

func (s *testUnsafeRecoverSuite) SetUpTest(c *C) {
	s.ctx, s.cancel = context.WithCancel(context.Background())
}

func (s *testUnsafeRecoverSuite) TestOneHealthyRegion(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	recoveryController.failedStores = map[uint64]string{
		3: "",
	}
	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	// Rely on PD replica checker to remove failed stores.
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 0)
}

func (s *testUnsafeRecoverSuite) TestOneUnhealthyRegion(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	recoveryController.failedStores = map[uint64]string{
		2: "",
		3: "",
	}
	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 1)
	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
	c.Assert(ok, IsTrue)
	c.Assert(len(store1Plan.Updates), Equals, 1)
	update := store1Plan.Updates[0]
	c.Assert(bytes.Compare(update.StartKey, []byte("")), Equals, 0)
	c.Assert(bytes.Compare(update.EndKey, []byte("")), Equals, 0)
	c.Assert(len(update.Peers), Equals, 1)
	c.Assert(update.Peers[0].StoreId, Equals, uint64(1))
}

func (s *testUnsafeRecoverSuite) TestEmptyRange(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	recoveryController.failedStores = map[uint64]string{
		3: "",
	}
	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 1)
	for storeID, plan := range recoveryController.storeRecoveryPlans {
		c.Assert(len(plan.Creates), Equals, 1)
		create := plan.Creates[0]
		c.Assert(bytes.Compare(create.StartKey, []byte("c")), Equals, 0)
		c.Assert(bytes.Compare(create.EndKey, []byte("")), Equals, 0)
		c.Assert(len(create.Peers), Equals, 1)
		c.Assert(create.Peers[0].StoreId, Equals, storeID)
		c.Assert(create.Peers[0].Role, Equals, metapb.PeerRole_Voter)
	}
}

func (s *testUnsafeRecoverSuite) TestUseNewestRanges(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	recoveryController.failedStores = map[uint64]string{
		3: "",
		4: "",
	}
	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 20},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 31, StoreId: 3}, {Id: 41, StoreId: 4}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          2,
						StartKey:    []byte("a"),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 12, StoreId: 1}, {Id: 22, StoreId: 2}, {Id: 32, StoreId: 3}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          4,
						StartKey:    []byte("m"),
						EndKey:      []byte("p"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 14, StoreId: 1}, {Id: 24, StoreId: 2}, {Id: 44, StoreId: 4}}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          3,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 5},
						Peers: []*metapb.Peer{
							{Id: 23, StoreId: 2}, {Id: 33, StoreId: 3}, {Id: 43, StoreId: 4}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          2,
						StartKey:    []byte("a"),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 12, StoreId: 1}, {Id: 22, StoreId: 2}, {Id: 32, StoreId: 3}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          4,
						StartKey:    []byte("m"),
						EndKey:      []byte("p"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 10},
						Peers: []*metapb.Peer{
							{Id: 14, StoreId: 1}, {Id: 24, StoreId: 2}, {Id: 44, StoreId: 4}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 2)
	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
	c.Assert(ok, IsTrue)
	updatedRegion1 := store1Plan.Updates[0]
	c.Assert(updatedRegion1.Id, Equals, uint64(1))
	c.Assert(len(updatedRegion1.Peers), Equals, 1)
	c.Assert(bytes.Compare(updatedRegion1.StartKey, []byte("")), Equals, 0)
	c.Assert(bytes.Compare(updatedRegion1.EndKey, []byte("a")), Equals, 0)

	store2Plan := recoveryController.storeRecoveryPlans[2]
	updatedRegion3 := store2Plan.Updates[0]
	c.Assert(updatedRegion3.Id, Equals, uint64(3))
	c.Assert(len(updatedRegion3.Peers), Equals, 1)
	c.Assert(bytes.Compare(updatedRegion3.StartKey, []byte("c")), Equals, 0)
	c.Assert(bytes.Compare(updatedRegion3.EndKey, []byte("m")), Equals, 0)
	create := store2Plan.Creates[0]
	c.Assert(bytes.Compare(create.StartKey, []byte("p")), Equals, 0)
	c.Assert(bytes.Compare(create.EndKey, []byte("")), Equals, 0)
}

func (s *testUnsafeRecoverSuite) TestMembershipChange(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	recoveryController.failedStores = map[uint64]string{
		4: "",
		5: "",
	}
	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 2, Version: 2},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 41, StoreId: 4}, {Id: 51, StoreId: 5}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          2,
						StartKey:    []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          2,
						StartKey:    []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
		3: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          2,
						StartKey:    []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 3)
	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
	c.Assert(ok, IsTrue)
	updatedRegion1 := store1Plan.Updates[0]
	c.Assert(updatedRegion1.Id, Equals, uint64(1))
	c.Assert(len(updatedRegion1.Peers), Equals, 1)
	c.Assert(bytes.Compare(updatedRegion1.StartKey, []byte("")), Equals, 0)
	c.Assert(bytes.Compare(updatedRegion1.EndKey, []byte("c")), Equals, 0)

	store2Plan := recoveryController.storeRecoveryPlans[2]
	deleteStaleRegion1 := store2Plan.Deletes[0]
	c.Assert(deleteStaleRegion1, Equals, uint64(1))

	store3Plan := recoveryController.storeRecoveryPlans[3]
	deleteStaleRegion1 = store3Plan.Deletes[0]
	c.Assert(deleteStaleRegion1, Equals, uint64(1))
}

func (s *testUnsafeRecoverSuite) TestPromotingLearner(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	recoveryController.failedStores = map[uint64]string{
		2: "",
		3: "",
	}
	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1, Role: metapb.PeerRole_Learner}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 1)
	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
	c.Assert(ok, IsTrue)
	c.Assert(len(store1Plan.Updates), Equals, 1)
	update := store1Plan.Updates[0]
	c.Assert(bytes.Compare(update.StartKey, []byte("")), Equals, 0)
	c.Assert(bytes.Compare(update.EndKey, []byte("")), Equals, 0)
	c.Assert(len(update.Peers), Equals, 1)
	c.Assert(update.Peers[0].StoreId, Equals, uint64(1))
	c.Assert(update.Peers[0].Role, Equals, metapb.PeerRole_Voter)
}

func (s *testUnsafeRecoverSuite) TestKeepingOneReplica(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	recoveryController.failedStores = map[uint64]string{
		3: "",
		4: "",
	}
	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
		1: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}, {Id: 41, StoreId: 4}}}}},
		}},
		2: {PeerReports: []*pdpb.PeerReport{
			{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							{Id: 11, StoreId: 1}, {Id: 21, StoreId: 2}, {Id: 31, StoreId: 3}, {Id: 41, StoreId: 4}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 2)
	foundUpdate := false
	foundDelete := false
	for storeID, plan := range recoveryController.storeRecoveryPlans {
		if len(plan.Updates) == 1 {
			foundUpdate = true
			update := plan.Updates[0]
			c.Assert(bytes.Compare(update.StartKey, []byte("")), Equals, 0)
			c.Assert(bytes.Compare(update.EndKey, []byte("")), Equals, 0)
			c.Assert(len(update.Peers), Equals, 1)
			c.Assert(update.Peers[0].StoreId, Equals, storeID)
		} else if len(plan.Deletes) == 1 {
			foundDelete = true
			c.Assert(plan.Deletes[0], Equals, uint64(1))
		}
	}
	c.Assert(foundUpdate, Equals, true)
	c.Assert(foundDelete, Equals, true)
}
