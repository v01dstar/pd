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
		1: &pdpb.StoreReport{StoreId: 1, Reports: []*pdpb.PeerReport{
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 11, StoreId: 1}, &metapb.Peer{Id: 21, StoreId: 2}, &metapb.Peer{Id: 31, StoreId: 3}}}}},
		}},
		2: &pdpb.StoreReport{StoreId: 2, Reports: []*pdpb.PeerReport{
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 11, StoreId: 1}, &metapb.Peer{Id: 21, StoreId: 2}, &metapb.Peer{Id: 31, StoreId: 3}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 2)
	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
	c.Assert(ok, IsTrue)
	updatedRegion1 := store1Plan.Updates[0]
	c.Assert(updatedRegion1.Id, Equals, uint64(1))
	c.Assert(len(updatedRegion1.Peers), Equals, 2)
	c.Assert(updatedRegion1.Peers[0].Id, Equals, uint64(11))
	c.Assert(updatedRegion1.Peers[1].Id, Equals, uint64(21))

	store2Plan, ok := recoveryController.storeRecoveryPlans[2]
	updatedRegion1 = store2Plan.Updates[0]
	c.Assert(updatedRegion1.Id, Equals, uint64(1))
	c.Assert(len(updatedRegion1.Peers), Equals, 2)
	c.Assert(updatedRegion1.Peers[0].Id, Equals, uint64(11))
	c.Assert(updatedRegion1.Peers[1].Id, Equals, uint64(21))
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
		1: &pdpb.StoreReport{StoreId: 1, Reports: []*pdpb.PeerReport{
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 11, StoreId: 1}, &metapb.Peer{Id: 21, StoreId: 2}, &metapb.Peer{Id: 31, StoreId: 3}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 1)
	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
	c.Assert(ok, IsTrue)
	c.Assert(len(store1Plan.Updates), Equals, 0)
	c.Assert(store1Plan.Deletes[0], Equals, uint64(1))
	create := store1Plan.Creates[0]
	c.Assert(bytes.Compare(create.StartKey, []byte("")), Equals, 0)
	c.Assert(bytes.Compare(create.EndKey, []byte("")), Equals, 0)
	c.Assert(len(create.Peers), Equals, 1)
	c.Assert(create.Peers[0].StoreId, Equals, uint64(1))
}

func (s *testUnsafeRecoverSuite) TestEmptyRange(c *C) {
	_, opt, _ := newTestScheduleConfig()
	cluster := newTestRaftCluster(s.ctx, mockid.NewIDAllocator(), opt, core.NewStorage(kv.NewMemoryKV()), core.NewBasicCluster())
	recoveryController := newUnsafeRecoveryController(cluster)
	recoveryController.failedStores = map[uint64]string{
		3: "",
	}
	recoveryController.storeReports = map[uint64]*pdpb.StoreReport{
		1: &pdpb.StoreReport{StoreId: 1, Reports: []*pdpb.PeerReport{
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 11, StoreId: 1}, &metapb.Peer{Id: 21, StoreId: 2}, &metapb.Peer{Id: 31, StoreId: 3}}}}},
		}},
		2: &pdpb.StoreReport{StoreId: 2, Reports: []*pdpb.PeerReport{
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 11, StoreId: 1}, &metapb.Peer{Id: 21, StoreId: 2}, &metapb.Peer{Id: 31, StoreId: 3}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	create := recoveryController.storeRecoveryPlans[1].Creates[0]
	c.Assert(bytes.Compare(create.StartKey, []byte("c")), Equals, 0)
	c.Assert(bytes.Compare(create.EndKey, []byte("")), Equals, 0)
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
		1: &pdpb.StoreReport{StoreId: 1, Reports: []*pdpb.PeerReport{
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          1,
						StartKey:    []byte(""),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 2},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 11, StoreId: 1}, &metapb.Peer{Id: 31, StoreId: 3}, &metapb.Peer{Id: 41, StoreId: 4}}}}},
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          2,
						StartKey:    []byte("a"),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 12, StoreId: 1}, &metapb.Peer{Id: 22, StoreId: 2}, &metapb.Peer{Id: 32, StoreId: 3}}}}},
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          4,
						StartKey:    []byte("m"),
						EndKey:      []byte("p"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 14, StoreId: 1}, &metapb.Peer{Id: 24, StoreId: 2}, &metapb.Peer{Id: 44, StoreId: 4}}}}},
		}},
		2: &pdpb.StoreReport{StoreId: 2, Reports: []*pdpb.PeerReport{
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          3,
						StartKey:    []byte(""),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 23, StoreId: 2}, &metapb.Peer{Id: 33, StoreId: 3}, &metapb.Peer{Id: 43, StoreId: 4}}}}},
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          2,
						StartKey:    []byte("a"),
						EndKey:      []byte("c"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 12, StoreId: 1}, &metapb.Peer{Id: 22, StoreId: 2}, &metapb.Peer{Id: 32, StoreId: 3}}}}},
			&pdpb.PeerReport{
				RaftState: &raft_serverpb.RaftLocalState{LastIndex: 10},
				RegionState: &raft_serverpb.RegionLocalState{
					Region: &metapb.Region{
						Id:          4,
						StartKey:    []byte("m"),
						EndKey:      []byte("p"),
						RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
						Peers: []*metapb.Peer{
							&metapb.Peer{Id: 14, StoreId: 1}, &metapb.Peer{Id: 24, StoreId: 2}, &metapb.Peer{Id: 44, StoreId: 4}}}}},
		}},
	}
	recoveryController.generateRecoveryPlan()
	c.Assert(len(recoveryController.storeRecoveryPlans), Equals, 2)
	store1Plan, ok := recoveryController.storeRecoveryPlans[1]
	c.Assert(ok, IsTrue)
	updatedRegion2 := store1Plan.Updates[0]
	c.Assert(updatedRegion2.Id, Equals, uint64(2))
	c.Assert(len(updatedRegion2.Peers), Equals, 2)
	updatedRegion4 := store1Plan.Updates[1]
	c.Assert(updatedRegion4.Id, Equals, uint64(4))
	c.Assert(len(updatedRegion4.Peers), Equals, 2)
	create := store1Plan.Creates[0]
	c.Assert(bytes.Compare(create.StartKey, []byte("")), Equals, 0)
	c.Assert(bytes.Compare(create.EndKey, []byte("a")), Equals, 0)
	c.Assert(store1Plan.Deletes[0], Equals, uint64(1))

	store2Plan, ok := recoveryController.storeRecoveryPlans[2]
	updatedRegion2 = store2Plan.Updates[0]
	c.Assert(updatedRegion2.Id, Equals, uint64(2))
	c.Assert(len(updatedRegion2.Peers), Equals, 2)
	updatedRegion4 = store2Plan.Updates[1]
	c.Assert(updatedRegion4.Id, Equals, uint64(4))
	c.Assert(len(updatedRegion4.Peers), Equals, 2)
	create21 := store2Plan.Creates[0]
	create22 := store2Plan.Creates[1]
	c.Assert(bytes.Compare(create21.StartKey, []byte("c")), Equals, 0)
	c.Assert(bytes.Compare(create21.EndKey, []byte("m")), Equals, 0)
	c.Assert(bytes.Compare(create22.StartKey, []byte("p")), Equals, 0)
	c.Assert(bytes.Compare(create22.EndKey, []byte("")), Equals, 0)
}
