package tests

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/kv"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
)

func startSingleNode(t *testing.T, dataDir string, id uint32) (*raft.RaftNode, *kv.KVStore, func()) {
	t.Helper()

	applyCh := make(chan *proto.LogEntry, 256)
	node := raft.NewRaftNodeWithConfig(raft.Config{
		ID:                  id,
		Peers:               nil,
		ApplyCh:             applyCh,
		StrictApplyAcks:     true,
		DataDir:             dataDir,
		HeartbeatInterval:   50 * time.Millisecond,
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		ProposalTimeout:     2 * time.Second,
		ReadQuorumTimeout:   400 * time.Millisecond,
		SnapshotThreshold:   64,
		MaxEntriesPerAppend: 64,
		EnablePreVote:       true,
		EnableCheckQuorum:   true,
	})
	store := kv.NewKVStore(node)
	node.SetSnapshotter(store)

	stopApply := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopApply:
				return
			case entry := <-applyCh:
				store.ApplyCommand(entry)
			}
		}
	}()

	cleanup := func() {
		node.Close()
		close(stopApply)
	}

	return node, store, cleanup
}

func waitForSingleLeader(t *testing.T, node *raft.RaftNode, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if node.CurrentState() == raft.Leader {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("leader was not elected in time")
}

func waitForValue(t *testing.T, store *kv.KVStore, key, expected string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := store.Get(context.Background(), &proto.GetRequest{Key: key})
		if err == nil && resp.Success && resp.Value == expected {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("value for key=%s did not converge to %q", key, expected)
}

func TestPersistentReplayAcrossRestart(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-1")

	node1, store1, cleanup1 := startSingleNode(t, dataDir, 1)
	waitForSingleLeader(t, node1, 3*time.Second)

	if resp, err := store1.Set(context.Background(), &proto.SetRequest{Key: "k", Value: "v1"}); err != nil || !resp.Success {
		t.Fatalf("set v1 failed: resp=%v err=%v", resp, err)
	}
	if resp, err := store1.Set(context.Background(), &proto.SetRequest{Key: "k", Value: "v2"}); err != nil || !resp.Success {
		t.Fatalf("set v2 failed: resp=%v err=%v", resp, err)
	}
	waitForValue(t, store1, "k", "v2", 2*time.Second)
	cleanup1()

	node2, store2, cleanup2 := startSingleNode(t, dataDir, 1)
	defer cleanup2()
	waitForSingleLeader(t, node2, 3*time.Second)
	waitForValue(t, store2, "k", "v2", 2*time.Second)
}

func TestInstallSnapshotRestoresStateMachine(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-2")

	node, store, cleanup := startSingleNode(t, dataDir, 2)
	defer cleanup()

	snapMap := map[string]string{"alpha": "one", "beta": "two"}
	snapData, err := json.Marshal(snapMap)
	if err != nil {
		t.Fatalf("snapshot marshal failed: %v", err)
	}

	resp := node.HandleInstallSnapshot(&proto.InstallSnapshotRequest{
		Term:              3,
		LeaderId:          99,
		LastIncludedIndex: 12,
		LastIncludedTerm:  3,
		Data:              snapData,
	})
	if !resp.Success {
		t.Fatalf("install snapshot failed: %+v", resp)
	}

	waitForValue(t, store, "alpha", "one", 2*time.Second)
	waitForValue(t, store, "beta", "two", 2*time.Second)
}
