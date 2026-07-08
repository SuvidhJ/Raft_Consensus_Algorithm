package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
)

func waitForNodeLeader(t *testing.T, node *raft.RaftNode, timeout time.Duration) {
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

func TestReplayCommittedEntriesAfterRestart(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-10")

	applyCh1 := make(chan *proto.LogEntry, 64)
	node1 := raft.NewRaftNodeWithConfig(raft.Config{
		ID:                  10,
		Peers:               nil,
		ApplyCh:             applyCh1,
		DataDir:             dataDir,
		HeartbeatInterval:   50 * time.Millisecond,
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		ProposalTimeout:     2 * time.Second,
		ReadQuorumTimeout:   400 * time.Millisecond,
		SnapshotThreshold:   1024,
		MaxEntriesPerAppend: 64,
		EnablePreVote:       true,
		EnableCheckQuorum:   true,
	})
	waitForNodeLeader(t, node1, 3*time.Second)

	if !node1.Propose("cmd-1") || !node1.Propose("cmd-2") {
		node1.Close()
		t.Fatal("proposals failed on first node")
	}

	seen := 0
	deadline := time.Now().Add(2 * time.Second)
	for seen < 2 && time.Now().Before(deadline) {
		select {
		case <-applyCh1:
			seen++
		case <-time.After(25 * time.Millisecond):
		}
	}
	node1.Close()
	if seen != 2 {
		t.Fatalf("expected 2 applied entries before restart, got %d", seen)
	}

	applyCh2 := make(chan *proto.LogEntry, 64)
	node2 := raft.NewRaftNodeWithConfig(raft.Config{
		ID:                  10,
		Peers:               nil,
		ApplyCh:             applyCh2,
		DataDir:             dataDir,
		HeartbeatInterval:   50 * time.Millisecond,
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		ProposalTimeout:     2 * time.Second,
		ReadQuorumTimeout:   400 * time.Millisecond,
		SnapshotThreshold:   1024,
		MaxEntriesPerAppend: 64,
		EnablePreVote:       true,
		EnableCheckQuorum:   true,
	})
	defer node2.Close()
	waitForNodeLeader(t, node2, 3*time.Second)

	seenAfterRestart := 0
	deadline = time.Now().Add(2 * time.Second)
	for seenAfterRestart < 2 && time.Now().Before(deadline) {
		select {
		case <-applyCh2:
			seenAfterRestart++
		case <-time.After(25 * time.Millisecond):
		}
	}
	if seenAfterRestart != 2 {
		t.Fatalf("expected replay of 2 committed entries after restart, got %d", seenAfterRestart)
	}
}

func TestCloseWithBlockedApplyChannel(t *testing.T) {
	applyCh := make(chan *proto.LogEntry)
	node := raft.NewRaftNodeWithConfig(raft.Config{
		ID:                  11,
		Peers:               nil,
		ApplyCh:             applyCh,
		DataDir:             filepath.Join(t.TempDir(), "node-11"),
		HeartbeatInterval:   50 * time.Millisecond,
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		ProposalTimeout:     2 * time.Second,
		ReadQuorumTimeout:   400 * time.Millisecond,
		SnapshotThreshold:   1024,
		MaxEntriesPerAppend: 64,
		EnablePreVote:       true,
		EnableCheckQuorum:   true,
	})
	waitForNodeLeader(t, node, 3*time.Second)

	if !node.Propose("cmd-close") {
		node.Close()
		t.Fatal("proposal failed")
	}

	done := make(chan struct{})
	go func() {
		node.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("close blocked with pending apply delivery")
	}
}

func TestSetAckImpliesImmediateReadVisibility(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-12")

	node, store, cleanup := startSingleNode(t, dataDir, 12)
	defer cleanup()
	waitForSingleLeader(t, node, 3*time.Second)

	for i := 0; i < 25; i++ {
		v := fmt.Sprintf("value-%d", i)
		setResp, err := store.Set(context.Background(), &proto.SetRequest{Key: "ack", Value: v})
		if err != nil || !setResp.Success {
			t.Fatalf("set failed at iter=%d resp=%v err=%v", i, setResp, err)
		}
		getResp, err := store.Get(context.Background(), &proto.GetRequest{Key: "ack"})
		if err != nil || !getResp.Success || getResp.Value != v {
			t.Fatalf("immediate read mismatch at iter=%d got=%v err=%v want=%q", i, getResp, err, v)
		}
	}
}
