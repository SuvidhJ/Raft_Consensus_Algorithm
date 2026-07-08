package tests

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
)

func waitForMembers(t *testing.T, n *testNode, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(n.node.Status().Members) == want {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("node %d never converged to %d members (have %v)", n.id, want, n.node.Status().Members)
}

func TestAddServerExpandsCluster(t *testing.T) {
	nodes, cleanup := startThreeNodeCluster(t)
	defer cleanup()

	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}

	// Seed some state the newcomer must catch up on.
	if resp, err := leader.store.Set(context.Background(), &proto.SetRequest{Key: "pre", Value: "join"}); err != nil || !resp.Success {
		t.Fatalf("seed set failed: resp=%v err=%v", resp, err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	newcomer := startTestNodeJoining(t, 4, lis, nil, t.TempDir(), true)
	defer stopTestNode(newcomer)

	if err := leader.node.AddServer(context.Background(), newcomer.addr); err != nil {
		t.Fatalf("AddServer failed: %v", err)
	}

	for _, n := range append(nodes, newcomer) {
		waitForMembers(t, n, 4, 5*time.Second)
	}

	// The newcomer must have replicated pre-join state.
	waitForValue(t, newcomer.store, "pre", "join", 3*time.Second)

	// Writes must still commit under the 4-member configuration (quorum 3).
	if resp, err := leader.store.Set(context.Background(), &proto.SetRequest{Key: "post", Value: "add"}); err != nil || !resp.Success {
		t.Fatalf("post-add set failed: resp=%v err=%v", resp, err)
	}
	waitForValue(t, newcomer.store, "post", "add", 3*time.Second)
}

func TestRemoveServerShrinksCluster(t *testing.T) {
	nodes, cleanup := startThreeNodeCluster(t)
	defer cleanup()

	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}

	var victim *testNode
	for _, n := range nodes {
		if n != leader {
			victim = n
			break
		}
	}

	if err := leader.node.RemoveServer(context.Background(), victim.addr); err != nil {
		t.Fatalf("RemoveServer failed: %v", err)
	}

	for _, n := range nodes {
		if n == victim {
			continue
		}
		waitForMembers(t, n, 2, 5*time.Second)
	}

	// Take the removed node fully offline; the 2-member cluster must still
	// commit writes (quorum 2 of 2).
	stopTestNode(victim)
	deadline := time.Now().Add(4 * time.Second)
	for {
		resp, err := leader.store.Set(context.Background(), &proto.SetRequest{Key: "post", Value: "remove"})
		if err == nil && resp.Success {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("post-remove set never succeeded: resp=%v err=%v", resp, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestAddServerRejectedOnFollower(t *testing.T) {
	nodes, cleanup := startThreeNodeCluster(t)
	defer cleanup()

	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}
	var follower *testNode
	for _, n := range nodes {
		if n != leader {
			follower = n
			break
		}
	}

	err := follower.node.AddServer(context.Background(), "127.0.0.1:1")
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader from follower, got %v", err)
	}
}

func TestRemoveLeaderIsRejected(t *testing.T) {
	nodes, cleanup := startThreeNodeCluster(t)
	defer cleanup()

	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}

	err := leader.node.RemoveServer(context.Background(), leader.addr)
	if !errors.Is(err, raft.ErrRemoveSelfLeader) {
		t.Fatalf("expected ErrRemoveSelfLeader, got %v", err)
	}
}

func TestMembershipSurvivesRestart(t *testing.T) {
	dataRoot := t.TempDir()

	listeners := make([]net.Listener, 3)
	for i := range listeners {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen failed: %v", err)
		}
		listeners[i] = lis
	}
	addrs := []string{listeners[0].Addr().String(), listeners[1].Addr().String(), listeners[2].Addr().String()}
	nodes := make([]*testNode, 0, 3)
	for i := 0; i < 3; i++ {
		peers := make([]string, 0, 2)
		for j := 0; j < 3; j++ {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}
		nodes = append(nodes, startTestNode(t, i+1, listeners[i], peers, dataRoot))
	}
	defer func() {
		for _, n := range nodes {
			stopTestNode(n)
		}
	}()

	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}

	var victim *testNode
	var victimIdx int
	for i, n := range nodes {
		if n != leader {
			victim = n
			victimIdx = i
			break
		}
	}
	if err := leader.node.RemoveServer(context.Background(), victim.addr); err != nil {
		t.Fatalf("RemoveServer failed: %v", err)
	}
	stopTestNode(victim)

	// Restart a surviving follower; it must recover the 2-member config
	// from its WAL, not the 3-member bootstrap list.
	var survivor *testNode
	var survivorIdx int
	for i, n := range nodes {
		if n != leader && n != victim {
			survivor = n
			survivorIdx = i
			break
		}
	}
	stopTestNode(survivor)
	lis, err := net.Listen("tcp", survivor.addr)
	if err != nil {
		t.Fatalf("relisten failed: %v", err)
	}
	peers := make([]string, 0, 2)
	for j := 0; j < 3; j++ {
		if j != survivorIdx {
			peers = append(peers, addrs[j])
		}
	}
	restarted := startTestNode(t, survivorIdx+1, lis, peers, dataRoot)
	nodes[survivorIdx] = restarted
	nodes[victimIdx] = nil
	defer stopTestNode(restarted)

	waitForMembers(t, restarted, 2, 5*time.Second)
}

func TestJoiningNodeNeverSelfElects(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	joiner := startTestNodeJoining(t, 9, lis, nil, t.TempDir(), true)
	defer stopTestNode(joiner)

	// Without joining mode an empty-peer node elects itself within ~300ms.
	time.Sleep(1200 * time.Millisecond)
	st := joiner.node.Status()
	if st.State == raft.Leader {
		t.Fatal("joining node must not elect itself as a single-node majority")
	}
	if st.Term != 0 {
		t.Fatalf("joining node should not have campaigned, term=%d", st.Term)
	}
}
