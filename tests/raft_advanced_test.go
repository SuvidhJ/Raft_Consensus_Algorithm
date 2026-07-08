package tests

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startThreeNodeCluster(t *testing.T) ([]*testNode, func()) {
	t.Helper()
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

	cleanup := func() {
		for _, n := range nodes {
			stopTestNode(n)
		}
	}
	return nodes, cleanup
}

func TestLeaderStepsDownWhenQuorumLost(t *testing.T) {
	nodes, cleanup := startThreeNodeCluster(t)
	defer cleanup()

	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}

	for _, n := range nodes {
		if n != leader {
			n.server.Stop()
			n.node.Close()
			_ = n.lis.Close()
		}
	}

	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if leader.node.CurrentState() != raft.Leader {
			return
		}
		time.Sleep(40 * time.Millisecond)
	}
	t.Fatal("leader did not step down after quorum loss")
}

func TestTransferLeadership(t *testing.T) {
	nodes, cleanup := startThreeNodeCluster(t)
	defer cleanup()

	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}

	var target *testNode
	for _, n := range nodes {
		if n != leader {
			target = n
			break
		}
	}
	if target == nil {
		t.Fatal("no transfer target found")
	}

	if ok := leader.node.TransferLeadership(target.addr); !ok {
		t.Fatal("leadership transfer request failed")
	}

	oldLeaderID := leader.id
	deadline := time.Now().Add(5 * time.Second)
	var newLeader *testNode
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.node.CurrentState() == raft.Leader && n.id != oldLeaderID {
				newLeader = n
				break
			}
		}
		if newLeader != nil {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("leadership did not move to another node")
	}
}

func TestLinearizableReadRPC(t *testing.T) {
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
	if follower == nil {
		t.Fatal("follower not found")
	}

	leaderConn, err := grpc.Dial(leader.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial leader failed: %v", err)
	}
	defer leaderConn.Close()
	leaderClient := proto.NewKVServiceClient(leaderConn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	setResp, err := leaderClient.Set(ctx, &proto.SetRequest{Key: "lr", Value: "ok"})
	cancel()
	if err != nil || !setResp.Success {
		t.Fatalf("set failed: resp=%v err=%v", setResp, err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	linResp, err := leaderClient.GetLinearizable(ctx, &proto.GetRequest{Key: "lr"})
	cancel()
	if err != nil || !linResp.Success || linResp.Value != "ok" {
		t.Fatalf("leader linearizable get failed: resp=%v err=%v", linResp, err)
	}

	followerConn, err := grpc.Dial(follower.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial follower failed: %v", err)
	}
	defer followerConn.Close()
	followerClient := proto.NewKVServiceClient(followerConn)

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	followerResp, err := followerClient.GetLinearizable(ctx, &proto.GetRequest{Key: "lr"})
	cancel()
	if err != nil {
		t.Fatalf("follower linearizable get rpc failed: %v", err)
	}
	if followerResp.Success {
		t.Fatalf("expected follower linearizable read rejection, got success with value=%q", followerResp.Value)
	}
}
