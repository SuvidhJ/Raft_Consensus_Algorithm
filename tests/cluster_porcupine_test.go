package tests

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/kv"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
	"github.com/anishathalye/porcupine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testNode struct {
	id     int
	addr   string
	node   *raft.RaftNode
	store  *kv.KVStore
	server *grpc.Server
	lis    net.Listener

	stopApply chan struct{}
	applyWG   sync.WaitGroup
	stopOnce  sync.Once
}

func startTestNode(t *testing.T, id int, lis net.Listener, peers []string, dataRoot string) *testNode {
	return startTestNodeJoining(t, id, lis, peers, dataRoot, false)
}

func startTestNodeJoining(t *testing.T, id int, lis net.Listener, peers []string, dataRoot string, joining bool) *testNode {
	t.Helper()
	applyCh := make(chan *proto.LogEntry, 256)
	node := raft.NewRaftNodeWithConfig(raft.Config{
		ID:                  uint32(id),
		Peers:               peers,
		ApplyCh:             applyCh,
		StrictApplyAcks:     true,
		DataDir:             filepath.Join(dataRoot, fmt.Sprintf("node-%d", id)),
		HeartbeatInterval:   80 * time.Millisecond,
		ElectionTimeoutMin:  250 * time.Millisecond,
		ElectionTimeoutMax:  500 * time.Millisecond,
		ProposalTimeout:     2 * time.Second,
		ReadQuorumTimeout:   500 * time.Millisecond,
		SnapshotThreshold:   128,
		MaxEntriesPerAppend: 64,
		EnablePreVote:       true,
		EnableCheckQuorum:   true,
		AdvertiseAddr:       lis.Addr().String(),
		Joining:             joining,
	})
	store := kv.NewKVStore(node)
	node.SetSnapshotter(store)
	stopApply := make(chan struct{})

	tn := &testNode{
		id:        id,
		addr:      lis.Addr().String(),
		node:      node,
		store:     store,
		server:    nil,
		lis:       lis,
		stopApply: stopApply,
	}

	tn.applyWG.Add(1)
	go func() {
		defer tn.applyWG.Done()
		for {
			select {
			case <-tn.stopApply:
				return
			case entry := <-applyCh:
				if entry != nil {
					store.ApplyCommand(entry)
				}
			}
		}
	}()

	s := grpc.NewServer()
	tn.server = s
	proto.RegisterKVServiceServer(s, store)
	proto.RegisterRaftServiceServer(s, raft.NewService(node))
	go func() {
		_ = s.Serve(lis)
	}()

	return tn
}

func stopTestNode(n *testNode) {
	if n == nil {
		return
	}
	n.stopOnce.Do(func() {
		n.server.Stop()
		n.node.Close()
		close(n.stopApply)
		n.applyWG.Wait()
		_ = n.lis.Close()
	})
}

func waitForLeader(nodes []*testNode, timeout time.Duration) *testNode {
	deadline := time.Now().Add(timeout)
	var candidate *testNode
	stableTicks := 0
	const stableRequiredTicks = 4

	for time.Now().Before(deadline) {
		leaderCount := 0
		var observedLeader *testNode
		for _, n := range nodes {
			if n.node.CurrentState() == raft.Leader {
				leaderCount++
				observedLeader = n
			}
		}
		if leaderCount == 1 {
			if observedLeader == candidate {
				stableTicks++
			} else {
				candidate = observedLeader
				stableTicks = 1
			}
			if stableTicks >= stableRequiredTicks {
				return observedLeader
			}
		} else {
			candidate = nil
			stableTicks = 0
		}
		time.Sleep(30 * time.Millisecond)
	}
	return nil
}

type modelInput struct {
	Op    string
	Value string
}

type modelOutput struct {
	OK    bool
	Value string
}

func TestPorcupineLinearizableUnderPartition(t *testing.T) {
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

	leader := waitForLeader(nodes, 4*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}

	conn, err := grpc.Dial(leader.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial leader failed: %v", err)
	}
	defer conn.Close()
	client := proto.NewKVServiceClient(conn)

	model := porcupine.Model{
		Init: func() interface{} { return "" },
		Step: func(state interface{}, input interface{}, output interface{}) (bool, interface{}) {
			cur := state.(string)
			in := input.(modelInput)
			out := output.(modelOutput)
			switch in.Op {
			case "set":
				if !out.OK {
					return true, cur
				}
				return true, in.Value
			case "get":
				if !out.OK {
					return true, cur
				}
				return out.Value == cur, cur
			default:
				return false, cur
			}
		},
		Equal: func(a interface{}, b interface{}) bool {
			return a.(string) == b.(string)
		},
	}

	history := make([]porcupine.Operation, 0, 32)
	var histMu sync.Mutex

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := fmt.Sprintf("v-%d", i)

			call := time.Now().UnixNano()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			setResp, _ := client.Set(ctx, &proto.SetRequest{Key: "k", Value: v})
			cancel()
			ret := time.Now().UnixNano()
			setOK := setResp != nil && setResp.Success
			histMu.Lock()
			history = append(history, porcupine.Operation{ClientId: i, Input: modelInput{Op: "set", Value: v}, Output: modelOutput{OK: setOK}, Call: call, Return: ret})
			histMu.Unlock()

			call = time.Now().UnixNano()
			ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			getResp, _ := client.Get(ctx, &proto.GetRequest{Key: "k"})
			cancel()
			ret = time.Now().UnixNano()
			getOK := getResp != nil && getResp.Success
			getVal := ""
			if getResp != nil {
				getVal = getResp.Value
			}
			histMu.Lock()
			history = append(history, porcupine.Operation{ClientId: i, Input: modelInput{Op: "get"}, Output: modelOutput{OK: getOK, Value: getVal}, Call: call, Return: ret})
			histMu.Unlock()
		}()
	}
	wg.Wait()

	// Simulate partition: stop both followers so leader cannot reach quorum.
	for _, n := range nodes {
		if n != leader {
			n.server.Stop()
			n.node.Close()
			_ = n.lis.Close()
		}
	}

	call := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	setResp, _ := client.Set(ctx, &proto.SetRequest{Key: "k", Value: "after-partition"})
	cancel()
	ret := time.Now().UnixNano()
	setOK := setResp != nil && setResp.Success
	history = append(history, porcupine.Operation{ClientId: 99, Input: modelInput{Op: "set", Value: "after-partition"}, Output: modelOutput{OK: setOK}, Call: call, Return: ret})
	if setOK {
		t.Fatal("set unexpectedly succeeded without quorum during partition")
	}

	if !porcupine.CheckOperations(model, history) {
		t.Fatal("history is not linearizable according to porcupine")
	}
}
