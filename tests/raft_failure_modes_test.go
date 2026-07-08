package tests

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/kv"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
	"google.golang.org/grpc"
)

func expectPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic but function returned normally")
		}
	}()
	fn()
}

func defaultNodeConfig(id uint32, applyCh chan<- *proto.LogEntry, dataDir string) raft.Config {
	return raft.Config{
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
		SnapshotThreshold:   1024,
		MaxEntriesPerAppend: 64,
		EnablePreVote:       true,
		EnableCheckQuorum:   true,
	}
}

type alwaysFailSnapshotter struct {
	mu           sync.Mutex
	restoreCalls int
}

func (s *alwaysFailSnapshotter) Snapshot() ([]byte, error) {
	return nil, nil
}

func (s *alwaysFailSnapshotter) Restore(_ []byte) error {
	s.mu.Lock()
	s.restoreCalls++
	s.mu.Unlock()
	return errors.New("forced restore failure")
}

func (s *alwaysFailSnapshotter) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restoreCalls
}

type countingSnapshotter struct {
	mu           sync.Mutex
	restoreCalls int
}

func (s *countingSnapshotter) Snapshot() ([]byte, error) {
	return nil, nil
}

func (s *countingSnapshotter) Restore(_ []byte) error {
	s.mu.Lock()
	s.restoreCalls++
	s.mu.Unlock()
	return nil
}

func (s *countingSnapshotter) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.restoreCalls
}

type countingFollower struct {
	proto.UnimplementedRaftServiceServer

	mu          sync.Mutex
	inFlightAE  int
	maxInFlight int
}

func (f *countingFollower) PreVote(_ context.Context, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error) {
	return &proto.PreVoteResponse{Term: 0, VoteGranted: true}, nil
}

func (f *countingFollower) RequestVote(_ context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	return &proto.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (f *countingFollower) AppendEntries(_ context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	f.mu.Lock()
	f.inFlightAE++
	if f.inFlightAE > f.maxInFlight {
		f.maxInFlight = f.inFlightAE
	}
	f.mu.Unlock()

	time.Sleep(180 * time.Millisecond)

	f.mu.Lock()
	f.inFlightAE--
	f.mu.Unlock()
	return &proto.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (f *countingFollower) InstallSnapshot(_ context.Context, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	return &proto.InstallSnapshotResponse{Term: req.Term, Success: true}, nil
}

func (f *countingFollower) TimeoutNow(_ context.Context, req *proto.TimeoutNowRequest) (*proto.TimeoutNowResponse, error) {
	return &proto.TimeoutNowResponse{Term: req.Term, Success: true}, nil
}

func (f *countingFollower) maxConcurrentAppendEntries() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxInFlight
}

type blockingFollower struct {
	proto.UnimplementedRaftServiceServer

	entered chan struct{}
	once    sync.Once
}

func (f *blockingFollower) PreVote(_ context.Context, _ *proto.PreVoteRequest) (*proto.PreVoteResponse, error) {
	return &proto.PreVoteResponse{Term: 0, VoteGranted: true}, nil
}

func (f *blockingFollower) RequestVote(_ context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	return &proto.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (f *blockingFollower) AppendEntries(ctx context.Context, _ *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	f.once.Do(func() {
		close(f.entered)
	})
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *blockingFollower) InstallSnapshot(ctx context.Context, _ *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *blockingFollower) TimeoutNow(_ context.Context, req *proto.TimeoutNowRequest) (*proto.TimeoutNowResponse, error) {
	return &proto.TimeoutNowResponse{Term: req.Term, Success: true}, nil
}

func TestAppendEntriesTermBumpPersistsOnRejectPath(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-201")

	applyCh := make(chan *proto.LogEntry, 8)
	node := raft.NewRaftNodeWithConfig(defaultNodeConfig(201, applyCh, dataDir))
	rej := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term:         5,
		LeaderId:     99,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
	})
	if rej.Success {
		node.Close()
		t.Fatal("expected append reject for missing prev log")
	}
	node.Close()

	node2 := raft.NewRaftNodeWithConfig(defaultNodeConfig(201, make(chan *proto.LogEntry, 8), dataDir))
	defer node2.Close()
	voteResp := node2.HandleRequestVote(&proto.RequestVoteRequest{
		Term:         0,
		CandidateId:  7,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if voteResp.Term != 5 {
		t.Fatalf("expected persisted term 5 after restart, got %d", voteResp.Term)
	}
}

func TestInstallSnapshotStalePathPersistsTermBump(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-202")

	applyCh := make(chan *proto.LogEntry, 8)
	node := raft.NewRaftNodeWithConfig(defaultNodeConfig(202, applyCh, dataDir))
	resp := node.HandleInstallSnapshot(&proto.InstallSnapshotRequest{
		Term:              7,
		LeaderId:          99,
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
	})
	if !resp.Success {
		node.Close()
		t.Fatal("expected stale install-snapshot to return success")
	}
	node.Close()

	node2 := raft.NewRaftNodeWithConfig(defaultNodeConfig(202, make(chan *proto.LogEntry, 8), dataDir))
	defer node2.Close()
	voteResp := node2.HandleRequestVote(&proto.RequestVoteRequest{
		Term:         0,
		CandidateId:  9,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if voteResp.Term != 7 {
		t.Fatalf("expected persisted term 7 after restart, got %d", voteResp.Term)
	}
}

func TestInstallSnapshotRestoreFailureDoesNotAdvanceProgress(t *testing.T) {
	applyCh := make(chan *proto.LogEntry, 8)
	node := raft.NewRaftNodeWithConfig(defaultNodeConfig(203, applyCh, filepath.Join(t.TempDir(), "node-203")))
	defer node.Close()

	failSM := &alwaysFailSnapshotter{}
	node.SetSnapshotter(failSM)

	req := &proto.InstallSnapshotRequest{
		Term:              3,
		LeaderId:          41,
		LastIncludedIndex: 8,
		LastIncludedTerm:  3,
		Data:              []byte(`{"k":"v"}`),
	}
	resp1 := node.HandleInstallSnapshot(req)
	if resp1.Success {
		t.Fatal("expected first install snapshot to fail when restore fails")
	}
	if failSM.calls() != 1 {
		t.Fatalf("expected exactly one failed restore attempt, got %d", failSM.calls())
	}

	okSM := &countingSnapshotter{}
	node.SetSnapshotter(okSM)
	resp2 := node.HandleInstallSnapshot(req)
	if !resp2.Success {
		t.Fatalf("expected retry install snapshot success, got %+v", resp2)
	}
	if okSM.calls() != 1 {
		t.Fatalf("expected retry to invoke restore once, got %d", okSM.calls())
	}
}

func TestSetWaitsForStateMachineApply(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-204")
	applyCh := make(chan *proto.LogEntry, 64)
	cfg := defaultNodeConfig(204, applyCh, dataDir)
	cfg.SnapshotThreshold = 2048

	node := raft.NewRaftNodeWithConfig(cfg)
	store := kv.NewKVStore(node)
	node.SetSnapshotter(store)

	stopApply := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopApply:
				return
			case entry := <-applyCh:
				time.Sleep(220 * time.Millisecond)
				store.ApplyCommand(entry)
			}
		}
	}()

	defer func() {
		node.Close()
		close(stopApply)
	}()

	waitForNodeLeader(t, node, 3*time.Second)

	start := time.Now()
	setResp, err := store.Set(context.Background(), &proto.SetRequest{Key: "slow", Value: "ack"})
	if err != nil || !setResp.Success {
		t.Fatalf("set failed: resp=%v err=%v", setResp, err)
	}
	elapsed := time.Since(start)
	if elapsed < 180*time.Millisecond {
		t.Fatalf("set returned before apply completion: elapsed=%s", elapsed)
	}
}

func TestSnapshotCapturesAppliedStateOnly(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-205")

	applyCh := make(chan *proto.LogEntry, 64)
	cfg := defaultNodeConfig(205, applyCh, dataDir)
	cfg.SnapshotThreshold = 2
	node := raft.NewRaftNodeWithConfig(cfg)
	store := kv.NewKVStore(node)
	node.SetSnapshotter(store)

	stopApply := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopApply:
				return
			case entry := <-applyCh:
				time.Sleep(220 * time.Millisecond)
				store.ApplyCommand(entry)
			}
		}
	}()

	waitForNodeLeader(t, node, 3*time.Second)
	setResp, err := store.Set(context.Background(), &proto.SetRequest{Key: "snap", Value: "alive"})
	if err != nil || !setResp.Success {
		node.Close()
		close(stopApply)
		t.Fatalf("set failed: resp=%v err=%v", setResp, err)
	}
	waitForValue(t, store, "snap", "alive", 2*time.Second)
	time.Sleep(150 * time.Millisecond)
	node.Close()
	close(stopApply)

	node2, store2, cleanup2 := startSingleNode(t, dataDir, 205)
	defer cleanup2()
	waitForSingleLeader(t, node2, 3*time.Second)
	resp, err := store2.Get(context.Background(), &proto.GetRequest{Key: "snap"})
	if err != nil || !resp.Success || resp.Value != "alive" {
		t.Fatalf("expected value to survive snapshot+restart, got resp=%v err=%v", resp, err)
	}
}

func TestLinearizableReadFailsImmediatelyAfterQuorumLoss(t *testing.T) {
	nodes, cleanup := startThreeNodeCluster(t)
	defer cleanup()

	leader := waitForLeader(nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("leader not elected")
	}

	var (
		setResp *proto.SetResponse
		err     error
	)
	for attempt := 0; attempt < 5; attempt++ {
		setResp, err = leader.store.Set(context.Background(), &proto.SetRequest{Key: "lr-loss", Value: "v"})
		if err == nil && setResp != nil && setResp.Success {
			break
		}
		leader = waitForLeader(nodes, 2*time.Second)
		if leader == nil {
			break
		}
		time.Sleep(40 * time.Millisecond)
	}
	if err != nil || setResp == nil || !setResp.Success {
		t.Fatalf("set failed before partition: resp=%v err=%v", setResp, err)
	}

	for _, n := range nodes {
		if n != leader {
			stopTestNode(n)
		}
	}

	readResp, err := leader.store.GetLinearizable(context.Background(), &proto.GetRequest{Key: "lr-loss"})
	if err != nil {
		t.Fatalf("linearizable read failed with rpc error: %v", err)
	}
	if readResp.Success {
		t.Fatalf("expected immediate linearizable read rejection after quorum loss, got value=%q", readResp.Value)
	}
}

func TestReplicationUsesSingleInFlightPerPeer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer lis.Close()

	mockFollower := &countingFollower{}
	srv := grpc.NewServer()
	proto.RegisterRaftServiceServer(srv, mockFollower)
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	applyCh := make(chan *proto.LogEntry, 16)
	cfg := defaultNodeConfig(206, applyCh, filepath.Join(t.TempDir(), "node-206"))
	cfg.Peers = []string{lis.Addr().String()}
	cfg.HeartbeatInterval = 40 * time.Millisecond
	cfg.ElectionTimeoutMin = 120 * time.Millisecond
	cfg.ElectionTimeoutMax = 220 * time.Millisecond
	node := raft.NewRaftNodeWithConfig(cfg)
	defer node.Close()

	waitForNodeLeader(t, node, 3*time.Second)
	time.Sleep(700 * time.Millisecond)

	if maxInFlight := mockFollower.maxConcurrentAppendEntries(); maxInFlight > 1 {
		t.Fatalf("expected single in-flight AppendEntries per peer, observed %d", maxInFlight)
	}
}

func TestCloseCancelsInFlightReplicationRPC(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}
	defer lis.Close()

	blocker := &blockingFollower{entered: make(chan struct{})}
	srv := grpc.NewServer()
	proto.RegisterRaftServiceServer(srv, blocker)
	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.Stop()

	applyCh := make(chan *proto.LogEntry, 16)
	cfg := defaultNodeConfig(213, applyCh, filepath.Join(t.TempDir(), "node-213"))
	cfg.Peers = []string{lis.Addr().String()}
	cfg.HeartbeatInterval = 40 * time.Millisecond
	cfg.ElectionTimeoutMin = 120 * time.Millisecond
	cfg.ElectionTimeoutMax = 220 * time.Millisecond
	node := raft.NewRaftNodeWithConfig(cfg)

	waitForNodeLeader(t, node, 3*time.Second)
	select {
	case <-blocker.entered:
	case <-time.After(2 * time.Second):
		node.Close()
		t.Fatal("did not observe in-flight replication rpc")
	}

	start := time.Now()
	node.Close()
	if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
		t.Fatalf("close took too long with cancellable in-flight rpc: %s", elapsed)
	}
}

func TestTimeoutNowRejectsUnknownLeaderInCurrentTerm(t *testing.T) {
	cfg := defaultNodeConfig(207, make(chan *proto.LogEntry, 8), filepath.Join(t.TempDir(), "node-207"))
	cfg.ElectionTimeoutMin = 4 * time.Second
	cfg.ElectionTimeoutMax = 5 * time.Second
	node := raft.NewRaftNodeWithConfig(cfg)
	defer node.Close()

	resp := node.HandleTimeoutNow(&proto.TimeoutNowRequest{Term: 0, LeaderId: 77})
	if resp.Success {
		t.Fatalf("expected timeout-now rejection when leader is unknown, got success term=%d", resp.Term)
	}
}

func TestSnapshotInstallRejectsStaleQueuedApply(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-209")

	node, store, cleanup := startSingleNode(t, dataDir, 209)
	defer cleanup()

	resp := node.HandleInstallSnapshot(&proto.InstallSnapshotRequest{
		Term:              5,
		LeaderId:          99,
		LastIncludedIndex: 50,
		LastIncludedTerm:  5,
		Data:              []byte(`{"k":"snap"}`),
	})
	if !resp.Success {
		t.Fatalf("install snapshot failed: %+v", resp)
	}

	store.ApplyCommand(&proto.LogEntry{Term: 4, Index: 51, Command: "SET k stale"})
	got, err := store.Get(context.Background(), &proto.GetRequest{Key: "k"})
	if err != nil || !got.Success || got.Value != "snap" {
		t.Fatalf("stale apply unexpectedly mutated state: resp=%v err=%v", got, err)
	}
}

func TestReadBarrierRequiresAppliedCommit(t *testing.T) {
	applyCh := make(chan *proto.LogEntry)
	cfg := defaultNodeConfig(210, applyCh, filepath.Join(t.TempDir(), "node-210"))
	cfg.Peers = nil
	cfg.StrictApplyAcks = true
	node := raft.NewRaftNodeWithConfig(cfg)
	defer node.Close()

	waitForNodeLeader(t, node, 3*time.Second)
	appendResp := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term:         10,
		LeaderId:     77,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []*proto.LogEntry{{Term: 10, Command: "SET x blocked"}},
		LeaderCommit: 2,
	})
	if !appendResp.Success {
		t.Fatalf("append injection failed: %+v", appendResp)
	}

	waitForNodeLeader(t, node, 4*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if ok := node.ReadBarrierWithContext(ctx); ok {
		t.Fatal("read barrier unexpectedly succeeded with unapplied committed entry")
	}
}

func TestSetHonorsCallerContextDeadline(t *testing.T) {
	applyCh := make(chan *proto.LogEntry)
	cfg := defaultNodeConfig(211, applyCh, filepath.Join(t.TempDir(), "node-211"))
	cfg.Peers = nil
	cfg.StrictApplyAcks = true
	node := raft.NewRaftNodeWithConfig(cfg)
	store := kv.NewKVStore(node)
	node.SetSnapshotter(store)
	defer node.Close()

	waitForNodeLeader(t, node, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	start := time.Now()
	resp, err := store.Set(ctx, &proto.SetRequest{Key: "ctx", Value: "deadline"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected rpc error: %v", err)
	}
	if resp.Success {
		t.Fatal("expected set failure on caller deadline")
	}
	if elapsed > 600*time.Millisecond {
		t.Fatalf("set ignored caller deadline, elapsed=%s", elapsed)
	}
}

func TestAppendAndSnapshotRejectInvalidLeaderIdentity(t *testing.T) {
	cfg := defaultNodeConfig(212, make(chan *proto.LogEntry, 8), filepath.Join(t.TempDir(), "node-212"))
	node := raft.NewRaftNodeWithConfig(cfg)
	defer node.Close()

	invalidAppend := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term:         2,
		LeaderId:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if invalidAppend.Success {
		t.Fatal("append entries with leader_id=0 should be rejected")
	}

	firstAppend := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term:         3,
		LeaderId:     55,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if !firstAppend.Success {
		t.Fatalf("expected initial append success, got %+v", firstAppend)
	}

	mismatchedAppend := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term:         3,
		LeaderId:     56,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if mismatchedAppend.Success {
		t.Fatal("append entries with mismatched leader identity in same term should be rejected")
	}

	invalidSnap := node.HandleInstallSnapshot(&proto.InstallSnapshotRequest{
		Term:              4,
		LeaderId:          0,
		LastIncludedIndex: 1,
		LastIncludedTerm:  4,
		Data:              []byte(`{"k":"v"}`),
	})
	if invalidSnap.Success {
		t.Fatal("install snapshot with leader_id=0 should be rejected")
	}

	firstSnap := node.HandleInstallSnapshot(&proto.InstallSnapshotRequest{
		Term:              5,
		LeaderId:          77,
		LastIncludedIndex: 1,
		LastIncludedTerm:  5,
		Data:              []byte(`{"k":"v"}`),
	})
	if !firstSnap.Success {
		t.Fatalf("expected initial install snapshot success, got %+v", firstSnap)
	}

	mismatchedSnap := node.HandleInstallSnapshot(&proto.InstallSnapshotRequest{
		Term:              5,
		LeaderId:          78,
		LastIncludedIndex: 2,
		LastIncludedTerm:  5,
		Data:              []byte(`{"k":"v2"}`),
	})
	if mismatchedSnap.Success {
		t.Fatal("install snapshot with mismatched leader identity in same term should be rejected")
	}
}

func TestInstallSnapshotFailurePathConcurrentTermUpdates(t *testing.T) {
	applyCh := make(chan *proto.LogEntry, 8)
	node := raft.NewRaftNodeWithConfig(defaultNodeConfig(208, applyCh, filepath.Join(t.TempDir(), "node-208")))
	defer node.Close()

	node.SetSnapshotter(&alwaysFailSnapshotter{})

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			node.HandleInstallSnapshot(&proto.InstallSnapshotRequest{
				Term:              5 + uint64(i%3),
				LeaderId:          42,
				LastIncludedIndex: 50 + uint64(i),
				LastIncludedTerm:  4,
				Data:              []byte("race-check"),
			})
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			node.HandleAppendEntries(&proto.AppendEntriesRequest{
				Term:         20 + uint64(i),
				LeaderId:     99,
				PrevLogIndex: 0,
				PrevLogTerm:  0,
			})
		}
	}()

	wg.Wait()
}

func TestInvalidPeerAddressPanicsOnStartup(t *testing.T) {
	expectPanic(t, func() {
		_ = raft.NewRaftNodeWithConfig(raft.Config{
			ID:                  214,
			Peers:               []string{"invalid-peer"},
			ApplyCh:             make(chan *proto.LogEntry, 8),
			StrictApplyAcks:     true,
			DataDir:             filepath.Join(t.TempDir(), "node-214"),
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
	})
}

func TestCorruptPersistedStateCausesStartupFailure(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-215")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}
	badState := `{"current_term":2,"voted_for":1,"log":[{"term":2,"command":"x","index":2}],"commit_index":2,"last_included_index":0,"last_included_term":0,"snapshot_data":""}`
	if err := os.WriteFile(filepath.Join(dataDir, "raft-state.json"), []byte(badState), 0o644); err != nil {
		t.Fatalf("write state failed: %v", err)
	}

	expectPanic(t, func() {
		_ = raft.NewRaftNodeWithConfig(defaultNodeConfig(215, make(chan *proto.LogEntry, 8), dataDir))
	})
}

func TestSetSnapshotterRestoreFailurePanics(t *testing.T) {
	applyCh := make(chan *proto.LogEntry, 8)
	node := raft.NewRaftNodeWithConfig(defaultNodeConfig(216, applyCh, filepath.Join(t.TempDir(), "node-216")))
	defer node.Close()

	resp := node.HandleInstallSnapshot(&proto.InstallSnapshotRequest{
		Term:              5,
		LeaderId:          88,
		LastIncludedIndex: 3,
		LastIncludedTerm:  5,
		Data:              []byte(`{"k":"v"}`),
	})
	if !resp.Success {
		t.Fatalf("install snapshot failed: %+v", resp)
	}

	expectPanic(t, func() {
		node.SetSnapshotter(&alwaysFailSnapshotter{})
	})
}

func TestNotifyAppliedNeverExceedsCommitIndex(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-217")
	node, store, cleanup := startSingleNode(t, dataDir, 217)
	defer cleanup()
	waitForSingleLeader(t, node, 3*time.Second)

	if resp, err := store.Set(context.Background(), &proto.SetRequest{Key: "k", Value: "v"}); err != nil || !resp.Success {
		t.Fatalf("set failed: resp=%v err=%v", resp, err)
	}

	commit := node.Status().CommitIndex
	node.NotifyApplied(commit + 100)
	if applied := node.Status().AppliedIndex; applied > commit {
		t.Fatalf("lastApplied %d ran past commitIndex %d via bogus notification", applied, commit)
	}
}
