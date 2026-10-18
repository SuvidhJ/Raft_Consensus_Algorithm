package tests

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/kv"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
)

// Regressions found by the multi-process leader-crash benchmark
// (cmd/failover). Each forces its failure interleaving deterministically,
// mostly by driving one node through a scripted transport.

const (
	peerP = "127.0.0.1:7002"
	peerQ = "127.0.0.1:7003"
)

var errUnreachable = errors.New("peer unreachable")

// scriptedTransport answers each outbound RPC with a per-kind handler; a nil
// handler behaves like a crashed peer whose calls fail fast.
type scriptedTransport struct {
	preVote       func(ctx context.Context, target string, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error)
	requestVote   func(ctx context.Context, target string, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error)
	appendEntries func(ctx context.Context, target string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error)
}

func (s *scriptedTransport) PreVote(ctx context.Context, target string, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error) {
	if s.preVote == nil {
		return nil, errUnreachable
	}
	return s.preVote(ctx, target, req)
}

func (s *scriptedTransport) RequestVote(ctx context.Context, target string, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	if s.requestVote == nil {
		return nil, errUnreachable
	}
	return s.requestVote(ctx, target, req)
}

func (s *scriptedTransport) AppendEntries(ctx context.Context, target string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	if s.appendEntries == nil {
		return nil, errUnreachable
	}
	return s.appendEntries(ctx, target, req)
}

func (s *scriptedTransport) InstallSnapshot(context.Context, string, *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	return nil, errUnreachable
}

func (s *scriptedTransport) TimeoutNow(context.Context, string, *proto.TimeoutNowRequest) (*proto.TimeoutNowResponse, error) {
	return nil, errUnreachable
}

func (s *scriptedTransport) Close() {}

// hang models a peer whose host is down: the call neither succeeds nor
// fails until the caller's deadline.
func hang[T any](ctx context.Context) (T, error) {
	<-ctx.Done()
	var zero T
	return zero, ctx.Err()
}

func scriptedNodeConfig(id uint32, tr raft.Transport) raft.Config {
	cfg := defaultNodeConfig(id, make(chan *proto.LogEntry, 64), "")
	cfg.Peers = []string{peerP, peerQ}
	cfg.Transport = tr
	return cfg
}

// With one peer crashed, a candidate that has a majority must win at once
// rather than wait out the dead peer's pre-vote and vote RPC timeouts.
func TestElectionDoesNotWaitForUnresponsivePeer(t *testing.T) {
	tr := &scriptedTransport{
		preVote: func(ctx context.Context, target string, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error) {
			if target == peerQ {
				return hang[*proto.PreVoteResponse](ctx)
			}
			return &proto.PreVoteResponse{Term: req.Term - 1, VoteGranted: true}, nil
		},
		requestVote: func(ctx context.Context, target string, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
			if target == peerQ {
				return hang[*proto.RequestVoteResponse](ctx)
			}
			return &proto.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
		},
		appendEntries: func(ctx context.Context, target string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
			if target == peerQ {
				return hang[*proto.AppendEntriesResponse](ctx)
			}
			return &proto.AppendEntriesResponse{Term: req.Term, Success: true}, nil
		},
	}

	start := time.Now()
	node := raft.NewRaftNodeWithConfig(scriptedNodeConfig(250, tr))
	defer node.Close()
	waitForNodeLeader(t, node, 3*time.Second)

	// The election timeout is at most 300ms; waiting on the hung peer adds
	// its pre-vote and vote timeouts (300ms + 350ms) on top.
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Fatalf("election took %s: it waited on the unresponsive peer", elapsed)
	}
}

// A pre-vote that succeeds after we already voted in another candidate's
// election is stale; acting on it would depose the leader just elected.
func TestStalePreVoteDoesNotDeposeNewLeader(t *testing.T) {
	var node atomic.Pointer[raft.RaftNode]
	var rounds atomic.Int32
	tr := &scriptedTransport{
		preVote: func(ctx context.Context, target string, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error) {
			n := node.Load()
			if target != peerP || n == nil {
				return nil, errUnreachable
			}
			if rounds.Add(1) > 1 {
				// P now follows the leader it became and rejects pre-votes.
				return &proto.PreVoteResponse{Term: req.Term - 1}, nil
			}
			// While our pre-vote is in flight, P campaigns for the same term
			// and wins our vote, then grants the now-stale pre-vote.
			n.HandleRequestVote(&proto.RequestVoteRequest{
				Term: req.Term, CandidateId: 2, LastLogIndex: 1 << 20, LastLogTerm: 1 << 20,
			})
			return &proto.PreVoteResponse{Term: req.Term - 1, VoteGranted: true}, nil
		},
	}

	n := raft.NewRaftNodeWithConfig(scriptedNodeConfig(251, tr))
	node.Store(n)
	defer n.Close()

	// Elections run one at a time, so once a second pre-vote round starts
	// the first round's outcome is final.
	deadline := time.Now().Add(5 * time.Second)
	for rounds.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if rounds.Load() < 2 {
		t.Fatal("node never ran a second pre-vote round")
	}
	if st := n.Status(); st.ElectionsStarted != 0 || st.Term != 1 {
		t.Fatalf("stale pre-vote started an election: elections=%d term=%d (want 0, 1)", st.ElectionsStarted, st.Term)
	}
}

// A follower may only commit entries the current request proved identical
// to the leader's log. A matching batch that ends before the follower's
// stale suffix (a deposed leader's uncommitted writes) must not commit it.
func TestFollowerDoesNotCommitStaleSuffix(t *testing.T) {
	node := raft.NewRaftNodeWithConfig(scriptedNodeConfig(252, &scriptedTransport{}))
	defer node.Close()

	entries := func(term, from, to uint64) []*proto.LogEntry {
		var out []*proto.LogEntry
		for i := from; i <= to; i++ {
			out = append(out, &proto.LogEntry{Index: i, Term: term, Payload: []byte("x")})
		}
		return out
	}

	// A term-1 leader replicates 1..5, but only 1..3 commit before it dies.
	if r := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term: 1, LeaderId: 91, Entries: entries(1, 1, 5), LeaderCommit: 3,
	}); !r.Success {
		t.Fatal("initial append rejected")
	}

	// The term-2 leader holds 1..3 (term 1) and 4..5 (term 2), committed
	// through 5. While backtracking it sends a batch matching ours that
	// ends before our stale entries 4..5.
	if r := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term: 2, LeaderId: 92, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(1, 2, 3), LeaderCommit: 5,
	}); !r.Success {
		t.Fatal("matching append rejected")
	}
	if c := node.Status().CommitIndex; c != 3 {
		t.Fatalf("commitIndex=%d: the unverified stale suffix 4..5 was committed", c)
	}

	// The leader's next batch must be able to overwrite the stale suffix.
	r := node.HandleAppendEntries(&proto.AppendEntriesRequest{
		Term: 2, LeaderId: 92, PrevLogIndex: 3, PrevLogTerm: 1, Entries: entries(2, 4, 5), LeaderCommit: 5,
	})
	if st := node.Status(); !r.Success || st.CommitIndex != 5 || st.LastLogTerm != 2 {
		t.Fatalf("stale suffix not replaced: success=%v commit=%d lastLogTerm=%d", r.Success, st.CommitIndex, st.LastLogTerm)
	}
}

// A leader no-op is consumed by Raft itself, but must not count as applied
// ahead of entries still queued for the state machine: that let a snapshot
// capture state missing those entries, after which they were skipped.
func TestNoopDoesNotOvertakeQueuedEntries(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-253")

	// First run: commit two writes, then stop.
	node, store, cleanup := startSingleNode(t, dataDir, 253)
	waitForSingleLeader(t, node, 3*time.Second)
	for _, k := range []string{"a", "b"} {
		if resp, err := store.Set(context.Background(), &proto.SetRequest{Key: k, Value: k}); err != nil || !resp.Success {
			cleanup()
			t.Fatalf("set %s failed: resp=%v err=%v", k, resp, err)
		}
	}
	lastWrite := node.Status().CommitIndex
	cleanup()

	// Restart with the state machine paused: replay queues both writes on
	// applyCh, then the new term's no-op commits behind them. A tiny
	// snapshot threshold makes a premature "applied" snapshot immediate.
	applyCh := make(chan *proto.LogEntry, 8)
	cfg := defaultNodeConfig(253, applyCh, dataDir)
	cfg.SnapshotThreshold = 2
	node2 := raft.NewRaftNodeWithConfig(cfg)
	store2 := kv.NewKVStore(node2)
	node2.SetSnapshotter(store2)
	defer node2.Close()
	waitForSingleLeader(t, node2, 3*time.Second)

	deadline := time.Now().Add(3 * time.Second)
	for node2.Status().CommitIndex <= lastWrite && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond) // let the apply loop reach the no-op
	if applied := node2.Status().AppliedIndex; applied >= lastWrite {
		t.Fatalf("AppliedIndex=%d while writes through %d are still queued for the state machine", applied, lastWrite)
	}

	// Resume the state machine: both writes must still be applied.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case entry := <-applyCh:
				store2.ApplyCommand(entry)
			}
		}
	}()
	waitForValue(t, store2, "a", "a", 3*time.Second)
	waitForValue(t, store2, "b", "b", 3*time.Second)
}
