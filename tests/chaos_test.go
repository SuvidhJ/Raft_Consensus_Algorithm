package tests

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/kv"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
	"github.com/anishathalye/porcupine"
)

// Chaos harness: an in-memory transport hub with seeded fault injection.
// Nodes persist to real WALs and are crash/restarted mid-run, so recovery is
// exercised under load. Faults cover symmetric partitions, message drops on
// both the request and the response path (the latter forces duplicate
// deliveries), random delays, message duplication, and process crashes.

type chaosHub struct {
	mu       sync.Mutex
	rng      *rand.Rand
	nodes    map[string]*raft.RaftNode
	blocked  map[string]bool // "a|b" symmetric partition pairs
	down     map[string]bool
	dropRate float64
	dupRate  float64
	maxDelay time.Duration

	// Fault accounting, reported when a run passes.
	msgsDropped    uint64 // lost on the request path (noise, partition, or crashed peer)
	repliesDropped uint64 // processed, but the response was lost
	msgsDuplicated uint64
	leadersElected atomic.Uint64 // BecameLeader totals of nodes already stopped
}

func newChaosHub(seed int64) *chaosHub {
	return &chaosHub{
		rng:     rand.New(rand.NewSource(seed)),
		nodes:   make(map[string]*raft.RaftNode),
		blocked: make(map[string]bool),
		down:    make(map[string]bool),
	}
}

func pairKey(a, b string) string {
	if a < b {
		return a + "|" + b
	}
	return b + "|" + a
}

func (h *chaosHub) register(addr string, node *raft.RaftNode) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nodes[addr] = node
	h.down[addr] = false
}

func (h *chaosHub) crash(addr string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.down[addr] = true
	delete(h.nodes, addr)
}

func (h *chaosHub) setPartitioned(addr string, others []string, partitioned bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, o := range others {
		if o == addr {
			continue
		}
		if partitioned {
			h.blocked[pairKey(addr, o)] = true
		} else {
			delete(h.blocked, pairKey(addr, o))
		}
	}
}

func (h *chaosHub) healAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.blocked = make(map[string]bool)
	h.dropRate = 0
	h.dupRate = 0
	h.maxDelay = 0
}

func (h *chaosHub) setNoise(drop, dup float64, maxDelay time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dropRate = drop
	h.dupRate = dup
	h.maxDelay = maxDelay
}

// plan decides the fate of one message leg and resolves the target handler.
func (h *chaosHub) plan(from, to string) (target *raft.RaftNode, delay time.Duration, drop, dropReply, dup bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.down[from] || h.down[to] || h.blocked[pairKey(from, to)] {
		h.msgsDropped++
		return nil, 0, true, false, false
	}
	target = h.nodes[to]
	if target == nil {
		h.msgsDropped++
		return nil, 0, true, false, false
	}
	if h.rng.Float64() < h.dropRate {
		h.msgsDropped++
		return nil, 0, true, false, false
	}
	dropReply = h.rng.Float64() < h.dropRate
	dup = h.rng.Float64() < h.dupRate
	if dropReply {
		h.repliesDropped++
	}
	if dup {
		h.msgsDuplicated++
	}
	if h.maxDelay > 0 {
		delay = time.Duration(h.rng.Int63n(int64(h.maxDelay)))
	}
	return target, delay, false, dropReply, dup
}

type chaosTransport struct {
	hub  *chaosHub
	self string
}

var errChaosDropped = fmt.Errorf("chaos: message dropped")

func chaosCall[Req any, Resp any](t *chaosTransport, ctx context.Context, target string, req Req,
	handle func(*raft.RaftNode, Req) Resp) (Resp, error) {

	var zero Resp
	node, delay, drop, dropReply, dup := t.hub.plan(t.self, target)
	if drop {
		return zero, errChaosDropped
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
	resp := handle(node, req)
	if dup {
		// Duplicate delivery: the handler runs twice; the first response is
		// discarded. Exercises idempotency of AppendEntries/InstallSnapshot.
		resp = handle(node, req)
	}
	if dropReply {
		// The request was processed but the reply is lost; the sender will
		// retry, producing duplicate deliveries.
		return zero, errChaosDropped
	}
	return resp, nil
}

func (t *chaosTransport) PreVote(ctx context.Context, target string, req *proto.PreVoteRequest) (*proto.PreVoteResponse, error) {
	return chaosCall(t, ctx, target, req, (*raft.RaftNode).HandlePreVote)
}

func (t *chaosTransport) RequestVote(ctx context.Context, target string, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	return chaosCall(t, ctx, target, req, (*raft.RaftNode).HandleRequestVote)
}

func (t *chaosTransport) AppendEntries(ctx context.Context, target string, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	return chaosCall(t, ctx, target, req, (*raft.RaftNode).HandleAppendEntries)
}

func (t *chaosTransport) InstallSnapshot(ctx context.Context, target string, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	return chaosCall(t, ctx, target, req, (*raft.RaftNode).HandleInstallSnapshot)
}

func (t *chaosTransport) TimeoutNow(ctx context.Context, target string, req *proto.TimeoutNowRequest) (*proto.TimeoutNowResponse, error) {
	return chaosCall(t, ctx, target, req, (*raft.RaftNode).HandleTimeoutNow)
}

func (t *chaosTransport) Close() {}

// chaosNode bundles a node with its state machine and apply pump.
type chaosNode struct {
	addr      string
	id        uint32
	node      *raft.RaftNode
	store     *kv.KVStore
	stopApply chan struct{}
	applyWG   sync.WaitGroup
}

func startChaosNode(hub *chaosHub, id uint32, addr string, peers []string, dataDir string) *chaosNode {
	applyCh := make(chan *proto.LogEntry, 256)
	node := raft.NewRaftNodeWithConfig(raft.Config{
		ID:                  id,
		Peers:               peers,
		ApplyCh:             applyCh,
		StrictApplyAcks:     true,
		DataDir:             dataDir,
		HeartbeatInterval:   50 * time.Millisecond,
		ElectionTimeoutMin:  150 * time.Millisecond,
		ElectionTimeoutMax:  300 * time.Millisecond,
		ProposalTimeout:     1 * time.Second,
		ReadQuorumTimeout:   400 * time.Millisecond,
		SnapshotThreshold:   32,
		MaxEntriesPerAppend: 8,
		EnablePreVote:       true,
		EnableCheckQuorum:   true,
		AdvertiseAddr:       addr,
		Transport:           &chaosTransport{hub: hub, self: addr},
	})
	store := kv.NewKVStore(node)
	node.SetSnapshotter(store)

	cn := &chaosNode{addr: addr, id: id, node: node, store: store, stopApply: make(chan struct{})}
	cn.applyWG.Add(1)
	go func() {
		defer cn.applyWG.Done()
		for {
			select {
			case <-cn.stopApply:
				return
			case entry := <-applyCh:
				if entry != nil {
					store.ApplyCommand(entry)
				}
			}
		}
	}()

	hub.register(addr, node)
	return cn
}

func (cn *chaosNode) stop(hub *chaosHub) {
	hub.leadersElected.Add(cn.node.Status().BecameLeader)
	hub.crash(cn.addr)
	cn.node.Close()
	close(cn.stopApply)
	cn.applyWG.Wait()
}

func TestChaosLinearizabilityUnderFaults(t *testing.T) {
	seeds := []int64{1, 42}
	if raw := os.Getenv("CHAOS_SEED"); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil {
			seeds = []int64{v}
		}
	}
	for _, seed := range seeds {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runChaos(t, seed)
		})
	}
}

func runChaos(t *testing.T, seed int64) {
	const n = 5
	hub := newChaosHub(seed)
	root := t.TempDir()

	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = fmt.Sprintf("10.0.0.%d:7000", i+1)
	}

	nodes := make([]*chaosNode, n)
	var nodesMu sync.Mutex
	for i := 0; i < n; i++ {
		peers := make([]string, 0, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				peers = append(peers, addrs[j])
			}
		}
		nodes[i] = startChaosNode(hub, uint32(i+1), addrs[i], peers, filepath.Join(root, fmt.Sprintf("node-%d", i+1)))
	}
	defer func() {
		nodesMu.Lock()
		defer nodesMu.Unlock()
		for _, cn := range nodes {
			if cn != nil {
				cn.stop(hub)
			}
		}
	}()

	chaosDone := make(chan struct{})
	var chaosWG sync.WaitGroup

	// Fault injector: one mutation of the failure environment every ~150ms,
	// driven entirely by the seeded RNG.
	disable := os.Getenv("CHAOS_DISABLE") // comma list: partition,crash,noise

	// Effective fault actions; read only after chaosWG.Wait().
	var nPartitions, nCrashes, nRestarts, nNoise int

	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		rng := rand.New(rand.NewSource(seed * 7919))
		crashed := -1
		partitioned := -1
		for {
			select {
			case <-chaosDone:
				return
			case <-time.After(time.Duration(100+rng.Intn(150)) * time.Millisecond):
			}

			action := rng.Intn(6)
			switch {
			case (action == 0 || action == 1) && strings.Contains(disable, "partition"):
				continue
			case (action == 2 || action == 3) && strings.Contains(disable, "crash"):
				continue
			case (action == 4 || action == 5) && strings.Contains(disable, "noise"):
				continue
			}
			switch action {
			case 0: // partition one node away
				if partitioned >= 0 {
					hub.setPartitioned(addrs[partitioned], addrs, false)
				}
				partitioned = rng.Intn(n)
				hub.setPartitioned(addrs[partitioned], addrs, true)
				nPartitions++
			case 1: // heal partitions
				if partitioned >= 0 {
					hub.setPartitioned(addrs[partitioned], addrs, false)
					partitioned = -1
				}
			case 2: // crash a node (never two at once)
				if crashed < 0 {
					idx := rng.Intn(n)
					if idx == partitioned {
						break
					}
					nodesMu.Lock()
					nodes[idx].stop(hub)
					nodes[idx] = nil
					nodesMu.Unlock()
					crashed = idx
					nCrashes++
				}
			case 3: // restart the crashed node (WAL recovery under load)
				if crashed >= 0 {
					idx := crashed
					peers := make([]string, 0, n-1)
					for j := 0; j < n; j++ {
						if j != idx {
							peers = append(peers, addrs[j])
						}
					}
					cn := startChaosNode(hub, uint32(idx+1), addrs[idx], peers, filepath.Join(root, fmt.Sprintf("node-%d", idx+1)))
					nodesMu.Lock()
					nodes[idx] = cn
					nodesMu.Unlock()
					crashed = -1
					nRestarts++
				}
			case 4: // packet loss + jitter
				hub.setNoise(rng.Float64()*0.15, rng.Float64()*0.1, time.Duration(rng.Intn(20))*time.Millisecond)
				nNoise++
			case 5: // quiet network
				hub.setNoise(0, 0, 0)
			}
		}
	}()

	// Client workers with sessions and retries. Only ops with definite
	// outcomes get definite return times; indeterminate writes are left
	// pending (resolved to the end of the history) so porcupine may
	// linearize them anywhere after their call.
	type opRecord struct {
		op      porcupine.Operation
		pending bool
	}
	var histMu sync.Mutex
	history := make([]opRecord, 0, 1024)

	const clients = 3
	clientsDone := make(chan struct{})
	var clientWG sync.WaitGroup
	for c := 0; c < clients; c++ {
		c := c
		clientWG.Add(1)
		go func() {
			defer clientWG.Done()
			rng := rand.New(rand.NewSource(seed*104729 + int64(c)))
			clientID := uint64(c + 1)
			seq := uint64(0)
			val := 0
			for {
				select {
				case <-clientsDone:
					return
				default:
				}

				nodesMu.Lock()
				var targets []*chaosNode
				for _, cn := range nodes {
					if cn != nil {
						targets = append(targets, cn)
					}
				}
				nodesMu.Unlock()
				if len(targets) == 0 {
					time.Sleep(50 * time.Millisecond)
					continue
				}

				if rng.Intn(2) == 0 {
					// Linearizable read from a random node.
					target := targets[rng.Intn(len(targets))]
					call := time.Now().UnixNano()
					ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
					resp, err := target.store.GetLinearizable(ctx, &proto.GetRequest{Key: "chaos"})
					cancel()
					ret := time.Now().UnixNano()
					if err == nil && resp != nil && resp.Success {
						histMu.Lock()
						history = append(history, opRecord{op: porcupine.Operation{
							ClientId: c,
							Input:    modelInput{Op: "get"},
							Output:   modelOutput{OK: true, Value: resp.Value},
							Call:     call, Return: ret,
						}})
						histMu.Unlock()
					} else if err == nil && resp != nil && !resp.Success && resp.ErrorMessage == "Key not found" {
						histMu.Lock()
						history = append(history, opRecord{op: porcupine.Operation{
							ClientId: c,
							Input:    modelInput{Op: "get"},
							Output:   modelOutput{OK: true, Value: ""},
							Call:     call, Return: ret,
						}})
						histMu.Unlock()
					}
				} else {
					// Write, retrying the same session sequence number until
					// it definitely commits. Only the final in-flight write of
					// each client can be left indeterminate when the test
					// ends, which keeps the porcupine search space small
					// while staying sound.
					seq++
					val++
					v := fmt.Sprintf("c%d-%d", c, val)
					call := time.Now().UnixNano()
					committed := false
					abandoned := false
					for !committed && !abandoned {
						if len(targets) == 0 {
							time.Sleep(50 * time.Millisecond)
							nodesMu.Lock()
							for _, cn := range nodes {
								if cn != nil {
									targets = append(targets, cn)
								}
							}
							nodesMu.Unlock()
							continue
						}
						target := targets[rng.Intn(len(targets))]
						ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
						resp, err := target.store.Set(ctx, &proto.SetRequest{
							Key: "chaos", Value: v, ClientId: clientID, Sequence: seq,
						})
						cancel()
						if err == nil && resp != nil && resp.Success {
							committed = true
							break
						}
						select {
						case <-clientsDone:
							abandoned = true
						default:
							nodesMu.Lock()
							targets = targets[:0]
							for _, cn := range nodes {
								if cn != nil {
									targets = append(targets, cn)
								}
							}
							nodesMu.Unlock()
							if len(targets) == 0 {
								time.Sleep(50 * time.Millisecond)
							}
						}
					}
					ret := time.Now().UnixNano()
					histMu.Lock()
					history = append(history, opRecord{
						op: porcupine.Operation{
							ClientId: c,
							Input:    modelInput{Op: "set", Value: v},
							Output:   modelOutput{OK: true},
							Call:     call, Return: ret,
						},
						pending: !committed,
					})
					histMu.Unlock()
					if abandoned {
						return
					}
				}

				time.Sleep(time.Duration(10+rng.Intn(40)) * time.Millisecond)
			}
		}()
	}

	time.Sleep(6 * time.Second)
	close(chaosDone)
	chaosWG.Wait()
	close(clientsDone)
	clientWG.Wait()

	// Heal everything and restart any crashed node so the cluster can
	// converge.
	hub.healAll()
	nodesMu.Lock()
	for i, cn := range nodes {
		if cn == nil {
			peers := make([]string, 0, n-1)
			for j := 0; j < n; j++ {
				if j != i {
					peers = append(peers, addrs[j])
				}
			}
			nodes[i] = startChaosNode(hub, uint32(i+1), addrs[i], peers, filepath.Join(root, fmt.Sprintf("node-%d", i+1)))
		}
	}
	live := append([]*chaosNode(nil), nodes...)
	nodesMu.Unlock()

	// Convergence: a single leader, and every node applied up to the same
	// commit index.
	deadline := time.Now().Add(10 * time.Second)
	converged := false
	for time.Now().Before(deadline) && !converged {
		leaders := 0
		var commit uint64
		for _, cn := range live {
			st := cn.node.Status()
			if st.State == raft.Leader {
				leaders++
				commit = st.CommitIndex
			}
		}
		if leaders == 1 {
			converged = true
			for _, cn := range live {
				st := cn.node.Status()
				if st.CommitIndex != commit || st.AppliedIndex != commit {
					converged = false
					break
				}
			}
		}
		if !converged {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if !converged {
		for _, cn := range live {
			st := cn.node.Status()
			t.Logf("node %d: state=%s term=%d commit=%d applied=%d", st.ID, st.State, st.Term, st.CommitIndex, st.AppliedIndex)
		}
		t.Fatal("cluster did not converge after healing")
	}

	// Invariant: replicated state machines are identical.
	var reference []byte
	for i, cn := range live {
		snap, err := cn.store.Snapshot()
		if err != nil {
			t.Fatalf("snapshot of node %d failed: %v", cn.id, err)
		}
		if i == 0 {
			reference = snap
		} else if string(snap) != string(reference) {
			t.Fatalf("state machine divergence between node %d and node %d:\n%s\nvs\n%s",
				live[0].id, cn.id, reference, snap)
		}
	}

	// Resolve pending writes to a return time after every other operation,
	// then check linearizability.
	histMu.Lock()
	var maxT int64
	for _, r := range history {
		if r.op.Call > maxT {
			maxT = r.op.Call
		}
		if !r.pending && r.op.Return > maxT {
			maxT = r.op.Return
		}
	}
	ops := make([]porcupine.Operation, 0, len(history))
	var nSets, nGets, nPending int
	for _, r := range history {
		op := r.op
		if op.Input.(modelInput).Op == "set" {
			nSets++
		} else {
			nGets++
		}
		if r.pending {
			nPending++
			// An indeterminate write may take effect at any point after its
			// call; modeling it as returning after every other operation
			// lets porcupine linearize it anywhere in that window.
			op.Return = maxT + 1
		}
		ops = append(ops, op)
	}
	histMu.Unlock()

	model := porcupine.Model{
		Init: func() interface{} { return "" },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			cur := state.(string)
			in := input.(modelInput)
			out := output.(modelOutput)
			switch in.Op {
			case "set":
				return true, in.Value
			case "get":
				return out.Value == cur, cur
			default:
				return false, cur
			}
		},
		Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
	}

	res, info := porcupine.CheckOperationsVerbose(model, ops, 30*time.Second)
	if res != porcupine.Ok {
		dumpChaosArtifacts(t, seed, ops, model, info)
		verdict := "NOT LINEARIZABLE"
		if res == porcupine.Unknown {
			verdict = "checker timed out (inconclusive)"
		}
		t.Fatalf("linearizability check failed: %s (%d ops)", verdict, len(ops))
	}
	t.Logf("chaos run passed: %d operations linearizable, state machines identical", len(ops))

	leaders := hub.leadersElected.Load()
	for _, cn := range live {
		leaders += cn.node.Status().BecameLeader
	}
	hub.mu.Lock()
	t.Logf("chaos stats: seed=%d ops=%d sets=%d gets=%d pending=%d crashes=%d restarts=%d partitions=%d noise=%d dropped=%d reply_dropped=%d duplicated=%d leaders_elected=%d",
		seed, len(ops), nSets, nGets, nPending, nCrashes, nRestarts, nPartitions, nNoise,
		hub.msgsDropped, hub.repliesDropped, hub.msgsDuplicated, leaders)
	hub.mu.Unlock()
}

func dumpChaosArtifacts(t *testing.T, seed int64, ops []porcupine.Operation, model porcupine.Model, info porcupine.LinearizationInfo) {
	t.Helper()
	_ = os.MkdirAll("../logs", 0o755)

	sorted := append([]porcupine.Operation(nil), ops...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Call < sorted[j].Call })
	base := sorted[0].Call

	var b strings.Builder
	for _, op := range sorted {
		in := op.Input.(modelInput)
		out := op.Output.(modelOutput)
		fmt.Fprintf(&b, "c%d %-4s val=%-10q out=%-10q call=%8.3fms ret=%8.3fms\n",
			op.ClientId, in.Op, in.Value, out.Value,
			float64(op.Call-base)/1e6, float64(op.Return-base)/1e6)
	}
	txt := fmt.Sprintf("../logs/chaos-seed-%d.txt", seed)
	if err := os.WriteFile(txt, []byte(b.String()), 0o644); err == nil {
		t.Logf("wrote op history to %s", txt)
	}
	html := fmt.Sprintf("../logs/chaos-seed-%d.html", seed)
	if f, err := os.Create(html); err == nil {
		_ = porcupine.Visualize(model, info, f)
		_ = f.Close()
		t.Logf("wrote porcupine visualization to %s", html)
	}
}
