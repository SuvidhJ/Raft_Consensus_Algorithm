package raft

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// maxIncomingAppendEntries bounds a single received AppendEntries batch.
// Senders are capped at 1024 by the config guardrails; the slack tolerates
// version skew without admitting unbounded allocations.
const maxIncomingAppendEntries = 4096

type Snapshotter interface {
	Snapshot() ([]byte, error)
	Restore([]byte) error
}

type Config struct {
	ID                  uint32
	Peers               []string
	ApplyCh             chan<- *proto.LogEntry
	StrictApplyAcks     bool
	DataDir             string
	HeartbeatInterval   time.Duration
	ElectionTimeoutMin  time.Duration
	ElectionTimeoutMax  time.Duration
	ProposalTimeout     time.Duration
	ReadQuorumTimeout   time.Duration
	SnapshotThreshold   uint64
	MaxEntriesPerAppend int
	EnablePreVote       bool
	EnableCheckQuorum   bool

	// AdvertiseAddr is this node's address as other members reach it.
	// Required for dynamic membership changes.
	AdvertiseAddr string

	// Joining marks a node that starts outside the cluster and waits to be
	// added via AddServer. A joining node never starts elections until a
	// configuration that includes it arrives, so it cannot disrupt the
	// cluster by campaigning as a single-node majority.
	Joining bool

	// Transport overrides the default gRPC transport (used by tests to
	// inject an in-memory network with fault injection).
	Transport Transport
}

type persistentState struct {
	CurrentTerm       uint64            `json:"current_term"`
	VotedFor          uint32            `json:"voted_for"`
	Log               []*proto.LogEntry `json:"log"`
	CommitIndex       uint64            `json:"commit_index"`
	LastIncludedIndex uint64            `json:"last_included_index"`
	LastIncludedTerm  uint64            `json:"last_included_term"`
	SnapshotData      []byte            `json:"snapshot_data"`
}

// RaftNode encapsulates a production-style Raft consensus node.
type RaftNode struct {
	mu sync.Mutex

	id     uint32
	peers  []string
	state  State
	leader uint32

	// Persistent state on all servers.
	currentTerm uint64
	votedFor    uint32
	log         []*proto.LogEntry

	// Snapshot metadata.
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshotData      []byte
	snapshotConfig    []string

	// Membership.
	advertiseAddr    string
	bootstrapMembers []string
	joining          bool

	// Volatile state on all servers.
	commitIndex    uint64
	lastApplied    uint64
	lastDispatched uint64

	// Volatile state on leaders.
	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	replicating map[string]bool

	transport Transport

	applyCh chan<- *proto.LogEntry

	strictApplyAcks bool

	snapshotter Snapshotter

	heartbeatInterval   time.Duration
	electionTimeoutMin  time.Duration
	electionTimeoutMax  time.Duration
	proposalTimeout     time.Duration
	readQuorumTimeout   time.Duration
	snapshotThreshold   uint64
	maxEntriesPerAppend int
	enablePreVote       bool
	enableCheckQuorum   bool

	electionDeadline time.Time
	rng              *rand.Rand
	leaderSince      time.Time
	peerLastContact  map[string]time.Time

	// lastLeaderContact is when this node last accepted an AppendEntries or
	// InstallSnapshot from the current leader. Pre-vote rejection is based
	// on this, never on electionDeadline: the deadline is reset by our own
	// failed pre-votes, and using it would let a cluster of followers
	// livelock by perpetually rejecting each other's pre-votes.
	lastLeaderContact time.Time

	// termStartIndex is the index of the no-op this node appended when it
	// became leader. Linearizable reads must wait until commitIndex covers
	// it (Raft §8): a new leader's commitIndex may lag entries the previous
	// leader already committed and acknowledged.
	termStartIndex uint64

	dataDir string
	wal     *walStorage

	// progressCh is closed and replaced whenever observable progress occurs
	// (commit advance, apply advance, state change, peer contact). Waiters
	// grab the current channel under mu and select on it alongside their
	// context/deadline, avoiding busy-polling.
	progressCh chan struct{}

	replicateCh chan struct{}
	stopped     chan struct{}
	stopOnce    sync.Once
	loopWG      sync.WaitGroup
	replicateWG sync.WaitGroup

	rpcCtx    context.Context
	rpcCancel context.CancelFunc

	// Monotonic counters for observability (atomic; read without mu).
	ctrElectionsStarted   atomic.Uint64
	ctrBecameLeader       atomic.Uint64
	ctrSnapshotsCreated   atomic.Uint64
	ctrSnapshotsInstalled atomic.Uint64
}

func NewRaftNode(id uint32, peers []string, applyCh chan<- *proto.LogEntry) *RaftNode {
	cfg := Config{
		ID:                  id,
		Peers:               peers,
		ApplyCh:             applyCh,
		StrictApplyAcks:     true,
		HeartbeatInterval:   80 * time.Millisecond,
		ElectionTimeoutMin:  250 * time.Millisecond,
		ElectionTimeoutMax:  500 * time.Millisecond,
		ProposalTimeout:     2 * time.Second,
		ReadQuorumTimeout:   500 * time.Millisecond,
		SnapshotThreshold:   512,
		MaxEntriesPerAppend: 64,
		EnablePreVote:       true,
		EnableCheckQuorum:   true,
	}

	if raw := strings.TrimSpace(os.Getenv("RAFT_HEARTBEAT_MS")); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			cfg.HeartbeatInterval = time.Duration(ms) * time.Millisecond
		}
	}
	if raw := strings.TrimSpace(os.Getenv("RAFT_ELECTION_MIN_MS")); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			cfg.ElectionTimeoutMin = time.Duration(ms) * time.Millisecond
		}
	}
	if raw := strings.TrimSpace(os.Getenv("RAFT_ELECTION_MAX_MS")); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			cfg.ElectionTimeoutMax = time.Duration(ms) * time.Millisecond
		}
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 200*time.Millisecond
	}
	if raw := strings.TrimSpace(os.Getenv("RAFT_SNAPSHOT_THRESHOLD")); raw != "" {
		if v, err := strconv.ParseUint(raw, 10, 64); err == nil {
			cfg.SnapshotThreshold = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv("RAFT_MAX_APPEND_ENTRIES")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			cfg.MaxEntriesPerAppend = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv("RAFT_READ_QUORUM_TIMEOUT_MS")); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			cfg.ReadQuorumTimeout = time.Duration(ms) * time.Millisecond
		}
	}
	if raw := strings.TrimSpace(os.Getenv("RAFT_ENABLE_PREVOTE")); raw != "" {
		cfg.EnablePreVote = parseBoolEnv(raw, true)
	}
	if raw := strings.TrimSpace(os.Getenv("RAFT_ENABLE_CHECK_QUORUM")); raw != "" {
		cfg.EnableCheckQuorum = parseBoolEnv(raw, true)
	}
	if raw := strings.TrimSpace(os.Getenv("RAFT_STRICT_APPLY_ACKS")); raw != "" {
		cfg.StrictApplyAcks = parseBoolEnv(raw, true)
	}
	cfg.AdvertiseAddr = strings.TrimSpace(os.Getenv("RAFT_ADVERTISE_ADDR"))
	if raw := strings.TrimSpace(os.Getenv("RAFT_JOIN")); raw != "" {
		cfg.Joining = parseBoolEnv(raw, false)
	}

	if os.Getenv("RAFT_DISABLE_PERSIST") != "1" {
		root := strings.TrimSpace(os.Getenv("RAFT_DATA_DIR"))
		if root == "" {
			root = "data"
		}
		cfg.DataDir = filepath.Join(root, fmt.Sprintf("node-%d", id))
	}

	return NewRaftNodeWithConfig(cfg)
}

func NewRaftNodeWithConfig(cfg Config) *RaftNode {
	if cfg.ID == 0 {
		panic("raft node ID must be non-zero")
	}
	if cfg.ApplyCh == nil {
		panic("raft apply channel must be non-nil")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 80 * time.Millisecond
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = 250 * time.Millisecond
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 200*time.Millisecond
	}
	if cfg.ProposalTimeout <= 0 {
		cfg.ProposalTimeout = 2 * time.Second
	}
	if cfg.ReadQuorumTimeout <= 0 {
		cfg.ReadQuorumTimeout = 500 * time.Millisecond
	}
	if cfg.MaxEntriesPerAppend <= 0 {
		cfg.MaxEntriesPerAppend = 64
	}
	enforceConfigGuardrails(&cfg)

	peers := normalizePeers(cfg.Peers)
	cfg.AdvertiseAddr = strings.TrimSpace(cfg.AdvertiseAddr)
	if cfg.AdvertiseAddr != "" {
		if err := validatePeerAddress(cfg.AdvertiseAddr); err != nil {
			panic(fmt.Sprintf("invalid advertise address: %v", err))
		}
		// Never list ourselves as our own peer.
		filtered := peers[:0]
		for _, p := range peers {
			if p != cfg.AdvertiseAddr {
				filtered = append(filtered, p)
			}
		}
		peers = filtered
	}
	validatePeerAddressesOrPanic(peers)

	// A joining node has no configuration of its own: it waits for the
	// cluster's config (via replication) to include it. Giving it a
	// bootstrap config would let it form a single-node majority.
	var bootstrapMembers []string
	if !cfg.Joining {
		bootstrapMembers = append([]string(nil), peers...)
		if cfg.AdvertiseAddr != "" {
			bootstrapMembers = append(bootstrapMembers, cfg.AdvertiseAddr)
		}
	}

	rpcCtx, rpcCancel := context.WithCancel(context.Background())

	transport := cfg.Transport
	if transport == nil {
		transport = NewGRPCTransport()
	}

	rn := &RaftNode{
		id:                  cfg.ID,
		peers:               peers,
		advertiseAddr:       cfg.AdvertiseAddr,
		bootstrapMembers:    bootstrapMembers,
		joining:             cfg.Joining,
		state:               Follower,
		leader:              0,
		log:                 make([]*proto.LogEntry, 0),
		nextIndex:           make(map[string]uint64),
		matchIndex:          make(map[string]uint64),
		replicating:         make(map[string]bool),
		transport:           transport,
		applyCh:             cfg.ApplyCh,
		strictApplyAcks:     cfg.StrictApplyAcks,
		heartbeatInterval:   cfg.HeartbeatInterval,
		electionTimeoutMin:  cfg.ElectionTimeoutMin,
		electionTimeoutMax:  cfg.ElectionTimeoutMax,
		proposalTimeout:     cfg.ProposalTimeout,
		readQuorumTimeout:   cfg.ReadQuorumTimeout,
		snapshotThreshold:   cfg.SnapshotThreshold,
		maxEntriesPerAppend: cfg.MaxEntriesPerAppend,
		enablePreVote:       cfg.EnablePreVote,
		enableCheckQuorum:   cfg.EnableCheckQuorum,
		progressCh:          make(chan struct{}),
		replicateCh:         make(chan struct{}, 1),
		stopped:             make(chan struct{}),
		rng:                 rand.New(rand.NewSource(time.Now().UnixNano() + int64(cfg.ID)*7919)),
		dataDir:             cfg.DataDir,
		peerLastContact:     make(map[string]time.Time),
		rpcCtx:              rpcCtx,
		rpcCancel:           rpcCancel,
	}
	if rn.dataDir != "" {
		wal, rec, err := openWAL(rn.dataDir)
		if err != nil {
			panic(fmt.Sprintf("[Node %d] failed to load persisted state: %v", rn.id, err))
		}
		rn.wal = wal
		rn.currentTerm = rec.term
		rn.votedFor = rec.votedFor
		rn.commitIndex = rec.commitIndex
		rn.lastIncludedIndex = rec.lastIncludedIndex
		rn.lastIncludedTerm = rec.lastIncludedTerm
		rn.snapshotData = rec.snapshotData
		rn.snapshotConfig = rec.snapshotConfig
		rn.log = rec.entries
	}

	rn.mu.Lock()
	if rn.commitIndex < rn.lastIncludedIndex {
		rn.commitIndex = rn.lastIncludedIndex
	}
	rn.lastApplied = rn.lastIncludedIndex
	rn.lastDispatched = rn.lastIncludedIndex
	rn.state = Follower
	rn.leader = 0
	// Membership may differ from the bootstrap peer list if config entries
	// or a snapshot config were recovered.
	rn.recomputeConfigLocked()
	rn.resetElectionDeadlineLocked()
	rn.mu.Unlock()

	rn.loopWG.Add(3)
	go rn.electionLoop()
	go rn.replicationLoop()
	go rn.applyLoop()

	return rn
}

func normalizePeers(peers []string) []string {
	out := make([]string, 0, len(peers))
	seen := make(map[string]struct{}, len(peers))
	for _, p := range peers {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}

func (rn *RaftNode) SetSnapshotter(sm Snapshotter) {
	rn.mu.Lock()
	rn.snapshotter = sm
	snapshot := append([]byte(nil), rn.snapshotData...)
	rn.mu.Unlock()

	if sm != nil && len(snapshot) > 0 {
		if err := sm.Restore(snapshot); err != nil {
			panic(fmt.Sprintf("[Node %d] snapshot restore failed: %v", rn.id, err))
		}
	}
}

func enforceConfigGuardrails(cfg *Config) {
	if cfg == nil {
		return
	}

	if cfg.HeartbeatInterval < 20*time.Millisecond {
		cfg.HeartbeatInterval = 20 * time.Millisecond
	}
	if cfg.ElectionTimeoutMin < 100*time.Millisecond {
		cfg.ElectionTimeoutMin = 100 * time.Millisecond
	}
	if min := 3 * cfg.HeartbeatInterval; cfg.ElectionTimeoutMin < min {
		cfg.ElectionTimeoutMin = min
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 200*time.Millisecond
	}
	if cfg.ElectionTimeoutMax < cfg.ElectionTimeoutMin+50*time.Millisecond {
		cfg.ElectionTimeoutMax = cfg.ElectionTimeoutMin + 50*time.Millisecond
	}
	if cfg.ProposalTimeout < cfg.ElectionTimeoutMax {
		cfg.ProposalTimeout = cfg.ElectionTimeoutMax * 2
	}
	if cfg.ProposalTimeout < 500*time.Millisecond {
		cfg.ProposalTimeout = 500 * time.Millisecond
	}
	minReadTimeout := 2 * cfg.HeartbeatInterval
	if minReadTimeout < 100*time.Millisecond {
		minReadTimeout = 100 * time.Millisecond
	}
	if cfg.ReadQuorumTimeout < minReadTimeout {
		cfg.ReadQuorumTimeout = minReadTimeout
	}
	if cfg.MaxEntriesPerAppend > 1024 {
		cfg.MaxEntriesPerAppend = 1024
	}
}

func validatePeerAddress(peer string) error {
	host, port, err := net.SplitHostPort(peer)
	if err != nil {
		return fmt.Errorf("invalid peer address %q: expected host:port", peer)
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("invalid peer address %q: host is empty", peer)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil || portNum < 1 || portNum > 65535 {
		return fmt.Errorf("invalid peer address %q: invalid port", peer)
	}
	return nil
}

func validatePeerAddressesOrPanic(peers []string) {
	for _, peer := range peers {
		if err := validatePeerAddress(peer); err != nil {
			panic(err.Error())
		}
	}
}

// EnableApplyAcks makes Propose wait for state-machine acknowledgements.
func (rn *RaftNode) EnableApplyAcks() {
	rn.mu.Lock()
	rn.strictApplyAcks = true
	rn.mu.Unlock()
}

// NotifyApplied records that the state machine has applied entry index.
func (rn *RaftNode) NotifyApplied(index uint64) {
	if index == 0 {
		return
	}

	rn.mu.Lock()
	if index <= rn.lastApplied {
		rn.mu.Unlock()
		return
	}
	if index > rn.commitIndex {
		// Never let lastApplied run ahead of commitIndex: read barriers use
		// lastApplied as evidence of applied state, and an over-eager
		// notification (e.g. for an entry rejected by ShouldApplyEntry)
		// must not satisfy them.
		rn.mu.Unlock()
		return
	}
	rn.lastApplied = index
	if rn.lastDispatched < index {
		rn.lastDispatched = index
	}
	needSnapshot := rn.snapshotThreshold > 0 && rn.lastApplied-rn.lastIncludedIndex >= rn.snapshotThreshold
	rn.broadcastProgressLocked()
	rn.mu.Unlock()

	if needSnapshot {
		rn.maybeCreateSnapshot(index)
	}
}

// ShouldApplyEntry reports whether an entry is still valid for state-machine application.
func (rn *RaftNode) ShouldApplyEntry(entry *proto.LogEntry) bool {
	if entry == nil || entry.Index == 0 {
		return false
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if entry.Index <= rn.lastIncludedIndex || entry.Index > rn.commitIndex {
		return false
	}
	term, ok := rn.termAtLocked(entry.Index)
	if !ok {
		return false
	}
	return term == entry.Term
}

func (rn *RaftNode) electionLoop() {
	defer rn.loopWG.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopped:
			return
		case <-ticker.C:
			rn.mu.Lock()
			shouldStart := rn.state != Leader && !rn.joining && time.Now().After(rn.electionDeadline)
			rn.mu.Unlock()
			if shouldStart {
				rn.startElection()
			}
		}
	}
}

func (rn *RaftNode) replicationLoop() {
	defer rn.loopWG.Done()
	ticker := time.NewTicker(rn.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopped:
			return
		case <-ticker.C:
			rn.replicateAll()
			rn.maybeStepDownOnLostQuorum()
		case <-rn.replicateCh:
			rn.replicateAll()
		}
	}
}

func (rn *RaftNode) applyLoop() {
	defer rn.loopWG.Done()

	for {
		rn.mu.Lock()
		for rn.lastDispatched >= rn.commitIndex {
			progress := rn.progressCh
			rn.mu.Unlock()
			select {
			case <-rn.stopped:
				return
			case <-progress:
			}
			rn.mu.Lock()
		}

		next := rn.lastDispatched + 1
		entry, ok := rn.entryAtLocked(next)
		if !ok {
			rn.lastDispatched = rn.commitIndex
			if !rn.strictApplyAcks && rn.lastApplied < rn.lastDispatched {
				rn.lastApplied = rn.lastDispatched
			}
			rn.broadcastProgressLocked()
			rn.mu.Unlock()
			continue
		}
		rn.lastDispatched = next
		rn.broadcastProgressLocked()
		rn.mu.Unlock()

		// Leader no-ops and configuration entries are consumed by Raft
		// itself and never reach the state machine.
		if entry.Command == "INIT" || entry.Type == proto.EntryType_ENTRY_CONFIG {
			rn.NotifyApplied(next)
			continue
		}

		select {
		case <-rn.stopped:
			return
		case rn.applyCh <- cloneEntry(entry):
		}

		needSnapshot := false
		rn.mu.Lock()
		if !rn.strictApplyAcks && rn.lastApplied < next {
			rn.lastApplied = next
			needSnapshot = rn.snapshotThreshold > 0 && rn.lastApplied-rn.lastIncludedIndex >= rn.snapshotThreshold
			rn.broadcastProgressLocked()
		}
		rn.mu.Unlock()

		if needSnapshot {
			rn.maybeCreateSnapshot(next)
		}
	}
}

func (rn *RaftNode) startElection() {
	rn.startElectionInternal(false)
}

func (rn *RaftNode) startElectionInternal(skipPreVote bool) {
	// Elections can also be triggered from a TimeoutNow handler goroutine,
	// so re-check lifecycle and joining state here rather than relying on
	// the election loop's checks.
	select {
	case <-rn.stopped:
		return
	default:
	}
	rn.mu.Lock()
	if rn.joining {
		rn.mu.Unlock()
		return
	}
	rn.mu.Unlock()

	if rn.enablePreVote && !skipPreVote {
		if !rn.runPreVote() {
			rn.mu.Lock()
			rn.resetElectionDeadlineLocked()
			rn.mu.Unlock()
			return
		}
	}

	rn.mu.Lock()
	if rn.state == Leader {
		rn.mu.Unlock()
		return
	}
	rn.state = Candidate
	rn.currentTerm++
	term := rn.currentTerm
	rn.votedFor = rn.id
	rn.leader = 0
	rn.resetElectionDeadlineLocked()
	lastLogIndex := rn.lastLogIndexLocked()
	lastLogTerm := rn.lastLogTermLocked()
	peers := append([]string(nil), rn.peers...)
	rn.persistHardStateLocked("start-election", true)
	rn.mu.Unlock()

	rn.ctrElectionsStarted.Add(1)
	log.Printf("[Node %d] starting election for term %d", rn.id, term)

	votes := 1
	majority := (len(peers)+1)/2 + 1
	var wg sync.WaitGroup
	var voteMu sync.Mutex

	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := rn.newRPCTimeoutContext(350 * time.Millisecond)
			defer cancel()

			resp, err := rn.transport.RequestVote(ctx, peer, &proto.RequestVoteRequest{
				Term:         term,
				CandidateId:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil || resp == nil {
				return
			}

			rn.mu.Lock()
			if resp.Term > rn.currentTerm {
				rn.becomeFollowerLocked(resp.Term, 0)
				rn.persistHardStateLocked("request-vote-response-term-bump", true)
				rn.mu.Unlock()
				return
			}
			if rn.state != Candidate || rn.currentTerm != term {
				rn.mu.Unlock()
				return
			}
			rn.mu.Unlock()

			if resp.VoteGranted {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}()
	}

	wg.Wait()

	rn.mu.Lock()
	won := rn.state == Candidate && rn.currentTerm == term && votes >= majority
	if won {
		rn.becomeLeaderLocked()
		rn.ctrBecameLeader.Add(1)
		log.Printf("[Node %d] became leader for term %d with %d votes", rn.id, term, votes)
	}
	rn.mu.Unlock()

	if won {
		rn.notifyReplication()
	}
}

func (rn *RaftNode) runPreVote() bool {
	rn.mu.Lock()
	if rn.state == Leader {
		rn.mu.Unlock()
		return false
	}
	term := rn.currentTerm + 1
	lastLogIndex := rn.lastLogIndexLocked()
	lastLogTerm := rn.lastLogTermLocked()
	peers := append([]string(nil), rn.peers...)
	rn.mu.Unlock()

	votes := 1
	majority := (len(peers)+1)/2 + 1
	var wg sync.WaitGroup
	var voteMu sync.Mutex

	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := rn.newRPCTimeoutContext(300 * time.Millisecond)
			defer cancel()
			resp, err := rn.transport.PreVote(ctx, peer, &proto.PreVoteRequest{
				Term:         term,
				CandidateId:  rn.id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})
			if err != nil || resp == nil {
				return
			}

			rn.mu.Lock()
			if resp.Term > rn.currentTerm {
				rn.becomeFollowerLocked(resp.Term, 0)
				rn.persistHardStateLocked("pre-vote-response-term-bump", true)
				rn.mu.Unlock()
				return
			}
			if rn.state == Leader {
				rn.mu.Unlock()
				return
			}
			rn.mu.Unlock()

			if resp.VoteGranted {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}()
	}

	wg.Wait()
	return votes >= majority
}

func (rn *RaftNode) becomeFollowerLocked(term uint64, leaderID uint32) {
	if term > rn.currentTerm {
		rn.currentTerm = term
		rn.votedFor = 0
	}
	rn.state = Follower
	rn.leader = leaderID
	rn.leaderSince = time.Time{}
	for _, peer := range rn.peers {
		rn.replicating[peer] = false
	}
	rn.resetElectionDeadlineLocked()
	rn.broadcastProgressLocked()
}

func (rn *RaftNode) becomeLeaderLocked() {
	rn.state = Leader
	rn.leader = rn.id
	rn.leaderSince = time.Now()
	last := rn.lastLogIndexLocked()
	for _, peer := range rn.peers {
		rn.nextIndex[peer] = last + 1
		rn.matchIndex[peer] = 0
		rn.peerLastContact[peer] = time.Time{}
		rn.replicating[peer] = false
	}

	noop := &proto.LogEntry{Term: rn.currentTerm, Command: "INIT", Index: last + 1}
	rn.log = append(rn.log, noop)
	rn.termStartIndex = noop.Index
	rn.persistAppendLocked("leader-noop-append", []*proto.LogEntry{noop})
	rn.maybeAdvanceCommitLocked()
	rn.broadcastProgressLocked()
}

func (rn *RaftNode) resetElectionDeadlineLocked() {
	if rn.electionTimeoutMax <= rn.electionTimeoutMin {
		rn.electionDeadline = time.Now().Add(rn.electionTimeoutMin)
		return
	}
	delta := rn.rng.Int63n(int64(rn.electionTimeoutMax - rn.electionTimeoutMin))
	rn.electionDeadline = time.Now().Add(rn.electionTimeoutMin + time.Duration(delta))
}

// Propose appends a command and waits for commit by quorum.
func (rn *RaftNode) Propose(command string) bool {
	return rn.ProposeWithContext(context.Background(), command)
}

// ProposeWithContext appends a command and respects caller cancellation.
func (rn *RaftNode) ProposeWithContext(ctx context.Context, command string) bool {
	if strings.TrimSpace(command) == "" {
		return false
	}
	_, ok := rn.proposeEntry(ctx, &proto.LogEntry{Command: command})
	return ok
}

// ProposePayload appends a typed command payload and waits for it to commit
// and apply. It returns the assigned log index so callers can correlate
// apply-time results with this exact proposal (the index is only returned
// when the entry committed in the proposing term).
func (rn *RaftNode) ProposePayload(ctx context.Context, payload []byte) (uint64, bool) {
	if len(payload) == 0 {
		return 0, false
	}
	return rn.proposeEntry(ctx, &proto.LogEntry{Payload: payload})
}

func (rn *RaftNode) proposeEntry(ctx context.Context, entry *proto.LogEntry) (uint64, bool) {
	if ctx == nil {
		ctx = context.Background()
	}

	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return 0, false
	}

	term := rn.currentTerm
	index := rn.lastLogIndexLocked() + 1
	entry.Term = term
	entry.Index = index
	rn.log = append(rn.log, entry)
	rn.persistAppendLocked("proposal-append", []*proto.LogEntry{entry})
	rn.maybeAdvanceCommitLocked()
	rn.mu.Unlock()

	rn.notifyReplication()
	if !rn.waitForCommitWithContext(ctx, index, term) {
		return 0, false
	}
	if !rn.waitForApplyWithContext(ctx, index) {
		return 0, false
	}
	return index, true
}

func (rn *RaftNode) waitForCommitWithContext(ctx context.Context, index, term uint64) bool {
	if ctx == nil {
		ctx = context.Background()
	}

	timer := time.NewTimer(rn.proposalTimeout)
	defer timer.Stop()

	rn.notifyReplication()
	rn.mu.Lock()
	for {
		if rn.commitIndex >= index {
			// Confirm the committed entry at index is the one we proposed:
			// after losing leadership, a different entry from a newer term
			// can commit at the same index.
			committedTerm, ok := rn.termAtLocked(index)
			success := index <= rn.lastIncludedIndex || (ok && committedTerm == term)
			rn.mu.Unlock()
			return success
		}
		if rn.state != Leader || rn.currentTerm != term {
			rn.mu.Unlock()
			return false
		}
		progress := rn.progressCh
		rn.mu.Unlock()

		select {
		case <-rn.stopped:
			return false
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-progress:
		}
		rn.mu.Lock()
	}
}

func (rn *RaftNode) waitForApplyWithContext(ctx context.Context, index uint64) bool {
	if ctx == nil {
		ctx = context.Background()
	}

	timer := time.NewTimer(rn.proposalTimeout)
	defer timer.Stop()

	rn.mu.Lock()
	for {
		observedApplied := rn.lastApplied
		if !rn.strictApplyAcks && rn.lastDispatched > observedApplied {
			observedApplied = rn.lastDispatched
		}
		if observedApplied >= index {
			rn.mu.Unlock()
			return true
		}
		progress := rn.progressCh
		rn.mu.Unlock()

		select {
		case <-rn.stopped:
			return false
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-progress:
		}
		rn.mu.Lock()
	}
}

func (rn *RaftNode) notifyReplication() {
	select {
	case rn.replicateCh <- struct{}{}:
	default:
	}
}

// broadcastProgressLocked wakes all goroutines waiting for commit/apply/state
// progress. Must be called with rn.mu held.
func (rn *RaftNode) broadcastProgressLocked() {
	close(rn.progressCh)
	rn.progressCh = make(chan struct{})
}

func (rn *RaftNode) replicateAll() {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return
	}
	peers := make([]string, 0, len(rn.peers))
	for _, peer := range rn.peers {
		if rn.replicating[peer] {
			continue
		}
		rn.replicating[peer] = true
		peers = append(peers, peer)
	}
	rn.mu.Unlock()

	for _, peer := range peers {
		peer := peer
		rn.replicateWG.Add(1)
		go func() {
			defer rn.replicateWG.Done()
			rn.replicatePeer(peer, false)
		}()
	}
}

func (rn *RaftNode) maybeStepDownOnLostQuorum() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if !rn.enableCheckQuorum || rn.state != Leader {
		return
	}
	if rn.leaderSince.IsZero() || time.Since(rn.leaderSince) < rn.electionTimeoutMax {
		return
	}

	if rn.recentQuorumCountLocked(rn.electionTimeoutMax) >= rn.majorityLocked() {
		return
	}

	log.Printf("[Node %d] stepping down: quorum not active", rn.id)
	// Same-term stepdown changes no persistent state (term and vote are
	// unchanged), so no WAL write is needed.
	rn.becomeFollowerLocked(rn.currentTerm, 0)
}

func (rn *RaftNode) ReadBarrier() bool {
	return rn.ReadBarrierWithContext(context.Background())
}

func (rn *RaftNode) ReadBarrierWithContext(ctx context.Context) bool {
	if ctx == nil {
		ctx = context.Background()
	}

	since := time.Now()
	timer := time.NewTimer(rn.readQuorumTimeout)
	defer timer.Stop()

	kickCadence := rn.heartbeatInterval / 2
	if kickCadence <= 0 {
		kickCadence = 20 * time.Millisecond
	}
	kicker := time.NewTicker(kickCadence)
	defer kicker.Stop()

	rn.notifyReplication()
	rn.mu.Lock()
	// ReadIndex: the read must observe everything committed before it
	// began. A new leader's commitIndex may not yet cover entries the
	// previous leader committed, so the barrier additionally requires the
	// current term's no-op to be committed (Raft §8).
	readIndex := rn.commitIndex
	if rn.termStartIndex > readIndex {
		readIndex = rn.termStartIndex
	}
	for {
		if rn.state != Leader {
			rn.mu.Unlock()
			return false
		}
		quorumReady := rn.quorumSinceLocked(since) >= rn.majorityLocked()
		if quorumReady && rn.commitIndex >= readIndex && rn.lastApplied >= readIndex {
			rn.mu.Unlock()
			return true
		}
		progress := rn.progressCh
		rn.mu.Unlock()

		select {
		case <-rn.stopped:
			return false
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-kicker.C:
			rn.notifyReplication()
		case <-progress:
		}
		rn.mu.Lock()
	}
}

func (rn *RaftNode) TransferLeadership(targetPeer string) bool {
	rn.mu.Lock()
	if rn.state != Leader {
		rn.mu.Unlock()
		return false
	}
	if !rn.isKnownPeerLocked(targetPeer) {
		rn.mu.Unlock()
		return false
	}
	term := rn.currentTerm
	rn.mu.Unlock()

	deadline := time.Now().Add(rn.proposalTimeout)
	upToDate := false
	for time.Now().Before(deadline) {
		rn.notifyReplication()
		rn.replicatePeerNow(targetPeer, false)

		rn.mu.Lock()
		if rn.state != Leader || rn.currentTerm != term {
			rn.mu.Unlock()
			return false
		}
		upToDate = rn.matchIndex[targetPeer] >= rn.lastLogIndexLocked()
		rn.mu.Unlock()
		if upToDate {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !upToDate {
		return false
	}

	rn.mu.Lock()
	if rn.state != Leader || rn.currentTerm != term {
		rn.mu.Unlock()
		return false
	}
	rn.mu.Unlock()

	ctx, cancel := rn.newRPCTimeoutContext(500 * time.Millisecond)
	resp, err := rn.transport.TimeoutNow(ctx, targetPeer, &proto.TimeoutNowRequest{Term: term, LeaderId: rn.id})
	cancel()
	if err != nil || resp == nil {
		return false
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()
	if resp.Term > rn.currentTerm {
		rn.becomeFollowerLocked(resp.Term, 0)
		rn.persistHardStateLocked("transfer-leadership-term-bump", true)
		return false
	}
	return resp.Success
}

func (rn *RaftNode) recentQuorumCountLocked(window time.Duration) int {
	count := 1
	now := time.Now()
	for _, peer := range rn.peers {
		last := rn.peerLastContact[peer]
		if !last.IsZero() && now.Sub(last) <= window {
			count++
		}
	}
	return count
}

func (rn *RaftNode) quorumSinceLocked(since time.Time) int {
	count := 1
	for _, peer := range rn.peers {
		last := rn.peerLastContact[peer]
		if !last.IsZero() && !last.Before(since) {
			count++
		}
	}
	return count
}

func (rn *RaftNode) isKnownPeerLocked(peer string) bool {
	for _, p := range rn.peers {
		if p == peer {
			return true
		}
	}
	return false
}

// replicatePeerNow synchronously replicates to one address. allowNonMember
// permits targeting servers outside the active configuration (AddServer
// catch-up).
func (rn *RaftNode) replicatePeerNow(peer string, allowNonMember bool) {
	rn.mu.Lock()
	if rn.state != Leader || rn.replicating[peer] {
		rn.mu.Unlock()
		return
	}
	rn.replicating[peer] = true
	rn.replicateWG.Add(1)
	rn.mu.Unlock()
	defer rn.replicateWG.Done()
	rn.replicatePeer(peer, allowNonMember)
}

func (rn *RaftNode) replicatePeer(peer string, allowNonMember bool) {
	defer func() {
		rn.mu.Lock()
		rn.replicating[peer] = false
		rn.mu.Unlock()
	}()

	for {
		select {
		case <-rn.stopped:
			return
		default:
		}

		rn.mu.Lock()
		if rn.state != Leader {
			rn.mu.Unlock()
			return
		}
		if !allowNonMember && !rn.isKnownPeerLocked(peer) {
			// The peer was removed from the configuration mid-replication.
			rn.mu.Unlock()
			return
		}
		term := rn.currentTerm
		next := rn.nextIndex[peer]
		if next == 0 {
			next = rn.lastLogIndexLocked() + 1
			rn.nextIndex[peer] = next
		}

		if next <= rn.lastIncludedIndex {
			req := &proto.InstallSnapshotRequest{
				Term:              term,
				LeaderId:          rn.id,
				LastIncludedIndex: rn.lastIncludedIndex,
				LastIncludedTerm:  rn.lastIncludedTerm,
				Data:              append([]byte(nil), rn.snapshotData...),
				Config:            &proto.ClusterConfig{Members: append([]string(nil), rn.snapshotConfig...)},
			}
			rn.mu.Unlock()

			// Stamp contact with the RPC start time, not response receipt:
			// a successful round-trip only proves the peer acknowledged our
			// leadership at some point after the request was sent. Using
			// receipt time would let ReadIndex barriers count stale
			// acknowledgements that predate the read.
			rpcStart := time.Now()
			ctx, cancel := rn.newRPCTimeoutContext(900 * time.Millisecond)
			resp, err := rn.transport.InstallSnapshot(ctx, peer, req)
			cancel()
			if err != nil || resp == nil {
				return
			}

			rn.mu.Lock()
			if resp.Term > rn.currentTerm {
				rn.becomeFollowerLocked(resp.Term, 0)
				rn.persistHardStateLocked("install-snapshot-response-term-bump", true)
				rn.mu.Unlock()
				return
			}
			if rn.state != Leader || rn.currentTerm != term {
				rn.mu.Unlock()
				return
			}
			if rpcStart.After(rn.peerLastContact[peer]) {
				rn.peerLastContact[peer] = rpcStart
			}
			rn.broadcastProgressLocked()
			if resp.Success {
				rn.matchIndex[peer] = req.LastIncludedIndex
				rn.nextIndex[peer] = req.LastIncludedIndex + 1
			}
			rn.mu.Unlock()
			continue
		}

		prevLogIndex := next - 1
		prevLogTerm, ok := rn.termAtLocked(prevLogIndex)
		if !ok {
			rn.nextIndex[peer] = rn.lastIncludedIndex + 1
			rn.mu.Unlock()
			continue
		}
		entries := rn.entriesFromLocked(next, rn.maxEntriesPerAppend)
		leaderCommit := rn.commitIndex
		rn.mu.Unlock()

		rpcStart := time.Now()
		ctx, cancel := rn.newRPCTimeoutContext(500 * time.Millisecond)
		resp, err := rn.transport.AppendEntries(ctx, peer, &proto.AppendEntriesRequest{
			Term:         term,
			LeaderId:     rn.id,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: leaderCommit,
		})
		cancel()
		if err != nil || resp == nil {
			return
		}

		rn.mu.Lock()
		if resp.Term > rn.currentTerm {
			rn.becomeFollowerLocked(resp.Term, 0)
			rn.persistHardStateLocked("append-entries-response-term-bump", true)
			rn.mu.Unlock()
			return
		}
		if rn.state != Leader || rn.currentTerm != term {
			rn.mu.Unlock()
			return
		}
		if rpcStart.After(rn.peerLastContact[peer]) {
			rn.peerLastContact[peer] = rpcStart
		}
		rn.broadcastProgressLocked()

		if resp.Success {
			replicatedTo := prevLogIndex + uint64(len(entries))
			if replicatedTo > rn.matchIndex[peer] {
				rn.matchIndex[peer] = replicatedTo
			}
			rn.nextIndex[peer] = replicatedTo + 1
			rn.maybeAdvanceCommitLocked()
			done := len(entries) == 0 || rn.nextIndex[peer] > rn.lastLogIndexLocked()
			rn.mu.Unlock()
			if done {
				return
			}
			continue
		}

		if resp.NeedSnapshot {
			rn.nextIndex[peer] = rn.lastIncludedIndex + 1
			rn.mu.Unlock()
			continue
		}

		if resp.ConflictTerm != 0 {
			if idx := rn.findLastIndexOfTermLocked(resp.ConflictTerm); idx > 0 {
				rn.nextIndex[peer] = idx + 1
			} else if resp.ConflictIndex > 0 {
				rn.nextIndex[peer] = resp.ConflictIndex
			} else if rn.nextIndex[peer] > 1 {
				rn.nextIndex[peer]--
			}
		} else if resp.ConflictIndex > 0 {
			rn.nextIndex[peer] = resp.ConflictIndex
		} else if rn.nextIndex[peer] > 1 {
			rn.nextIndex[peer]--
		}
		if rn.nextIndex[peer] < 1 {
			rn.nextIndex[peer] = 1
		}
		rn.mu.Unlock()
	}
}

func (rn *RaftNode) newRPCTimeoutContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	rn.mu.Lock()
	base := rn.rpcCtx
	rn.mu.Unlock()
	if base == nil {
		base = context.Background()
	}
	return context.WithTimeout(base, timeout)
}

func (rn *RaftNode) maybeAdvanceCommitLocked() {
	majority := rn.majorityLocked()
	last := rn.lastLogIndexLocked()
	for n := last; n > rn.commitIndex; n-- {
		term, ok := rn.termAtLocked(n)
		if !ok || term != rn.currentTerm {
			continue
		}

		votes := 1
		for _, peer := range rn.peers {
			if rn.matchIndex[peer] >= n {
				votes++
			}
		}
		if votes >= majority {
			rn.commitIndex = n
			rn.persistHardStateLocked("advance-commit", false)
			rn.broadcastProgressLocked()
			break
		}
	}
}

func (rn *RaftNode) majorityLocked() int {
	return (len(rn.peers)+1)/2 + 1
}

func (rn *RaftNode) HandlePreVote(req *proto.PreVoteRequest) *proto.PreVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &proto.PreVoteResponse{Term: rn.currentTerm, VoteGranted: false}
	if req == nil {
		return resp
	}
	if req.CandidateId == 0 {
		return resp
	}
	if req.Term < rn.currentTerm {
		return resp
	}

	// Reject disruptive pre-votes while a leader is actively heartbeating
	// (check-quorum companion rule). The window is measured from the last
	// genuine leader contact so that a leaderless cluster can always make
	// progress.
	if rn.enableCheckQuorum && rn.state == Leader {
		return resp
	}
	if rn.enableCheckQuorum && rn.leader != 0 && !rn.lastLeaderContact.IsZero() &&
		time.Since(rn.lastLeaderContact) < rn.electionTimeoutMin {
		return resp
	}

	upToDate := rn.isCandidateUpToDateLocked(req.LastLogIndex, req.LastLogTerm)
	if upToDate {
		resp.VoteGranted = true
	}
	return resp
}

func (rn *RaftNode) HandleRequestVote(req *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &proto.RequestVoteResponse{Term: rn.currentTerm, VoteGranted: false}
	if req == nil {
		return resp
	}
	if req.CandidateId == 0 {
		return resp
	}
	if req.Term < rn.currentTerm {
		return resp
	}

	dirty := false
	if req.Term > rn.currentTerm {
		rn.becomeFollowerLocked(req.Term, 0)
		dirty = true
	}

	upToDate := rn.isCandidateUpToDateLocked(req.LastLogIndex, req.LastLogTerm)
	canVote := rn.votedFor == 0 || rn.votedFor == req.CandidateId
	if canVote && upToDate {
		rn.votedFor = req.CandidateId
		rn.resetElectionDeadlineLocked()
		dirty = true
		resp.VoteGranted = true
	}

	if dirty {
		rn.persistHardStateLocked("handle-request-vote", true)
	}
	resp.Term = rn.currentTerm
	return resp
}

func (rn *RaftNode) HandleTimeoutNow(req *proto.TimeoutNowRequest) *proto.TimeoutNowResponse {
	rn.mu.Lock()
	resp := &proto.TimeoutNowResponse{Term: rn.currentTerm, Success: false}
	if req == nil {
		rn.mu.Unlock()
		return resp
	}
	if req.LeaderId == 0 {
		rn.mu.Unlock()
		return resp
	}
	if req.Term < rn.currentTerm {
		rn.mu.Unlock()
		return resp
	}
	if rn.state == Leader && req.LeaderId != rn.id {
		rn.mu.Unlock()
		return resp
	}
	if req.Term == rn.currentTerm && (rn.leader == 0 || rn.leader != req.LeaderId) {
		rn.mu.Unlock()
		return resp
	}

	if req.Term > rn.currentTerm {
		rn.becomeFollowerLocked(req.Term, req.LeaderId)
		rn.persistHardStateLocked("handle-timeout-now-term-bump", true)
	} else {
		rn.state = Follower
		rn.leader = req.LeaderId
		rn.resetElectionDeadlineLocked()
	}
	resp.Term = rn.currentTerm
	resp.Success = true
	rn.mu.Unlock()

	go rn.startElectionInternal(true)
	return resp
}

func (rn *RaftNode) HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &proto.AppendEntriesResponse{Term: rn.currentTerm, Success: false}
	if req == nil || req.LeaderId == 0 {
		return resp
	}
	if len(req.Entries) > maxIncomingAppendEntries {
		// Well-behaved leaders cap batches at 1024 (config guardrail); an
		// oversized batch indicates a bug or abuse, not load.
		return resp
	}
	if req.Term < rn.currentTerm {
		return resp
	}
	if req.Term == rn.currentTerm && rn.leader != 0 && rn.leader != req.LeaderId {
		return resp
	}

	dirty := false
	if req.Term > rn.currentTerm {
		rn.becomeFollowerLocked(req.Term, req.LeaderId)
		dirty = true
	} else {
		if rn.state != Follower {
			rn.state = Follower
		}
		rn.leader = req.LeaderId
		rn.resetElectionDeadlineLocked()
	}
	rn.lastLeaderContact = time.Now()

	if req.PrevLogIndex < rn.lastIncludedIndex {
		resp.Term = rn.currentTerm
		resp.NeedSnapshot = true
		resp.ConflictIndex = rn.lastIncludedIndex + 1
		if dirty {
			rn.persistHardStateLocked("handle-append-entries-term-bump", true)
		}
		return resp
	}

	prevTerm, ok := rn.termAtLocked(req.PrevLogIndex)
	if !ok {
		resp.Term = rn.currentTerm
		resp.ConflictIndex = rn.lastLogIndexLocked() + 1
		if dirty {
			rn.persistHardStateLocked("handle-append-entries-term-bump", true)
		}
		return resp
	}
	if prevTerm != req.PrevLogTerm {
		resp.Term = rn.currentTerm
		resp.ConflictTerm = prevTerm
		resp.ConflictIndex = rn.firstIndexOfTermLocked(prevTerm)
		if resp.ConflictIndex == 0 {
			resp.ConflictIndex = rn.firstLogIndexLocked()
		}
		if dirty {
			rn.persistHardStateLocked("handle-append-entries-term-bump", true)
		}
		return resp
	}

	insertAt := req.PrevLogIndex + 1
	appendedFrom := uint64(0)
	for i, incoming := range req.Entries {
		idx := insertAt + uint64(i)
		if idx <= rn.lastLogIndexLocked() {
			existingTerm, _ := rn.termAtLocked(idx)
			if existingTerm != incoming.Term {
				if idx <= rn.commitIndex {
					resp.Term = rn.currentTerm
					resp.ConflictTerm = existingTerm
					resp.ConflictIndex = rn.firstIndexOfTermLocked(existingTerm)
					if resp.ConflictIndex == 0 {
						resp.ConflictIndex = idx
					}
					if dirty {
						rn.persistHardStateLocked("handle-append-entries-term-bump", true)
					}
					return resp
				}
				rn.truncateFromLocked(idx)
				for j := i; j < len(req.Entries); j++ {
					e := cloneEntry(req.Entries[j])
					e.Index = insertAt + uint64(j)
					rn.log = append(rn.log, e)
				}
				appendedFrom = idx
				break
			}
			continue
		}
		e := cloneEntry(incoming)
		e.Index = idx
		rn.log = append(rn.log, e)
		if appendedFrom == 0 {
			appendedFrom = idx
		}
	}

	if appendedFrom > 0 {
		// The appended batch may contain (or may have truncated away)
		// configuration entries; re-derive the active membership.
		rn.recomputeConfigLocked()
	}

	commitAdvanced := false
	if req.LeaderCommit > rn.commitIndex {
		rn.commitIndex = minUint64(req.LeaderCommit, rn.lastLogIndexLocked())
		commitAdvanced = true
		rn.broadcastProgressLocked()
	}

	if appendedFrom > 0 {
		// One synced WAL batch covers the (implicit) truncation, the new
		// entries, and the hard state including any term bump and commit.
		rn.persistAppendLocked("handle-append-entries", rn.entriesFromLocked(appendedFrom, 0))
	} else if dirty {
		rn.persistHardStateLocked("handle-append-entries-term-bump", true)
	} else if commitAdvanced {
		rn.persistHardStateLocked("handle-append-entries-commit", false)
	}

	resp.Term = rn.currentTerm
	resp.Success = true
	return resp
}

func (rn *RaftNode) HandleInstallSnapshot(req *proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &proto.InstallSnapshotResponse{Term: rn.currentTerm, Success: false}
	if req == nil || req.LeaderId == 0 {
		return resp
	}
	if req.Term < rn.currentTerm {
		return resp
	}
	if req.Term == rn.currentTerm && rn.leader != 0 && rn.leader != req.LeaderId {
		return resp
	}
	if req.LastIncludedIndex > 0 && req.LastIncludedTerm == 0 {
		return resp
	}

	termBumped := false
	if req.Term > rn.currentTerm {
		rn.becomeFollowerLocked(req.Term, req.LeaderId)
		termBumped = true
	} else {
		rn.state = Follower
		rn.leader = req.LeaderId
		rn.resetElectionDeadlineLocked()
	}
	rn.lastLeaderContact = time.Now()

	if termBumped {
		rn.persistHardStateLocked("handle-install-snapshot-term-bump", true)
	}
	resp.Term = rn.currentTerm

	if req.LastIncludedIndex <= rn.lastIncludedIndex || req.LastIncludedIndex <= rn.lastApplied {
		resp.Success = true
		return resp
	}

	sm := rn.snapshotter
	data := append([]byte(nil), req.Data...)
	if sm != nil && len(data) > 0 {
		// Restore runs under rn.mu deliberately: it must be atomic with the
		// metadata updates below, or a concurrent install/append could leave
		// the state machine and the Raft indexes describing different
		// states. The cost is blocking this node's Raft progress for the
		// duration of one in-memory restore.
		if err := sm.Restore(data); err != nil {
			log.Printf("[Node %d] install-snapshot restore failed: %v", rn.id, err)
			return &proto.InstallSnapshotResponse{Term: rn.currentTerm, Success: false}
		}
	}

	oldFirst := rn.firstLogIndexLocked()
	oldLast := rn.lastLogIndexLocked()

	rn.lastIncludedIndex = req.LastIncludedIndex
	rn.lastIncludedTerm = req.LastIncludedTerm
	rn.snapshotData = data
	if req.Config != nil {
		rn.snapshotConfig = append([]string(nil), req.Config.Members...)
	}

	if req.LastIncludedIndex >= oldLast {
		rn.log = rn.log[:0]
	} else {
		keepFrom := req.LastIncludedIndex + 1
		if keepFrom >= oldFirst {
			offset := int(keepFrom - oldFirst)
			rn.log = append([]*proto.LogEntry(nil), rn.log[offset:]...)
		}
	}

	for i, e := range rn.log {
		e.Index = rn.lastIncludedIndex + 1 + uint64(i)
	}
	rn.recomputeConfigLocked()

	rn.commitIndex = maxUint64(rn.commitIndex, rn.lastIncludedIndex)
	rn.lastApplied = maxUint64(rn.lastApplied, rn.lastIncludedIndex)
	rn.lastDispatched = maxUint64(rn.lastDispatched, rn.lastIncludedIndex)
	rn.broadcastProgressLocked()

	rn.persistCheckpointLocked("handle-install-snapshot")
	rn.ctrSnapshotsInstalled.Add(1)
	resp.Term = rn.currentTerm
	resp.Success = true
	return resp
}

func (rn *RaftNode) maybeCreateSnapshot(appliedIndex uint64) {
	rn.mu.Lock()
	sm := rn.snapshotter
	if sm == nil || appliedIndex <= rn.lastIncludedIndex {
		rn.mu.Unlock()
		return
	}
	term, ok := rn.termAtLocked(appliedIndex)
	if !ok {
		rn.mu.Unlock()
		return
	}
	rn.mu.Unlock()

	data, err := sm.Snapshot()
	if err != nil {
		log.Printf("[Node %d] snapshot capture failed: %v", rn.id, err)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()
	if appliedIndex <= rn.lastIncludedIndex {
		return
	}
	term, ok = rn.termAtLocked(appliedIndex)
	if !ok {
		return
	}

	rn.snapshotData = append([]byte(nil), data...)
	// Capture the membership as of the snapshot point before the config
	// entries that defined it are compacted away.
	rn.snapshotConfig = rn.configAtLocked(appliedIndex)
	rn.compactLogLocked(appliedIndex)
	rn.lastIncludedIndex = appliedIndex
	rn.lastIncludedTerm = term
	if rn.commitIndex < rn.lastIncludedIndex {
		rn.commitIndex = rn.lastIncludedIndex
	}
	if rn.lastApplied < rn.lastIncludedIndex {
		rn.lastApplied = rn.lastIncludedIndex
	}
	if rn.lastDispatched < rn.lastIncludedIndex {
		rn.lastDispatched = rn.lastIncludedIndex
	}
	for _, peer := range rn.peers {
		if rn.nextIndex[peer] < rn.lastIncludedIndex+1 {
			rn.nextIndex[peer] = rn.lastIncludedIndex + 1
		}
	}
	rn.persistCheckpointLocked("create-snapshot")
	rn.ctrSnapshotsCreated.Add(1)
}

func (rn *RaftNode) firstLogIndexLocked() uint64 {
	return rn.lastIncludedIndex + 1
}

func (rn *RaftNode) lastLogIndexLocked() uint64 {
	if len(rn.log) == 0 {
		return rn.lastIncludedIndex
	}
	return rn.log[len(rn.log)-1].Index
}

func (rn *RaftNode) lastLogTermLocked() uint64 {
	if len(rn.log) == 0 {
		return rn.lastIncludedTerm
	}
	return rn.log[len(rn.log)-1].Term
}

func (rn *RaftNode) termAtLocked(index uint64) (uint64, bool) {
	if index == rn.lastIncludedIndex {
		return rn.lastIncludedTerm, true
	}
	first := rn.firstLogIndexLocked()
	last := rn.lastLogIndexLocked()
	if index < first || index > last {
		return 0, false
	}
	offset := int(index - first)
	return rn.log[offset].Term, true
}

func (rn *RaftNode) entryAtLocked(index uint64) (*proto.LogEntry, bool) {
	first := rn.firstLogIndexLocked()
	last := rn.lastLogIndexLocked()
	if index < first || index > last {
		return nil, false
	}
	return rn.log[int(index-first)], true
}

func (rn *RaftNode) entriesFromLocked(start uint64, max int) []*proto.LogEntry {
	last := rn.lastLogIndexLocked()
	if start > last {
		return nil
	}
	first := rn.firstLogIndexLocked()
	offset := int(start - first)
	end := len(rn.log)
	if max > 0 && offset+max < end {
		end = offset + max
	}
	out := make([]*proto.LogEntry, 0, end-offset)
	for i := offset; i < end; i++ {
		out = append(out, cloneEntry(rn.log[i]))
	}
	return out
}

func (rn *RaftNode) truncateFromLocked(index uint64) {
	first := rn.firstLogIndexLocked()
	if index <= first {
		rn.log = rn.log[:0]
		return
	}
	last := rn.lastLogIndexLocked()
	if index > last {
		return
	}
	offset := int(index - first)
	rn.log = append([]*proto.LogEntry(nil), rn.log[:offset]...)
}

func (rn *RaftNode) compactLogLocked(appliedIndex uint64) {
	first := rn.firstLogIndexLocked()
	if appliedIndex < first {
		return
	}
	last := rn.lastLogIndexLocked()
	if appliedIndex >= last {
		rn.log = rn.log[:0]
		return
	}
	offset := int(appliedIndex - first + 1)
	rn.log = append([]*proto.LogEntry(nil), rn.log[offset:]...)
}

func (rn *RaftNode) isCandidateUpToDateLocked(candidateLastIndex, candidateLastTerm uint64) bool {
	localTerm := rn.lastLogTermLocked()
	localIndex := rn.lastLogIndexLocked()
	if candidateLastTerm != localTerm {
		return candidateLastTerm > localTerm
	}
	return candidateLastIndex >= localIndex
}

func (rn *RaftNode) firstIndexOfTermLocked(term uint64) uint64 {
	if term == rn.lastIncludedTerm {
		return rn.lastIncludedIndex
	}
	for _, e := range rn.log {
		if e.Term == term {
			return e.Index
		}
	}
	return 0
}

func (rn *RaftNode) findLastIndexOfTermLocked(term uint64) uint64 {
	if term == rn.lastIncludedTerm {
		return rn.lastIncludedIndex
	}
	for i := len(rn.log) - 1; i >= 0; i-- {
		if rn.log[i].Term == term {
			return rn.log[i].Index
		}
	}
	return 0
}

// persistHardStateLocked durably records term/vote/commit. Pass sync=false
// only for commit-index-only changes (a recovery hint, not a safety
// requirement; it becomes durable with the next synced record).
func (rn *RaftNode) persistHardStateLocked(op string, sync bool) {
	if rn.wal == nil {
		return
	}
	if err := rn.wal.saveHardState(rn.currentTerm, rn.votedFor, rn.commitIndex, sync); err != nil {
		panic(fmt.Sprintf("[Node %d] persist failed during %s: %v", rn.id, op, err))
	}
}

// persistAppendLocked durably records appended entries together with the
// current hard state in a single fsync. The first entry's index implicitly
// truncates any conflicting suffix on recovery.
func (rn *RaftNode) persistAppendLocked(op string, entries []*proto.LogEntry) {
	if rn.wal == nil || len(entries) == 0 {
		return
	}
	if err := rn.wal.appendEntries(entries, rn.currentTerm, rn.votedFor, rn.commitIndex); err != nil {
		panic(fmt.Sprintf("[Node %d] persist failed during %s: %v", rn.id, op, err))
	}
}

// persistCheckpointLocked writes the snapshot file and rewrites the WAL as a
// single fresh segment containing the full current state.
func (rn *RaftNode) persistCheckpointLocked(op string) {
	if rn.wal == nil {
		return
	}
	if err := rn.wal.checkpoint(rn.currentTerm, rn.votedFor, rn.commitIndex,
		rn.lastIncludedIndex, rn.lastIncludedTerm, rn.snapshotData, rn.snapshotConfig, rn.log); err != nil {
		panic(fmt.Sprintf("[Node %d] persist failed during %s: %v", rn.id, op, err))
	}
}

func cloneEntry(e *proto.LogEntry) *proto.LogEntry {
	if e == nil {
		return nil
	}
	return &proto.LogEntry{
		Term:    e.Term,
		Command: e.Command,
		Index:   e.Index,
		Payload: append([]byte(nil), e.Payload...),
		Type:    e.Type,
	}
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func maxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func parseBoolEnv(raw string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(raw))
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func (rn *RaftNode) Close() {
	rn.stopOnce.Do(func() {
		close(rn.stopped)
	})

	rn.mu.Lock()
	rn.state = Follower
	rn.leader = 0
	if rn.rpcCancel != nil {
		rn.rpcCancel()
		rn.rpcCancel = nil
	}
	rn.mu.Unlock()

	rn.loopWG.Wait()
	rn.replicateWG.Wait()

	rn.transport.Close()

	rn.mu.Lock()
	if rn.wal != nil {
		rn.wal.close()
		rn.wal = nil
	}
	rn.mu.Unlock()
}

func (rn *RaftNode) CurrentState() State {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state
}

func (rn *RaftNode) IsLeader() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.state == Leader
}

func (rn *RaftNode) LeaderID() uint32 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.leader
}
