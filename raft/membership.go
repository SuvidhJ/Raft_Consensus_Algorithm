package raft

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	gproto "google.golang.org/protobuf/proto"
)

// Dynamic membership via single-server configuration changes (Raft thesis
// §4.1). Each ENTRY_CONFIG log entry carries the complete member list
// (advertised addresses). Configurations take effect as soon as the entry is
// appended — not committed — and are reverted by recomputation if the entry
// is truncated. Because members change one at a time, any majority of the
// new configuration overlaps any majority of the old one, so two disjoint
// quorums can never form.

var (
	ErrNotLeader        = errors.New("not the leader")
	ErrNoAdvertiseAddr  = errors.New("membership changes require an advertise address")
	ErrConfigInFlight   = errors.New("another configuration change is in progress")
	ErrCatchUpFailed    = errors.New("new server failed to catch up")
	ErrCommitTimeout    = errors.New("configuration change did not commit in time")
	ErrRemoveSelfLeader = errors.New("cannot remove the current leader; transfer leadership first")
)

// activeConfigLocked returns the membership in effect: the latest config
// entry in the log, else the snapshot's config, else the bootstrap set.
func (rn *RaftNode) activeConfigLocked() []string {
	return rn.configAtLocked(^uint64(0))
}

// configAtLocked returns the membership as of maxIndex.
func (rn *RaftNode) configAtLocked(maxIndex uint64) []string {
	for i := len(rn.log) - 1; i >= 0; i-- {
		e := rn.log[i]
		if e.Index > maxIndex {
			continue
		}
		if e.Type != proto.EntryType_ENTRY_CONFIG {
			continue
		}
		var cc proto.ClusterConfig
		if err := gproto.Unmarshal(e.Payload, &cc); err != nil {
			log.Printf("[Node %d] undecodable config entry idx=%d: %v", rn.id, e.Index, err)
			continue
		}
		return append([]string(nil), cc.Members...)
	}
	if rn.snapshotConfig != nil {
		return append([]string(nil), rn.snapshotConfig...)
	}
	return append([]string(nil), rn.bootstrapMembers...)
}

// recomputeConfigLocked re-derives the peer set from the active
// configuration. Called whenever the log gains/loses config entries or a
// snapshot replaces the log prefix.
func (rn *RaftNode) recomputeConfigLocked() {
	members := rn.activeConfigLocked()

	peers := make([]string, 0, len(members))
	for _, m := range members {
		if m != "" && m != rn.advertiseAddr {
			peers = append(peers, m)
		}
	}
	rn.peers = peers

	known := make(map[string]struct{}, len(peers))
	for _, p := range peers {
		known[p] = struct{}{}
	}
	for addr := range rn.nextIndex {
		if _, ok := known[addr]; !ok {
			delete(rn.nextIndex, addr)
			delete(rn.matchIndex, addr)
			delete(rn.peerLastContact, addr)
		}
	}
	for addr, inFlight := range rn.replicating {
		// Only reap idle entries: an in-flight replicator owns its flag and
		// clears it on exit; deleting it here could let a second replicator
		// start concurrently for the same address.
		if _, ok := known[addr]; !ok && !inFlight {
			delete(rn.replicating, addr)
		}
	}

	if rn.joining && rn.advertiseAddr != "" && containsAddr(members, rn.advertiseAddr) {
		rn.joining = false
		log.Printf("[Node %d] joined cluster: %v", rn.id, members)
	}
}

func (rn *RaftNode) hasUncommittedConfigLocked() bool {
	for i := len(rn.log) - 1; i >= 0; i-- {
		e := rn.log[i]
		if e.Index <= rn.commitIndex {
			return false
		}
		if e.Type == proto.EntryType_ENTRY_CONFIG {
			return true
		}
	}
	return false
}

func containsAddr(list []string, addr string) bool {
	for _, a := range list {
		if a == addr {
			return true
		}
	}
	return false
}

// AddServer brings a new member into the cluster. The new server must
// already be running (in joining mode). It is first caught up via normal
// replication so the configuration change commits quickly, then the new
// membership is appended and replicated.
func (rn *RaftNode) AddServer(ctx context.Context, addr string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validatePeerAddress(addr); err != nil {
		return err
	}

	rn.mu.Lock()
	if rn.advertiseAddr == "" {
		rn.mu.Unlock()
		return ErrNoAdvertiseAddr
	}
	if rn.state != Leader {
		rn.mu.Unlock()
		return ErrNotLeader
	}
	if addr == rn.advertiseAddr {
		rn.mu.Unlock()
		return nil // already a member (ourselves)
	}
	if rn.hasUncommittedConfigLocked() {
		rn.mu.Unlock()
		return ErrConfigInFlight
	}
	members := rn.activeConfigLocked()
	if containsAddr(members, addr) {
		rn.mu.Unlock()
		return nil // idempotent
	}
	term := rn.currentTerm
	rn.mu.Unlock()

	// Catch-up phase: replicate to the joining server before changing the
	// configuration, so the cluster is never stalled waiting for a cold
	// follower inside its own quorum.
	deadline := time.Now().Add(rn.proposalTimeout)
	caughtUp := false
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rn.stopped:
			return ErrNotLeader
		default:
		}

		rn.replicatePeerNow(addr, true)

		rn.mu.Lock()
		if rn.state != Leader || rn.currentTerm != term {
			rn.mu.Unlock()
			return ErrNotLeader
		}
		caughtUp = rn.matchIndex[addr] >= rn.lastLogIndexLocked()
		rn.mu.Unlock()
		if caughtUp {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !caughtUp {
		return ErrCatchUpFailed
	}

	rn.mu.Lock()
	if rn.state != Leader || rn.currentTerm != term {
		rn.mu.Unlock()
		return ErrNotLeader
	}
	if rn.hasUncommittedConfigLocked() {
		rn.mu.Unlock()
		return ErrConfigInFlight
	}
	members = rn.activeConfigLocked()
	if containsAddr(members, addr) {
		rn.mu.Unlock()
		return nil
	}
	newMembers := append(members, addr)
	index, err := rn.appendConfigEntryLocked(newMembers)
	if err != nil {
		rn.mu.Unlock()
		return err
	}
	rn.mu.Unlock()

	log.Printf("[Node %d] proposing membership add %s -> %v", rn.id, addr, newMembers)
	rn.notifyReplication()
	if !rn.waitForCommitWithContext(ctx, index, term) {
		return ErrCommitTimeout
	}
	return nil
}

// RemoveServer takes a member out of the cluster. Removing the current
// leader is rejected; transfer leadership first.
func (rn *RaftNode) RemoveServer(ctx context.Context, addr string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validatePeerAddress(addr); err != nil {
		return err
	}

	rn.mu.Lock()
	if rn.advertiseAddr == "" {
		rn.mu.Unlock()
		return ErrNoAdvertiseAddr
	}
	if rn.state != Leader {
		rn.mu.Unlock()
		return ErrNotLeader
	}
	if addr == rn.advertiseAddr {
		rn.mu.Unlock()
		return ErrRemoveSelfLeader
	}
	if rn.hasUncommittedConfigLocked() {
		rn.mu.Unlock()
		return ErrConfigInFlight
	}
	members := rn.activeConfigLocked()
	if !containsAddr(members, addr) {
		rn.mu.Unlock()
		return nil // idempotent
	}
	newMembers := make([]string, 0, len(members)-1)
	for _, m := range members {
		if m != addr {
			newMembers = append(newMembers, m)
		}
	}
	term := rn.currentTerm
	index, err := rn.appendConfigEntryLocked(newMembers)
	if err != nil {
		rn.mu.Unlock()
		return err
	}
	rn.mu.Unlock()

	log.Printf("[Node %d] proposing membership remove %s -> %v", rn.id, addr, newMembers)
	rn.notifyReplication()
	if !rn.waitForCommitWithContext(ctx, index, term) {
		return ErrCommitTimeout
	}
	return nil
}

// appendConfigEntryLocked appends an ENTRY_CONFIG entry carrying the full
// new membership and applies it immediately (configurations take effect on
// append, not on commit).
func (rn *RaftNode) appendConfigEntryLocked(members []string) (uint64, error) {
	payload, err := gproto.Marshal(&proto.ClusterConfig{Members: members})
	if err != nil {
		return 0, fmt.Errorf("marshal config: %w", err)
	}
	index := rn.lastLogIndexLocked() + 1
	entry := &proto.LogEntry{
		Term:    rn.currentTerm,
		Index:   index,
		Type:    proto.EntryType_ENTRY_CONFIG,
		Payload: payload,
	}
	rn.log = append(rn.log, entry)
	rn.persistAppendLocked("config-append", []*proto.LogEntry{entry})
	rn.recomputeConfigLocked()
	rn.maybeAdvanceCommitLocked()
	return index, nil
}

// Status is a point-in-time view of the node, used by the admin RPC and the
// metrics endpoint.
type Status struct {
	ID            uint32
	State         State
	Term          uint64
	LeaderID      uint32
	CommitIndex   uint64
	AppliedIndex  uint64
	LastLogIndex  uint64
	LastLogTerm   uint64
	SnapshotIndex uint64
	Members       []string
	Peers         []string

	ElectionsStarted   uint64
	BecameLeader       uint64
	SnapshotsCreated   uint64
	SnapshotsInstalled uint64
}

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

func (rn *RaftNode) Status() Status {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	members := rn.activeConfigLocked()
	if members == nil {
		members = []string{}
	}
	return Status{
		ID:            rn.id,
		State:         rn.state,
		Term:          rn.currentTerm,
		LeaderID:      rn.leader,
		CommitIndex:   rn.commitIndex,
		AppliedIndex:  rn.lastApplied,
		LastLogIndex:  rn.lastLogIndexLocked(),
		LastLogTerm:   rn.lastLogTermLocked(),
		SnapshotIndex: rn.lastIncludedIndex,
		Members:       members,
		Peers:         append([]string(nil), rn.peers...),

		ElectionsStarted:   rn.ctrElectionsStarted.Load(),
		BecameLeader:       rn.ctrBecameLeader.Load(),
		SnapshotsCreated:   rn.ctrSnapshotsCreated.Load(),
		SnapshotsInstalled: rn.ctrSnapshotsInstalled.Load(),
	}
}
