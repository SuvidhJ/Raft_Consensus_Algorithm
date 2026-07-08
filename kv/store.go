package kv

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
	gproto "google.golang.org/protobuf/proto"
)

const (
	setLegacyPrefix = "SET "
	setB64Prefix    = "SETB64 "

	// maxSessions bounds the dedup table. Eviction is deterministic (lowest
	// last-touched log index first) so replicas stay identical.
	maxSessions = 4096

	// resultWindow bounds how many per-index apply results are retained for
	// proposer result correlation.
	resultWindow = 4096
)

// applyResult captures the outcome of applying one command, used both for
// proposer responses and for replay-safe duplicate answers.
type applyResult struct {
	Existed bool   `json:"existed,omitempty"`
	Swapped bool   `json:"swapped,omitempty"`
	Current string `json:"current,omitempty"`
}

// sessionState is the per-client exactly-once bookkeeping. It is part of the
// replicated state machine and therefore included in snapshots.
type sessionState struct {
	LastSeq uint64      `json:"last_seq"`
	Result  applyResult `json:"result"`
	Touched uint64      `json:"touched"` // log index of last activity, for deterministic eviction
}

type snapshotEnvelope struct {
	Version  int                      `json:"version"`
	Data     map[string]string        `json:"data"`
	Sessions map[uint64]*sessionState `json:"sessions"`
}

// KVStore is the replicated key-value state machine.
type KVStore struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[uint64]*sessionState

	// results correlates apply outcomes with log indexes so the proposing
	// RPC handler can report per-command results (CAS swap status etc.).
	results   map[uint64]applyResult
	resultMin uint64

	raft *raft.RaftNode
	proto.UnimplementedKVServiceServer

	// Op counters for observability (atomic; read without mu).
	ctrSets     atomic.Uint64
	ctrDeletes  atomic.Uint64
	ctrCas      atomic.Uint64
	ctrGets     atomic.Uint64
	ctrLinReads atomic.Uint64
	ctrRejected atomic.Uint64
}

// Stats is a point-in-time view of KV activity for the metrics endpoint.
type Stats struct {
	Keys     int
	Sets     uint64
	Deletes  uint64
	Cas      uint64
	Gets     uint64
	LinReads uint64
	Rejected uint64
}

func (s *KVStore) Stats() Stats {
	return Stats{
		Keys:     s.Keys(),
		Sets:     s.ctrSets.Load(),
		Deletes:  s.ctrDeletes.Load(),
		Cas:      s.ctrCas.Load(),
		Gets:     s.ctrGets.Load(),
		LinReads: s.ctrLinReads.Load(),
		Rejected: s.ctrRejected.Load(),
	}
}

func NewKVStore(r *raft.RaftNode) *KVStore {
	if r != nil {
		r.EnableApplyAcks()
	}
	return &KVStore{
		data:     make(map[string]string),
		sessions: make(map[uint64]*sessionState),
		results:  make(map[uint64]applyResult),
		raft:     r,
	}
}

// ApplyCommand executes committed log entries in deterministic order.
func (s *KVStore) ApplyCommand(entry *proto.LogEntry) {
	if entry == nil {
		return
	}
	if s.raft != nil && !s.raft.ShouldApplyEntry(entry) {
		return
	}
	defer func() {
		if s.raft != nil {
			s.raft.NotifyApplied(entry.Index)
		}
	}()

	cmd, ok := decodeCommand(entry)
	if !ok {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var result applyResult
	if cmd.ClientId != 0 {
		sess := s.sessions[cmd.ClientId]
		if sess != nil && cmd.Sequence <= sess.LastSeq {
			// Duplicate delivery of a retried command: do not re-apply, but
			// surface the original outcome to the caller.
			s.recordResultLocked(entry.Index, sess.Result)
			return
		}
	}

	switch cmd.Type {
	case proto.Command_TYPE_SET:
		s.data[cmd.Key] = cmd.Value
		log.Printf("[KV] applied SET idx=%d key=%q", entry.Index, cmd.Key)

	case proto.Command_TYPE_DELETE:
		_, existed := s.data[cmd.Key]
		delete(s.data, cmd.Key)
		result.Existed = existed
		log.Printf("[KV] applied DELETE idx=%d key=%q existed=%v", entry.Index, cmd.Key, existed)

	case proto.Command_TYPE_CAS:
		current, exists := s.data[cmd.Key]
		match := false
		if cmd.ExpectAbsent {
			match = !exists
		} else {
			match = exists && current == cmd.ExpectedValue
		}
		if match {
			s.data[cmd.Key] = cmd.Value
			result.Swapped = true
		} else {
			result.Current = current
		}
		log.Printf("[KV] applied CAS idx=%d key=%q swapped=%v", entry.Index, cmd.Key, result.Swapped)

	default:
		log.Printf("[KV] ignored unknown command type=%v idx=%d", cmd.Type, entry.Index)
		return
	}

	if cmd.ClientId != 0 {
		s.touchSessionLocked(cmd.ClientId, cmd.Sequence, result, entry.Index)
	}
	s.recordResultLocked(entry.Index, result)
}

func (s *KVStore) recordResultLocked(index uint64, r applyResult) {
	s.results[index] = r
	if s.resultMin == 0 || index < s.resultMin {
		s.resultMin = index
	}
	for len(s.results) > resultWindow {
		delete(s.results, s.resultMin)
		s.resultMin++
	}
}

func (s *KVStore) touchSessionLocked(clientID, seq uint64, r applyResult, index uint64) {
	sess := s.sessions[clientID]
	if sess == nil {
		if len(s.sessions) >= maxSessions {
			s.evictOldestSessionLocked()
		}
		sess = &sessionState{}
		s.sessions[clientID] = sess
	}
	sess.LastSeq = seq
	sess.Result = r
	sess.Touched = index
}

func (s *KVStore) evictOldestSessionLocked() {
	var victim uint64
	var oldest uint64 = ^uint64(0)
	for id, sess := range s.sessions {
		if sess.Touched < oldest || (sess.Touched == oldest && id < victim) {
			oldest = sess.Touched
			victim = id
		}
	}
	if victim != 0 {
		delete(s.sessions, victim)
	}
}

func (s *KVStore) resultAt(index uint64) (applyResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.results[index]
	return r, ok
}

// propose marshals and replicates a command. committed reports whether the
// command was committed and applied; resultOK whether its apply result is
// still available for callers that need per-command outcomes (CAS/Delete).
func (s *KVStore) propose(ctx context.Context, cmd *proto.Command) (result applyResult, resultOK, committed bool) {
	payload, err := gproto.Marshal(cmd)
	if err != nil {
		return applyResult{}, false, false
	}
	index, ok := s.raft.ProposePayload(ctx, payload)
	if !ok {
		return applyResult{}, false, false
	}
	r, found := s.resultAt(index)
	return r, found, true
}

// Set handles gRPC Set requests. Requires leader status.
func (s *KVStore) Set(ctx context.Context, req *proto.SetRequest) (*proto.SetResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req == nil {
		return &proto.SetResponse{Success: false, ErrorMessage: "invalid request"}, nil
	}
	if s.raft == nil {
		return &proto.SetResponse{Success: false, ErrorMessage: "raft unavailable"}, nil
	}

	s.ctrSets.Add(1)
	_, _, committed := s.propose(ctx, &proto.Command{
		Type:     proto.Command_TYPE_SET,
		Key:      req.Key,
		Value:    req.Value,
		ClientId: req.ClientId,
		Sequence: req.Sequence,
	})
	if !committed {
		s.ctrRejected.Add(1)
		return &proto.SetResponse{Success: false, ErrorMessage: "Not leader or quorum unavailable"}, nil
	}
	return &proto.SetResponse{Success: true}, nil
}

// Delete removes a key through the replicated log.
func (s *KVStore) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req == nil {
		return &proto.DeleteResponse{Success: false, ErrorMessage: "invalid request"}, nil
	}
	if s.raft == nil {
		return &proto.DeleteResponse{Success: false, ErrorMessage: "raft unavailable"}, nil
	}

	s.ctrDeletes.Add(1)
	result, resultOK, committed := s.propose(ctx, &proto.Command{
		Type:     proto.Command_TYPE_DELETE,
		Key:      req.Key,
		ClientId: req.ClientId,
		Sequence: req.Sequence,
	})
	if !committed {
		s.ctrRejected.Add(1)
		return &proto.DeleteResponse{Success: false, ErrorMessage: "Not leader or quorum unavailable"}, nil
	}
	if !resultOK {
		// The delete committed, but its result window entry is gone; never
		// fabricate an answer for the Existed field.
		return &proto.DeleteResponse{Success: false, ErrorMessage: "committed but result unavailable; retry with a client session"}, nil
	}
	return &proto.DeleteResponse{Success: true, Existed: result.Existed}, nil
}

// CompareAndSwap atomically replaces a value when the expectation holds.
func (s *KVStore) CompareAndSwap(ctx context.Context, req *proto.CasRequest) (*proto.CasResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req == nil {
		return &proto.CasResponse{Success: false, ErrorMessage: "invalid request"}, nil
	}
	if s.raft == nil {
		return &proto.CasResponse{Success: false, ErrorMessage: "raft unavailable"}, nil
	}

	s.ctrCas.Add(1)
	result, resultOK, committed := s.propose(ctx, &proto.Command{
		Type:          proto.Command_TYPE_CAS,
		Key:           req.Key,
		Value:         req.NewValue,
		ExpectedValue: req.ExpectedValue,
		ExpectAbsent:  req.ExpectAbsent,
		ClientId:      req.ClientId,
		Sequence:      req.Sequence,
	})
	if !committed {
		s.ctrRejected.Add(1)
		return &proto.CasResponse{Success: false, ErrorMessage: "Not leader or quorum unavailable"}, nil
	}
	if !resultOK {
		// Reporting an unknown swap outcome as false would be a lie with
		// real consequences for callers using CAS for locking.
		return &proto.CasResponse{Success: false, ErrorMessage: "committed but result unavailable; retry with a client session"}, nil
	}
	return &proto.CasResponse{Success: true, Swapped: result.Swapped, CurrentValue: result.Current}, nil
}

// Get handles local (possibly stale) reads.
func (s *KVStore) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	if req == nil {
		return &proto.GetResponse{Success: false, ErrorMessage: "invalid request"}, nil
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}
	s.ctrGets.Add(1)
	return s.getLocal(req.Key), nil
}

// GetLinearizable serves reads through a Raft quorum barrier.
func (s *KVStore) GetLinearizable(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req == nil {
		return &proto.GetResponse{Success: false, ErrorMessage: "invalid request"}, nil
	}
	if s.raft == nil {
		return &proto.GetResponse{Success: false, ErrorMessage: "raft unavailable"}, nil
	}
	s.ctrLinReads.Add(1)
	if !s.raft.ReadBarrierWithContext(ctx) {
		s.ctrRejected.Add(1)
		return &proto.GetResponse{Success: false, ErrorMessage: "Not leader or quorum unavailable"}, nil
	}
	return s.getLocal(req.Key), nil
}

func (s *KVStore) getLocal(key string) *proto.GetResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, ok := s.data[key]
	if !ok {
		return &proto.GetResponse{Success: false, ErrorMessage: "Key not found"}
	}
	return &proto.GetResponse{Success: true, Value: val}
}

// Keys returns the number of stored keys (for metrics).
func (s *KVStore) Keys() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

func (s *KVStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(snapshotEnvelope{
		Version:  2,
		Data:     s.data,
		Sessions: s.sessions,
	})
}

func (s *KVStore) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(data) == 0 {
		s.data = make(map[string]string)
		s.sessions = make(map[uint64]*sessionState)
		return nil
	}

	var env snapshotEnvelope
	if err := json.Unmarshal(data, &env); err == nil && env.Version == 2 {
		if env.Data == nil {
			env.Data = make(map[string]string)
		}
		if env.Sessions == nil {
			env.Sessions = make(map[uint64]*sessionState)
		}
		s.data = env.Data
		s.sessions = env.Sessions
		return nil
	}

	// Legacy snapshot: a flat string map without session state.
	restored := make(map[string]string)
	if err := json.Unmarshal(data, &restored); err != nil {
		return err
	}
	s.data = restored
	s.sessions = make(map[uint64]*sessionState)
	return nil
}

// decodeCommand parses a log entry into a typed command, accepting both the
// protobuf payload format and the legacy string encodings.
func decodeCommand(entry *proto.LogEntry) (*proto.Command, bool) {
	if entry.Type != proto.EntryType_ENTRY_NORMAL {
		return nil, false
	}
	if len(entry.Payload) > 0 {
		cmd := &proto.Command{}
		if err := gproto.Unmarshal(entry.Payload, cmd); err != nil {
			log.Printf("[KV] undecodable payload idx=%d: %v", entry.Index, err)
			return nil, false
		}
		return cmd, true
	}

	if entry.Command == "" || entry.Command == "INIT" {
		return nil, false
	}
	if key, value, ok := decodeLegacySet(entry.Command); ok {
		return &proto.Command{Type: proto.Command_TYPE_SET, Key: key, Value: value}, true
	}
	log.Printf("[KV] ignored unknown command=%q", entry.Command)
	return nil, false
}

func decodeLegacySet(command string) (string, string, bool) {
	if strings.HasPrefix(command, setB64Prefix) {
		parts := strings.SplitN(strings.TrimPrefix(command, setB64Prefix), " ", 2)
		if len(parts) != 2 {
			return "", "", false
		}
		keyBytes, err := base64.StdEncoding.DecodeString(parts[0])
		if err != nil {
			return "", "", false
		}
		valBytes, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			return "", "", false
		}
		return string(keyBytes), string(valBytes), true
	}

	if strings.HasPrefix(command, setLegacyPrefix) {
		parts := strings.SplitN(command, " ", 3)
		if len(parts) == 3 {
			return parts[1], parts[2], true
		}
	}

	return "", "", false
}
