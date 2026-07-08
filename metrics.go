package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/kv"
	"github.com/SuvidhJ/Raft_Consensus_Algorithm/raft"
)

// startObservabilityServer exposes Prometheus-format metrics and a JSON
// status endpoint with zero external dependencies:
//
//	GET /metrics  Prometheus text exposition format
//	GET /status   JSON snapshot of Raft + KV state
//	GET /healthz  liveness probe
//
// Returns a shutdown function.
func startObservabilityServer(addr string, node *raft.RaftNode, store *kv.KVStore) (func(), error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		st := node.Status()
		stats := store.Stats()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id":             st.ID,
			"state":               st.State.String(),
			"term":                st.Term,
			"leader_id":           st.LeaderID,
			"commit_index":        st.CommitIndex,
			"applied_index":       st.AppliedIndex,
			"last_log_index":      st.LastLogIndex,
			"last_log_term":       st.LastLogTerm,
			"snapshot_index":      st.SnapshotIndex,
			"members":             st.Members,
			"peers":               st.Peers,
			"elections_started":   st.ElectionsStarted,
			"became_leader":       st.BecameLeader,
			"snapshots_created":   st.SnapshotsCreated,
			"snapshots_installed": st.SnapshotsInstalled,
			"kv": map[string]interface{}{
				"keys":               stats.Keys,
				"sets":               stats.Sets,
				"deletes":            stats.Deletes,
				"cas":                stats.Cas,
				"gets":               stats.Gets,
				"linearizable_reads": stats.LinReads,
				"rejected":           stats.Rejected,
			},
		})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		st := node.Status()
		stats := store.Stats()
		var b strings.Builder

		gauge := func(name, help string, value interface{}) {
			fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %v\n", name, help, name, name, value)
		}
		counter := func(name, help string, value uint64) {
			fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, value)
		}

		isLeader := 0
		if st.State == raft.Leader {
			isLeader = 1
		}
		gauge("raft_term", "Current Raft term.", st.Term)
		gauge("raft_is_leader", "1 if this node is the leader.", isLeader)
		gauge("raft_leader_id", "Node ID of the known leader (0 if unknown).", st.LeaderID)
		gauge("raft_commit_index", "Highest committed log index.", st.CommitIndex)
		gauge("raft_applied_index", "Highest applied log index.", st.AppliedIndex)
		gauge("raft_last_log_index", "Last log index.", st.LastLogIndex)
		gauge("raft_snapshot_index", "Last snapshot index.", st.SnapshotIndex)
		gauge("raft_cluster_members", "Number of members in the active configuration.", len(st.Members))
		counter("raft_elections_started_total", "Elections this node has started.", st.ElectionsStarted)
		counter("raft_became_leader_total", "Times this node won an election.", st.BecameLeader)
		counter("raft_snapshots_created_total", "Snapshots created locally.", st.SnapshotsCreated)
		counter("raft_snapshots_installed_total", "Snapshots installed from the leader.", st.SnapshotsInstalled)
		gauge("kv_keys", "Number of keys in the store.", stats.Keys)
		counter("kv_sets_total", "SET requests received.", stats.Sets)
		counter("kv_deletes_total", "DELETE requests received.", stats.Deletes)
		counter("kv_cas_total", "Compare-and-swap requests received.", stats.Cas)
		counter("kv_gets_total", "Local GET requests received.", stats.Gets)
		counter("kv_linearizable_reads_total", "Linearizable read requests received.", stats.LinReads)
		counter("kv_rejected_total", "Requests rejected (not leader / no quorum).", stats.Rejected)

		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = w.Write([]byte(b.String()))
	})

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = srv.Serve(lis)
	}()

	return func() {
		_ = srv.Close()
	}, nil
}
