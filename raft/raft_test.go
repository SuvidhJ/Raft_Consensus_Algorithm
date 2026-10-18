package raft

import (
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
)

func TestAwaitVotesReturnsOnceDecided(t *testing.T) {
	cases := []struct {
		name            string
		peers, majority int
		replies         []bool // the remaining peers never reply
		want            int
	}{
		{"3 nodes, one grant wins", 2, 2, []bool{true}, 2},
		{"5 nodes, two grants win", 4, 3, []bool{false, true, true}, 3},
		{"5 nodes, three refusals lose", 4, 3, []bool{false, false, false}, 1},
		{"single node wins alone", 0, 1, nil, 1},
	}
	for _, tc := range cases {
		ch := make(chan bool, tc.peers)
		for _, r := range tc.replies {
			ch <- r
		}
		got := make(chan int, 1)
		go func() { got <- awaitVotes(ch, tc.peers, tc.majority) }()
		select {
		case v := <-got:
			if v != tc.want {
				t.Errorf("%s: votes=%d, want %d", tc.name, v, tc.want)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s: blocked on a reply the outcome did not need", tc.name)
		}
	}
}

// Entries after a snapshot can share the snapshot's term. Returning the
// snapshot boundary for that term would rewind a follower's nextIndex to the
// snapshot on every conflict.
func TestFindLastIndexOfTermSearchesPastSnapshot(t *testing.T) {
	rn := &RaftNode{lastIncludedIndex: 10, lastIncludedTerm: 3}
	for i := uint64(11); i <= 15; i++ {
		rn.log = append(rn.log, &proto.LogEntry{Index: i, Term: 3})
	}
	rn.log = append(rn.log, &proto.LogEntry{Index: 16, Term: 4})

	for term, want := range map[uint64]uint64{3: 15, 4: 16, 2: 0} {
		if got := rn.findLastIndexOfTermLocked(term); got != want {
			t.Errorf("term %d: got %d, want %d", term, got, want)
		}
	}
	rn.log = nil
	if got := rn.findLastIndexOfTermLocked(3); got != 10 {
		t.Errorf("fully compacted log, term 3: got %d, want snapshot index 10", got)
	}
}
