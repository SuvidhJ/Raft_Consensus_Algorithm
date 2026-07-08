package tests

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
)

func TestExactlyOnceDuplicateRetryIsNotReapplied(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-301")
	node, store, cleanup := startSingleNode(t, dataDir, 301)
	defer cleanup()
	waitForSingleLeader(t, node, 3*time.Second)

	first, err := store.Set(context.Background(), &proto.SetRequest{
		Key: "dup", Value: "original", ClientId: 71, Sequence: 1,
	})
	if err != nil || !first.Success {
		t.Fatalf("first set failed: resp=%v err=%v", first, err)
	}

	// A client retry replays the same (client_id, sequence). Even though it
	// carries different bytes, the state machine must not apply it twice.
	retry, err := store.Set(context.Background(), &proto.SetRequest{
		Key: "dup", Value: "evil-twin", ClientId: 71, Sequence: 1,
	})
	if err != nil || !retry.Success {
		t.Fatalf("retry set failed: resp=%v err=%v", retry, err)
	}

	got, err := store.Get(context.Background(), &proto.GetRequest{Key: "dup"})
	if err != nil || !got.Success {
		t.Fatalf("get failed: resp=%v err=%v", got, err)
	}
	if got.Value != "original" {
		t.Fatalf("duplicate was re-applied: got %q want %q", got.Value, "original")
	}
}

func TestDeleteReportsExistence(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-302")
	node, store, cleanup := startSingleNode(t, dataDir, 302)
	defer cleanup()
	waitForSingleLeader(t, node, 3*time.Second)

	if resp, err := store.Set(context.Background(), &proto.SetRequest{Key: "d", Value: "v"}); err != nil || !resp.Success {
		t.Fatalf("set failed: resp=%v err=%v", resp, err)
	}

	del1, err := store.Delete(context.Background(), &proto.DeleteRequest{Key: "d", ClientId: 5, Sequence: 1})
	if err != nil || !del1.Success {
		t.Fatalf("delete failed: resp=%v err=%v", del1, err)
	}
	if !del1.Existed {
		t.Fatal("first delete should report the key existed")
	}

	del2, err := store.Delete(context.Background(), &proto.DeleteRequest{Key: "d", ClientId: 5, Sequence: 2})
	if err != nil || !del2.Success {
		t.Fatalf("second delete failed: resp=%v err=%v", del2, err)
	}
	if del2.Existed {
		t.Fatal("second delete should report the key was absent")
	}

	if got, _ := store.Get(context.Background(), &proto.GetRequest{Key: "d"}); got.Success {
		t.Fatalf("key should be gone, got %q", got.Value)
	}
}

func TestCompareAndSwapSemantics(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "node-303")
	node, store, cleanup := startSingleNode(t, dataDir, 303)
	defer cleanup()
	waitForSingleLeader(t, node, 3*time.Second)

	if resp, err := store.Set(context.Background(), &proto.SetRequest{Key: "c", Value: "a"}); err != nil || !resp.Success {
		t.Fatalf("seed set failed: resp=%v err=%v", resp, err)
	}

	swap, err := store.CompareAndSwap(context.Background(), &proto.CasRequest{
		Key: "c", ExpectedValue: "a", NewValue: "b",
	})
	if err != nil || !swap.Success {
		t.Fatalf("cas rpc failed: resp=%v err=%v", swap, err)
	}
	if !swap.Swapped {
		t.Fatal("cas with matching expectation should swap")
	}

	miss, err := store.CompareAndSwap(context.Background(), &proto.CasRequest{
		Key: "c", ExpectedValue: "a", NewValue: "x",
	})
	if err != nil || !miss.Success {
		t.Fatalf("cas rpc failed: resp=%v err=%v", miss, err)
	}
	if miss.Swapped {
		t.Fatal("cas with stale expectation must not swap")
	}
	if miss.CurrentValue != "b" {
		t.Fatalf("cas should report the current value, got %q", miss.CurrentValue)
	}

	create, err := store.CompareAndSwap(context.Background(), &proto.CasRequest{
		Key: "fresh", ExpectAbsent: true, NewValue: "born",
	})
	if err != nil || !create.Success || !create.Swapped {
		t.Fatalf("expect-absent cas on missing key should swap: resp=%v err=%v", create, err)
	}

	recreate, err := store.CompareAndSwap(context.Background(), &proto.CasRequest{
		Key: "fresh", ExpectAbsent: true, NewValue: "again",
	})
	if err != nil || !recreate.Success {
		t.Fatalf("cas rpc failed: resp=%v err=%v", recreate, err)
	}
	if recreate.Swapped {
		t.Fatal("expect-absent cas on existing key must not swap")
	}
}

func TestSessionDedupSurvivesSnapshotAndRestart(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "node-304")

	node, store, cleanup := startSingleNode(t, dataDir, 304)
	waitForSingleLeader(t, node, 3*time.Second)

	if resp, err := store.Set(context.Background(), &proto.SetRequest{
		Key: "s", Value: "v1", ClientId: 9, Sequence: 4,
	}); err != nil || !resp.Success {
		t.Fatalf("set failed: resp=%v err=%v", resp, err)
	}
	// Push enough writes to cross the snapshot threshold (64 in the helper).
	for i := 0; i < 80; i++ {
		if resp, err := store.Set(context.Background(), &proto.SetRequest{Key: "filler", Value: "x"}); err != nil || !resp.Success {
			t.Fatalf("filler set %d failed: resp=%v err=%v", i, resp, err)
		}
	}
	cleanup()

	node2, store2, cleanup2 := startSingleNode(t, dataDir, 304)
	defer cleanup2()
	waitForSingleLeader(t, node2, 3*time.Second)

	// The retried duplicate from before the snapshot must still be deduped.
	retry, err := store2.Set(context.Background(), &proto.SetRequest{
		Key: "s", Value: "rogue", ClientId: 9, Sequence: 4,
	})
	if err != nil || !retry.Success {
		t.Fatalf("retry set failed: resp=%v err=%v", retry, err)
	}
	got, err := store2.Get(context.Background(), &proto.GetRequest{Key: "s"})
	if err != nil || !got.Success || got.Value != "v1" {
		t.Fatalf("dedup state lost across snapshot+restart: got=%v err=%v", got, err)
	}
}
