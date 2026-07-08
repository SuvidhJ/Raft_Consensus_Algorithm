package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
)

func entry(idx, term uint64, cmd string) *proto.LogEntry {
	return &proto.LogEntry{Index: idx, Term: term, Command: cmd}
}

func mustOpenWAL(t *testing.T, dir string) (*walStorage, *recoveredWAL) {
	t.Helper()
	w, rec, err := openWAL(dir)
	if err != nil {
		t.Fatalf("openWAL failed: %v", err)
	}
	t.Cleanup(w.close)
	return w, rec
}

func TestWALRoundTrip(t *testing.T) {
	dir := t.TempDir()

	w, rec := mustOpenWAL(t, dir)
	if rec.term != 0 || len(rec.entries) != 0 {
		t.Fatalf("expected empty recovery, got %+v", rec)
	}

	if err := w.appendEntries([]*proto.LogEntry{entry(1, 1, "a"), entry(2, 1, "b")}, 1, 7, 0); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := w.saveHardState(2, 9, 2, true); err != nil {
		t.Fatalf("hard state failed: %v", err)
	}
	w.close()

	_, rec2 := mustOpenWAL(t, dir)
	if rec2.term != 2 || rec2.votedFor != 9 || rec2.commitIndex != 2 {
		t.Fatalf("hard state mismatch: %+v", rec2)
	}
	if len(rec2.entries) != 2 || rec2.entries[0].Command != "a" || rec2.entries[1].Command != "b" {
		t.Fatalf("entries mismatch: %+v", rec2.entries)
	}
}

func TestWALImplicitSuffixTruncation(t *testing.T) {
	dir := t.TempDir()

	w, _ := mustOpenWAL(t, dir)
	if err := w.appendEntries([]*proto.LogEntry{
		entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c"), entry(4, 1, "d"),
	}, 1, 0, 0); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	// A later append at index 3 supersedes the old suffix.
	if err := w.appendEntries([]*proto.LogEntry{entry(3, 2, "c2")}, 2, 0, 0); err != nil {
		t.Fatalf("conflicting append failed: %v", err)
	}
	w.close()

	_, rec := mustOpenWAL(t, dir)
	if len(rec.entries) != 3 {
		t.Fatalf("expected 3 entries after truncation, got %d", len(rec.entries))
	}
	if rec.entries[2].Command != "c2" || rec.entries[2].Term != 2 {
		t.Fatalf("expected superseding entry at index 3, got %+v", rec.entries[2])
	}
}

func TestWALTornTailDiscarded(t *testing.T) {
	dir := t.TempDir()

	w, _ := mustOpenWAL(t, dir)
	if err := w.appendEntries([]*proto.LogEntry{entry(1, 1, "keep")}, 1, 0, 1); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	seg := w.segmentPath(w.segSeq)
	w.close()

	// Simulate a torn write: garbage half-record at the segment tail.
	f, err := os.OpenFile(seg, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := f.Write([]byte{0x10, 0x00, 0x00, 0x00, 0xde, 0xad}); err != nil {
		t.Fatalf("write garbage: %v", err)
	}
	_ = f.Close()

	_, rec := mustOpenWAL(t, dir)
	if len(rec.entries) != 1 || rec.entries[0].Command != "keep" {
		t.Fatalf("expected clean prefix to survive torn tail, got %+v", rec.entries)
	}
	if rec.commitIndex != 1 {
		t.Fatalf("expected commit index 1, got %d", rec.commitIndex)
	}
}

func TestWALCorruptionInOlderSegmentFails(t *testing.T) {
	dir := t.TempDir()

	w, _ := mustOpenWAL(t, dir)
	w.segLimit = 1 // force rotation after every record batch
	firstSeg := w.segmentPath(w.segSeq)
	if err := w.appendEntries([]*proto.LogEntry{entry(1, 1, "a")}, 1, 0, 0); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := w.appendEntries([]*proto.LogEntry{entry(2, 1, "b")}, 1, 0, 0); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	w.close()

	// Flip a byte inside the first (non-last) segment.
	data, err := os.ReadFile(firstSeg)
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if len(data) < 12 {
		t.Fatalf("segment unexpectedly small: %d bytes", len(data))
	}
	data[10] ^= 0xff
	if err := os.WriteFile(firstSeg, data, 0o644); err != nil {
		t.Fatalf("rewrite segment: %v", err)
	}

	if _, _, err := openWAL(dir); err == nil {
		t.Fatal("expected recovery failure for corruption in non-tail segment")
	}
}

func TestWALCheckpointCompactsAndDeletesOldSegments(t *testing.T) {
	dir := t.TempDir()

	w, _ := mustOpenWAL(t, dir)
	if err := w.appendEntries([]*proto.LogEntry{
		entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c"),
	}, 1, 0, 3); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	if err := w.checkpoint(1, 0, 3, 2, 1, []byte(`{"snap":"data"}`), []string{"a:1", "b:2"}, []*proto.LogEntry{entry(3, 1, "c")}); err != nil {
		t.Fatalf("checkpoint failed: %v", err)
	}
	w.close()

	segs, err := listSegments(dir)
	if err != nil {
		t.Fatalf("list segments: %v", err)
	}
	if len(segs) != 1 {
		t.Fatalf("expected exactly 1 segment after checkpoint, got %d", len(segs))
	}

	_, rec := mustOpenWAL(t, dir)
	if rec.lastIncludedIndex != 2 || rec.lastIncludedTerm != 1 {
		t.Fatalf("snapshot meta mismatch: %+v", rec)
	}
	if string(rec.snapshotData) != `{"snap":"data"}` {
		t.Fatalf("snapshot data mismatch: %q", rec.snapshotData)
	}
	if len(rec.snapshotConfig) != 2 || rec.snapshotConfig[0] != "a:1" || rec.snapshotConfig[1] != "b:2" {
		t.Fatalf("snapshot config mismatch: %+v", rec.snapshotConfig)
	}
	if len(rec.entries) != 1 || rec.entries[0].Index != 3 {
		t.Fatalf("expected only entry 3 to remain, got %+v", rec.entries)
	}
}

func TestWALSnapshotFileAheadOfMetaIsAuthoritative(t *testing.T) {
	dir := t.TempDir()

	w, _ := mustOpenWAL(t, dir)
	if err := w.appendEntries([]*proto.LogEntry{
		entry(1, 1, "a"), entry(2, 1, "b"), entry(3, 1, "c"),
	}, 1, 0, 3); err != nil {
		t.Fatalf("append failed: %v", err)
	}
	w.close()

	// Simulate a crash after the snapshot file was written but before the
	// checkpoint segment recorded its metadata.
	if err := writeSnapshotFile(filepath.Join(dir, snapshotFileName), 2, 1, []byte(`{"ahead":"yes"}`), []string{"m:1"}); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	_, rec := mustOpenWAL(t, dir)
	if rec.lastIncludedIndex != 2 || rec.lastIncludedTerm != 1 {
		t.Fatalf("expected snapshot file to win: %+v", rec)
	}
	if string(rec.snapshotData) != `{"ahead":"yes"}` {
		t.Fatalf("snapshot data mismatch: %q", rec.snapshotData)
	}
	if len(rec.entries) != 1 || rec.entries[0].Index != 3 {
		t.Fatalf("expected entries <= snapshot index to be dropped, got %+v", rec.entries)
	}
	if rec.commitIndex != 3 {
		t.Fatalf("expected commit index 3, got %d", rec.commitIndex)
	}
}

func TestWALLegacyJSONMigration(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	legacy := persistentState{
		CurrentTerm: 4,
		VotedFor:    2,
		Log: []*proto.LogEntry{
			{Index: 1, Term: 1, Command: "x"},
			{Index: 2, Term: 4, Command: "y"},
		},
		CommitIndex: 2,
	}
	b, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy: %v", err)
	}
	legacyPath := filepath.Join(dir, legacyStateFile)
	if err := os.WriteFile(legacyPath, b, 0o644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	_, rec := mustOpenWAL(t, dir)
	if rec.term != 4 || rec.votedFor != 2 || rec.commitIndex != 2 || len(rec.entries) != 2 {
		t.Fatalf("legacy state not migrated: %+v", rec)
	}

	if _, err := os.Stat(legacyPath); !os.IsNotExist(err) {
		t.Fatal("legacy file should have been renamed after migration")
	}
	if _, err := os.Stat(legacyPath + ".migrated"); err != nil {
		t.Fatalf("expected .migrated backup: %v", err)
	}

	// Second boot must recover identical state from the WAL alone.
	_, rec2 := mustOpenWAL(t, dir)
	if rec2.term != 4 || rec2.commitIndex != 2 || len(rec2.entries) != 2 {
		t.Fatalf("post-migration recovery mismatch: %+v", rec2)
	}
}

func TestWALInvalidLegacyStateFails(t *testing.T) {
	dir := t.TempDir()
	bad := `{"current_term":2,"voted_for":1,"log":[{"term":2,"command":"x","index":2}],"commit_index":2}`
	if err := os.WriteFile(filepath.Join(dir, legacyStateFile), []byte(bad), 0o644); err != nil {
		t.Fatalf("write legacy: %v", err)
	}
	if _, _, err := openWAL(dir); err == nil {
		t.Fatal("expected migration failure for gapped legacy log")
	}
}

func TestWALSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	w, _ := mustOpenWAL(t, dir)
	w.segLimit = 256
	for i := uint64(1); i <= 50; i++ {
		if err := w.appendEntries([]*proto.LogEntry{entry(i, 1, "payload-payload-payload")}, 1, 0, i); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if w.segSeq < 2 {
		t.Fatalf("expected segment rotation, still at seq %d", w.segSeq)
	}
	w.close()

	_, rec := mustOpenWAL(t, dir)
	if len(rec.entries) != 50 || rec.commitIndex != 50 {
		t.Fatalf("multi-segment recovery mismatch: entries=%d commit=%d", len(rec.entries), rec.commitIndex)
	}
}
