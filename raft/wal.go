package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/SuvidhJ/Raft_Consensus_Algorithm/proto"
	gproto "google.golang.org/protobuf/proto"
)

// Write-ahead log storage.
//
// Layout inside the node data directory:
//
//	wal-<seq>.log   append-only segments of checksummed records
//	snapshot.snap   latest state-machine snapshot (written via tmp+rename)
//
// Record framing: [u32 payload length][u32 CRC32-C of payload][payload].
// The first payload byte is the record type. All multi-byte integers are
// little-endian.
//
// Durability contract: every record that Raft safety depends on (terms,
// votes, log entries) is fsynced before the corresponding RPC response or
// client acknowledgement is released. Commit-index records are written
// unsynced — they are a recovery hint, not a safety requirement, and are
// made durable by the next synced record's fsync.
//
// Torn-write recovery: a truncated or corrupt record at the tail of the
// last segment is treated as an interrupted write and discarded; corruption
// anywhere else is fatal.

const (
	recHardState byte = 1 // term, votedFor, commitIndex
	recEntries   byte = 2 // log append; implicitly truncates from entries[0].Index
	recSnapMeta  byte = 3 // snapshot metadata; compacts log prefix

	walSegmentPrefix = "wal-"
	walSegmentSuffix = ".log"
	snapshotFileName = "snapshot.snap"
	legacyStateFile  = "raft-state.json"

	defaultSegmentLimit = 4 << 20 // rotate segments at 4 MiB

	snapshotMagic = uint32(0x52414653) // "RAFS"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type recoveredWAL struct {
	term              uint64
	votedFor          uint32
	commitIndex       uint64
	lastIncludedIndex uint64
	lastIncludedTerm  uint64
	snapshotData      []byte
	snapshotConfig    []string // cluster membership at the snapshot point
	entries           []*proto.LogEntry
}

type walStorage struct {
	dir      string
	seg      *os.File
	segSeq   uint64
	segSize  int64
	segLimit int64
}

// openWAL recovers persisted state from dir (migrating a legacy JSON state
// file if present) and returns a storage handle positioned for appending.
// After recovery it writes a fresh checkpoint segment and removes older
// segments, so every boot starts from a compact, verified baseline.
func openWAL(dir string) (*walStorage, *recoveredWAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, err
	}
	// A leftover .tmp means a snapshot write was interrupted before its
	// atomic rename; the previous snapshot (if any) is still authoritative.
	_ = os.Remove(filepath.Join(dir, snapshotFileName+".tmp"))

	w := &walStorage{dir: dir, segLimit: defaultSegmentLimit}

	rec, err := w.recover()
	if err != nil {
		return nil, nil, err
	}

	migrated, legacy, err := loadLegacyState(filepath.Join(dir, legacyStateFile))
	if err != nil {
		return nil, nil, err
	}
	if migrated {
		// Legacy state is only authoritative when no WAL exists yet.
		if w.segSeq == 0 {
			rec = legacy
		}
	}

	if err := w.checkpoint(rec.term, rec.votedFor, rec.commitIndex,
		rec.lastIncludedIndex, rec.lastIncludedTerm, rec.snapshotData, rec.snapshotConfig, rec.entries); err != nil {
		return nil, nil, err
	}

	if migrated {
		old := filepath.Join(dir, legacyStateFile)
		_ = os.Rename(old, old+".migrated")
	}

	return w, rec, nil
}

// recover scans all segments in sequence order and folds records into state.
func (w *walStorage) recover() (*recoveredWAL, error) {
	rec := &recoveredWAL{}

	snapIdx, snapTerm, snapData, snapConfig, err := readSnapshotFile(filepath.Join(w.dir, snapshotFileName))
	if err != nil {
		return nil, err
	}

	seqs, err := listSegments(w.dir)
	if err != nil {
		return nil, err
	}

	for i, seq := range seqs {
		last := i == len(seqs)-1
		if err := w.replaySegment(seq, last, rec); err != nil {
			return nil, err
		}
		if seq > w.segSeq {
			w.segSeq = seq
		}
	}

	// The snapshot file may be newer than the last durable snap-meta record
	// (crash between snapshot write and checkpoint). The file is written
	// first, so it is always authoritative when ahead.
	if snapIdx > rec.lastIncludedIndex {
		rec.lastIncludedIndex = snapIdx
		rec.lastIncludedTerm = snapTerm
		rec.entries = dropEntriesUpTo(rec.entries, snapIdx)
	}
	if rec.lastIncludedIndex > 0 && snapIdx < rec.lastIncludedIndex {
		// The snapshot file is always written and fsynced before the
		// snap-meta record, so this indicates external tampering or loss.
		return nil, fmt.Errorf("wal references snapshot at index %d but snapshot file holds index %d", rec.lastIncludedIndex, snapIdx)
	}
	if snapIdx >= rec.lastIncludedIndex {
		rec.snapshotData = snapData
		rec.snapshotConfig = snapConfig
	}
	if rec.commitIndex < rec.lastIncludedIndex {
		rec.commitIndex = rec.lastIncludedIndex
	}

	if err := validateRecovered(rec); err != nil {
		return nil, err
	}
	return rec, nil
}

func (w *walStorage) replaySegment(seq uint64, tolerateTornTail bool, rec *recoveredWAL) error {
	path := w.segmentPath(seq)
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	off := 0
	for off < len(data) {
		if len(data)-off < 8 {
			if tolerateTornTail {
				return nil
			}
			return fmt.Errorf("wal segment %d: truncated record header at offset %d", seq, off)
		}
		length := binary.LittleEndian.Uint32(data[off:])
		sum := binary.LittleEndian.Uint32(data[off+4:])
		if length == 0 || length > 64<<20 || off+8+int(length) > len(data) {
			if tolerateTornTail {
				return nil
			}
			return fmt.Errorf("wal segment %d: invalid record length %d at offset %d", seq, length, off)
		}
		payload := data[off+8 : off+8+int(length)]
		if crc32.Checksum(payload, crcTable) != sum {
			if tolerateTornTail {
				return nil
			}
			return fmt.Errorf("wal segment %d: checksum mismatch at offset %d", seq, off)
		}
		if err := applyRecord(payload, rec); err != nil {
			return fmt.Errorf("wal segment %d: %w", seq, err)
		}
		off += 8 + int(length)
	}
	return nil
}

func applyRecord(payload []byte, rec *recoveredWAL) error {
	if len(payload) == 0 {
		return errors.New("empty record")
	}
	body := payload[1:]
	switch payload[0] {
	case recHardState:
		if len(body) != 20 {
			return fmt.Errorf("hard-state record: bad length %d", len(body))
		}
		rec.term = binary.LittleEndian.Uint64(body[0:])
		rec.votedFor = binary.LittleEndian.Uint32(body[8:])
		rec.commitIndex = binary.LittleEndian.Uint64(body[12:])
		return nil

	case recEntries:
		entries, err := decodeEntriesRecord(body)
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		first := entries[0].Index
		lastIdx := rec.lastIncludedIndex
		if len(rec.entries) > 0 {
			lastIdx = rec.entries[len(rec.entries)-1].Index
		}
		if first > lastIdx+1 {
			return fmt.Errorf("entries record: gap (first=%d, log tail=%d)", first, lastIdx)
		}
		// Implicit suffix truncation: an append at index i supersedes any
		// previously recorded entries at i and beyond.
		rec.entries = dropEntriesFrom(rec.entries, first)
		rec.entries = append(rec.entries, entries...)
		return nil

	case recSnapMeta:
		if len(body) != 16 {
			return fmt.Errorf("snap-meta record: bad length %d", len(body))
		}
		idx := binary.LittleEndian.Uint64(body[0:])
		term := binary.LittleEndian.Uint64(body[8:])
		if idx < rec.lastIncludedIndex {
			return nil
		}
		rec.lastIncludedIndex = idx
		rec.lastIncludedTerm = term
		rec.entries = dropEntriesUpTo(rec.entries, idx)
		if rec.commitIndex < idx {
			rec.commitIndex = idx
		}
		return nil

	default:
		return fmt.Errorf("unknown record type %d", payload[0])
	}
}

func validateRecovered(rec *recoveredWAL) error {
	if rec.lastIncludedIndex > 0 && rec.lastIncludedTerm == 0 {
		return fmt.Errorf("invalid snapshot metadata: index=%d with zero term", rec.lastIncludedIndex)
	}
	if rec.term == 0 && rec.votedFor != 0 {
		return fmt.Errorf("invalid vote: voted_for=%d with term=0", rec.votedFor)
	}
	expected := rec.lastIncludedIndex + 1
	for _, e := range rec.entries {
		if e.Index != expected {
			return fmt.Errorf("log gap: got index %d, want %d", e.Index, expected)
		}
		if e.Term == 0 {
			return fmt.Errorf("invalid log entry at index %d: zero term", e.Index)
		}
		expected++
	}
	lastIdx := rec.lastIncludedIndex
	if len(rec.entries) > 0 {
		lastIdx = rec.entries[len(rec.entries)-1].Index
	}
	if rec.commitIndex > lastIdx {
		return fmt.Errorf("commit index %d beyond log tail %d", rec.commitIndex, lastIdx)
	}
	return nil
}

// --- write paths -----------------------------------------------------------

func (w *walStorage) saveHardState(term uint64, votedFor uint32, commitIndex uint64, sync bool) error {
	body := make([]byte, 21)
	body[0] = recHardState
	binary.LittleEndian.PutUint64(body[1:], term)
	binary.LittleEndian.PutUint32(body[9:], votedFor)
	binary.LittleEndian.PutUint64(body[13:], commitIndex)
	return w.writeRecords([][]byte{body}, sync)
}

// appendEntries durably appends entries and the accompanying hard state in a
// single fsync.
func (w *walStorage) appendEntries(entries []*proto.LogEntry, term uint64, votedFor uint32, commitIndex uint64) error {
	entRec, err := encodeEntriesRecord(entries)
	if err != nil {
		return err
	}
	hard := make([]byte, 21)
	hard[0] = recHardState
	binary.LittleEndian.PutUint64(hard[1:], term)
	binary.LittleEndian.PutUint32(hard[9:], votedFor)
	binary.LittleEndian.PutUint64(hard[13:], commitIndex)
	return w.writeRecords([][]byte{entRec, hard}, true)
}

// checkpoint persists the snapshot file, then starts a fresh segment holding
// the complete current state, then deletes older segments. Safe at every
// crash point: the snapshot file is authoritative when ahead of the WAL, and
// stale segments are superseded by the records in the new one.
func (w *walStorage) checkpoint(term uint64, votedFor uint32, commitIndex uint64,
	snapIndex, snapTerm uint64, snapData []byte, snapConfig []string, entries []*proto.LogEntry) error {

	if snapIndex > 0 {
		if err := writeSnapshotFile(filepath.Join(w.dir, snapshotFileName), snapIndex, snapTerm, snapData, snapConfig); err != nil {
			return err
		}
	}

	if w.seg != nil {
		_ = w.seg.Close()
		w.seg = nil
	}

	newSeq := w.segSeq + 1
	f, err := os.OpenFile(w.segmentPath(newSeq), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	records := make([][]byte, 0, 3)
	hard := make([]byte, 21)
	hard[0] = recHardState
	binary.LittleEndian.PutUint64(hard[1:], term)
	binary.LittleEndian.PutUint32(hard[9:], votedFor)
	binary.LittleEndian.PutUint64(hard[13:], commitIndex)
	records = append(records, hard)

	if snapIndex > 0 {
		meta := make([]byte, 17)
		meta[0] = recSnapMeta
		binary.LittleEndian.PutUint64(meta[1:], snapIndex)
		binary.LittleEndian.PutUint64(meta[9:], snapTerm)
		records = append(records, meta)
	}

	if len(entries) > 0 {
		entRec, err := encodeEntriesRecord(entries)
		if err != nil {
			_ = f.Close()
			return err
		}
		records = append(records, entRec)
	}

	buf := frameRecords(records)
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	syncDir(w.dir)

	oldSeq := w.segSeq
	w.seg = f
	w.segSeq = newSeq
	w.segSize = int64(len(buf))

	for seq := uint64(1); seq <= oldSeq; seq++ {
		// Best effort: leftover old segments are superseded on replay.
		_ = os.Remove(w.segmentPath(seq))
	}
	return nil
}

func (w *walStorage) writeRecords(payloads [][]byte, sync bool) error {
	if w.seg == nil {
		return errors.New("wal: no active segment")
	}
	buf := frameRecords(payloads)
	if _, err := w.seg.Write(buf); err != nil {
		return err
	}
	w.segSize += int64(len(buf))
	if sync {
		if err := w.seg.Sync(); err != nil {
			return err
		}
	}
	if w.segSize >= w.segLimit {
		return w.rotate()
	}
	return nil
}

func (w *walStorage) rotate() error {
	if err := w.seg.Sync(); err != nil {
		return err
	}
	if err := w.seg.Close(); err != nil {
		return err
	}
	newSeq := w.segSeq + 1
	f, err := os.OpenFile(w.segmentPath(newSeq), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	syncDir(w.dir)
	w.seg = f
	w.segSeq = newSeq
	w.segSize = 0
	return nil
}

func (w *walStorage) close() {
	if w.seg != nil {
		_ = w.seg.Sync()
		_ = w.seg.Close()
		w.seg = nil
	}
}

func (w *walStorage) segmentPath(seq uint64) string {
	return filepath.Join(w.dir, fmt.Sprintf("%s%020d%s", walSegmentPrefix, seq, walSegmentSuffix))
}

// --- encoding helpers --------------------------------------------------------

func frameRecords(payloads [][]byte) []byte {
	total := 0
	for _, p := range payloads {
		total += 8 + len(p)
	}
	buf := make([]byte, 0, total)
	var hdr [8]byte
	for _, p := range payloads {
		binary.LittleEndian.PutUint32(hdr[0:], uint32(len(p)))
		binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(p, crcTable))
		buf = append(buf, hdr[:]...)
		buf = append(buf, p...)
	}
	return buf
}

func encodeEntriesRecord(entries []*proto.LogEntry) ([]byte, error) {
	buf := make([]byte, 0, 64*len(entries)+5)
	buf = append(buf, recEntries)
	buf = binary.AppendUvarint(buf, uint64(len(entries)))
	for _, e := range entries {
		b, err := gproto.Marshal(e)
		if err != nil {
			return nil, err
		}
		buf = binary.AppendUvarint(buf, uint64(len(b)))
		buf = append(buf, b...)
	}
	return buf, nil
}

func decodeEntriesRecord(body []byte) ([]*proto.LogEntry, error) {
	count, n := binary.Uvarint(body)
	if n <= 0 {
		return nil, errors.New("entries record: bad count")
	}
	body = body[n:]
	out := make([]*proto.LogEntry, 0, count)
	for i := uint64(0); i < count; i++ {
		sz, n := binary.Uvarint(body)
		if n <= 0 || uint64(len(body)-n) < sz {
			return nil, errors.New("entries record: truncated entry")
		}
		e := &proto.LogEntry{}
		if err := gproto.Unmarshal(body[n:n+int(sz)], e); err != nil {
			return nil, err
		}
		out = append(out, e)
		body = body[n+int(sz):]
	}
	return out, nil
}

// --- snapshot file -----------------------------------------------------------

// Snapshot file format: magic u32 | index u64 | term u64 | data len u32 |
// data | member count u32 | (member len u32 | member bytes)* | crc u32
// (over everything before it).
func writeSnapshotFile(path string, index, term uint64, data []byte, config []string) error {
	buf := make([]byte, 0, 32+len(data))
	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[:4], snapshotMagic)
	buf = append(buf, scratch[:4]...)
	binary.LittleEndian.PutUint64(scratch[:8], index)
	buf = append(buf, scratch[:8]...)
	binary.LittleEndian.PutUint64(scratch[:8], term)
	buf = append(buf, scratch[:8]...)
	binary.LittleEndian.PutUint32(scratch[:4], uint32(len(data)))
	buf = append(buf, scratch[:4]...)
	buf = append(buf, data...)
	binary.LittleEndian.PutUint32(scratch[:4], uint32(len(config)))
	buf = append(buf, scratch[:4]...)
	for _, m := range config {
		binary.LittleEndian.PutUint32(scratch[:4], uint32(len(m)))
		buf = append(buf, scratch[:4]...)
		buf = append(buf, m...)
	}
	binary.LittleEndian.PutUint32(scratch[:4], crc32.Checksum(buf, crcTable))
	buf = append(buf, scratch[:4]...)

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	syncDir(filepath.Dir(path))
	return nil
}

func readSnapshotFile(path string) (index, term uint64, data []byte, config []string, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, 0, nil, nil, nil
		}
		return 0, 0, nil, nil, err
	}
	if len(raw) < 32 {
		return 0, 0, nil, nil, fmt.Errorf("snapshot file: too short (%d bytes)", len(raw))
	}
	if binary.LittleEndian.Uint32(raw[0:]) != snapshotMagic {
		return 0, 0, nil, nil, errors.New("snapshot file: bad magic")
	}
	stored := binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if crc32.Checksum(raw[:len(raw)-4], crcTable) != stored {
		return 0, 0, nil, nil, errors.New("snapshot file: checksum mismatch")
	}
	index = binary.LittleEndian.Uint64(raw[4:])
	term = binary.LittleEndian.Uint64(raw[12:])
	size := binary.LittleEndian.Uint32(raw[20:])
	off := 24 + int(size)
	if off+8 > len(raw) {
		return 0, 0, nil, nil, errors.New("snapshot file: length mismatch")
	}
	data = append([]byte(nil), raw[24:off]...)
	memberCount := binary.LittleEndian.Uint32(raw[off:])
	off += 4
	for i := uint32(0); i < memberCount; i++ {
		if off+4 > len(raw)-4 {
			return 0, 0, nil, nil, errors.New("snapshot file: truncated member list")
		}
		mlen := int(binary.LittleEndian.Uint32(raw[off:]))
		off += 4
		if off+mlen > len(raw)-4 {
			return 0, 0, nil, nil, errors.New("snapshot file: truncated member entry")
		}
		config = append(config, string(raw[off:off+mlen]))
		off += mlen
	}
	if off != len(raw)-4 {
		return 0, 0, nil, nil, errors.New("snapshot file: trailing bytes")
	}
	return index, term, data, config, nil
}

// --- legacy JSON migration ----------------------------------------------------

func loadLegacyState(path string) (bool, *recoveredWAL, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil, nil
		}
		return false, nil, err
	}
	if len(b) == 0 {
		return false, nil, nil
	}

	var s persistentState
	if err := json.Unmarshal(b, &s); err != nil {
		return false, nil, fmt.Errorf("legacy state: %w", err)
	}

	rec := &recoveredWAL{
		term:              s.CurrentTerm,
		votedFor:          s.VotedFor,
		commitIndex:       s.CommitIndex,
		lastIncludedIndex: s.LastIncludedIndex,
		lastIncludedTerm:  s.LastIncludedTerm,
		snapshotData:      append([]byte(nil), s.SnapshotData...),
	}
	for i, e := range s.Log {
		if e == nil {
			return false, nil, fmt.Errorf("legacy state: nil entry at position %d", i)
		}
		rec.entries = append(rec.entries, cloneEntry(e))
	}
	if err := validateRecovered(rec); err != nil {
		return false, nil, fmt.Errorf("legacy state: %w", err)
	}
	return true, rec, nil
}

// --- misc ----------------------------------------------------------------------

func listSegments(dir string) ([]uint64, error) {
	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seqs := make([]uint64, 0, len(names))
	for _, de := range names {
		name := de.Name()
		if !strings.HasPrefix(name, walSegmentPrefix) || !strings.HasSuffix(name, walSegmentSuffix) {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, walSegmentPrefix), walSegmentSuffix)
		seq, err := strconv.ParseUint(mid, 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	return seqs, nil
}

func dropEntriesFrom(entries []*proto.LogEntry, from uint64) []*proto.LogEntry {
	for i, e := range entries {
		if e.Index >= from {
			return entries[:i]
		}
	}
	return entries
}

func dropEntriesUpTo(entries []*proto.LogEntry, upTo uint64) []*proto.LogEntry {
	for i, e := range entries {
		if e.Index > upTo {
			return append([]*proto.LogEntry(nil), entries[i:]...)
		}
	}
	return nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
