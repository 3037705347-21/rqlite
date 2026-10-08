// Package durable implements the crash-safe ledger backing durable queued
// writes. Each admitted request is stored locally on the accepting node as
// a Pending record and fsynced before the HTTP layer acknowledges it; once
// the request has been submitted through Raft it is marked Committed.
//
// The ledger is the node-local "at-least-once" half of exactly-once queued
// writes: it guarantees requests are never lost across restarts and leader
// changes, while the Raft FSM (see package db) is responsible for
// deduplicating replays by request id.
package durable

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	// ErrCorrupt is returned when the ledger fails its integrity check:
	// a record checksum/hash mismatch, a sequence gap, or invalid state.
	ErrCorrupt = errors.New("durable queue ledger is corrupt")

	// ErrIDConflict is returned when an existing id is re-admitted with a
	// payload whose hash differs from the stored one. Reusing an id with
	// different content is a client-side correctness violation.
	ErrIDConflict = errors.New("request id reused with different payload")

	// ErrIDEmpty is returned when Admit is called with an empty id.
	ErrIDEmpty = errors.New("request id is empty")

	// ErrClosed is returned when the ledger is used after Close.
	ErrClosed = errors.New("durable queue ledger is closed")
)

// State is the lifecycle state of a ledger entry.
type State uint8

const (
	stateUnknown State = 0
	// StatePending means the request has been admitted and fsynced but is
	// not known to have been committed through Raft yet.
	StatePending State = 1
	// StateCommitted means the request was successfully submitted through
	// Raft (including an FSM duplicate hit).
	StateCommitted State = 2
)

// Entry is an in-memory view of a ledger record.
type Entry struct {
	ID         string
	Seq        int64
	State      State
	Payload    []byte
	Hash       string // hex-encoded SHA-256 of Payload
	ReceivedAt int64  // UnixNano at admission
}

var (
	entriesBucket = []byte("entries")
	metaBucket    = []byte("meta")
	seqKey        = []byte("seq")
	watermarkKey  = []byte("watermark")
)

// recordWire is the on-disk JSON encoding of a ledger entry.
type recordWire struct {
	Seq        int64  `json:"seq"`
	State      State  `json:"state"`
	Payload    []byte `json:"payload"`
	Hash       string `json:"hash"`
	ReceivedAt int64  `json:"received_at"`
	Checksum   string `json:"checksum"`
}

// Ledger is a bbolt-backed durable queue ledger. A single file holds all
// state; every bbolt transaction is fsynced on commit by default.
type Ledger struct {
	db     *bolt.DB
	closed atomic.Bool
}

// Open opens (creating if needed) the ledger file at path and verifies its
// contents before returning. A failed verification still returns a usable
// error without panicking, so callers can place the system into a frozen
// state rather than crashing.
func Open(path string) (*Ledger, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create ledger directory: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open ledger at %s: %w", path, err)
	}
	l := &Ledger{db: db}
	if err := l.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(entriesBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists(metaBucket)
		return err
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize ledger buckets: %w", err)
	}
	if err := l.Verify(); err != nil {
		return l, err
	}
	return l, nil
}

// Path returns the ledger file path.
func (l *Ledger) Path() string {
	return l.db.Path()
}

// Close releases the ledger file.
func (l *Ledger) Close() error {
	if !l.closed.CompareAndSwap(false, true) {
		return nil
	}
	return l.db.Close()
}

// Admit persists a new Pending record for id. If id is already known it is
// handled idempotently:
//   - identical payload: returns already=true (and committed=true when the
//     existing record is already Committed) without writing a second record;
//   - different payload: returns ErrIDConflict.
//
// For new records the returned seq is a ledger-wide monotonic sequence
// number assigned inside the fsynced transaction.
func (l *Ledger) Admit(id string, payload []byte) (already, committed bool, seq int64, err error) {
	if l.closed.Load() {
		return false, false, 0, ErrClosed
	}
	if id == "" {
		return false, false, 0, ErrIDEmpty
	}
	hash := hashBytes(payload)
	err = l.db.Update(func(tx *bolt.Tx) error {
		eb := tx.Bucket(entriesBucket)
		mb := tx.Bucket(metaBucket)
		if v := eb.Get([]byte(id)); v != nil {
			rec, derr := decodeRecord(id, v)
			if derr != nil {
				return derr
			}
			if rec.Hash != hash {
				return ErrIDConflict
			}
			already = true
			committed = rec.State == StateCommitted
			seq = rec.Seq
			return nil
		}

		next := mb.Sequence() + 1
		if err := mb.SetSequence(next); err != nil {
			return err
		}
		rec := recordWire{
			Seq:        int64(next),
			State:      StatePending,
			Payload:    append([]byte(nil), payload...),
			Hash:       hash,
			ReceivedAt: time.Now().UnixNano(),
		}
		rec.Checksum = recordChecksum(rec.Seq, rec.State, id, rec.Hash, rec.Payload)
		b, err := encodeRecord(&rec)
		if err != nil {
			return err
		}
		if err := eb.Put([]byte(id), b); err != nil {
			return err
		}
		seq = int64(next)
		return nil
	})
	if err != nil {
		return false, false, 0, err
	}
	return already, committed, seq, nil
}

// MarkCommitted atomically flips an entry to Committed. It returns
// found=false when no record exists for id. Marking an already-Committed
// record is a successful no-op.
func (l *Ledger) MarkCommitted(id string) (found bool, err error) {
	if l.closed.Load() {
		return false, ErrClosed
	}
	err = l.db.Update(func(tx *bolt.Tx) error {
		eb := tx.Bucket(entriesBucket)
		v := eb.Get([]byte(id))
		if v == nil {
			return nil
		}
		rec, derr := decodeRecord(id, v)
		if derr != nil {
			return derr
		}
		if rec.State == StateCommitted {
			found = true
			return nil
		}
		rec.State = StateCommitted
		rec.Checksum = recordChecksum(rec.Seq, rec.State, id, rec.Hash, rec.Payload)
		b, merr := encodeRecord(rec)
		if merr != nil {
			return merr
		}
		if err := eb.Put([]byte(id), b); err != nil {
			return err
		}
		found = true
		return nil
	})
	return found, err
}

// Pending returns all Pending entries ordered by ascending sequence number.
// The returned entries own their byte slices and remain valid after the call.
func (l *Ledger) Pending() ([]*Entry, error) {
	var out []*Entry
	err := l.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(entriesBucket).ForEach(func(k, v []byte) error {
			rec, err := decodeRecord(string(k), v)
			if err != nil {
				return err
			}
			if rec.State == StatePending {
				out = append(out, toEntry(string(k), rec))
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// PendingDepth returns the number of Pending entries.
func (l *Ledger) PendingDepth() (int, error) {
	n := 0
	err := l.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(entriesBucket).ForEach(func(_, v []byte) error {
			rec, err := decodeRecordRaw(v)
			if err != nil {
				return err
			}
			if rec.State == StatePending {
				n++
			}
			return nil
		})
	})
	return n, err
}

// PruneCommitted deletes the oldest contiguous Committed prefix of the
// ledger (everything committed before the oldest Pending entry), retaining
// the keep most recent records of that prefix, and advances the sequence
// watermark past the deleted records. It returns the number deleted.
func (l *Ledger) PruneCommitted(keep int) (int, error) {
	if keep < 0 {
		keep = 0
	}
	pruned := 0
	err := l.db.Update(func(tx *bolt.Tx) error {
		eb := tx.Bucket(entriesBucket)
		mb := tx.Bucket(metaBucket)

		type stored struct {
			id  []byte
			rec *recordWire
		}
		var records []*stored
		if err := eb.ForEach(func(k, v []byte) error {
			rec, err := decodeRecord(string(k), v)
			if err != nil {
				return err
			}
			records = append(records, &stored{id: append([]byte(nil), k...), rec: rec})
			return nil
		}); err != nil {
			return err
		}
		sort.Slice(records, func(i, j int) bool { return records[i].rec.Seq < records[j].rec.Seq })

		// Entries prunable form a contiguous committed prefix; a Pending
		// entry pins itself and every later record.
		var prefix []*stored
		for _, s := range records {
			if s.rec.State != StateCommitted {
				break
			}
			prefix = append(prefix, s)
		}
		if keep < len(prefix) {
			prefix = prefix[:len(prefix)-keep]
		} else {
			prefix = nil
		}
		if len(prefix) == 0 {
			return nil
		}

		var watermark int64
		for _, s := range prefix {
			if err := eb.Delete(s.id); err != nil {
				return err
			}
			if s.rec.Seq > watermark {
				watermark = s.rec.Seq
			}
			pruned++
		}
		var wb [8]byte
		binary.BigEndian.PutUint64(wb[:], uint64(watermark))
		return mb.Put(watermarkKey, wb[:])
	})
	return pruned, err
}

// Verify validates every record (checksum and payload hash), valid states,
// and sequence continuity above the prune watermark.
func (l *Ledger) Verify() error {
	return l.db.View(func(tx *bolt.Tx) error {
		eb := tx.Bucket(entriesBucket)
		mb := tx.Bucket(metaBucket)

		watermark := int64(0)
		if wv := mb.Get(watermarkKey); wv != nil {
			watermark = int64(binary.BigEndian.Uint64(wv))
		}

		var records []*recordWire
		if err := eb.ForEach(func(k, v []byte) error {
			rec, err := decodeRecord(string(k), v)
			if err != nil {
				return err
			}
			records = append(records, rec)
			return nil
		}); err != nil {
			return err
		}
		sort.Slice(records, func(i, j int) bool { return records[i].Seq < records[j].Seq })

		seen := make(map[int64]struct{}, len(records))
		want := watermark + 1
		for _, rec := range records {
			if rec.Seq <= watermark {
				return fmt.Errorf("%w: record seq %d is below watermark %d", ErrCorrupt, rec.Seq, watermark)
			}
			if rec.Seq != want {
				return fmt.Errorf("%w: sequence gap, expected %d got %d", ErrCorrupt, want, rec.Seq)
			}
			if _, dup := seen[rec.Seq]; dup {
				return fmt.Errorf("%w: duplicate sequence %d", ErrCorrupt, rec.Seq)
			}
			seen[rec.Seq] = struct{}{}
			want++
		}
		return nil
	})
}

// Stats reports ledger counters.
func (l *Ledger) Stats() (map[string]any, error) {
	stats := map[string]any{}
	err := l.db.View(func(tx *bolt.Tx) error {
		eb := tx.Bucket(entriesBucket)
		mb := tx.Bucket(metaBucket)
		var total, pending, committed int
		if err := eb.ForEach(func(_, v []byte) error {
			rec, err := decodeRecordRaw(v)
			if err != nil {
				return err
			}
			total++
			switch rec.State {
			case StatePending:
				pending++
			case StateCommitted:
				committed++
			}
			return nil
		}); err != nil {
			return err
		}
		stats["total"] = total
		stats["pending"] = pending
		stats["committed"] = committed
		stats["last_seq"] = int64(mb.Sequence())
		if wv := mb.Get(watermarkKey); wv != nil {
			stats["watermark"] = int64(binary.BigEndian.Uint64(wv))
		} else {
			stats["watermark"] = int64(0)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func recordChecksum(seq int64, state State, id, hash string, payload []byte) string {
	h := sha256.New()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(seq))
	h.Write(b[:])
	h.Write([]byte{byte(state)})
	h.Write([]byte(id))
	h.Write([]byte(hash))
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func encodeRecord(r *recordWire) ([]byte, error) {
	return json.Marshal(r)
}

// decodeRecordRaw decodes without integrity verification; callers that act
// on record contents (Pending/Verify/Admit) must use decodeRecord.
func decodeRecordRaw(v []byte) (*recordWire, error) {
	var r recordWire
	if err := json.Unmarshal(v, &r); err != nil {
		return nil, fmt.Errorf("%w: record decode: %v", ErrCorrupt, err)
	}
	return &r, nil
}

func decodeRecord(id string, v []byte) (*recordWire, error) {
	r, err := decodeRecordRaw(v)
	if err != nil {
		return nil, err
	}
	if r.State != StatePending && r.State != StateCommitted {
		return nil, fmt.Errorf("%w: invalid state %d for id %q", ErrCorrupt, r.State, id)
	}
	if hashBytes(r.Payload) != r.Hash {
		return nil, fmt.Errorf("%w: payload hash mismatch for id %q seq %d", ErrCorrupt, id, r.Seq)
	}
	if recordChecksum(r.Seq, r.State, id, r.Hash, r.Payload) != r.Checksum {
		return nil, fmt.Errorf("%w: record checksum mismatch for id %q seq %d", ErrCorrupt, id, r.Seq)
	}
	return r, nil
}

func toEntry(id string, r *recordWire) *Entry {
	return &Entry{
		ID:         id,
		Seq:        r.Seq,
		State:      r.State,
		Payload:    append([]byte(nil), r.Payload...),
		Hash:       r.Hash,
		ReceivedAt: r.ReceivedAt,
	}
}
