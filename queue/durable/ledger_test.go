package durable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func newTestLedger(t *testing.T) (*Ledger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue-state.db")
	l, err := Open(path)
	if err != nil {
		t.Fatalf("failed to open ledger: %v", err)
	}
	return l, path
}

func reopenLedger(t *testing.T, path string) (*Ledger, error) {
	t.Helper()
	return Open(path)
}

// TestLedger_AdmitAndReopen verifies fsynced records survive reopen with
// full content and that Pending is seq-ordered.
func TestLedger_AdmitAndReopen(t *testing.T) {
	l, path := newTestLedger(t)

	payloads := map[string][]byte{
		"a": []byte("payload-a"),
		"b": []byte("payload-b"),
		"c": []byte("payload-c"),
	}
	var seqs []int64
	for _, id := range []string{"a", "b", "c"} {
		already, committed, seq, err := l.Admit(id, payloads[id])
		if err != nil || already || committed || seq == 0 {
			t.Fatalf("unexpected admit result for %s: %v %v %d %v", id, already, committed, seq, err)
		}
		seqs = append(seqs, seq)
	}
	if seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("expected monotonic seqs 1,2,3 got %v", seqs)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	l2, err := reopenLedger(t, path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	pending, err := l2.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 3 {
		t.Fatalf("expected 3 pending entries, got %d", len(pending))
	}
	for i, id := range []string{"a", "b", "c"} {
		if pending[i].ID != id || pending[i].Seq != int64(i+1) {
			t.Fatalf("entry %d mismatch: %+v", i, pending[i])
		}
		if !bytes.Equal(pending[i].Payload, payloads[id]) {
			t.Fatalf("payload mismatch for %s: %q", id, pending[i].Payload)
		}
		if pending[i].State != StatePending || pending[i].Hash == "" {
			t.Fatalf("unexpected entry state/hash: %+v", pending[i])
		}
	}
}

// TestLedger_AdmitIdempotent verifies same-id same-payload admission never
// creates a second record, and that different payloads conflict.
func TestLedger_AdmitIdempotent(t *testing.T) {
	l, path := newTestLedger(t)
	defer l.Close()

	_, _, seq1, err := l.Admit("x", []byte("p"))
	if err != nil {
		t.Fatalf("first admit: %v", err)
	}
	already, committed, seq2, err := l.Admit("x", []byte("p"))
	if err != nil || !already || committed || seq2 != seq1 {
		t.Fatalf("identical retry must be idempotent: %v %v %d %v", already, committed, seq2, err)
	}

	found, err := l.MarkCommitted("x")
	if err != nil || !found {
		t.Fatalf("mark committed: %v %v", found, err)
	}
	already, committed, _, err = l.Admit("x", []byte("p"))
	if err != nil || !already || !committed {
		t.Fatalf("post-commit identical retry should report committed: %v %v %v", already, committed, err)
	}

	_, _, _, err = l.Admit("x", []byte("different"))
	if !errors.Is(err, ErrIDConflict) {
		t.Fatalf("expected ErrIDConflict, got %v", err)
	}

	stats, err := l.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats["total"] != 1 {
		t.Fatalf("conflict must not create a record, total=%v", stats["total"])
	}

	// Reopen verifies cleanly.
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	l2, err := reopenLedger(t, path)
	if err != nil {
		t.Fatalf("reopen after conflict: %v", err)
	}
	defer l2.Close()
}

// TestLedger_MarkPruneVerify covers committed transitions, pending
// exclusion, watermark advance and clean verification after prune.
func TestLedger_MarkPruneVerify(t *testing.T) {
	l, path := newTestLedger(t)

	for i := 0; i < 5; i++ {
		if _, _, _, err := l.Admit(fmt.Sprintf("id-%d", i), []byte(fmt.Sprintf("payload-%d", i))); err != nil {
			t.Fatalf("admit %d: %v", i, err)
		}
	}
	for _, id := range []string{"id-0", "id-1"} {
		if _, err := l.MarkCommitted(id); err != nil {
			t.Fatalf("mark %s: %v", id, err)
		}
	}

	pending, err := l.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 3 || pending[0].ID != "id-2" || pending[2].ID != "id-4" {
		t.Fatalf("unexpected pending set: %+v", pending)
	}

	// Keep one committed record; id-0 is pruned, watermark advances to 1.
	pruned, err := l.PruneCommitted(1)
	if err != nil || pruned != 1 {
		t.Fatalf("prune: %d %v", pruned, err)
	}
	if err := l.Verify(); err != nil {
		t.Fatalf("verify after prune: %v", err)
	}
	stats, err := l.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats["total"] != 4 || stats["watermark"] != int64(1) {
		t.Fatalf("unexpected stats after prune: %v", stats)
	}

	// id-1/id-2 form a contiguous committed prefix before the oldest
	// Pending (id-3); both are prunable while id-3/id-4 stay pinned.
	if _, err := l.MarkCommitted("id-2"); err != nil {
		t.Fatalf("mark id-2: %v", err)
	}
	pruned, err = l.PruneCommitted(0)
	if err != nil || pruned != 2 {
		t.Fatalf("contiguous committed prefix should be prunable: %d %v", pruned, err)
	}
	if err := l.Verify(); err != nil {
		t.Fatalf("verify after second prune: %v", err)
	}

	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	l2, err := reopenLedger(t, path)
	if err != nil {
		t.Fatalf("reopen after prune: %v", err)
	}
	defer l2.Close()
	pending, err = l2.Pending()
	if err != nil {
		t.Fatalf("pending after reopen: %v", err)
	}
	if len(pending) != 2 || pending[0].Seq != 4 {
		t.Fatalf("unexpected pending after reopen: %+v", pending)
	}
}

// mutateRecordRaw opens the bbolt file directly and rewrites one record's
// raw bytes, returning the id of the modified record.
func mutateRecordRaw(t *testing.T, path string, mutate func(v []byte) []byte) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		eb := tx.Bucket(entriesBucket)
		k, v := eb.Cursor().First()
		nv := mutate(append([]byte(nil), v...))
		return eb.Put(k, nv)
	}); err != nil {
		t.Fatalf("raw mutate: %v", err)
	}
}

func deleteRecordRaw(t *testing.T, path, id string) {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(entriesBucket).Delete([]byte(id))
	}); err != nil {
		t.Fatalf("raw delete: %v", err)
	}
}

// TestLedger_VerifyCorruption covers checksum tampering, sequence gaps and a
// truncated file.
func TestLedger_VerifyCorruption(t *testing.T) {
	// 1) Flipping a checksum hex digit trips checksum verification.
	l, path := newTestLedger(t)
	for i := 0; i < 4; i++ {
		if _, _, _, err := l.Admit(fmt.Sprintf("id-%d", i), []byte(fmt.Sprintf("p-%d", i))); err != nil {
			t.Fatalf("admit: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	mutateRecordRaw(t, path, func(v []byte) []byte {
		marker := []byte(`"checksum":"`)
		idx := bytes.Index(v, marker)
		if idx < 0 {
			t.Fatalf("checksum field not found in record")
		}
		pos := idx + len(marker)
		if v[pos] == '0' {
			v[pos] = '1'
		} else {
			v[pos] = '0'
		}
		return v
	})
	if _, err := reopenLedger(t, path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("checksum tamper should be ErrCorrupt, got %v", err)
	}

	// 2) Deleting a record creates a sequence gap.
	l, path = newTestLedger(t)
	for i := 0; i < 4; i++ {
		if _, _, _, err := l.Admit(fmt.Sprintf("id-%d", i), []byte(fmt.Sprintf("p-%d", i))); err != nil {
			t.Fatalf("admit: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	deleteRecordRaw(t, path, "id-2")
	if _, err := reopenLedger(t, path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("sequence gap should be ErrCorrupt, got %v", err)
	}

	// 3) Corrupting both bbolt meta pages must fail to open (no panic).
	l, path = newTestLedger(t)
	if _, _, _, err := l.Admit("id-0", []byte("p")); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open raw file: %v", err)
	}
	garbage := bytes.Repeat([]byte{0xff}, 16)
	// Meta structs live at the start of pages 0 and 1 (after the 16-byte
	// page header); hit their checksum fields on both pages so no valid
	// meta remains. The page size is OS-dependent (16 KiB on arm64 macOS).
	pageSz := int64(os.Getpagesize())
	if _, err := f.WriteAt(garbage, 16+16); err != nil {
		t.Fatalf("corrupt meta page 0: %v", err)
	}
	if _, err := f.WriteAt(garbage, pageSz+16+16); err != nil {
		t.Fatalf("corrupt meta page 1: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close raw file: %v", err)
	}
	if l3, err := reopenLedger(t, path); err == nil {
		_ = l3.Close()
		t.Fatalf("meta-corrupt ledger must fail to open")
	}
}

// TestLedger_ConcurrentAdmit verifies race-free admission: unique ids get
// unique consecutive seqs, and concurrent retries of one id produce exactly
// one record.
func TestLedger_ConcurrentAdmit(t *testing.T) {
	l, _ := newTestLedger(t)
	defer l.Close()

	const n = 100
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("id-%d", i)
			if _, _, _, err := l.Admit(id, []byte(id)); err != nil {
				t.Errorf("admit %s: %v", id, err)
			}
		}(i)
	}
	wg.Wait()

	stats, err := l.Stats()
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats["total"] != n {
		t.Fatalf("expected %d records, got %v", n, stats["total"])
	}
	pending, err := l.Pending()
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	seen := make(map[int64]bool, n)
	for _, e := range pending {
		if seen[e.Seq] {
			t.Fatalf("duplicate seq %d", e.Seq)
		}
		seen[e.Seq] = true
	}

	// Concurrent identical retries of a single id.
	const r = 32
	var wg2 sync.WaitGroup
	var newCount, oldCount int64
	var mu sync.Mutex
	wg2.Add(r)
	for i := 0; i < r; i++ {
		go func() {
			defer wg2.Done()
			already, _, _, err := l.Admit("hot-id", []byte("same"))
			if err != nil {
				t.Errorf("retry admit: %v", err)
				return
			}
			mu.Lock()
			if already {
				oldCount++
			} else {
				newCount++
			}
			mu.Unlock()
		}()
	}
	wg2.Wait()
	if newCount != 1 || oldCount != r-1 {
		t.Fatalf("expected 1 new and %d idempotent admits, got new=%d old=%d", r-1, newCount, oldCount)
	}
}

// TestLedger_ClosedRejects verifies operations after Close fail fast.
func TestLedger_ClosedRejects(t *testing.T) {
	l, _ := newTestLedger(t)
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, _, _, err := l.Admit("z", []byte("p")); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
	if _, err := l.MarkCommitted("z"); !errors.Is(err, ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestGate(t *testing.T) {
	var g Gate
	if fatal, _ := g.Fatal(); fatal {
		t.Fatalf("gate must start open")
	}
	if !g.Trip("first") {
		t.Fatalf("first trip must report true")
	}
	if g.Trip("second") {
		t.Fatalf("second trip must report false")
	}
	fatal, reason := g.Fatal()
	if !fatal || reason != "first" {
		t.Fatalf("first reason must win, got %v %q", fatal, reason)
	}
}
