package snapshot

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/snapshot/plan"
)

const testFullDB = "testdata/db-and-wals/backup.db"

func mustWritePolicy(t *testing.T, store *Store, p *RetentionPolicy) {
	t.Helper()
	if err := WriteRetentionPolicyFile(store.retentionPolicyPath, p); err != nil {
		t.Fatalf("failed to write retention policy: %v", err)
	}
}

func mustWriteRawPolicy(t *testing.T, store *Store, content string) {
	t.Helper()
	if err := os.WriteFile(store.retentionPolicyPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write raw policy: %v", err)
	}
}

// markSnapshotForeign removes the local origin marker from a snapshot,
// simulating a directory copied into the Store from outside.
func markSnapshotForeign(t *testing.T, store *Store, id string) {
	t.Helper()
	if err := os.Remove(filepath.Join(store.Dir(), id, originMarkerFile)); err != nil {
		t.Fatalf("failed to remove origin marker from %s: %v", id, err)
	}
}

func snapshotDirExists(store *Store, id string) bool {
	return fsutil.DirExists(filepath.Join(store.Dir(), id))
}

func listReconcileRecords(t *testing.T, store *Store) []*ReconcileRecord {
	t.Helper()
	paths, err := reconcileRecordPaths(store.Dir())
	if err != nil {
		t.Fatalf("listing reconcile records: %v", err)
	}
	sort.Strings(paths)
	recs := make([]*ReconcileRecord, 0, len(paths))
	for _, p := range paths {
		rec, err := readReconcileRecord(p)
		if err != nil {
			t.Fatalf("reading reconcile record %s: %v", p, err)
		}
		recs = append(recs, rec)
	}
	return recs
}

func createLocalFulls(t *testing.T, store *Store, ids ...string) {
	t.Helper()
	for i, id := range ids {
		createSnapshotInStore(t, store, id, uint64(100*(i+1)), 2, 1, testFullDB)
	}
}

func TestReconcile_NoPolicy(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	createLocalFulls(t, store, "2-100-1704807719996")

	rec, err := store.Reconcile()
	if !errors.Is(err, ErrNoRetentionPolicy) {
		t.Fatalf("expected ErrNoRetentionPolicy, got %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil record without policy, got %+v", rec)
	}
	if recs := listReconcileRecords(t, store); len(recs) != 0 {
		t.Fatalf("expected no reconcile records, got %d", len(recs))
	}
}

func TestReconcile_InvalidPolicyDeletesNothing(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	f1, f2 := "2-100-1704807719996", "2-200-1704807720976"
	createLocalFulls(t, store, f1, f2)

	mustWriteRawPolicy(t, store, `{not valid json`)
	rec, err := store.Reconcile()
	if err == nil {
		t.Fatal("expected error for malformed policy")
	}
	if rec == nil || rec.Status != ReconcileAbortedInvalidPolicy {
		t.Fatalf("expected aborted_invalid_policy record, got %+v", rec)
	}
	if !snapshotDirExists(store, f1) || !snapshotDirExists(store, f2) {
		t.Fatal("malformed policy must not delete any snapshot")
	}

	mustWriteRawPolicy(t, store, `{"retain_full":0}`)
	rec, err = store.Reconcile()
	if err == nil || rec.Status != ReconcileAbortedInvalidPolicy {
		t.Fatalf("expected abort for retain_full=0, got %v %+v", err, rec)
	}
	if !snapshotDirExists(store, f1) || !snapshotDirExists(store, f2) {
		t.Fatal("invalid policy must not delete any snapshot")
	}

	mustWriteRawPolicy(t, store, `{"retain_full":1,"retain_extra":2}`)
	if _, err = store.Reconcile(); err == nil {
		t.Fatal("expected unknown policy field to be rejected")
	}
	if !snapshotDirExists(store, f1) || !snapshotDirExists(store, f2) {
		t.Fatal("unknown-field policy must not delete any snapshot")
	}

	if fsutil.FileExists(store.reconcilePlanPath) {
		t.Fatal("invalid policy runs must not leave a plan file")
	}
}

func TestReconcile_DanglingPinDeletesNothing(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	f1, f2 := "2-100-1704807719996", "2-200-1704807720976"
	createLocalFulls(t, store, f1, f2)
	mustWritePolicy(t, store, &RetentionPolicy{RetainFull: 1, Pinned: []string{"2-999-1704809999000"}})

	rec, err := store.Reconcile()
	if err == nil {
		t.Fatal("expected dangling pin to abort reconciliation")
	}
	if rec == nil || rec.Status != ReconcileAbortedInvalidPolicy {
		t.Fatalf("expected abort record, got %+v", rec)
	}
	if !snapshotDirExists(store, f1) || !snapshotDirExists(store, f2) {
		t.Fatal("dangling pin must not delete any snapshot")
	}
	if len(rec.Observed) != 2 {
		t.Fatalf("expected 2 observed snapshots in record, got %d", len(rec.Observed))
	}
}

func TestReconcile_RetainsNewestFulls(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	f1, f2, f3 := "2-100-1704807719996", "2-200-1704807720976", "2-300-1704807721976"
	createLocalFulls(t, store, f1, f2, f3)
	mustWritePolicy(t, store, &RetentionPolicy{RetainFull: 2})

	rec, err := store.Reconcile()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rec.Status != ReconcileApplied {
		t.Fatalf("expected applied, got %s", rec.Status)
	}
	if rec.SnapshotsDeleted != 1 || len(rec.Deleted) != 1 || rec.Deleted[0] != f1 {
		t.Fatalf("unexpected deletions: %+v", rec.Deleted)
	}
	if snapshotDirExists(store, f1) {
		t.Fatalf("%s should have been deleted", f1)
	}
	if !snapshotDirExists(store, f2) || !snapshotDirExists(store, f3) {
		t.Fatal("retained fulls must survive")
	}
	if len(rec.Retained) != 2 {
		t.Fatalf("expected 2 retained IDs, got %v", rec.Retained)
	}
	if rec.ConsolidatedFrom != "" {
		t.Fatalf("did not expect consolidation, got %s->%s", rec.ConsolidatedFrom, rec.ConsolidatedTo)
	}

	// Retained snapshots remain intact and openable.
	if _, rc, err := store.Open(f3); err != nil {
		t.Fatalf("opening retained full %s: %v", f3, err)
	} else {
		rc.Close()
	}
}

func TestReconcile_PinnedOldFullSurvives(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	f1, f2, f3 := "2-100-1704807719996", "2-200-1704807720976", "2-300-1704807721976"
	createLocalFulls(t, store, f1, f2, f3)
	mustWritePolicy(t, store, &RetentionPolicy{RetainFull: 1, Pinned: []string{f1}})

	rec, err := store.Reconcile()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !snapshotDirExists(store, f1) {
		t.Fatalf("pinned oldest full %s must survive", f1)
	}
	if snapshotDirExists(store, f2) {
		t.Fatalf("out-of-window local %s should be deleted", f2)
	}
	if !snapshotDirExists(store, f3) {
		t.Fatalf("newest full %s must survive", f3)
	}
	if len(rec.Pinned) != 1 || rec.Pinned[0] != f1 {
		t.Fatalf("expected f1 pinned in record, got %v", rec.Pinned)
	}
	// The pinned full remains openable.
	if _, rc, err := store.Open(f1); err != nil {
		t.Fatalf("opening pinned full: %v", err)
	} else {
		rc.Close()
	}

	// Remove the policy: legacy reaping is again free to consolidate, proving
	// the protection derives from the active policy.
	if err := os.Remove(store.retentionPolicyPath); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Reap(); err != nil {
		t.Fatalf("legacy reap after policy removal: %v", err)
	}
	if snapshotDirExists(store, f1) {
		t.Fatal("once unpinned, the old full should be reaped by legacy reaping")
	}
}

func TestReconcile_ForeignSnapshotSurvives(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	f1, f2, f3 := "2-100-1704807719996", "2-200-1704807720976", "2-300-1704807721976"
	createLocalFulls(t, store, f1, f2, f3)
	markSnapshotForeign(t, store, f1) // operator copied f1 in
	mustWritePolicy(t, store, &RetentionPolicy{RetainFull: 1})

	rec, err := store.Reconcile()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !snapshotDirExists(store, f1) {
		t.Fatal("foreign snapshot must never be deleted")
	}
	if snapshotDirExists(store, f2) {
		t.Fatalf("local out-of-window %s should be deleted", f2)
	}
	if !snapshotDirExists(store, f3) {
		t.Fatal("newest full must survive")
	}
	if len(rec.Foreign) != 1 || rec.Foreign[0] != f1 {
		t.Fatalf("expected f1 listed foreign, got %v", rec.Foreign)
	}
}

func TestReconcile_TrailingFoldStopsAtForeignIncremental(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()

	fullID := "2-100-1704807719996"
	incA := "2-200-1704807720976"
	incB := "2-300-1704807721976"
	createSnapshotInStore(t, store, fullID, 100, 2, 1, testFullDB)
	createSnapshotInStore(t, store, incA, 200, 2, 1, "", "testdata/db-and-wals/wal-00")
	createSnapshotInStore(t, store, incB, 300, 2, 1, "", "testdata/db-and-wals/wal-01")
	markSnapshotForeign(t, store, incB)
	mustWritePolicy(t, store, &RetentionPolicy{RetainFull: 1})

	rec, err := store.Reconcile()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rec.ConsolidatedFrom != fullID {
		t.Fatalf("expected consolidation from %s, got %s", fullID, rec.ConsolidatedFrom)
	}
	if rec.ConsolidatedTo == "" {
		t.Fatal("expected a consolidated snapshot ID")
	}
	if snapshotDirExists(store, incA) {
		t.Fatal("folded local incremental should be removed")
	}
	if !snapshotDirExists(store, incB) {
		t.Fatal("foreign incremental must survive")
	}
	if !snapshotDirExists(store, rec.ConsolidatedTo) {
		t.Fatal("consolidated snapshot directory missing")
	}
	if rec.WALsCheckpointed != 1 {
		t.Fatalf("expected 1 checkpointed WAL, got %d", rec.WALsCheckpointed)
	}

	// The rename keeps the local provenance marker on the consolidated full.
	set, err := store.getSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	consolidated, ok := set.WithID(rec.ConsolidatedTo)
	if !ok || !consolidated.IsLocal() || !consolidated.typ.IsFull() {
		t.Fatalf("consolidated snapshot must remain a local full, got %+v", consolidated)
	}

	// The protected incremental must still resolve, against the consolidated
	// full now sitting immediately before it.
	if _, rc, err := store.Open(incB); err != nil {
		t.Fatalf("opening surviving foreign incremental after fold: %v", err)
	} else {
		rc.Close()
	}
}

func TestReconcile_ProtectedNewestFullNotRewritten(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	f1, f2 := "2-100-1704807719996", "2-200-1704807720976"
	createLocalFulls(t, store, f1, f2)
	mustWritePolicy(t, store, &RetentionPolicy{RetainFull: 1, Pinned: []string{f2}})

	rec, err := store.Reconcile()
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if rec.ConsolidatedFrom != "" {
		t.Fatalf("pinned newest full must not be consolidated, got %s", rec.ConsolidatedFrom)
	}
	if !snapshotDirExists(store, f2) {
		t.Fatal("pinned newest full must keep its directory and ID")
	}
	if snapshotDirExists(store, f1) {
		t.Fatal("out-of-window local full should be deleted")
	}
}

func TestReconcile_RecordsPerRunAndIdempotence(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	f1, f2, f3 := "2-100-1704807719996", "2-200-1704807720976", "2-300-1704807721976"
	createLocalFulls(t, store, f1, f2, f3)
	mustWritePolicy(t, store, &RetentionPolicy{RetainFull: 1})

	first, err := store.Reconcile()
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if first.SnapshotsDeleted != 2 {
		t.Fatalf("expected 2 deletions first run, got %d", first.SnapshotsDeleted)
	}
	second, err := store.Reconcile()
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if second.SnapshotsDeleted != 0 || len(second.Deleted) != 0 {
		t.Fatalf("expected converged no-op second run, got %v", second.Deleted)
	}
	if len(second.Observed) != 1 {
		t.Fatalf("expected 1 observed snapshot second run, got %d", len(second.Observed))
	}
	if first.ID == second.ID {
		t.Fatal("each run must get its own record file")
	}
	recs := listReconcileRecords(t, store)
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if recs[0].ID > recs[1].ID {
		t.Fatal("record IDs must be ordered by run time")
	}
	for _, r := range recs {
		if r.Status != ReconcileApplied || r.FinishedAt == nil {
			t.Fatalf("record %s not finished applied: %+v", r.ID, r)
		}
	}
}

func TestReconcile_AutoReaperUsesPolicy(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	store.SetReapThreshold(3)
	f1, f2, f3 := "2-100-1704807719996", "2-200-1704807720976", "2-300-1704807721976"
	createLocalFulls(t, store, f1, f2, f3)
	// Pin the oldest full: legacy reaping would remove it, reconciliation must
	// keep it.
	mustWritePolicy(t, store, &RetentionPolicy{RetainFull: 1, Pinned: []string{f1}})

	store.signalReap()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if store.Len() == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if store.Len() != 2 {
		t.Fatalf("expected policy-driven auto reconcile to leave 2 snapshots, got %d", store.Len())
	}
	if !snapshotDirExists(store, f1) {
		t.Fatal("pinned oldest snapshot must survive auto reconciliation")
	}
	if !snapshotDirExists(store, f3) {
		t.Fatal("newest full must survive auto reconciliation")
	}
}

func TestReconcile_OriginMarkerClassification(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	id := "2-100-1704807719996"
	createLocalFulls(t, store, id)
	if !fsutil.FileExists(filepath.Join(store.Dir(), id, originMarkerFile)) {
		t.Fatal("sink-created snapshot must carry the ORIGIN marker")
	}
	set, err := store.getSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	sn, ok := set.WithID(id)
	if !ok || !sn.IsLocal() {
		t.Fatal("expected sink-created snapshot to classify as local")
	}

	markSnapshotForeign(t, store, id)
	set, err = store.getSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	sn, ok = set.WithID(id)
	if !ok || sn.IsLocal() {
		t.Fatal("expected marker-less snapshot to classify as foreign")
	}
}

// injectInterruptedReconcile writes a "running" record and a reconcile plan
// removing removeID, simulating a crash mid-reconcile.
func injectInterruptedReconcile(t *testing.T, store *Store, removeID string) *ReconcileRecord {
	t.Helper()
	rec := &ReconcileRecord{
		ID:               fmt.Sprintf("%s%d%s", reconcileRecordPrefix, 1700000000000, reconcileRecordSuffix),
		StartedAt:        time.Now(),
		Status:           ReconcileRunning,
		Observed:         []observedSnapshot{},
		Deleted:          []string{removeID},
		SnapshotsDeleted: 1,
	}
	if err := writeReconcileRecord(store.Dir(), rec); err != nil {
		t.Fatalf("writing record: %v", err)
	}
	p := plan.New()
	p.RecordPath = recordFilePath(store.Dir(), rec)
	p.AddRemoveAll(filepath.Join(store.Dir(), removeID))
	p.NReaped = 1
	if err := plan.WriteToFile(p, store.reconcilePlanPath); err != nil {
		t.Fatalf("writing reconcile plan: %v", err)
	}
	return rec
}

func TestReconcile_ResumePlanOnRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	f1, f2, f3 := "2-100-1704807719996", "2-200-1704807720976", "2-300-1704807721976"
	createLocalFulls(t, store, f1, f2, f3)
	rec := injectInterruptedReconcile(t, store, f1)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopening resumes and converges the interrupted plan.
	store2, err := NewStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store2.Close()

	if snapshotDirExists(store2, f1) {
		t.Fatal("interrupted reconcile plan should have completed on restart")
	}
	if !snapshotDirExists(store2, f2) || !snapshotDirExists(store2, f3) {
		t.Fatal("plan only removed f1; f2/f3 must remain")
	}
	if fsutil.FileExists(store2.reconcilePlanPath) {
		t.Fatal("resume must remove the reconcile plan file")
	}
	recs := listReconcileRecords(t, store2)
	if len(recs) != 1 || recs[0].Status != ReconcileApplied || recs[0].FinishedAt == nil {
		t.Fatalf("expected single finalized applied record, got %+v", recs)
	}
	if recs[0].ID != rec.ID {
		t.Fatalf("record identity changed: %s != %s", recs[0].ID, rec.ID)
	}
}

func TestReconcile_OrphanRunningRecordMarkedInterrupted(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	f1 := "2-100-1704807719996"
	createLocalFulls(t, store, f1)
	rec := &ReconcileRecord{
		ID:        fmt.Sprintf("%s%d%s", reconcileRecordPrefix, 1700000000001, reconcileRecordSuffix),
		StartedAt: time.Now(),
		Status:    ReconcileRunning,
		Observed:  []observedSnapshot{},
	}
	if err := writeReconcileRecord(store.Dir(), rec); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := NewStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	recs := listReconcileRecords(t, store2)
	if len(recs) != 1 || recs[0].Status != ReconcileInterrupted {
		t.Fatalf("expected orphan running record marked interrupted, got %+v", recs)
	}
	if !snapshotDirExists(store2, f1) {
		t.Fatal("orphan record run deleted nothing; snapshot must remain")
	}
}

func TestReconcile_StaleRecordTempFileSwept(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	f1 := "2-100-1704807719996"
	createLocalFulls(t, store, f1)
	stale := filepath.Join(store.Dir(), reconcileRecordPrefix+"1700000000002"+reconcileRecordSuffix+tmpSuffix)
	if err := os.WriteFile(stale, []byte("partial"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store2, err := NewStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()
	if fsutil.PathExists(stale) {
		t.Fatal("stale reconcile record temp file should be removed on startup")
	}
}

func TestReap_ResumesInterruptedReconcilePlan(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer store.Close()
	f1, f2, f3 := "2-100-1704807719996", "2-200-1704807720976", "2-300-1704807721976"
	createLocalFulls(t, store, f1, f2, f3)
	injectInterruptedReconcile(t, store, f1)

	n, _, err := store.Reap()
	if err != nil {
		t.Fatalf("Reap resuming reconcile: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected resumed reconcile to report 1 deletion, got %d", n)
	}
	if snapshotDirExists(store, f1) {
		t.Fatal("resumed reconcile must remove f1")
	}
	if !snapshotDirExists(store, f2) || !snapshotDirExists(store, f3) {
		t.Fatal("resumed reconcile must not expand into a new reap")
	}
	if fsutil.FileExists(store.reconcilePlanPath) {
		t.Fatal("plan file must be removed after resume")
	}
}
