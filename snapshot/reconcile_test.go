package snapshot

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
)

const (
	reconcileFullDB  = "testdata/db-and-wals/backup.db"
	reconcileFull2DB = "testdata/db-and-wals/full2.db"
	reconcileWAL     = "testdata/db-and-wals/wal-00"
)

func newTestReconciler(t *testing.T, dir string, staging ...string) *Reconciler {
	t.Helper()
	return NewReconciler(dir, staging, log.New(os.Stderr, "[reconcile-test] ", log.LstdFlags))
}

// createOrphanIncremental builds a store with a full baseline and an
// incremental, then removes the baseline so only an incremental without a
// full remains -- a broken chain.
func createOrphanIncremental(t *testing.T, store *Store) string {
	t.Helper()
	baseID := "2-10-1704807719000"
	incID := "2-20-1704807720000"
	createSnapshotInStore(t, store, baseID, 10, 2, 1, reconcileFullDB)
	createSnapshotInStore(t, store, incID, 20, 2, 1, "", reconcileWAL)
	if err := fsutil.RemoveAll(filepath.Join(store.Dir(), baseID)); err != nil {
		t.Fatalf("failed to remove full baseline to simulate broken chain: %v", err)
	}
	return incID
}

func Test_Reconcile_EmptyStore_OK(t *testing.T) {
	dir := t.TempDir()
	rep, err := newTestReconciler(t, dir).Reconcile(nil)
	if err != nil {
		t.Fatalf("reconcile on empty store failed: %v", err)
	}
	if rep.Verdict != VerdictOK {
		t.Fatalf("expected VerdictOK on empty store, got %v", rep.Verdict)
	}
	if rep.FullNeeded {
		t.Fatalf("did not expect FULL_NEEDED on empty store")
	}
	if len(rep.RemovedTransient) != 0 {
		t.Fatalf("expected no removals on empty store, got %v", rep.RemovedTransient)
	}
}

func Test_Reconcile_HealthyChain_RemovesOnlyNodeTmp(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	createSnapshotInStore(t, store, "2-10-1704807719000", 10, 2, 1, reconcileFullDB)
	createSnapshotInStore(t, store, "2-20-1704807720000", 20, 2, 1, "", reconcileWAL)

	// Interrupted incoming install: node-owned tmp directory with junk.
	tmpDir := filepath.Join(dir, "2-99-1704807799999.tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("mkdir tmp failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, "data.db"), []byte("junk"), 0644); err != nil {
		t.Fatalf("write junk failed: %v", err)
	}

	rep, err := newTestReconciler(t, dir).Reconcile(nil)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if rep.Verdict != VerdictOK {
		t.Fatalf("expected VerdictOK, got %s: %s", rep.Verdict, rep.Reason)
	}
	if len(rep.RemovedTransient) != 1 || rep.RemovedTransient[0] != tmpDir {
		t.Fatalf("expected only the tmp dir removed, got %v", rep.RemovedTransient)
	}
	if fsutil.PathExists(tmpDir) {
		t.Fatalf("tmp dir should have been removed")
	}
	if store.Len() != 2 {
		t.Fatalf("committed snapshots must be untouched, expected 2 got %d", store.Len())
	}

	// Rerun must converge: no further removals, no error.
	rep2, err := newTestReconciler(t, dir).Reconcile(nil)
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if rep2.Verdict != VerdictOK || len(rep2.RemovedTransient) != 0 {
		t.Fatalf("expected convergent no-op rerun, got verdict=%s removed=%v", rep2.Verdict, rep2.RemovedTransient)
	}
}

func Test_Reconcile_OrphanIncremental_FullRequired_Converges(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	incID := createOrphanIncremental(t, store)

	rep, err := newTestReconciler(t, dir).Reconcile(nil)
	if err != nil {
		t.Fatalf("reconcile on broken chain should not hard-error, got: %v", err)
	}
	if rep.Verdict != VerdictFullRequired {
		t.Fatalf("expected VerdictFullRequired, got %v (%s)", rep.Verdict, rep.Reason)
	}
	if !rep.FullNeeded || !fsutil.FileExists(filepath.Join(dir, fullNeededFile)) {
		t.Fatalf("expected FULL_NEEDED flag to be set")
	}
	// The committed orphan incremental must not be deleted.
	if !fsutil.DirExists(filepath.Join(dir, incID)) {
		t.Fatalf("committed orphan incremental must not be removed")
	}

	// A rerun must converge to the same verdict without error and without
	// deleting anything.
	rep2, err := newTestReconciler(t, dir).Reconcile(nil)
	if err != nil {
		t.Fatalf("rerun reconcile failed: %v", err)
	}
	if rep2.Verdict != VerdictFullRequired || !rep2.FullNeeded {
		t.Fatalf("expected stable VerdictFullRequired on rerun, got %v", rep2.Verdict)
	}
	if len(rep2.RemovedTransient) != 0 {
		t.Fatalf("expected no removals on rerun, got %v", rep2.RemovedTransient)
	}
	if !fsutil.DirExists(filepath.Join(dir, incID)) {
		t.Fatalf("committed orphan incremental must still be present")
	}
}

func Test_Reconcile_FullInstallAfterBrokenChain_ConvergesToOK(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	createOrphanIncremental(t, store)

	rep, err := newTestReconciler(t, dir).Reconcile(nil)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if rep.Verdict != VerdictFullRequired {
		t.Fatalf("expected broken chain, got %v", rep.Verdict)
	}

	// The leader delivers a self-contained full snapshot at a newer index.
	// Closing the sink runs reconciliation internally; the successful install
	// clears FULL_NEEDED and anchors the chain.
	newFullID := "2-30-1704807730000"
	createSnapshotInStore(t, store, newFullID, 30, 2, 1, reconcileFull2DB)

	if fsutil.FileExists(filepath.Join(dir, fullNeededFile)) {
		t.Fatalf("FULL_NEEDED should be cleared after a full install")
	}
	meta, rc, err := store.Open(newFullID)
	if err != nil {
		t.Fatalf("failed to open the newly installed full snapshot: %v", err)
	}
	rc.Close()
	if meta.Index != 30 {
		t.Fatalf("unexpected snapshot index: %d", meta.Index)
	}

	rep2, err := store.Reconcile(nil)
	if err != nil {
		t.Fatalf("reconcile after full install failed: %v", err)
	}
	if rep2.Verdict != VerdictOK || rep2.FullNeeded {
		t.Fatalf("expected converged OK chain, got verdict=%s fullNeeded=%v: %s",
			rep2.Verdict, rep2.FullNeeded, rep2.Reason)
	}
}

func Test_Reconcile_ForeignEntries_PreservedAndNonBlocking(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()
	createSnapshotInStore(t, store, "2-10-1704807719000", 10, 2, 1, reconcileFullDB)

	foreignFile := filepath.Join(dir, "notes.txt")
	foreignDir := filepath.Join(dir, "operator-backup")
	if err := os.WriteFile(foreignFile, []byte("do not touch"), 0644); err != nil {
		t.Fatalf("write foreign file failed: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(foreignDir, "stuff"), 0755); err != nil {
		t.Fatalf("mkdir foreign dir failed: %v", err)
	}

	rep, err := newTestReconciler(t, dir).Reconcile(nil)
	if err != nil {
		t.Fatalf("foreign entries must not block reconciliation: %v", err)
	}
	if rep.Verdict != VerdictOK {
		t.Fatalf("expected OK, got %s", rep.Verdict)
	}
	if len(rep.Foreign) != 2 {
		t.Fatalf("expected 2 foreign entries reported, got %v", rep.Foreign)
	}
	if !fsutil.FileExists(foreignFile) || !fsutil.DirExists(foreignDir) {
		t.Fatalf("foreign entries must be left untouched")
	}
	// Reconciliation must still recognize the committed snapshot despite the
	// foreign entries being present.
	if rep.Snapshots != 1 {
		t.Fatalf("expected 1 committed snapshot recognized, got %d", rep.Snapshots)
	}
	if !fsutil.DirExists(filepath.Join(dir, "2-10-1704807719000")) {
		t.Fatalf("committed snapshot must be left in place")
	}
}

func Test_Reconcile_ExternallyCopiedValidSnapshot_Preserved(t *testing.T) {
	dir := t.TempDir()
	src, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create source store: %v", err)
	}
	defer src.Close()
	copiedID := "2-40-1704807740000"
	createSnapshotInStore(t, src, copiedID, 40, 2, 1, reconcileFullDB)

	// Simulate an operator copying a valid snapshot directory into the store.
	if err := fsutil.CopyDir(filepath.Join(src.Dir(), copiedID), filepath.Join(dir, copiedID)); err != nil {
		t.Fatalf("failed to copy snapshot in: %v", err)
	}

	rep, err := newTestReconciler(t, dir).Reconcile(nil)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if rep.Snapshots != 1 || rep.Verdict != VerdictOK {
		t.Fatalf("expected the copied snapshot to be accepted as committed, got %s", rep.Reason)
	}
	if !fsutil.DirExists(filepath.Join(dir, copiedID)) {
		t.Fatalf("externally copied snapshot must not be removed")
	}
}

func Test_Reconcile_SnapshotShapedInvalidDir_ConflictZeroMutation(t *testing.T) {
	dir := t.TempDir()

	// A directory that parses as a snapshot ID but is not a valid snapshot
	// (could be a half-finished external copy).
	badDir := filepath.Join(dir, "2-50-1704807750000")
	if err := os.MkdirAll(badDir, 0755); err != nil {
		t.Fatalf("mkdir bad snapshot dir failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "data.db"), []byte("not a database"), 0644); err != nil {
		t.Fatalf("write bad db failed: %v", err)
	}

	// A node-owned tmp dir that would normally be removed; on conflict it
	// must be left in place too.
	tmpDir := filepath.Join(dir, "2-99-1704807799999.tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("mkdir tmp failed: %v", err)
	}

	_, err := newTestReconciler(t, dir).Reconcile(nil)
	if err == nil {
		t.Fatalf("expected a conflict error, got nil")
	}
	if !errors.Is(err, ErrReconcileConflict) {
		t.Fatalf("expected ErrReconcileConflict, got %v", err)
	}
	var conflict *ReconcileConflictError
	if !errors.As(err, &conflict) || len(conflict.Reasons) == 0 {
		t.Fatalf("expected detailed conflict reasons, got %T: %v", err, err)
	}
	if !fsutil.DirExists(badDir) {
		t.Fatalf("conflict must not delete the suspicious snapshot-shaped directory")
	}
	if !fsutil.DirExists(tmpDir) {
		t.Fatalf("conflict must perform no removals at all")
	}
}

func Test_Reconcile_UnparseableTmpDir_Conflict(t *testing.T) {
	dir := t.TempDir()
	strangeTmp := filepath.Join(dir, "mystery.tmp")
	if err := os.MkdirAll(strangeTmp, 0755); err != nil {
		t.Fatalf("mkdir strange tmp failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(strangeTmp, "x"), []byte("x"), 0644); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	_, err := newTestReconciler(t, dir).Reconcile(nil)
	if !errors.Is(err, ErrReconcileConflict) {
		t.Fatalf("expected ErrReconcileConflict for unparseable tmp, got %v", err)
	}
	if !fsutil.DirExists(strangeTmp) {
		t.Fatalf("unrecognized tmp dir must not be removed")
	}
}

func Test_Reconcile_AuthoritativeChainMatchesLocal_OK(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()
	fullID := "2-10-1704807719000"
	incID := "2-20-1704807720000"
	createSnapshotInStore(t, store, fullID, 10, 2, 1, reconcileFullDB)
	createSnapshotInStore(t, store, incID, 20, 2, 1, "", reconcileWAL)

	chain := &IncomingChain{
		Target: ChainLink{ID: incID, Term: 2, Index: 20, Full: false},
		Links: []ChainLink{
			{ID: fullID, Term: 2, Index: 10, Full: true},
			{ID: incID, Term: 2, Index: 20, Full: false},
		},
	}
	rep, err := store.Reconcile(chain)
	if err != nil {
		t.Fatalf("matching chain should reconcile: %v", err)
	}
	if rep.Verdict != VerdictOK {
		t.Fatalf("expected OK, got %s: %s", rep.Verdict, rep.Reason)
	}
}

func Test_Reconcile_LocalAheadOfTarget_Conflict(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()
	createSnapshotInStore(t, store, "2-20-1704807720000", 20, 2, 1, reconcileFullDB)

	chain := &IncomingChain{
		Target: ChainLink{ID: "2-5-1704807705000", Term: 2, Index: 5, Full: true},
	}
	if _, err := store.Reconcile(chain); !errors.Is(err, ErrReconcileConflict) {
		t.Fatalf("expected conflict when local is ahead of target, got %v", err)
	}
}

func Test_Reconcile_LinkIdentityMismatch_Conflict(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()
	fullID := "2-10-1704807719000"
	createSnapshotInStore(t, store, fullID, 10, 2, 1, reconcileFullDB)

	chain := &IncomingChain{
		Target: ChainLink{ID: fullID, Term: 2, Index: 10, Full: true},
		Links: []ChainLink{
			{ID: fullID, Term: 9, Index: 10, Full: true}, // term disagrees
		},
	}
	if _, err := store.Reconcile(chain); !errors.Is(err, ErrReconcileConflict) {
		t.Fatalf("expected identity-mismatch conflict, got %v", err)
	}
	if store.Len() != 1 {
		t.Fatalf("conflict must not mutate snapshots, got %d", store.Len())
	}
}

func Test_Reconcile_LinkKindMismatch_Conflict(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()
	fullID := "2-10-1704807719000"
	createSnapshotInStore(t, store, fullID, 10, 2, 1, reconcileFullDB)

	chain := &IncomingChain{
		Target: ChainLink{ID: fullID, Term: 2, Index: 10, Full: true},
		Links: []ChainLink{
			{ID: fullID, Term: 2, Index: 10, Full: false}, // kind disagrees
		},
	}
	if _, err := store.Reconcile(chain); !errors.Is(err, ErrReconcileConflict) {
		t.Fatalf("expected kind-mismatch conflict, got %v", err)
	}
}

func Test_Reconcile_MissingChainLinksAreOK(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()
	fullID := "2-10-1704807719000"
	createSnapshotInStore(t, store, fullID, 10, 2, 1, reconcileFullDB)

	// A catching-up receiver does not hold the leader's older chain links;
	// the delivered full frame is self-contained, so this must be OK.
	chain := &IncomingChain{
		Target: ChainLink{ID: fullID, Term: 2, Index: 10, Full: true},
		Links: []ChainLink{
			{ID: "2-1-1704807701000", Term: 2, Index: 1, Full: true},
			{ID: "2-5-1704807705000", Term: 2, Index: 5, Full: false},
		},
	}
	rep, err := store.Reconcile(chain)
	if err != nil {
		t.Fatalf("missing local links must not conflict: %v", err)
	}
	if rep.Verdict != VerdictOK {
		t.Fatalf("expected OK, got %s", rep.Verdict)
	}
}

func Test_Reconcile_StagingPaths(t *testing.T) {
	dir := t.TempDir()
	stagingParent := t.TempDir()
	staging := filepath.Join(stagingParent, "wal-staging")
	unregistered := filepath.Join(stagingParent, "other-staging")
	for _, d := range []string{staging, unregistered} {
		if err := os.MkdirAll(d, 0755); err != nil {
			t.Fatalf("mkdir %s failed: %v", d, err)
		}
		if err := os.WriteFile(filepath.Join(d, "leftover.wal"), []byte("junk"), 0644); err != nil {
			t.Fatalf("write staging junk failed: %v", err)
		}
	}

	// Empty, relative, root, and the store directory itself must all be
	// rejected as unsafe staging paths and never removed.
	rec := NewReconciler(dir, []string{staging, "", "relative-staging", dir, "/"}, nil)
	rep, err := rec.Reconcile(nil)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if fsutil.PathExists(staging) {
		t.Fatalf("registered staging dir should be removed by reconciliation")
	}
	if !fsutil.PathExists(unregistered) {
		t.Fatalf("unregistered staging dir must never be removed")
	}
	if len(rep.RemovedTransient) != 1 || rep.RemovedTransient[0] != filepath.Clean(staging) {
		t.Fatalf("expected exactly the registered staging dir removed, got %v", rep.RemovedTransient)
	}

	// Rerun is a no-op (staging path already gone).
	rep2, err := NewReconciler(dir, []string{staging}, nil).Reconcile(nil)
	if err != nil || len(rep2.RemovedTransient) != 0 {
		t.Fatalf("expected convergent rerun, err=%v removed=%v", err, rep2.RemovedTransient)
	}
}

func Test_Reconcile_IncomingPassDefersWhileReadersActive(t *testing.T) {
	dir := t.TempDir()
	store, err := NewStore(dir)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()
	firstID := "2-10-1704807719000"
	createSnapshotInStore(t, store, firstID, 10, 2, 1, reconcileFullDB)

	// Hold a snapshot reader open, as a slow follower transfer would.
	_, rc, err := store.Open(firstID)
	if err != nil {
		t.Fatalf("failed to open snapshot: %v", err)
	}
	defer rc.Close()

	// Interrupted-install leftover present while the reader is active.
	tmpDir := filepath.Join(dir, "2-99-1704807799999.tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("mkdir tmp failed: %v", err)
	}

	// Completing a new install must not block on the active reader; the
	// reconciliation pass is deferred and the sink still closes successfully.
	createSnapshotInStore(t, store, "2-30-1704807730000", 30, 2, 1, reconcileFull2DB)
	if !fsutil.PathExists(tmpDir) {
		t.Fatalf("reconciliation must be deferred while readers are active (tmp should remain)")
	}

	// Once readers drain, the blocking manual pass runs and converges.
	rc.Close()
	rep, err := store.Reconcile(nil)
	if err != nil {
		t.Fatalf("reconcile after reader drain failed: %v", err)
	}
	if len(rep.RemovedTransient) != 1 || rep.RemovedTransient[0] != tmpDir {
		t.Fatalf("expected deferred cleanup to remove the tmp dir, got %v", rep.RemovedTransient)
	}
}

func Test_Store_Check_ReconcileConflictStopsStartup(t *testing.T) {
	dir := t.TempDir()
	badDir := filepath.Join(dir, "2-50-1704807750000")
	if err := os.MkdirAll(badDir, 0755); err != nil {
		t.Fatalf("mkdir bad dir failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(badDir, "meta.json"), []byte("{bad json"), 0644); err != nil {
		t.Fatalf("write bad meta failed: %v", err)
	}

	_, err := NewStore(dir)
	if !errors.Is(err, ErrReconcileConflict) {
		t.Fatalf("NewStore must stop with ErrReconcileConflict, got %v", err)
	}
	if !fsutil.DirExists(badDir) {
		t.Fatalf("startup conflict must not delete the suspicious directory")
	}
}

func Test_Store_Check_ReconcileRemovesTmpAndStagingOnStartup(t *testing.T) {
	dir := t.TempDir()
	staging := filepath.Join(t.TempDir(), "wal-staging")

	// Prime a healthy store first.
	store, err := NewStore(dir, WithStagingPaths(staging))
	if err != nil {
		t.Fatalf("NewStore failed: %v", err)
	}
	createSnapshotInStore(t, store, "2-10-1704807719000", 10, 2, 1, reconcileFullDB)
	if err := store.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Simulate crash leftovers after close: an interrupted install tmp dir
	// and a populated WAL staging directory.
	tmpDir := filepath.Join(dir, "2-99-1704807799999.tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("mkdir tmp failed: %v", err)
	}
	if err := os.MkdirAll(staging, 0755); err != nil {
		t.Fatalf("mkdir staging failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(staging, "001.wal"), []byte("junk"), 0644); err != nil {
		t.Fatalf("write staging junk failed: %v", err)
	}

	store2, err := NewStore(dir, WithStagingPaths(staging))
	if err != nil {
		t.Fatalf("reopen store failed: %v", err)
	}
	defer store2.Close()
	if fsutil.PathExists(tmpDir) {
		t.Fatalf("startup reconciliation should remove leftover tmp dir")
	}
	if fsutil.PathExists(staging) {
		t.Fatalf("startup reconciliation should remove leftover staging dir")
	}
	if store2.Len() != 1 {
		t.Fatalf("committed snapshot must survive startup reconciliation, got %d", store2.Len())
	}
}
