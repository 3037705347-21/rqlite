package snapshot

import (
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/snapshot/plan"
)

// ReconcileStatus is the outcome status recorded for one reconciliation run.
type ReconcileStatus string

const (
	// ReconcileRunning is the initial status of a record written before any
	// deletion begins. A record left in this status after restart means the run
	// was interrupted.
	ReconcileRunning ReconcileStatus = "running"

	// ReconcileApplied means the run completed (including runs that had nothing
	// to delete).
	ReconcileApplied ReconcileStatus = "applied"

	// ReconcileAbortedInvalidPolicy means the policy could not be read or was
	// invalid; the run deleted nothing.
	ReconcileAbortedInvalidPolicy ReconcileStatus = "aborted_invalid_policy"

	// ReconcileAppliedError means an error unrelated to policy validity stopped
	// the run before any deletion plan was written, so nothing was deleted.
	ReconcileAppliedError ReconcileStatus = "error"

	// ReconcileInterrupted marks a "running" record whose plan never reached
	// disk (a crash before any deletion). It is assigned during startup
	// recovery.
	ReconcileInterrupted ReconcileStatus = "interrupted"
)

// ErrNoRetentionPolicy is returned by Reconcile when no retention policy file
// is present in the Store. No reconciliation record is written in that case;
// the Store uses legacy reaping instead.
var ErrNoRetentionPolicy = errors.New("no retention policy file present")

// observedSnapshot is one entry of a reconciliation record's auditable view of
// the Store contents at the time of the run.
type observedSnapshot struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Term   uint64 `json:"term"`
	Index  uint64 `json:"index"`
	Local  bool   `json:"local"`
	Pinned bool   `json:"pinned"`
}

// ReconcileRecord is the per-run, auditable result of one retention
// reconciliation. One record file is written for every run that has a policy,
// before any snapshot is deleted.
type ReconcileRecord struct {
	// ID is the record file name, unique and monotonically ordered across runs.
	ID string `json:"id"`

	StartedAt  time.Time       `json:"started_at"`
	FinishedAt *time.Time      `json:"finished_at,omitempty"`
	Status     ReconcileStatus `json:"status"`

	// Policy is the policy snapshot the run used. Absent when the policy could
	// not even be parsed.
	Policy *RetentionPolicy `json:"policy,omitempty"`

	// PolicyError describes why the policy was rejected, when applicable.
	PolicyError string `json:"policy_error,omitempty"`

	// Observed lists every snapshot directory present when the run planned.
	Observed []observedSnapshot `json:"observed"`

	// Pinned lists the IDs protected by the policy; Foreign lists the IDs
	// protected because they were copied in from outside the Store.
	Pinned  []string `json:"pinned,omitempty"`
	Foreign []string `json:"foreign,omitempty"`

	// Retained lists every observed ID the run kept; Deleted lists every ID the
	// run removed or (for folded incremental snapshots) consolidated.
	Retained []string `json:"retained,omitempty"`
	Deleted  []string `json:"deleted,omitempty"`

	// ConsolidatedFrom/To name the newest full snapshot before and after WAL
	// consolidation, when consolidation occurred.
	ConsolidatedFrom string `json:"consolidated_from,omitempty"`
	ConsolidatedTo   string `json:"consolidated_to,omitempty"`

	SnapshotsDeleted int `json:"snapshots_deleted,omitempty"`
	WALsCheckpointed int `json:"wals_checkpointed,omitempty"`

	// Recovered is set when the record was finalized by crash recovery rather
	// than the run that started it.
	Recovered bool `json:"recovered,omitempty"`

	// Error records any non-policy error that stopped the run.
	Error string `json:"error,omitempty"`
}

// Reconcile runs one retention reconciliation against the policy file in the
// Store. It is the retention-aware counterpart of Reap:
//
//   - With no policy file it returns ErrNoRetentionPolicy and changes nothing;
//     the caller should use legacy reaping.
//   - With an unreadable or invalid policy it deletes nothing and returns both
//     an aborted record and an error.
//   - With a valid policy it keeps the configured number of newest full
//     snapshots plus every pinned and externally-copied snapshot, consolidates
//     the foldable trailing incrementals into the newest full, and removes only
//     locally-authored, unpinned snapshots older than the retention boundary.
//
// A per-run record is always written for policy-driven runs, and an interrupted
// run is resumed and converges to the same end state on retry.
func (s *Store) Reconcile() (*ReconcileRecord, error) {
	if err := s.mrsw.BeginWrite("reconcile"); err != nil {
		return nil, err
	}
	defer s.mrsw.EndWrite()
	return s.reconcile()
}

// reconcile performs reconciliation. The caller must hold the write lock.
// On success, registered observers are notified.
func (s *Store) reconcile() (*ReconcileRecord, error) {
	startTime := time.Now()
	defer recordDuration(reconcileDuration, startTime)

	rec, err := s.reconcileInternal()
	if err != nil {
		if rec != nil && rec.Status == ReconcileAbortedInvalidPolicy {
			stats.Add(reconcileInvalidPolicy, 1)
		} else {
			stats.Add(reconcileErrors, 1)
		}
		return rec, err
	}

	stats.Add(reconcileTotal, 1)
	n, c := 0, 0
	if rec != nil {
		n, c = rec.SnapshotsDeleted, rec.WALsCheckpointed
	}
	stats.Get(reconcileSnapshots).(*expvar.Int).Set(int64(n))
	stats.Get(reconcilePinned).(*expvar.Int).Set(int64(len(safePinned(rec))))
	stats.Get(reconcileForeign).(*expvar.Int).Set(int64(len(safeForeign(rec))))
	s.observers.notify(ReapObservation{
		SnapshotsReaped: n,
		WALsReaped:      c,
		Duration:        time.Since(startTime),
	})
	return rec, nil
}

// reconcileInternal computes and applies one reconciliation. The caller must
// hold the write lock.
func (s *Store) reconcileInternal() (*ReconcileRecord, error) {
	// An interrupted reconciliation is always resumed before a new run.
	if fsutil.FileExists(s.reconcilePlanPath) {
		s.logger.Printf("found interrupted reconcile plan at %s, resuming", s.reconcilePlanPath)
		p, err := plan.ReadFromFile(s.reconcilePlanPath)
		if err != nil {
			return nil, fmt.Errorf("reading reconcile plan: %w", err)
		}
		return s.executeReconcilePlan(p, s.reconcilePlanPath)
	}

	// A pending legacy reap from a previous policy configuration must finish
	// before a new reconciliation is planned.
	if fsutil.FileExists(s.reapPlanPath) {
		s.logger.Printf("found interrupted reap plan at %s while reconciling, resuming", s.reapPlanPath)
		p, err := plan.ReadFromFile(s.reapPlanPath)
		if err != nil {
			return nil, fmt.Errorf("reading reap plan: %w", err)
		}
		if _, _, err := s.executeReapPlan(p, s.reapPlanPath); err != nil {
			return nil, err
		}
	}

	rec := &ReconcileRecord{
		ID:        s.newReconcileRecordID(),
		StartedAt: time.Now(),
		Status:    ReconcileRunning,
		Observed:  []observedSnapshot{},
	}

	policy, perr := ReadRetentionPolicy(s.retentionPolicyPath)
	if perr != nil {
		// A missing policy means legacy reaping owns maintenance; signal that.
		if errors.Is(perr, os.ErrNotExist) {
			return nil, ErrNoRetentionPolicy
		}
		// Any other failure (unreadable file, malformed/invalid JSON policy)
		// aborts the run without deleting anything.
		rec.Status = ReconcileAbortedInvalidPolicy
		rec.PolicyError = perr.Error()
		now := time.Now()
		rec.FinishedAt = &now
		if err := writeReconcileRecord(s.dir, rec); err != nil {
			s.logger.Printf("failed to write reconcile record: %s", err)
		}
		return rec, perr
	}
	rec.Policy = policy

	// Reading/rewriting data files is about to become possible, so verify
	// integrity first (at most once over the Store's lifetime).
	if err := s.ensureVerified(); err != nil {
		return s.failReconcileRecord(rec, err), err
	}

	set, err := s.getSnapshots()
	if err != nil {
		return s.failReconcileRecord(rec, err), err
	}
	rec.Observed = observeSnapshots(set, policy)
	rec.Pinned = append(rec.Pinned, policy.Pinned...)
	rec.Foreign = foreignIDs(set)

	if err := policy.ValidateAgainst(set); err != nil {
		rec.Status = ReconcileAbortedInvalidPolicy
		rec.PolicyError = err.Error()
		now := time.Now()
		rec.FinishedAt = &now
		if werr := writeReconcileRecord(s.dir, rec); werr != nil {
			s.logger.Printf("failed to write reconcile record: %s", werr)
		}
		return rec, err
	}

	decision, err := planRetention(set, policy, s.snapshotNamer)
	if err != nil {
		return s.failReconcileRecord(rec, err), err
	}

	rec.Deleted = decision.deletionIDs()
	rec.Retained = retainedIDs(set, rec.Deleted)
	if decision.consolidate {
		rec.ConsolidatedFrom = decision.full.id
		rec.ConsolidatedTo = decision.newID
		rec.WALsCheckpointed = len(decision.walFiles)
	}
	rec.SnapshotsDeleted = len(rec.Deleted)

	// Build the plan first: failure here aborts before the record is finalized
	// or any plan is written, so nothing is deleted.
	p, err := s.buildReconcilePlan(decision, recordFilePath(s.dir, rec))
	if err != nil {
		return s.failReconcileRecord(rec, err), err
	}

	// The auditable record must exist describing every planned deletion before
	// any mutation begins.
	if err := writeReconcileRecord(s.dir, rec); err != nil {
		return nil, fmt.Errorf("writing reconcile record: %w", err)
	}

	if err := plan.WriteToFile(p, s.reconcilePlanPath); err != nil {
		return nil, fmt.Errorf("writing reconcile plan: %w", err)
	}

	return s.executeReconcilePlan(p, s.reconcilePlanPath)
}

// buildReconcilePlan translates a retention decision into the same ordered,
// idempotent operation sequence legacy reaping uses, but with the decision's
// protected retention set.
func (s *Store) buildReconcilePlan(d *retentionDecision, recordPath string) (*plan.Plan, error) {
	p := plan.New()
	p.RecordPath = recordPath

	if d.consolidate {
		dbPath := filepath.Join(d.full.path, dbfileName)

		// 1. Fold all collected WALs into the newest full's database.
		p.AddCheckpoint(dbPath, d.walFiles)
		p.NCheckpointed = len(d.walFiles)

		// 2. Recompute the DB CRC32 sidecar.
		p.AddCalcCRC32(dbPath, dbPath+crcSuffix)

		// 3. Remove the folded incremental directories.
		for _, sn := range d.collapsible {
			p.AddRemoveAll(sn.path)
			p.NReaped++
		}

		// 4. Remove eligible snapshots older than the retention boundary.
		for _, sn := range d.deletions {
			p.AddRemoveAll(sn.path)
			p.NReaped++
		}

		// 5. Rewrite the full's metadata to the newest folded point.
		metaJSON, err := json.Marshal(d.newMeta)
		if err != nil {
			return nil, fmt.Errorf("marshaling consolidated meta: %w", err)
		}
		p.AddWriteMeta(d.full.path, metaJSON)

		// 6. Verify the checkpointed database.
		if s.noVerifyDB.IsNot() {
			p.AddVerifyDB(dbPath)
		}

		// 7. Rename the consolidated full to its new ID.
		p.AddRename(d.full.path, filepath.Join(s.dir, d.newID))
		return p, nil
	}

	// No consolidation: remove only the eligible, out-of-window snapshots.
	for _, sn := range d.deletions {
		p.AddRemoveAll(sn.path)
		p.NReaped++
	}
	return p, nil
}

// executeReconcilePlan executes (or confirms) a persisted reconcile plan,
// finalizes its record, and removes the plan file. It is idempotent and safe to
// invoke after a crash.
func (s *Store) executeReconcilePlan(p *plan.Plan, planPath string) (*ReconcileRecord, error) {
	rec, err := loadOrSynthesizeRecord(s.dir, p)
	if err != nil {
		return nil, err
	}

	done, err := p.LastOpDone(plan.NewChecker())
	if err != nil {
		return nil, fmt.Errorf("checking reconcile plan completion: %w", err)
	}
	if !done {
		s.logger.Printf("re-executing interrupted reconcile plan at %s", planPath)
		executor := plan.NewExecutor()
		if err := p.Execute(executor); err != nil {
			return nil, fmt.Errorf("executing reconcile plan: %w", err)
		}
	}

	if err := fsutil.SyncDirMaybe(s.dir); err != nil {
		return nil, fmt.Errorf("syncing store dir: %w", err)
	}

	now := time.Now()
	rec.Status = ReconcileApplied
	rec.FinishedAt = &now
	if p.NReaped > rec.SnapshotsDeleted {
		rec.SnapshotsDeleted = p.NReaped
	}
	if p.NCheckpointed > rec.WALsCheckpointed {
		rec.WALsCheckpointed = p.NCheckpointed
	}
	if err := writeReconcileRecord(s.dir, rec); err != nil {
		return nil, fmt.Errorf("finalizing reconcile record: %w", err)
	}

	fsutil.Remove(planPath)
	return rec, nil
}

// failReconcileRecord records a non-policy error on the run's record. Such
// errors occur before any plan is written, so no snapshot is deleted.
func (s *Store) failReconcileRecord(rec *ReconcileRecord, cause error) *ReconcileRecord {
	rec.Status = ReconcileAppliedError
	rec.Error = cause.Error()
	now := time.Now()
	rec.FinishedAt = &now
	if err := writeReconcileRecord(s.dir, rec); err != nil {
		s.logger.Printf("failed to write reconcile record: %s", err)
	}
	return rec
}

// finalizeInterruptedRecords marks any record still in the "running" state as
// interrupted. This is only reached when no reconcile plan is present, which
// means the run crashed before the plan was atomically written and therefore
// before any deletion; nothing needs replaying.
func (s *Store) finalizeInterruptedRecords() error {
	paths, err := reconcileRecordPaths(s.dir)
	if err != nil {
		return err
	}
	for _, path := range paths {
		rec, err := readReconcileRecord(path)
		if err != nil {
			// An unreadable record is left untouched rather than deleting
			// audit data.
			s.logger.Printf("skipping unreadable reconcile record %s: %s", path, err)
			continue
		}
		if rec.Status != ReconcileRunning {
			continue
		}
		rec.Status = ReconcileInterrupted
		now := time.Now()
		rec.FinishedAt = &now
		if err := writeReconcileRecord(s.dir, rec); err != nil {
			return fmt.Errorf("finalizing interrupted reconcile record %s: %w", rec.ID, err)
		}
		s.logger.Printf("marked reconcile record %s as interrupted (no plan reached disk)", rec.ID)
	}
	return nil
}

// --- record persistence helpers ---

func recordFilePath(dir string, rec *ReconcileRecord) string {
	return filepath.Join(dir, rec.ID)
}

func writeReconcileRecord(dir string, rec *ReconcileRecord) error {
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(recordFilePath(dir, rec), append(data, '\n'), 0644)
}

func readReconcileRecord(path string) (*ReconcileRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	rec := &ReconcileRecord{}
	if err := json.Unmarshal(data, rec); err != nil {
		return nil, err
	}
	return rec, nil
}

// loadOrSynthesizeRecord loads the record referenced by a recovered plan. A
// missing or unreadable record is replaced by a minimal recovered record so a
// crash between record and bookkeeping can never block plan completion.
func loadOrSynthesizeRecord(dir string, p *plan.Plan) (*ReconcileRecord, error) {
	if p.RecordPath != "" {
		if rec, err := readReconcileRecord(p.RecordPath); err == nil {
			return rec, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("reading reconcile record %s: %w", p.RecordPath, err)
		}
	}
	return &ReconcileRecord{
		ID:               filepath.Base(p.RecordPath),
		StartedAt:        time.Now(),
		Status:           ReconcileRunning,
		Recovered:        true,
		SnapshotsDeleted: p.NReaped,
		WALsCheckpointed: p.NCheckpointed,
	}, nil
}

// newReconcileRecordID returns a unique, time-ordered reconcile record file
// name. The millisecond field is raised past any existing record so IDs stay
// ordered even if the wall clock moves backwards.
func (s *Store) newReconcileRecordID() string {
	msec := time.Now().UnixNano() / int64(time.Millisecond)
	for _, existing := range mustReconcileRecordMsecs(s.dir) {
		if existing+1 > msec {
			msec = existing + 1
		}
	}
	return fmt.Sprintf("%s%d%s", reconcileRecordPrefix, msec, reconcileRecordSuffix)
}

// reconcileRecordPaths returns all record file paths in dir, ordered by name.
func reconcileRecordPaths(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading store directory for reconcile records: %w", err)
	}
	var paths []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if len(name) > len(reconcileRecordPrefix)+len(reconcileRecordSuffix) &&
			hasPrefix(name, reconcileRecordPrefix) && hasSuffix(name, reconcileRecordSuffix) {
			paths = append(paths, filepath.Join(dir, name))
		}
	}
	return paths, nil
}

func mustReconcileRecordMsecs(dir string) []int64 {
	paths, err := reconcileRecordPaths(dir)
	if err != nil {
		return nil
	}
	msecs := make([]int64, 0, len(paths))
	for _, p := range paths {
		name := filepath.Base(p)
		num := name[len(reconcileRecordPrefix) : len(name)-len(reconcileRecordSuffix)]
		if m, err := strconv.ParseInt(num, 10, 64); err == nil {
			msecs = append(msecs, m)
		}
	}
	return msecs
}

// --- classification helpers ---

func observeSnapshots(set SnapshotSet, policy *RetentionPolicy) []observedSnapshot {
	pinned := make(map[string]struct{}, len(policy.Pinned))
	for _, id := range policy.Pinned {
		pinned[id] = struct{}{}
	}
	out := make([]observedSnapshot, 0, set.Len())
	for _, sn := range set.All() {
		_, isPinned := pinned[sn.id]
		out = append(out, observedSnapshot{
			ID:     sn.id,
			Type:   sn.typ.String(),
			Term:   sn.raftMeta.Term,
			Index:  sn.raftMeta.Index,
			Local:  sn.local,
			Pinned: isPinned,
		})
	}
	return out
}

func foreignIDs(set SnapshotSet) []string {
	var out []string
	for _, sn := range set.All() {
		if !sn.local {
			out = append(out, sn.id)
		}
	}
	return out
}

func retainedIDs(set SnapshotSet, deleted []string) []string {
	gone := make(map[string]struct{}, len(deleted))
	for _, id := range deleted {
		gone[id] = struct{}{}
	}
	var out []string
	for _, sn := range set.All() {
		if _, ok := gone[sn.id]; !ok {
			out = append(out, sn.id)
		}
	}
	return out
}

func safePinned(rec *ReconcileRecord) []string {
	if rec == nil {
		return nil
	}
	return rec.Pinned
}

func safeForeign(rec *ReconcileRecord) []string {
	if rec == nil {
		return nil
	}
	return rec.Foreign
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func hasSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

// atomicWriteFile writes data to path atomically: tmp file, sync, rename, sync
// parent directory.
func atomicWriteFile(path string, data []byte, mode os.FileMode) error {
	tmpPath := path + tmpSuffix
	if err := os.WriteFile(tmpPath, data, mode); err != nil {
		return err
	}
	fh, err := os.Open(tmpPath)
	if err != nil {
		return err
	}
	if err := fh.Sync(); err != nil {
		fh.Close()
		return err
	}
	fh.Close()
	if err := fsutil.Rename(tmpPath, path); err != nil {
		return err
	}
	return fsutil.SyncDirParentMaybe(path)
}
