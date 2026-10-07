package snapshot

import (
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/rqlite/rqlite/v10/internal/fsutil"
	"github.com/rqlite/rqlite/v10/snapshot/proto"
)

// ChainLink describes one link of the snapshot chain as presented by the
// authoritative source (the leader) to a receiving node.
type ChainLink struct {
	// ID is the snapshot ID of the link. It may be empty when the transport
	// does not carry leader-side snapshot IDs; in that case Term and Index are
	// the only available identity.
	ID string

	// Term and Index are the Raft coordinates of the link.
	Term  uint64
	Index uint64

	// Full reports whether the link is a full snapshot (the chain baseline)
	// rather than an incremental snapshot.
	Full bool
}

// IncomingChain is the leader-authoritative snapshot chain against which a
// receiving node reconciles its local snapshot store.
//
// Links are ordered from the full baseline to Target, inclusive. They may be
// empty when the wire transport delivers only the condensed frame (a full DB
// plus the ordered WAL sequence): Target, DBCRC and WALCRCs still carry
// authoritative evidence in that case. The reconciliation engine never
// invents identity matches for links it has not been told about.
type IncomingChain struct {
	// Target is the authoritative snapshot the receiver is adopting.
	Target ChainLink

	// Links is the full chain, oldest (full baseline) first and Target last.
	Links []ChainLink

	// DBCRC is the CRC32 of the database file delivered in the frame, and
	// WALCRCs are the CRC32 values of the delivered WAL files, in order.
	// Zero values mean "not provided".
	DBCRC   uint32
	WALCRCs []uint32
}

// chainForInstall builds the authoritative-chain descriptor for a FullSnapshot
// frame that has just been installed through a Sink. The wire protocol carries
// the target's Raft coordinates (in the sink metadata) and per-file CRC32
// values, but not the leader-side chain link IDs, so Links is left empty.
func chainForInstall(meta *raft.SnapshotMeta, full *proto.FullSnapshot) *IncomingChain {
	chain := &IncomingChain{
		Target: ChainLink{
			ID:    meta.ID,
			Term:  meta.Term,
			Index: meta.Index,
			Full:  true,
		},
	}
	if full != nil {
		if full.DbHeader != nil {
			chain.DBCRC = full.DbHeader.Crc32
		}
		for _, h := range full.WalHeaders {
			chain.WALCRCs = append(chain.WALCRCs, h.Crc32)
		}
	}
	return chain
}

// ReconcileVerdict is the outcome of a reconciliation pass.
type ReconcileVerdict int

const (
	// VerdictOK means the local committed snapshot chain is complete and
	// consistent with the authoritative chain (if one was supplied); any
	// node-owned transient artifacts have been removed.
	VerdictOK ReconcileVerdict = iota

	// VerdictFullRequired means the local committed chain is broken
	// (incremental snapshots with no full baseline) and cannot be continued.
	// FULL_NEEDED has been set so the next snapshot delivered must be a full
	// snapshot.
	VerdictFullRequired
)

// String returns a human-readable representation of the verdict.
func (v ReconcileVerdict) String() string {
	switch v {
	case VerdictFullRequired:
		return "full-required"
	default:
		return "ok"
	}
}

// ReconcileReport summarizes one reconciliation pass.
type ReconcileReport struct {
	// Verdict is the overall verdict.
	Verdict ReconcileVerdict

	// Snapshots is the number of committed snapshots observed.
	Snapshots int

	// ChainLinks is the number of authoritative chain links used in the
	// comparison. Zero for startup (local-only) reconciliation.
	ChainLinks int

	// RemovedTransient lists the node-owned intermediate artifacts that were
	// removed (interrupted sink directories and registered staging paths).
	RemovedTransient []string

	// FullNeeded reports whether FULL_NEEDED is set after reconciliation.
	FullNeeded bool

	// Foreign lists entries that were left untouched because reconciliation
	// could not attribute them to this node (externally copied files and
	// directories that do not look like snapshots).
	Foreign []string

	// Reason is a human-readable summary of the pass.
	Reason string
}

// ErrReconcileConflict is returned when reconciliation finds a discrepancy it
// cannot safely resolve. No state is changed in that case; the node must stop
// and the discrepancy must be investigated.
var ErrReconcileConflict = errors.New("snapshot chain reconciliation conflict")

// ReconcileConflictError carries the detailed reasons for an ErrReconcileConflict.
type ReconcileConflictError struct {
	// Reasons describes every discrepancy found.
	Reasons []string

	// SuspiciousPaths lists the paths involved in the conflict. None of them
	// are modified by reconciliation.
	SuspiciousPaths []string
}

// Error implements error.
func (e *ReconcileConflictError) Error() string {
	return fmt.Sprintf("%s:\n - %s", ErrReconcileConflict, strings.Join(e.Reasons, "\n - "))
}

// Unwrap enables errors.Is(err, ErrReconcileConflict).
func (e *ReconcileConflictError) Unwrap() error {
	return ErrReconcileConflict
}

// Reconciler performs receiving-side reconciliation of the local snapshot
// store against an authoritative incoming snapshot chain.
//
// Reconciliation never deletes committed snapshots, including snapshots
// copied in externally: only node-owned intermediate artifacts (interrupted
// sink temporary directories and explicitly registered staging paths) are
// removed. Anything that looks like a snapshot but cannot be validated, and
// any disagreement between the local and authoritative chain, is reported as
// a conflict and leaves the store completely untouched.
type Reconciler struct {
	dir          string
	stagingPaths []string
	logger       *log.Logger
}

// NewReconciler returns a Reconciler for the given snapshot store directory.
// stagingPaths are node-owned staging directories outside the store root (for
// example the FSM's WAL staging directory) that reconciliation is allowed to
// remove when present. A path is accepted only when it is absolute and differs
// from both the filesystem root and the store root; empty, relative, and
// otherwise unsafe paths are ignored (an empty or relative path could resolve
// to the process working directory and must never be removed).
func NewReconciler(dir string, stagingPaths []string, logger *log.Logger) *Reconciler {
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	root, err := filepath.Abs(filepath.Clean(dir))
	if err != nil {
		root = filepath.Clean(dir)
	}
	var safe []string
	for _, p := range stagingPaths {
		if !safeStagingPath(p, root) {
			logger.Printf("ignoring unsafe staging path %q during reconciliation", p)
			continue
		}
		safe = append(safe, filepath.Clean(p))
	}
	return &Reconciler{
		dir:          root,
		stagingPaths: safe,
		logger:       logger,
	}
}

// safeStagingPath reports whether p may be registered as a node-owned staging
// path that reconciliation is allowed to delete.
func safeStagingPath(p, storeRoot string) bool {
	if p == "" {
		return false
	}
	c := filepath.Clean(p)
	if !filepath.IsAbs(c) || c == "." || c == string(filepath.Separator) {
		return false
	}
	if c == storeRoot {
		return false
	}
	return true
}

// Reconcile compares the local snapshot store against the given authoritative
// chain. A nil chain requests startup (local-only) reconciliation.
//
// All read-only analysis completes before anything is removed or flagged, so a
// conflict verdict leaves the store byte-for-byte unchanged. The operation is
// idempotent: rerunning it after a conflict changes nothing, and rerunning it
// after cleanup or after a full snapshot has been installed converges to
// VerdictOK.
func (r *Reconciler) Reconcile(chain *IncomingChain) (*ReconcileReport, error) {
	report := &ReconcileReport{Verdict: VerdictOK}
	if chain != nil {
		report.ChainLinks = len(chain.Links)
	}

	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return nil, fmt.Errorf("reading snapshot store directory for reconciliation: %w", err)
	}

	var (
		conflictReasons []string
		suspicious      []string
		tmpDirs         []string
		snapshotDirs    []string
	)
	reserved := map[string]struct{}{
		fullNeededFile:           {},
		reapPlanFile:             {},
		reapPlanFile + tmpSuffix: {},
	}

	// Phase 1: classify every entry in the store root without mutating
	// anything.
	for _, entry := range entries {
		name := entry.Name()
		path := filepath.Join(r.dir, name)

		if entry.IsDir() {
			switch {
			case isTmpName(name):
				// Sink temporary directories are always named
				// "<snapshot-id>.tmp". Anything else ending in .tmp cannot be
				// proven to be node-owned and is a conflict.
				base := strings.TrimSuffix(name, tmpSuffix)
				if _, _, _, perr := ParseSnapshotName(base); perr == nil {
					tmpDirs = append(tmpDirs, path)
				} else {
					suspicious = append(suspicious, path)
					conflictReasons = append(conflictReasons, fmt.Sprintf(
						"temporary directory %q has an unrecognized name; cannot prove this node created it, leaving it untouched", name))
				}
			case isSnapshotNamed(name):
				snapshotDirs = append(snapshotDirs, path)
			default:
				// A directory that does not look like a snapshot is foreign
				// (e.g. an operator-created backup directory): never touch.
				report.Foreign = append(report.Foreign, path)
			}
			continue
		}

		if _, ok := reserved[name]; !ok {
			report.Foreign = append(report.Foreign, path)
		}
	}

	// Phase 2: load every snapshot-shaped directory. A directory that looks
	// like a snapshot but cannot be validated may be a half-copied external
	// snapshot: report it as a conflict rather than deleting it.
	catalog := &SnapshotCatalog{}
	var snaps []*Snapshot
	for _, path := range snapshotDirs {
		snap, lerr := catalog.loadSnapshot(path, filepath.Base(path))
		if lerr != nil {
			suspicious = append(suspicious, path)
			conflictReasons = append(conflictReasons, fmt.Sprintf(
				"snapshot-shaped directory %q is invalid: %v; remove or repair it manually", filepath.Base(path), lerr))
			continue
		}
		snaps = append(snaps, snap)
	}
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].Less(snaps[j])
	})
	local := SnapshotSet{dir: r.dir, items: snaps}
	report.Snapshots = local.Len()

	// Phase 3: compare the local committed chain against the authoritative
	// chain, if one was supplied.
	if chain != nil {
		if newest, ok := local.Newest(); ok {
			if newerThan(newest.raftMeta.Term, newest.raftMeta.Index, chain.Target.Term, chain.Target.Index) {
				suspicious = append(suspicious, newest.path)
				conflictReasons = append(conflictReasons, fmt.Sprintf(
					"local newest snapshot %s (term=%d, index=%d) is ahead of the authoritative target (term=%d, index=%d); adopting it would regress or fork the chain",
					newest.id, newest.raftMeta.Term, newest.raftMeta.Index, chain.Target.Term, chain.Target.Index))
			}
		}
		for _, link := range append([]ChainLink{chain.Target}, chain.Links...) {
			if link.ID == "" {
				continue
			}
			snap, found := local.WithID(link.ID)
			if !found {
				// The delivered full frame is self-contained, so a link that
				// is missing locally is expected on a catching-up node.
				continue
			}
			if snap.raftMeta.Term != link.Term || snap.raftMeta.Index != link.Index {
				suspicious = append(suspicious, snap.path)
				conflictReasons = append(conflictReasons, fmt.Sprintf(
					"snapshot %s locally claims term=%d index=%d but the authoritative chain link claims term=%d index=%d",
					link.ID, snap.raftMeta.Term, snap.raftMeta.Index, link.Term, link.Index))
			}
			if snap.typ.IsFull() != link.Full {
				suspicious = append(suspicious, snap.path)
				conflictReasons = append(conflictReasons, fmt.Sprintf(
					"snapshot %s is locally of kind %s but the authoritative chain expects %s",
					link.ID, snap.typ, expectedKind(link.Full)))
			}
		}
	}

	// Phase 4: determine the structural verdict for the local chain.
	verdict := VerdictOK
	chainBroken := false
	if local.Len() > 0 {
		if verr := local.ValidateIncrementalChain(); verr != nil {
			chainBroken = true
		}
	}
	switch {
	case len(conflictReasons) > 0:
		// Analysis happens before mutation: nothing has been touched yet.
		stats.Add(reconcileConflicts, 1)
		r.logger.Printf("snapshot chain reconciliation conflict; no changes made:\n - %s",
			strings.Join(conflictReasons, "\n - "))
		return nil, &ReconcileConflictError{
			Reasons:         conflictReasons,
			SuspiciousPaths: dedupe(suspicious),
		}
	case local.Len() == 0 && chain != nil && !targetPresent(local, chain):
		// An authoritative chain was supplied but its target is not installed
		// and there is nothing committed: a full snapshot is required.
		verdict = VerdictFullRequired
	case chainBroken && !anchoredByChain(local, chain):
		verdict = VerdictFullRequired
	}

	// Phase 5: execute the idempotent cleanup.
	for _, p := range tmpDirs {
		if err := fsutil.RemoveAll(p); err != nil {
			return nil, fmt.Errorf("removing interrupted snapshot directory %s: %w", p, err)
		}
		r.logger.Printf("reconciliation removed interrupted snapshot directory %s", p)
		report.RemovedTransient = append(report.RemovedTransient, p)
	}
	for _, p := range r.stagingPaths {
		if !safeStagingPath(p, r.dir) || !fsutil.PathExists(p) {
			continue
		}
		if err := fsutil.RemoveAll(p); err != nil {
			return nil, fmt.Errorf("removing staging directory %s: %w", p, err)
		}
		r.logger.Printf("reconciliation removed node staging directory %s", p)
		report.RemovedTransient = append(report.RemovedTransient, p)
	}

	if verdict == VerdictFullRequired {
		if err := markFullNeeded(r.dir); err != nil {
			return nil, fmt.Errorf("marking full snapshot needed: %w", err)
		}
		r.logger.Printf("incremental snapshot chain is broken with no full baseline; FULL_NEEDED set at %s",
			filepath.Join(r.dir, fullNeededFile))
	}
	report.FullNeeded = fsutil.FileExists(filepath.Join(r.dir, fullNeededFile))
	report.Verdict = verdict
	report.Reason = summarizeReconcile(report, chainBroken)

	stats.Add(reconcileTotal, 1)
	stats.Get(reconcileSnapshots).(*expvar.Int).Set(int64(report.Snapshots))
	stats.Get(reconcileTransients).(*expvar.Int).Set(int64(len(report.RemovedTransient)))
	if verdict == VerdictFullRequired {
		stats.Add(reconcileFullRequired, 1)
	}
	r.logger.Printf("snapshot chain reconciliation complete: %s", report.Reason)
	return report, nil
}

// isSnapshotNamed reports whether name has the term-index-millisecond form of
// a committed snapshot directory.
func isSnapshotNamed(name string) bool {
	_, _, _, err := ParseSnapshotName(name)
	return err == nil
}

// newerThan reports whether (term,index) sorts strictly after (otherTerm,otherIndex).
func newerThan(term, index, otherTerm, otherIndex uint64) bool {
	if term != otherTerm {
		return term > otherTerm
	}
	return index > otherIndex
}

// expectedKind returns the chain kind name expected for a link.
func expectedKind(full bool) Type {
	if full {
		return Full
	}
	return Incremental
}

// targetPresent reports whether the authoritative target snapshot is committed
// locally, matched by ID when known or else by term and index.
func targetPresent(local SnapshotSet, chain *IncomingChain) bool {
	if chain.Target.ID != "" {
		if snap, ok := local.WithID(chain.Target.ID); ok {
			return snap.raftMeta.Term == chain.Target.Term && snap.raftMeta.Index == chain.Target.Index
		}
	}
	for _, snap := range local.All() {
		if snap.raftMeta.Term == chain.Target.Term && snap.raftMeta.Index == chain.Target.Index {
			return true
		}
	}
	return false
}

// anchoredByChain reports whether the authoritative chain delivered a full
// snapshot that anchors the committed store. A startup reconciliation (nil
// chain) cannot supply an anchor.
func anchoredByChain(local SnapshotSet, chain *IncomingChain) bool {
	if chain == nil {
		return false
	}
	for _, snap := range local.All() {
		if snap.typ != Full {
			continue
		}
		if snap.raftMeta.Term == chain.Target.Term && snap.raftMeta.Index == chain.Target.Index {
			return true
		}
	}
	return false
}

// markFullNeeded creates the FULL_NEEDED flag file durably. It is a no-op when
// the flag already exists, so repeated reconciliations converge.
func markFullNeeded(dir string) error {
	flagPath := filepath.Join(dir, fullNeededFile)
	if fsutil.FileExists(flagPath) {
		return nil
	}
	f, err := os.Create(flagPath)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fsutil.SyncDirMaybe(dir)
}

// summarizeReconcile builds the human-readable summary for a report.
func summarizeReconcile(report *ReconcileReport, chainBroken bool) string {
	var b strings.Builder
	switch report.Verdict {
	case VerdictFullRequired:
		b.WriteString("snapshot chain broken (no full baseline); full snapshot explicitly required (FULL_NEEDED set)")
	default:
		b.WriteString("snapshot chain OK")
	}
	fmt.Fprintf(&b, "; %d committed snapshot(s)", report.Snapshots)
	if report.ChainLinks > 0 {
		fmt.Fprintf(&b, ", %d authoritative chain link(s) compared", report.ChainLinks)
	}
	if chainBroken {
		b.WriteString("; pre-existing incremental-only chain noted")
	}
	fmt.Fprintf(&b, ", %d node-owned transient artifact(s) removed", len(report.RemovedTransient))
	if len(report.Foreign) > 0 {
		fmt.Fprintf(&b, ", %d foreign entr(ies) left untouched", len(report.Foreign))
	}
	return b.String()
}

// dedupe returns paths without duplicates, preserving order.
func dedupe(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	var out []string
	for _, p := range paths {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		out = append(out, p)
	}
	return out
}
