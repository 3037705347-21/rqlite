package snapshot

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	"github.com/hashicorp/raft"
)

// RetentionPolicy declares which snapshots retention reconciliation keeps.
//
// The policy has two protections. First, the RetainFull newest full snapshots
// are retained as recovery points. Second, every snapshot whose ID is listed
// in Pinned is retained indefinitely, regardless of age or type.
//
// Independently of the policy, any snapshot directory not authored by this
// Store (no ORIGIN marker -- i.e. copied in from outside) is never deleted.
//
// A Store with no policy file falls back to legacy reaping.
type RetentionPolicy struct {
	// RetainFull is the number of most recent full snapshots to retain.
	// It must be at least 1.
	RetainFull int `json:"retain_full"`

	// Pinned lists snapshot IDs (term-index-millisecond directory names)
	// that must never be deleted while the policy is in effect. Entries must
	// be well-formed, unique, and present in the Store at reconcile time.
	Pinned []string `json:"pinned,omitempty"`
}

// Validate checks the policy's intrinsic validity, independent of the current
// Store contents. A policy that fails this check must cause reconciliation to
// delete nothing.
func (p *RetentionPolicy) Validate() error {
	if p.RetainFull < 1 {
		return fmt.Errorf("retention policy invalid: retain_full must be >= 1, got %d", p.RetainFull)
	}
	seen := make(map[string]struct{}, len(p.Pinned))
	for _, id := range p.Pinned {
		if id == "" {
			return fmt.Errorf("retention policy invalid: pinned contains an empty ID")
		}
		if _, _, _, err := ParseSnapshotName(id); err != nil {
			return fmt.Errorf("retention policy invalid: pinned ID %q is not a term-index-msec snapshot ID: %w", id, err)
		}
		if _, dup := seen[id]; dup {
			return fmt.Errorf("retention policy invalid: pinned ID %q is listed more than once", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// ValidateAgainst checks that every pinned ID names a snapshot currently in
// the set. A dangling pin is treated as an invalid policy so that a typo fails
// closed (nothing is deleted) instead of being silently ignored.
func (p *RetentionPolicy) ValidateAgainst(set SnapshotSet) error {
	for _, id := range p.Pinned {
		if _, ok := set.WithID(id); !ok {
			return fmt.Errorf("retention policy invalid: pinned snapshot %q not found in store", id)
		}
	}
	return nil
}

// ReadRetentionPolicy reads, strictly decodes, and intrinsically validates the
// RetentionPolicy at the given path. Unknown JSON fields are rejected so a
// mistyped key (e.g. "retain_ful") makes the policy invalid rather than being
// silently ignored.
func ReadRetentionPolicy(path string) (*RetentionPolicy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	p := &RetentionPolicy{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(p); err != nil {
		return nil, fmt.Errorf("parsing retention policy at %s: %w", path, err)
	}
	if err := p.Validate(); err != nil {
		return nil, err
	}
	return p, nil
}

// WriteRetentionPolicyFile validates the policy and writes it atomically to
// path (tmp file, sync, rename, sync parent directory).
func WriteRetentionPolicyFile(path string, p *RetentionPolicy) error {
	if p == nil {
		return fmt.Errorf("retention policy is nil")
	}
	if err := p.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(path, append(data, '\n'), 0644)
}

// retentionDecision is the output of the pure retention planning step: the
// complete set of filesystem actions reconciliation should take.
type retentionDecision struct {
	// full is the newest full snapshot in the Store.
	full *Snapshot

	// consolidate reports whether the newest full is rewritten by folding WALs
	// into it and renamed to a new ID. It is false when the newest full is
	// protected or there are no WALs to fold.
	consolidate bool

	// walFiles are the WAL files checkpointed into the full's DB when
	// consolidating: the full's own WALs plus those of the collapsible prefix.
	walFiles []string

	// collapsible are trailing incremental snapshots (newer than the newest
	// full) whose WALs are folded into that full and whose directories are
	// removed. They form a contiguous locally-authored, unpinned prefix; the
	// first protected trailing incremental, and everything after it, is kept.
	collapsible []*Snapshot

	// deletions are snapshots older than the retention boundary that are
	// eligible for removal: locally authored and not pinned.
	deletions []*Snapshot

	// newMeta is the metadata the consolidated full receives (the newest point
	// folded into it); newID is its post-rename directory name. Only set when
	// consolidating.
	newMeta *raft.SnapshotMeta
	newID   string
}

// deletionIDs returns every snapshot directory the decision removes.
func (d *retentionDecision) deletionIDs() []string {
	ids := make([]string, 0, len(d.deletions)+len(d.collapsible))
	for _, s := range d.deletions {
		ids = append(ids, s.id)
	}
	for _, s := range d.collapsible {
		ids = append(ids, s.id)
	}
	return ids
}

// planRetention computes the retention decision for the given set and policy.
// It performs no filesystem mutation. The policy must already have passed
// Validate and ValidateAgainst for the set.
func planRetention(set SnapshotSet, policy *RetentionPolicy, namer *SnapshotNamer) (*retentionDecision, error) {
	fullSet, newerSet := set.PartitionAtFull()
	if fullSet.Len() == 0 {
		return nil, fmt.Errorf("no full snapshot found")
	}
	if err := set.ValidateIncrementalChain(); err != nil {
		return nil, err
	}
	full, _ := fullSet.Newest()
	items := set.All()

	pinned := make(map[string]struct{}, len(policy.Pinned))
	for _, id := range policy.Pinned {
		pinned[id] = struct{}{}
	}
	isPinned := func(s *Snapshot) bool {
		_, ok := pinned[s.id]
		return ok
	}
	// Protected snapshots may never be deleted, rewritten, or renamed: pinned
	// snapshots and snapshots copied in from outside the Store.
	isProtected := func(s *Snapshot) bool {
		return isPinned(s) || !s.local
	}

	d := &retentionDecision{full: full}

	// Determine the collapsible prefix of trailing incrementals. Folding WALs
	// must preserve frame order, so consolidation stops at the first protected
	// trailing incremental and leaves it, and the suffix behind it, untouched.
	fullProtected := isProtected(full)
	if !fullProtected {
		for _, s := range newerSet.All() {
			if isProtected(s) {
				break
			}
			d.collapsible = append(d.collapsible, s)
		}
		for _, wf := range full.walFiles {
			d.walFiles = append(d.walFiles, wf.Path)
		}
		for _, s := range d.collapsible {
			for _, wf := range s.walFiles {
				d.walFiles = append(d.walFiles, wf.Path)
			}
		}
	}

	// The retention boundary is the Nth newest full snapshot. Everything at or
	// newer than the boundary is retained.
	fulls := set.Fulls().All()
	var boundary *Snapshot
	if n := policy.RetainFull; n < len(fulls) {
		boundary = fulls[len(fulls)-n]
	} else {
		boundary = fulls[0]
	}

	// Dependency closure: a protected incremental older than the boundary is
	// only resolvable together with its nearest preceding full. Pull the
	// boundary back to keep that full (and the snapshots between them). If no
	// preceding full exists, keep everything rather than risk an unresolvable
	// chain.
	boundaryIdx := indexOfSnapshot(items, boundary)
	for i := 0; i < boundaryIdx; i++ {
		s := items[i]
		if s.typ != Incremental || !isProtected(s) {
			continue
		}
		baseIdx := -1
		for j := i; j >= 0; j-- {
			if items[j].typ == Full {
				baseIdx = j
				break
			}
		}
		if baseIdx < 0 {
			boundaryIdx = 0
			break
		}
		if baseIdx < boundaryIdx {
			boundaryIdx = baseIdx
		}
	}

	// Everything strictly older than the boundary that is locally authored and
	// not pinned may be deleted.
	for i := 0; i < boundaryIdx; i++ {
		s := items[i]
		if s.local && !isPinned(s) {
			d.deletions = append(d.deletions, s)
		}
	}

	// Consolidation folds WALs into the newest full and advances it to the
	// newest folded point, mirroring legacy reaping but leaving older retained
	// full snapshots untouched.
	if !fullProtected && len(d.walFiles) > 0 {
		d.consolidate = true
		newestPoint := full
		if len(d.collapsible) > 0 {
			newestPoint = d.collapsible[len(d.collapsible)-1]
		}
		d.newMeta = copyRaftMeta(newestPoint.raftMeta)
		d.newID = namer.MakeName(set, newestPoint.raftMeta.Term, newestPoint.raftMeta.Index)
	}

	return d, nil
}

// indexOfSnapshot returns the index of target in items, or -1.
func indexOfSnapshot(items []*Snapshot, target *Snapshot) int {
	for i, s := range items {
		if s == target {
			return i
		}
	}
	return -1
}
