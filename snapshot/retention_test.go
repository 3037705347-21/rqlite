package snapshot

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestRetentionPolicyValidate(t *testing.T) {
	tests := []struct {
		name    string
		policy  *RetentionPolicy
		wantErr bool
	}{
		{name: "minimal valid", policy: &RetentionPolicy{RetainFull: 1}},
		{name: "valid with pins", policy: &RetentionPolicy{RetainFull: 3, Pinned: []string{"2-100-1704807719996"}}},
		{name: "zero retain", policy: &RetentionPolicy{RetainFull: 0}, wantErr: true},
		{name: "negative retain", policy: &RetentionPolicy{RetainFull: -1}, wantErr: true},
		{name: "empty pin", policy: &RetentionPolicy{RetainFull: 1, Pinned: []string{""}}, wantErr: true},
		{name: "malformed pin", policy: &RetentionPolicy{RetainFull: 1, Pinned: []string{"not-a-snapshot"}}, wantErr: true},
		{name: "duplicate pin", policy: &RetentionPolicy{RetainFull: 1, Pinned: []string{"2-1-1", "2-1-1"}}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.policy.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestReadRetentionPolicy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, retentionPolicyFile)

	if err := os.WriteFile(path, []byte(`{"retain_full":2,"pinned":["2-1-100"]}`), 0644); err != nil {
		t.Fatal(err)
	}
	p, err := ReadRetentionPolicy(path)
	if err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if p.RetainFull != 2 || len(p.Pinned) != 1 || p.Pinned[0] != "2-1-100" {
		t.Fatalf("unexpected policy decoded: %+v", p)
	}

	for name, content := range map[string]string{
		"bad json":         `{not json`,
		"unknown field":    `{"retain_full":2,"retain_inc":1}`,
		"misspelled key":   `{"retain_ful":2}`,
		"retain too small": `{"retain_full":0}`,
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
			if _, err := ReadRetentionPolicy(path); err == nil {
				t.Fatalf("expected invalid policy %q to be rejected", content)
			}
		})
	}
}

func TestWriteRetentionPolicyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, retentionPolicyFile)

	if err := WriteRetentionPolicyFile(path, &RetentionPolicy{RetainFull: 2}); err != nil {
		t.Fatalf("write valid policy: %v", err)
	}
	got, err := ReadRetentionPolicy(path)
	if err != nil {
		t.Fatalf("read-back failed: %v", err)
	}
	if got.RetainFull != 2 {
		t.Fatalf("expected retain_full 2, got %d", got.RetainFull)
	}
	if err := WriteRetentionPolicyFile(path, &RetentionPolicy{RetainFull: 0}); err == nil {
		t.Fatal("expected invalid policy write to fail")
	}
	if err := WriteRetentionPolicyFile(path, nil); err == nil {
		t.Fatal("expected nil policy write to fail")
	}
}

func TestPolicyValidateAgainst(t *testing.T) {
	set := buildSet(
		testSnapshot("2-100-1000", 2, 100, Full, true),
		testSnapshot("2-200-2000", 2, 200, Incremental, true),
	)
	p := &RetentionPolicy{RetainFull: 1, Pinned: []string{"2-100-1000"}}
	if err := p.ValidateAgainst(set); err != nil {
		t.Fatalf("existing pin rejected: %v", err)
	}
	p.Pinned = []string{"2-999-9999"}
	if err := p.ValidateAgainst(set); err == nil {
		t.Fatal("expected dangling pin to fail validation")
	}
}

// --- planRetention pure decision tests ---

func fixedNamer() *SnapshotNamer {
	return NewSnapshotNamer(func() time.Time {
		return time.UnixMilli(5000)
	})
}

func testSnapshot(id string, term, index uint64, typ Type, local bool) *Snapshot {
	s := &Snapshot{
		id:       id,
		path:     filepath.Join("/store", id),
		typ:      typ,
		raftMeta: &raft.SnapshotMeta{ID: id, Term: term, Index: index},
		local:    local,
	}
	if typ == Incremental {
		s.walFiles = []*ChecksummedFile{{Path: filepath.Join("/store", id, "001.wal")}}
	}
	return s
}

func buildSet(snaps ...*Snapshot) SnapshotSet {
	return SnapshotSet{dir: "/store", items: snaps}
}

func TestPlanRetention_RetainWindow(t *testing.T) {
	f1 := testSnapshot("2-100-1000", 2, 100, Full, true)
	f2 := testSnapshot("2-200-2000", 2, 200, Full, true)
	f3 := testSnapshot("2-300-3000", 2, 300, Full, true)
	set := buildSet(f1, f2, f3)

	d, err := planRetention(set, &RetentionPolicy{RetainFull: 2}, fixedNamer())
	if err != nil {
		t.Fatalf("planRetention: %v", err)
	}
	if d.consolidate {
		t.Fatal("expected no consolidation without WALs")
	}
	if ids := d.deletionIDs(); len(ids) != 1 || ids[0] != f1.id {
		t.Fatalf("expected only %s deleted, got %v", f1.id, ids)
	}
}

func TestPlanRetention_RetainOneKeepsOnlyNewestFull(t *testing.T) {
	f1 := testSnapshot("2-100-1000", 2, 100, Full, true)
	f2 := testSnapshot("2-200-2000", 2, 200, Full, true)
	f3 := testSnapshot("2-300-3000", 2, 300, Full, true)
	d, err := planRetention(buildSet(f1, f2, f3), &RetentionPolicy{RetainFull: 1}, fixedNamer())
	if err != nil {
		t.Fatalf("planRetention: %v", err)
	}
	if ids := d.deletionIDs(); len(ids) != 2 || ids[0] != f1.id || ids[1] != f2.id {
		t.Fatalf("expected f1,f2 deleted, got %v", ids)
	}
}

func TestPlanRetention_PinnedOldFullSurvives(t *testing.T) {
	f1 := testSnapshot("2-100-1000", 2, 100, Full, true)
	f2 := testSnapshot("2-200-2000", 2, 200, Full, true)
	f3 := testSnapshot("2-300-3000", 2, 300, Full, true)
	policy := &RetentionPolicy{RetainFull: 1, Pinned: []string{f1.id}}
	d, err := planRetention(buildSet(f1, f2, f3), policy, fixedNamer())
	if err != nil {
		t.Fatalf("planRetention: %v", err)
	}
	for _, id := range d.deletionIDs() {
		if id == f1.id {
			t.Fatal("pinned snapshot scheduled for deletion")
		}
	}
}

func TestPlanRetention_ForeignSnapshotsSurvive(t *testing.T) {
	f1 := testSnapshot("2-100-1000", 2, 100, Full, false) // copied in
	f2 := testSnapshot("2-200-2000", 2, 200, Full, true)
	f3 := testSnapshot("2-300-3000", 2, 300, Full, true)
	d, err := planRetention(buildSet(f1, f2, f3), &RetentionPolicy{RetainFull: 1}, fixedNamer())
	if err != nil {
		t.Fatalf("planRetention: %v", err)
	}
	if ids := d.deletionIDs(); len(ids) != 1 || ids[0] != f2.id {
		t.Fatalf("expected only local f2 deleted, got %v", ids)
	}
}

func TestPlanRetention_ForeignIncrementalKeepsBaseFull(t *testing.T) {
	// f1(local full) -- i1(foreign incremental) -- f2(local) -- f3(local),
	// retain 1. The foreign incremental must keep its base full f1 alive via
	// dependency closure; nothing is deletable.
	f1 := testSnapshot("2-100-1000", 2, 100, Full, true)
	i1 := testSnapshot("2-150-1500", 2, 150, Incremental, false)
	f2 := testSnapshot("2-200-2000", 2, 200, Full, true)
	f3 := testSnapshot("2-300-3000", 2, 300, Full, true)
	d, err := planRetention(buildSet(f1, i1, f2, f3), &RetentionPolicy{RetainFull: 1}, fixedNamer())
	if err != nil {
		t.Fatalf("planRetention: %v", err)
	}
	if len(d.deletionIDs()) != 0 {
		t.Fatalf("expected dependency closure to keep everything, got %v", d.deletionIDs())
	}
}

func TestPlanRetention_CollapsiblePrefixStopsAtProtected(t *testing.T) {
	// Newest local full, one local trailing incremental then one foreign one.
	// Only the local prefix folds; the foreign incremental blocks further
	// folding but remains resolvable against the consolidated full.
	f3 := testSnapshot("2-100-1000", 2, 100, Full, true)
	iA := testSnapshot("2-200-2000", 2, 200, Incremental, true)
	iB := testSnapshot("2-300-3000", 2, 300, Incremental, false)
	d, err := planRetention(buildSet(f3, iA, iB), &RetentionPolicy{RetainFull: 1}, fixedNamer())
	if err != nil {
		t.Fatalf("planRetention: %v", err)
	}
	if !d.consolidate {
		t.Fatal("expected consolidation of the local prefix")
	}
	if len(d.collapsible) != 1 || d.collapsible[0] != iA {
		t.Fatalf("expected only iA collapsible, got %v", d.collapsible)
	}
	if len(d.walFiles) != 1 {
		t.Fatalf("expected only iA WAL folded, got %v", d.walFiles)
	}
	if d.newMeta.Index != 200 {
		t.Fatalf("expected consolidated point at iA index 200, got %d", d.newMeta.Index)
	}
	for _, id := range d.deletionIDs() {
		if id == iB.id {
			t.Fatal("protected trailing incremental scheduled for deletion")
		}
	}
}

func TestPlanRetention_ProtectedNewestFullNotConsolidated(t *testing.T) {
	f1 := testSnapshot("2-100-1000", 2, 100, Full, true)
	f2 := testSnapshot("2-200-2000", 2, 200, Full, false) // foreign newest full
	iA := testSnapshot("2-300-3000", 2, 300, Incremental, true)
	d, err := planRetention(buildSet(f1, f2, iA), &RetentionPolicy{RetainFull: 1}, fixedNamer())
	if err != nil {
		t.Fatalf("planRetention: %v", err)
	}
	if d.consolidate {
		t.Fatal("protected newest full must not be rewritten")
	}
	if len(d.collapsible) != 0 {
		t.Fatalf("no trailing incremental may fold into a protected full, got %v", d.collapsible)
	}
	if ids := d.deletionIDs(); len(ids) != 1 || ids[0] != f1.id {
		t.Fatalf("expected only local out-of-window f1 deleted, got %v", ids)
	}
}

func TestPlanRetention_PinnedNewestFullNotConsolidated(t *testing.T) {
	f1 := testSnapshot("2-100-1000", 2, 100, Full, true)
	f2 := testSnapshot("2-200-2000", 2, 200, Full, true)
	policy := &RetentionPolicy{RetainFull: 1, Pinned: []string{f2.id}}
	d, err := planRetention(buildSet(f1, f2), policy, fixedNamer())
	if err != nil {
		t.Fatalf("planRetention: %v", err)
	}
	if d.consolidate {
		t.Fatal("pinned newest full must not be renamed")
	}
	if ids := d.deletionIDs(); len(ids) != 1 || ids[0] != f1.id {
		t.Fatalf("expected only out-of-window f1 deleted, got %v", ids)
	}
}

func TestPlanRetention_NoFull(t *testing.T) {
	iA := testSnapshot("2-200-2000", 2, 200, Incremental, true)
	if _, err := planRetention(buildSet(iA), &RetentionPolicy{RetainFull: 1}, fixedNamer()); err == nil {
		t.Fatal("expected error without a full snapshot")
	}
}
