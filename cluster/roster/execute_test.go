package roster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

// fakeCluster is a stateful simulation of the Raft membership configuration as
// seen by a rqlite leader. It deliberately mirrors the leader's safety rules:
// a join never evicts another ID holding the address, a join for an existing
// ID with a different address is rejected (the caller must remove first), and
// removing a missing ID is an error.
type fakeCluster struct {
	mu       sync.Mutex
	members  map[string]Member
	log      []string
	failJoin map[string]bool // IDs whose joins fail
}

func newFakeCluster(members ...Member) *fakeCluster {
	f := &fakeCluster{
		members:  make(map[string]Member, len(members)),
		failJoin: map[string]bool{},
	}
	for _, m := range members {
		f.members[m.ID] = m
	}
	return f
}

func (f *fakeCluster) Members(_ context.Context) ([]Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Member, 0, len(f.members))
	for _, m := range f.members {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (f *fakeCluster) Join(_ context.Context, id, addr string, voter bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, m := range f.members {
		if m.Addr == addr && m.ID != id {
			return fmt.Errorf("address %s already registered to node %s", addr, m.ID)
		}
	}
	if cur, ok := f.members[id]; ok && cur.Addr != addr {
		return fmt.Errorf("node %s still registered at %s, remove it first", id, cur.Addr)
	}
	f.log = append(f.log, fmt.Sprintf("join %s at %s voter=%t", id, addr, voter))
	if f.failJoin[id] {
		return fmt.Errorf("simulated join failure for node %s", id)
	}
	f.members[id] = Member{ID: id, Addr: addr, Voter: voter}
	return nil
}

func (f *fakeCluster) Remove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.members[id]; !ok {
		return fmt.Errorf("node %s is not a cluster member", id)
	}
	f.log = append(f.log, fmt.Sprintf("remove %s", id))
	delete(f.members, id)
	return nil
}

func (f *fakeCluster) snapshot() map[string]Member {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]Member, len(f.members))
	for k, v := range f.members {
		out[k] = v
	}
	return out
}

func (f *fakeCluster) callLog() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.log...)
}

func newTestReconciler(f *fakeCluster) *Reconciler {
	r := NewReconciler(f, f)
	// Avoid DNS dependencies in tests; address format is already validated.
	r.Resolver = func(_ context.Context, _ string) error { return nil }
	return r
}

func TestReconcileConvergesFullScenario(t *testing.T) {
	// Starting state after a messy scale-out:
	//  n1: present and correct
	//  n2: present but moved to a new address (stale record must collapse by ID)
	//  n3: demoted to non-voter in the roster
	//  n4: in roster but never registered
	//  manual: manually added node, absent from roster (must be flagged, kept)
	//  dead: decommissioned node, absent from roster (must be flagged, kept)
	f := newFakeCluster(
		Member{ID: "n1", Addr: "h1:1", Voter: true},
		Member{ID: "n2", Addr: "h2-old:1", Voter: true},
		Member{ID: "n3", Addr: "h3:1", Voter: true},
		Member{ID: "manual", Addr: "hm:1", Voter: true},
		Member{ID: "dead", Addr: "hd:1", Voter: false},
	)
	rs := rosterOf(
		Entry{ID: "n1", Addr: "h1:1", Voter: true},
		Entry{ID: "n2", Addr: "h2-new:1", Voter: true},
		Entry{ID: "n3", Addr: "h3:1", Voter: false},
		Entry{ID: "n4", Addr: "h4:1", Voter: false},
	)

	out, err := newTestReconciler(f).Reconcile(context.Background(), rs)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if !out.Changed() {
		t.Fatal("expected membership changes")
	}

	// n2's address move is exactly one remove (of its own stale record) then
	// one join; no unlisted node is ever removed.
	gotLog := strings.Join(f.callLog(), "\n")
	if strings.Contains(gotLog, "remove manual") || strings.Contains(gotLog, "remove dead") {
		t.Fatalf("unlisted node removed:\n%s", gotLog)
	}
	if want := "remove n2"; !strings.Contains(gotLog, want) {
		t.Fatalf("missing stale-record removal, log:\n%s", gotLog)
	}
	if want := "join n2 at h2-new:1"; !strings.Contains(gotLog, want) {
		t.Fatalf("missing relocation join, log:\n%s", gotLog)
	}

	final := f.snapshot()
	want := map[string]Member{
		"n1":     {ID: "n1", Addr: "h1:1", Voter: true},
		"n2":     {ID: "n2", Addr: "h2-new:1", Voter: true},
		"n3":     {ID: "n3", Addr: "h3:1", Voter: false},
		"n4":     {ID: "n4", Addr: "h4:1", Voter: false},
		"manual": {ID: "manual", Addr: "hm:1", Voter: true},
		"dead":   {ID: "dead", Addr: "hd:1", Voter: false},
	}
	if !reflect.DeepEqual(final, want) {
		t.Fatalf("final membership mismatch:\n got %+v\nwant %+v", final, want)
	}

	// Exactly one record exists per identity.
	if len(final) != 6 {
		t.Fatalf("expected 6 members, got %d", len(final))
	}

	// The unlisted members are surfaced in the report.
	var unknownIDs []string
	for _, m := range out.Plan.Unknown {
		unknownIDs = append(unknownIDs, m.ID)
	}
	if got, want := unknownIDs, []string{"dead", "manual"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unknown members got %v want %v", got, want)
	}
}

func TestReconcileIsIdempotentAcrossRestart(t *testing.T) {
	f := newFakeCluster(
		Member{ID: "n2", Addr: "h2-old:1", Voter: true},
		Member{ID: "extra", Addr: "hx:1", Voter: false},
	)
	rs := rosterOf(
		Entry{ID: "n1", Addr: "h1:1", Voter: true},
		Entry{ID: "n2", Addr: "h2-new:1", Voter: false},
	)

	// First run converges.
	if _, err := newTestReconciler(f).Reconcile(context.Background(), rs); err != nil {
		t.Fatal(err)
	}
	afterFirst := f.snapshot()
	firstLogLen := len(f.callLog())

	// Simulate a restart: a brand new reconciler with no in-memory state is
	// pointed at the same cluster and roster. Re-running must converge to the
	// same result with zero further mutations.
	out, err := newTestReconciler(f).Reconcile(context.Background(), rs)
	if err != nil {
		t.Fatal(err)
	}
	if out.Changed() {
		t.Fatalf("rerun after convergence must not apply changes, got %+v", out.Executed)
	}
	if !reflect.DeepEqual(f.snapshot(), afterFirst) {
		t.Fatal("rerun changed cluster state")
	}
	if len(f.callLog()) != firstLogLen {
		t.Fatalf("rerun issued membership calls: %v", f.callLog()[firstLogLen:])
	}

	// A third run is still a no-op (steady state).
	out2, err := newTestReconciler(f).Reconcile(context.Background(), rs)
	if err != nil {
		t.Fatal(err)
	}
	if out2.Changed() || len(f.callLog()) != firstLogLen {
		t.Fatal("third reconciliation run was not a no-op")
	}
}

func TestReconcileInvalidRosterMakesNoChanges(t *testing.T) {
	f := newFakeCluster(Member{ID: "n1", Addr: "h1:1", Voter: true})
	before := f.snapshot()
	rs := rosterOf(
		Entry{ID: "dup", Addr: "h:1", Voter: true},
		Entry{ID: "dup", Addr: "h:2", Voter: true},
	)
	if _, err := newTestReconciler(f).Reconcile(context.Background(), rs); err == nil {
		t.Fatal("expected error for invalid roster")
	}
	if got := f.callLog(); len(got) != 0 {
		t.Fatalf("invalid roster triggered calls %v", got)
	}
	if !reflect.DeepEqual(f.snapshot(), before) {
		t.Fatal("invalid roster mutated cluster state")
	}
}

func TestReconcileConflictMakesNoChanges(t *testing.T) {
	f := newFakeCluster(
		Member{ID: "n1", Addr: "h1:1", Voter: true},
		Member{ID: "squatter", Addr: "h2:1", Voter: true},
	)
	before := f.snapshot()
	rs := rosterOf(
		Entry{ID: "n1", Addr: "h1:1", Voter: true},
		Entry{ID: "n2", Addr: "h2:1", Voter: true}, // address held by unlisted node
	)
	_, err := newTestReconciler(f).Reconcile(context.Background(), rs)
	if !errors.Is(err, ErrPlanConflict) {
		t.Fatalf("expected ErrPlanConflict, got %v", err)
	}
	if len(f.callLog()) != 0 {
		t.Fatalf("conflict triggered calls %v", f.callLog())
	}
	if !reflect.DeepEqual(f.snapshot(), before) {
		t.Fatal("conflict mutated cluster state")
	}
}

func TestReconcilePreflightFailureMakesNoChanges(t *testing.T) {
	f := newFakeCluster()
	r := newTestReconciler(f)
	r.Resolver = func(_ context.Context, addr string) error {
		if addr == "bad:1" {
			return errors.New("unresolvable")
		}
		return nil
	}
	rs := rosterOf(
		Entry{ID: "n1", Addr: "ok:1", Voter: true},
		Entry{ID: "n2", Addr: "bad:1", Voter: true},
	)
	if _, err := r.Reconcile(context.Background(), rs); err == nil {
		t.Fatal("expected preflight error")
	}
	if len(f.callLog()) != 0 {
		t.Fatalf("preflight failure triggered calls %v", f.callLog())
	}

	// All preflight checks happen before any mutation: with an all-failing
	// resolver and a multi-action plan, no join is attempted.
	f2 := newFakeCluster(Member{ID: "a", Addr: "old:1", Voter: true})
	r2 := newTestReconciler(f2)
	r2.Resolver = func(_ context.Context, _ string) error { return errors.New("dns down") }
	rs2 := rosterOf(
		Entry{ID: "a", Addr: "new:1", Voter: true},
		Entry{ID: "b", Addr: "b:1", Voter: true},
	)
	if _, err := r2.Reconcile(context.Background(), rs2); err == nil {
		t.Fatal("expected preflight error")
	}
	if len(f2.callLog()) != 0 {
		t.Fatalf("expected zero calls when preflight fails, got %v", f2.callLog())
	}
}

func TestReconcileDryRunAppliesNothing(t *testing.T) {
	f := newFakeCluster(Member{ID: "old", Addr: "ho:1", Voter: true})
	before := f.snapshot()
	rs := rosterOf(
		Entry{ID: "new", Addr: "hn:1", Voter: true},
		Entry{ID: "old", Addr: "ho:1", Voter: false},
	)
	out, err := newTestReconciler(f).DryRun(context.Background(), rs)
	if err != nil {
		t.Fatal(err)
	}
	if !out.DryRun || len(out.Executed) != 0 {
		t.Fatalf("dry run must not execute: %+v", out)
	}
	if len(out.Plan.Actions) != 2 {
		t.Fatalf("dry run should still report the planned actions: %+v", out.Plan.Actions)
	}
	if reflect.DeepEqual(f.snapshot(), before) == false {
		t.Fatal("dry run mutated cluster state")
	}
}

func TestReconcileResumeAfterMidFlightFailure(t *testing.T) {
	f := newFakeCluster(
		Member{ID: "n1", Addr: "h1:1", Voter: true},
		Member{ID: "n2", Addr: "h2-old:1", Voter: true},
	)
	rs := rosterOf(
		Entry{ID: "n1", Addr: "h1:1", Voter: true},
		Entry{ID: "n2", Addr: "h2-new:1", Voter: true},
	)

	// Make the relocation rejoin fail after the stale record was removed.
	f.failJoin["n2"] = true
	out, err := newTestReconciler(f).Reconcile(context.Background(), rs)
	if err == nil {
		t.Fatal("expected join failure")
	}
	if out == nil || len(out.Executed) != 0 {
		// Relocation is one action; it counts only after both steps succeed.
		t.Fatalf("unexpected executed actions: %+v", out)
	}
	if _, present := f.snapshot()["n2"]; present {
		t.Fatal("n2 should have been removed before the failed rejoin")
	}

	// Operator fixes the node; rerun converges. Re-planning now sees n2 as
	// missing and adds it.
	f.failJoin["n2"] = false
	out2, err := newTestReconciler(f).Reconcile(context.Background(), rs)
	if err != nil {
		t.Fatalf("rerun failed: %v", err)
	}
	if len(out2.Executed) != 1 || out2.Executed[0].Kind != KindAdd {
		t.Fatalf("expected a single add on resume, got %+v", out2.Executed)
	}
	if got := f.snapshot()["n2"]; got.Addr != "h2-new:1" || !got.Voter {
		t.Fatalf("n2 not converged: %+v", got)
	}

	// And a further rerun is a no-op.
	out3, err := newTestReconciler(f).Reconcile(context.Background(), rs)
	if err != nil {
		t.Fatal(err)
	}
	if out3.Changed() {
		t.Fatal("final rerun should be a no-op")
	}
}

func TestApplyRelocationRemovesOwnRecordOnly(t *testing.T) {
	f := newFakeCluster(
		Member{ID: "n1", Addr: "old:1", Voter: true},
		Member{ID: "keep", Addr: "k:1", Voter: false},
	)
	p := &Plan{Actions: []Action{
		{Kind: KindRelocate, ID: "n1", Addr: "new:1", Voter: true, OldAddr: "old:1"},
	}}
	if _, err := Apply(context.Background(), p, f); err != nil {
		t.Fatal(err)
	}
	log := strings.Join(f.callLog(), ",")
	if !strings.Contains(log, "remove n1") || !strings.Contains(log, "join n1 at new:1") {
		t.Fatalf("unexpected call sequence: %s", log)
	}
	if strings.Index(log, "remove n1") > strings.Index(log, "join n1") {
		t.Fatalf("remove must precede join: %s", log)
	}
	if _, ok := f.snapshot()["keep"]; !ok {
		t.Fatal("unrelated member was removed")
	}
}

func TestReconcileRequiresDependencies(t *testing.T) {
	rs := rosterOf(Entry{ID: "n1", Addr: "h1:1", Voter: true})
	if _, err := (&Reconciler{}).Reconcile(context.Background(), rs); err == nil {
		t.Fatal("expected error without lister")
	}
	if _, err := (&Reconciler{Lister: newFakeCluster()}).Reconcile(context.Background(), rs); err == nil {
		t.Fatal("expected error without executor")
	}
	if _, err := Apply(context.Background(), &Plan{}, nil); err == nil {
		t.Fatal("expected error for nil executor")
	}
}
