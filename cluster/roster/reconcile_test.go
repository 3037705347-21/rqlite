package roster

import (
	"errors"
	"reflect"
	"testing"
)

func rosterOf(entries ...Entry) *Roster { return &Roster{Nodes: entries} }

func actionsOf(p *Plan) []Action { return p.Actions }

func TestMakePlanAddsMissingNodes(t *testing.T) {
	rs := rosterOf(
		Entry{ID: "n2", Addr: "h2:2", Voter: false},
		Entry{ID: "n1", Addr: "h1:1", Voter: true},
	)
	p, err := MakePlan(rs, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Adds are ordered by ID regardless of roster order.
	want := []Action{
		{Kind: KindAdd, ID: "n1", Addr: "h1:1", Voter: true},
		{Kind: KindAdd, ID: "n2", Addr: "h2:2", Voter: false},
	}
	if got := actionsOf(p); !reflect.DeepEqual(got, want) {
		t.Fatalf("actions mismatch:\n got %+v\nwant %+v", got, want)
	}
}

func TestMakePlanInSyncIsNoop(t *testing.T) {
	rs := rosterOf(
		Entry{ID: "n1", Addr: "h1:1", Voter: true},
		Entry{ID: "n2", Addr: "h2:2", Voter: false},
	)
	members := []Member{
		{ID: "n2", Addr: "h2:2", Voter: false},
		{ID: "n1", Addr: "h1:1", Voter: true},
	}
	p, err := MakePlan(rs, members)
	if err != nil {
		t.Fatal(err)
	}
	if !p.NoChange() {
		t.Fatalf("expected no actions, got %+v", p.Actions)
	}
	if len(p.InSync) != 2 {
		t.Fatalf("expected 2 in-sync entries, got %+v", p.InSync)
	}
	if len(p.Unknown) != 0 {
		t.Fatalf("expected no unknown members, got %+v", p.Unknown)
	}
}

func TestMakePlanUnknownMembersAreMarkedOnly(t *testing.T) {
	rs := rosterOf(Entry{ID: "n1", Addr: "h1:1", Voter: true})
	// n2 was manually added and is not in the roster; n3 is a decommissioned
	// node whose record was never removed.
	members := []Member{
		{ID: "n3", Addr: "h3:3", Voter: false},
		{ID: "n1", Addr: "h1:1", Voter: true},
		{ID: "n2", Addr: "h2:2", Voter: true},
	}
	p, err := MakePlan(rs, members)
	if err != nil {
		t.Fatal(err)
	}
	if !p.NoChange() {
		t.Fatalf("unlisted nodes must not trigger changes, got %+v", p.Actions)
	}
	wantUnknown := []Member{
		{ID: "n2", Addr: "h2:2", Voter: true},
		{ID: "n3", Addr: "h3:3", Voter: false},
	}
	if !reflect.DeepEqual(p.Unknown, wantUnknown) {
		t.Fatalf("unknown members mismatch, got %+v want %+v", p.Unknown, wantUnknown)
	}
}

func TestMakePlanRelocatesChangedAddress(t *testing.T) {
	rs := rosterOf(Entry{ID: "n1", Addr: "h1-new:1", Voter: true})
	members := []Member{{ID: "n1", Addr: "h1-old:1", Voter: true}}
	p, err := MakePlan(rs, members)
	if err != nil {
		t.Fatal(err)
	}
	want := []Action{{
		Kind: KindRelocate, ID: "n1", Addr: "h1-new:1", Voter: true, OldAddr: "h1-old:1",
	}}
	if !reflect.DeepEqual(p.Actions, want) {
		t.Fatalf("got %+v want %+v", p.Actions, want)
	}
}

func TestMakePlanRelocationCarriesNewSuffrage(t *testing.T) {
	rs := rosterOf(Entry{ID: "n1", Addr: "h1-new:1", Voter: false})
	members := []Member{{ID: "n1", Addr: "h1-old:1", Voter: true}}
	p, err := MakePlan(rs, members)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Actions) != 1 || p.Actions[0].Kind != KindRelocate || p.Actions[0].Voter {
		t.Fatalf("expected single relocation to non-voter, got %+v", p.Actions)
	}
}

func TestMakePlanChangesSuffrageOnly(t *testing.T) {
	rs := rosterOf(Entry{ID: "n1", Addr: "h1:1", Voter: false})
	members := []Member{{ID: "n1", Addr: "h1:1", Voter: true}}
	p, err := MakePlan(rs, members)
	if err != nil {
		t.Fatal(err)
	}
	want := []Action{{Kind: KindSetVoter, ID: "n1", Addr: "h1:1", Voter: false}}
	if !reflect.DeepEqual(p.Actions, want) {
		t.Fatalf("got %+v want %+v", p.Actions, want)
	}
}

func TestMakePlanActionOrderIsDeterministic(t *testing.T) {
	rs := rosterOf(
		Entry{ID: "setvoter", Addr: "a:1", Voter: false},
		Entry{ID: "add", Addr: "b:1", Voter: true},
		Entry{ID: "relocate", Addr: "c-new:1", Voter: true},
		Entry{ID: "z-in-sync", Addr: "d:1", Voter: true},
	)
	members := []Member{
		{ID: "setvoter", Addr: "a:1", Voter: true},
		{ID: "relocate", Addr: "c-old:1", Voter: true},
		{ID: "z-in-sync", Addr: "d:1", Voter: true},
	}
	p, err := MakePlan(rs, members)
	if err != nil {
		t.Fatal(err)
	}
	gotKinds := make([]Kind, len(p.Actions))
	gotIDs := make([]string, len(p.Actions))
	for i, a := range p.Actions {
		gotKinds[i] = a.Kind
		gotIDs[i] = a.ID
	}
	// Adds, then relocations, then suffrage changes; each sorted by ID.
	wantKinds := []Kind{KindAdd, KindRelocate, KindSetVoter}
	wantIDs := []string{"add", "relocate", "setvoter"}
	if !reflect.DeepEqual(gotKinds, wantKinds) || !reflect.DeepEqual(gotIDs, wantIDs) {
		t.Fatalf("order mismatch, kinds=%v ids=%v", gotKinds, gotIDs)
	}

	// Planning the same inputs twice must yield identical plans.
	p2, err := MakePlan(rs, members)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(p, p2) {
		t.Fatal("plan is not deterministic for identical inputs")
	}
}

func TestMakePlanConflictRosterAddrOwnedByUnlistedNode(t *testing.T) {
	// Roster says n1 lives at h2:2, but that address is held by n2, which is
	// not in the roster. Converging would mean deleting n2, which is never
	// done automatically.
	rs := rosterOf(
		Entry{ID: "n1", Addr: "h1:1", Voter: true},
		Entry{ID: "n3", Addr: "h2:2", Voter: true},
	)
	members := []Member{
		{ID: "n1", Addr: "h1:1", Voter: true},
		{ID: "n2", Addr: "h2:2", Voter: true},
	}
	_, err := MakePlan(rs, members)
	if !errors.Is(err, ErrPlanConflict) {
		t.Fatalf("expected ErrPlanConflict, got %v", err)
	}
}

func TestMakePlanOwnAddressIsNotAConflict(t *testing.T) {
	// n1 keeping its own declared address is fine even with other members.
	rs := rosterOf(Entry{ID: "n1", Addr: "h1:1", Voter: true})
	members := []Member{
		{ID: "n1", Addr: "h1:1", Voter: true},
		{ID: "n2", Addr: "h2:2", Voter: true},
	}
	p, err := MakePlan(rs, members)
	if err != nil {
		t.Fatal(err)
	}
	if !p.NoChange() || len(p.Unknown) != 1 || p.Unknown[0].ID != "n2" {
		t.Fatalf("unexpected plan: %+v", p)
	}
}

func TestMakePlanRejectsInconsistentObservedMembers(t *testing.T) {
	t.Run("duplicate member IDs", func(t *testing.T) {
		rs := rosterOf(Entry{ID: "n1", Addr: "h1:1", Voter: true})
		members := []Member{
			{ID: "n9", Addr: "h9a:1", Voter: true},
			{ID: "n9", Addr: "h9b:1", Voter: true},
		}
		if _, err := MakePlan(rs, members); err == nil {
			t.Fatal("expected error for duplicate member IDs")
		}
	})
	t.Run("duplicate member addresses", func(t *testing.T) {
		rs := rosterOf(Entry{ID: "n1", Addr: "h1:1", Voter: true})
		members := []Member{
			{ID: "n9", Addr: "h9:1", Voter: true},
			{ID: "n8", Addr: "h9:1", Voter: true},
		}
		if _, err := MakePlan(rs, members); err == nil {
			t.Fatal("expected error for duplicate member addresses")
		}
	})
	t.Run("empty member ID", func(t *testing.T) {
		rs := rosterOf(Entry{ID: "n1", Addr: "h1:1", Voter: true})
		if _, err := MakePlan(rs, []Member{{ID: "", Addr: "h9:1"}}); err == nil {
			t.Fatal("expected error for empty member ID")
		}
	})
}

func TestMakePlanInvalidRosterProducesNoPlan(t *testing.T) {
	rs := rosterOf()
	if _, err := MakePlan(rs, nil); err == nil {
		t.Fatal("expected error for empty roster")
	}
}
