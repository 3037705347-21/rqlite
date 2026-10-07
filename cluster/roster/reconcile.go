package roster

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Member is an observed cluster member: its Raft node ID (the stable
// identity), the Raft address it is currently registered with, and whether it
// is a voter.
type Member struct {
	ID    string `json:"id"`
	Addr  string `json:"addr"`
	Voter bool   `json:"voter"`
}

// Kind identifies the kind of membership change an Action performs.
type Kind int

const (
	kindUnknown Kind = iota
	// KindAdd joins a roster node whose ID is not a member of the cluster.
	KindAdd
	// KindRelocate converges a node whose ID is a member, but whose address
	// has changed: the stale record for the ID is removed and the node is
	// joined again at the roster address, collapsing the two records back to
	// one keyed by identity.
	KindRelocate
	// KindSetVoter re-joins a node whose address is unchanged but whose
	// voting status differs from the roster.
	KindSetVoter
)

// String returns a human-readable description of the action kind.
func (k Kind) String() string {
	switch k {
	case KindAdd:
		return "add"
	case KindRelocate:
		return "relocate"
	case KindSetVoter:
		return "set-voter"
	default:
		return "unknown"
	}
}

// Action is a single, deterministic membership change.
type Action struct {
	Kind    Kind   `json:"kind"`
	ID      string `json:"id"`
	Addr    string `json:"addr"`
	Voter   bool   `json:"voter"`
	OldAddr string `json:"old_addr,omitempty"`
}

// MarshalJSON renders the action kind as a string.
func (a Action) MarshalJSON() ([]byte, error) {
	type alias Action
	return json.Marshal(struct {
		alias
		Kind string `json:"kind"`
	}{
		alias: alias(a),
		Kind:  a.Kind.String(),
	})
}

func (a Action) String() string {
	switch a.Kind {
	case KindAdd:
		return fmt.Sprintf("add node %s at %s as %s", a.ID, a.Addr, voterWord(a.Voter))
	case KindRelocate:
		return fmt.Sprintf("relocate node %s from %s to %s as %s", a.ID, a.OldAddr, a.Addr, voterWord(a.Voter))
	case KindSetVoter:
		return fmt.Sprintf("set node %s at %s to %s", a.ID, a.Addr, voterWord(a.Voter))
	default:
		return "invalid action"
	}
}

func voterWord(v bool) string {
	if v {
		return "voter"
	}
	return "non-voter"
}

// Plan is the result of diffing a roster against the observed membership. It
// contains the actions to execute, in a fixed order, plus the bookkeeping
// entries used for reporting. A plan never contains an action that removes a
// node that is not in the roster.
type Plan struct {
	// Actions to execute, in execution order: adds first, then address
	// relocations, then voting-status-only changes. Each group is ordered by
	// node ID.
	Actions []Action `json:"actions"`
	// InSync lists roster entries that already match the cluster exactly.
	InSync []Entry `json:"in_sync"`
	// Unknown lists cluster members that are not in the roster. They are
	// reported only and are never removed by a reconciliation.
	Unknown []Member `json:"unknown"`
}

// NoChange reports whether the plan performs no membership changes.
func (p *Plan) NoChange() bool { return p != nil && len(p.Actions) == 0 }

// ErrPlanConflict is returned when a roster cannot be applied to the observed
// cluster state without removing an unlisted member or guessing an identity.
// When any such conflict exists, planning fails and no actions are produced,
// leaving the cluster untouched.
var ErrPlanConflict = errors.New("roster conflicts with observed cluster members")

// MakePlan builds the deterministic reconciliation plan for the given roster
// and observed members. It returns an error, with a nil plan, if the roster is
// invalid or the roster cannot be converged safely; in that case no action is
// taken.
func MakePlan(rs *Roster, members []Member) (*Plan, error) {
	if err := rs.Validate(); err != nil {
		return nil, err
	}

	byID := make(map[string]Member, len(members))
	addrOwner := make(map[string]string, len(members))
	for _, m := range members {
		if m.ID == "" {
			return nil, errors.New("observed cluster member has an empty node ID")
		}
		if other, dup := byID[m.ID]; dup {
			return nil, fmt.Errorf("observed cluster members are inconsistent: node ID %q appears more than once (addresses %q and %q)",
				m.ID, other.Addr, m.Addr)
		}
		byID[m.ID] = m
		if m.Addr == "" {
			return nil, fmt.Errorf("observed cluster member %q has an empty address", m.ID)
		}
		if other, taken := addrOwner[m.Addr]; taken {
			return nil, fmt.Errorf("observed cluster members are inconsistent: address %q is claimed by both node %q and node %q",
				m.Addr, other, m.ID)
		}
		addrOwner[m.Addr] = m.ID
	}

	p := &Plan{}
	var adds, relocations, suffrages []Action
	for _, e := range rs.sortedEntries() {
		// A roster address must not be occupied by a different node ID. The
		// reconciliation may not evict an unlisted node to make room, so this
		// is a hard conflict and the whole plan is abandoned.
		if owner, taken := addrOwner[e.Addr]; taken && owner != e.ID {
			return nil, fmt.Errorf("%w: roster declares address %s for node %q, but it belongs to unlisted node %q; remove or re-register that node manually",
				ErrPlanConflict, e.Addr, e.ID, owner)
		}

		m, present := byID[e.ID]
		switch {
		case !present:
			adds = append(adds, Action{Kind: KindAdd, ID: e.ID, Addr: e.Addr, Voter: e.Voter})
		case m.Addr != e.Addr:
			relocations = append(relocations, Action{
				Kind: KindRelocate, ID: e.ID, Addr: e.Addr, Voter: e.Voter, OldAddr: m.Addr,
			})
		case m.Voter != e.Voter:
			suffrages = append(suffrages, Action{Kind: KindSetVoter, ID: e.ID, Addr: e.Addr, Voter: e.Voter})
		default:
			p.InSync = append(p.InSync, e)
		}
	}
	p.Actions = append(p.Actions, adds...)
	p.Actions = append(p.Actions, relocations...)
	p.Actions = append(p.Actions, suffrages...)

	rosterIDs := make(map[string]struct{}, len(rs.Nodes))
	for _, e := range rs.Nodes {
		rosterIDs[e.ID] = struct{}{}
	}
	for _, m := range members {
		if _, listed := rosterIDs[m.ID]; !listed {
			p.Unknown = append(p.Unknown, m)
		}
	}
	sort.Slice(p.Unknown, func(i, j int) bool { return p.Unknown[i].ID < p.Unknown[j].ID })
	sort.Slice(p.InSync, func(i, j int) bool { return p.InSync[i].ID < p.InSync[j].ID })
	return p, nil
}

// String renders a short, deterministic summary of the plan.
func (p *Plan) String() string {
	var b strings.Builder
	for _, a := range p.Actions {
		b.WriteString(a.String())
		b.WriteByte('\n')
	}
	for _, m := range p.Unknown {
		fmt.Fprintf(&b, "unlisted node %s at %s left untouched\n", m.ID, m.Addr)
	}
	if p.NoChange() && len(p.Unknown) == 0 {
		b.WriteString("cluster membership matches roster\n")
	}
	return b.String()
}
