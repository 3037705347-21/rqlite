package roster

import (
	"context"
	"errors"
	"fmt"
	"net"
)

// Executor performs membership changes against a cluster. Implementations
// must be safe to call from Reconcile. Join adds or re-adds a node by ID at
// the given address with the given voting status; Remove removes a node by ID.
//
// Both operations are expected to converge rather than fail when the desired
// state already holds: a join for an identical ID/address/suffrage is a no-op,
// matching rqlite's leader behavior.
type Executor interface {
	Join(ctx context.Context, id, addr string, voter bool) error
	Remove(ctx context.Context, id string) error
}

// MemberLister returns the cluster's current membership. The IDs are the
// stable node identities; reconciliation always keys on ID.
type MemberLister interface {
	Members(ctx context.Context) ([]Member, error)
}

// Resolver preflights an address before any membership change is attempted. It
// lets a reconciliation fail closed when a roster address cannot be used,
// ensuring the whole plan is abandoned instead of failing halfway through.
type Resolver func(ctx context.Context, addr string) error

// DefaultResolver resolves the host part of a host:port address, mirroring the
// check the rqlite leader performs when it serves a join request.
func DefaultResolver(ctx context.Context, addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if _, err := net.DefaultResolver.LookupHost(ctx, host); err != nil {
		return fmt.Errorf("cannot resolve roster address %s: %w", addr, err)
	}
	return nil
}

// Outcome is the result of one reconciliation run.
type Outcome struct {
	// Plan is the plan that was produced.
	Plan *Plan `json:"plan"`
	// Executed are the actions that were applied successfully. On a dry run
	// it is always empty.
	Executed []Action `json:"executed"`
	// DryRun indicates no changes were attempted.
	DryRun bool `json:"dry_run"`
}

// Changed reports whether any membership change was applied.
func (o *Outcome) Changed() bool { return o != nil && len(o.Executed) > 0 }

// Reconciler reconciles cluster membership against rosters. It holds no state
// of its own: every run reads the current membership, replans from scratch and
// converges. Re-running after a restart therefore yields the same end state,
// and a rerun against an already-converged cluster performs no changes.
type Reconciler struct {
	// Lister provides the observed membership. Required.
	Lister MemberLister
	// Executor performs the planned changes. Required for non-dry runs.
	Executor Executor
	// Resolver, if non-nil, preflights every address touched by the plan
	// before any change is made. A failure aborts the run with no changes.
	// Set to nil to skip address preflight.
	Resolver Resolver
}

// NewReconciler returns a Reconciler using the default (DNS) address
// preflight.
func NewReconciler(lister MemberLister, executor Executor) *Reconciler {
	return &Reconciler{
		Lister:   lister,
		Executor: executor,
		Resolver: DefaultResolver,
	}
}

// DryRun plans without applying anything. It is useful for inspecting which
// nodes would be added or converged, and which members are present but
// unlisted.
func (r *Reconciler) DryRun(ctx context.Context, rs *Roster) (*Outcome, error) {
	return r.run(ctx, rs, true)
}

// Reconcile plans and, unless the roster is invalid or a conflict or preflight
// failure is detected, applies the plan. Application stops at the first
// failing action; rerunning Reconcile after fixing the cause resumes from the
// current membership and converges.
func (r *Reconciler) Reconcile(ctx context.Context, rs *Roster) (*Outcome, error) {
	return r.run(ctx, rs, false)
}

func (r *Reconciler) run(ctx context.Context, rs *Roster, dryRun bool) (*Outcome, error) {
	if r.Lister == nil {
		return nil, errors.New("roster reconciler requires a member lister")
	}
	// Validate the roster before touching the cluster, so an invalid roster
	// never triggers partial changes.
	if err := rs.Validate(); err != nil {
		return nil, fmt.Errorf("invalid roster, no changes made: %w", err)
	}

	members, err := r.Lister.Members(ctx)
	if err != nil {
		return nil, fmt.Errorf("read cluster members: %w", err)
	}
	plan, err := MakePlan(rs, members)
	if err != nil {
		// Includes roster validation failures and unsafe conflicts: no plan
		// exists, so nothing can be applied.
		return nil, err
	}

	out := &Outcome{Plan: plan, DryRun: dryRun}
	if dryRun {
		return out, nil
	}
	if r.Executor == nil {
		return nil, errors.New("roster reconciler requires an executor")
	}

	if r.Resolver != nil && len(plan.Actions) > 0 {
		// Resolve all target addresses before mutating anything.
		seen := make(map[string]struct{}, len(plan.Actions))
		for _, a := range plan.Actions {
			if _, ok := seen[a.Addr]; ok {
				continue
			}
			seen[a.Addr] = struct{}{}
			if err := r.Resolver(ctx, a.Addr); err != nil {
				return nil, fmt.Errorf("address preflight failed, no changes made: %w", err)
			}
		}
	}

	applied, err := Apply(ctx, plan, r.Executor)
	out.Executed = applied
	if err != nil {
		return out, err
	}
	return out, nil
}

// Apply executes the plan's actions in order. A relocation or suffrage change
// is a remove of the roster node's own record followed by a join; the remove
// never targets an unlisted node. Application stops at the first error, and
// the actions already applied are returned.
func Apply(ctx context.Context, p *Plan, ex Executor) ([]Action, error) {
	if ex == nil {
		return nil, errors.New("nil executor")
	}
	applied := make([]Action, 0, len(p.Actions))
	for _, a := range p.Actions {
		switch a.Kind {
		case KindAdd:
			if err := ex.Join(ctx, a.ID, a.Addr, a.Voter); err != nil {
				return applied, fmt.Errorf("add node %s: %w", a.ID, err)
			}
		case KindRelocate:
			if err := ex.Remove(ctx, a.ID); err != nil {
				return applied, fmt.Errorf("remove stale record for relocated node %s: %w", a.ID, err)
			}
			if err := ex.Join(ctx, a.ID, a.Addr, a.Voter); err != nil {
				return applied, fmt.Errorf("rejoin relocated node %s at %s: %w", a.ID, a.Addr, err)
			}
		case KindSetVoter:
			if err := ex.Remove(ctx, a.ID); err != nil {
				return applied, fmt.Errorf("remove node %s before suffrage change: %w", a.ID, err)
			}
			if err := ex.Join(ctx, a.ID, a.Addr, a.Voter); err != nil {
				return applied, fmt.Errorf("rejoin node %s with changed voting status: %w", a.ID, err)
			}
		default:
			return applied, fmt.Errorf("unknown action kind %d", a.Kind)
		}
		applied = append(applied, a)
	}
	return applied, nil
}
