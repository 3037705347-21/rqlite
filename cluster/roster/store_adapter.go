package roster

import (
	"context"
	"fmt"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/store"
)

// StoreAPI is the subset of *store.Store used to reconcile membership. It is
// implemented by *store.Store and must be invoked on the cluster leader; the
// store rejects join and remove operations on followers.
type StoreAPI interface {
	Nodes() ([]*store.Server, error)
	Join(jr *command.JoinRequest) error
	Remove(ctx context.Context, rn *command.RemoveNodeRequest) error
}

// StoreAdapter bridges a *store.Store to the MemberLister and Executor
// interfaces.
type StoreAdapter struct {
	Store StoreAPI
}

// NewStoreAdapter returns a StoreAdapter for the given store.
func NewStoreAdapter(s StoreAPI) *StoreAdapter {
	return &StoreAdapter{Store: s}
}

// Members returns the observed Raft membership.
func (a *StoreAdapter) Members(_ context.Context) ([]Member, error) {
	servers, err := a.Store.Nodes()
	if err != nil {
		return nil, err
	}
	members := make([]Member, 0, len(servers))
	for _, s := range servers {
		if s == nil {
			continue
		}
		members = append(members, Member{
			ID:    s.ID,
			Addr:  s.Addr,
			Voter: s.Suffrage == command.Suffrage_VOTER,
		})
	}
	return members, nil
}

// Join registers the node with the leader.
func (a *StoreAdapter) Join(_ context.Context, id, addr string, voter bool) error {
	if err := a.Store.Join(&command.JoinRequest{
		Id:      id,
		Address: addr,
		Voter:   voter,
	}); err != nil {
		return fmt.Errorf("join node %s at %s: %w", id, addr, err)
	}
	return nil
}

// Remove removes the node from Raft membership.
func (a *StoreAdapter) Remove(ctx context.Context, id string) error {
	if err := a.Store.Remove(ctx, &command.RemoveNodeRequest{Id: id}); err != nil {
		return fmt.Errorf("remove node %s: %w", id, err)
	}
	return nil
}
