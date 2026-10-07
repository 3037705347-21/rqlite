// Package roster implements reconciliation of a cluster's membership against a
// declarative roster: a list of the nodes the cluster should contain, each with
// its identity (node ID), Raft address, and voting status.
//
// Reconciliation rules:
//
//   - A roster entry whose node ID is absent from the cluster is added.
//   - A roster entry whose node ID is present, but whose address has changed,
//     is converged back to a single record keyed by that identity: the stale
//     record is removed and the node is joined again at the new address.
//   - A roster entry whose voting status has changed is removed and joined
//     again with the declared suffrage.
//   - Cluster members that are not present in the roster are never removed;
//     they are reported as unknown members only.
//   - If the roster itself is invalid, or it conflicts with the observed
//     cluster state in a way that cannot be resolved safely (for example a
//     declared address is already owned by a different, unlisted identity),
//     no membership changes are made at all.
//
// Planning is a pure, deterministic function of the roster and the observed
// membership, so rerunning a reconciliation after a restart produces the same
// outcome: once the cluster matches the roster, every rerun is a no-op.
package roster

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Entry is a single roster line: one node identity with the Raft address it
// should be reachable at and whether it should be a voting member.
type Entry struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
	// Voter is true for a voting node and false for a non-voting
	// (read-replica) node.
	Voter bool `json:"voter"`
}

// Roster is the declarative list of members a cluster should have.
type Roster struct {
	Nodes []Entry `json:"nodes"`
}

// Parse reads and validates a roster. Parsing is strict: unknown JSON fields
// are rejected so that typos in a roster cannot be silently ignored. A nil
// reader is an error.
func Parse(r io.Reader) (*Roster, error) {
	if r == nil {
		return nil, errors.New("nil roster reader")
	}
	dec := json.NewDecoder(r)
	dec.DisallowUnknownFields()
	var rs Roster
	if err := dec.Decode(&rs); err != nil {
		return nil, fmt.Errorf("parse roster: %w", err)
	}
	// Reject trailing data, such as a second JSON document.
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			err = errors.New("unexpected trailing data")
		}
		return nil, fmt.Errorf("parse roster: %w", err)
	}
	if err := rs.Validate(); err != nil {
		return nil, err
	}
	return &rs, nil
}

// LoadFile reads and validates a roster from the given file path.
func LoadFile(path string) (*Roster, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return Parse(f)
}

// Validate checks the roster in isolation, without looking at the cluster. A
// roster is valid when it is non-empty and every entry has a unique, non-empty
// ID, a unique host:port address with a numeric port, and an explicit voting
// flag (the JSON zero value false is treated as an explicit non-voter).
func (r *Roster) Validate() error {
	if r == nil {
		return errors.New("roster is nil")
	}
	var errs []error
	if len(r.Nodes) == 0 {
		errs = append(errs, errors.New("roster contains no nodes"))
	}
	seenIDs := make(map[string]int, len(r.Nodes))
	seenAddrs := make(map[string]int, len(r.Nodes))
	for i, e := range r.Nodes {
		where := fmt.Sprintf("entry %d", i)
		if e.ID == "" {
			errs = append(errs, fmt.Errorf("%s: node id is required", where))
		} else if strings.TrimSpace(e.ID) != e.ID {
			errs = append(errs, fmt.Errorf("%s: node id %q must not contain leading or trailing whitespace", where, e.ID))
		} else if line, dup := seenIDs[e.ID]; dup {
			errs = append(errs, fmt.Errorf("%s: duplicate node id %q (also used by entry %d)", where, e.ID, line))
		} else {
			seenIDs[e.ID] = i
		}

		if err := validateAddr(e.Addr); err != nil {
			errs = append(errs, fmt.Errorf("%s (id %q): %w", where, e.ID, err))
		} else if line, dup := seenAddrs[e.Addr]; dup {
			errs = append(errs, fmt.Errorf("%s: duplicate address %q (also used by entry %d)", where, e.Addr, line))
		} else {
			seenAddrs[e.Addr] = i
		}
	}
	return errors.Join(errs...)
}

func validateAddr(addr string) error {
	if addr == "" {
		return errors.New("node address is required")
	}
	if strings.TrimSpace(addr) != addr {
		return fmt.Errorf("address %q must not contain leading or trailing whitespace", addr)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("address %q must be host:port", addr)
	}
	if host == "" {
		return fmt.Errorf("address %q is missing a host", addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("address %q has invalid port %q", addr, port)
	}
	return nil
}

// sortedEntries returns the roster entries sorted by ID, the canonical order
// used for planning and reporting.
func (r *Roster) sortedEntries() []Entry {
	out := make([]Entry, len(r.Nodes))
	copy(out, r.Nodes)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
