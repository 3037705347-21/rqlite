package system

import (
	"testing"
	"time"
)

// waitForLeaderOtherThan polls the cluster until its leader differs from
// oldLeader, returning the new leader node.
func waitForLeaderOtherThan(t *testing.T, c Cluster, oldLeader *Node) *Node {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		l, err := c.Leader()
		if err == nil && l != nil && !l.SameAs(oldLeader) {
			return l
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("leadership never transferred away from %s", oldLeader.ID)
	return nil
}

// Test_MultiNodeDurableQueued_LeaderChange verifies that durable queued
// writes survive a Raft leadership transfer exactly-once: requests Pending
// on the old leader at the moment of the switch are retried against the new
// leader, follower-accepted queued writes proxy-forward correctly, and
// identical retries are deduplicated cluster-wide.
func Test_MultiNodeDurableQueued_LeaderChange(t *testing.T) {
	n1 := mustNewLeaderNode("dl1")
	defer n1.Deprovision()
	n2 := mustNewNode("dl2", false)
	defer n2.Deprovision()
	if err := n2.Join(n1); err != nil {
		t.Fatalf("node2 failed to join: %s", err)
	}
	n3 := mustNewNode("dl3", false)
	defer n3.Deprovision()
	if err := n3.Join(n1); err != nil {
		t.Fatalf("node3 failed to join: %s", err)
	}

	c := Cluster{n1, n2, n3}
	leader, err := c.Leader()
	if err != nil {
		t.Fatalf("failed to find initial leader: %s", err)
	}
	if _, err := leader.Execute("CREATE TABLE acct(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)"); err != nil {
		t.Fatalf("create table: %s", err)
	}
	if _, err := leader.Execute("INSERT INTO acct VALUES(1, 0)"); err != nil {
		t.Fatalf("insert seed row: %s", err)
	}

	for _, n := range c {
		enableDurableService(t, n)
	}
	follower := n2
	if leader.SameAs(n2) {
		follower = n3
	}

	// Queued writes accepted on a follower are persisted there and proxied
	// to the current leader.
	for i := 1; i <= 3; i++ {
		id := "follower-" + string(rune('0'+i))
		if code, body := postQueued(t, follower, id, `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, true); code != 200 {
			t.Fatalf("follower queued write %s failed: %d %s", id, code, body)
		}
	}
	waitForBalance(t, leader, "3", 10*time.Second)

	// Deterministic pending-across-leader-change window: stop the leader's
	// HTTP service, fsync Pending records straight to its ledger, transfer
	// leadership, then bring the durable service back up. Its drainer must
	// retry the backlog toward the new leader.
	leader.Service.Close()
	for i := 1; i <= 5; i++ {
		mustSeedLedgerEntry(t, durableLedgerPath(leader), "switch-"+string(rune('0'+i)))
	}
	if err := leader.Store.Stepdown(true, ""); err != nil {
		t.Fatalf("stepdown: %s", err)
	}
	newLeader := waitForLeaderOtherThan(t, c, leader)
	enableDurableService(t, leader) // old leader rejoins as follower
	waitForBalance(t, newLeader, "8", 15*time.Second)

	// Fresh asynchronous writes spread across all three nodes.
	allNodes := []*Node{n1, n2, n3}
	for i := 1; i <= 5; i++ {
		id := "after-" + string(rune('0'+i))
		n := allNodes[i%3]
		if code, body := postQueued(t, n, id, `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, false); code != 200 {
			t.Fatalf("post-switch queued write %s to %s failed: %d %s", id, n.ID, code, body)
		}
	}
	waitForBalance(t, newLeader, "13", 15*time.Second)

	// Every request id retried identically must be deduplicated by the FSM
	// cluster-wide (served from different nodes than the originals).
	for i := 1; i <= 3; i++ {
		id := "follower-" + string(rune('0'+i))
		if code, body := postQueued(t, newLeader, id, `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, true); code != 200 {
			t.Fatalf("retry %s from new leader failed: %d %s", id, code, body)
		}
	}
	for i := 1; i <= 5; i++ {
		id := "switch-" + string(rune('0'+i))
		n := allNodes[(i+1)%3]
		if code, body := postQueued(t, n, id, `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, true); code != 200 {
			t.Fatalf("retry %s from %s failed: %d %s", id, n.ID, code, body)
		}
	}
	for i := 1; i <= 5; i++ {
		id := "after-" + string(rune('0'+i))
		n := allNodes[(i+2)%3]
		if code, body := postQueued(t, n, id, `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, true); code != 200 {
			t.Fatalf("retry %s from %s failed: %d %s", id, n.ID, code, body)
		}
	}
	waitForBalance(t, newLeader, "13", 10*time.Second)

	// No node should be frozen.
	for _, n := range c {
		d := queueDurabilityStatus(t, n)
		if d["fatal"] == true {
			t.Fatalf("node %s unexpectedly frozen: %v", n.ID, d["fatal_reason"])
		}
	}
}
