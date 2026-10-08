package system

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	httpd "github.com/rqlite/rqlite/v10/http"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/queue/durable"
	gproto "google.golang.org/protobuf/proto"
)

// enableDurableService swaps the node's HTTP service for one with the
// durable queued-write ledger enabled. The store and cluster stay up, so it
// also doubles as an in-process service-restart simulation.
func enableDurableService(t *testing.T, n *Node) {
	t.Helper()
	n.Service.Close()

	pxy := proxy.New(n.Store, n.Client)
	svc := httpd.New("localhost:0", n.Store, n.Client, pxy, nil)
	svc.DefaultQueueBatchSz = 8
	svc.DefaultQueueCap = 64
	svc.QueueDurability = true
	svc.QueueLedgerPath = filepath.Join(n.Dir, "queue-state.db")
	if err := svc.Start(); err != nil {
		t.Fatalf("failed to start durable service: %s", err.Error())
	}
	n.Service = svc
	n.APIAddr = svc.Addr().String()
	n.Cluster.SetAPIAddr(n.APIAddr)
	pxy.SetAPIAddr(n.APIAddr)
}

func durableLedgerPath(n *Node) string {
	return filepath.Join(n.Dir, "queue-state.db")
}

func postQueued(t *testing.T, n *Node, id, body string, wait bool) (int, string) {
	t.Helper()
	u := "http://" + n.APIAddr + "/db/execute?queue&id=" + id
	if wait {
		u += "&wait&timeout=10s"
	}
	resp, err := http.Post(u, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("queued POST failed: %s", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func queueDurabilityStatus(t *testing.T, n *Node) map[string]any {
	t.Helper()
	resp, err := http.Get("http://" + n.APIAddr + "/status")
	if err != nil {
		t.Fatalf("status GET failed: %s", err)
	}
	defer resp.Body.Close()
	var st map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("status decode failed: %s", err)
	}
	httpSt, _ := st["http"].(map[string]any)
	q, _ := httpSt["queue"].(map[string]any)
	d, _ := q["durability"].(map[string]any)
	return d
}

func waitForBalance(t *testing.T, n *Node, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	wantFull := fmt.Sprintf(`{"results":[{"columns":["n"],"types":["integer"],"values":[[%s]]}]}`, want)
	for time.Now().Before(deadline) {
		r, err := n.Query("SELECT n FROM acct WHERE id=1")
		if err == nil && r == wantFull {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	r, err := n.Query("SELECT n FROM acct WHERE id=1")
	t.Fatalf("balance never reached %s (last: %s err: %v)", wantFull, r, err)
}

func waitForFrozen(t *testing.T, n *Node, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d := queueDurabilityStatus(t, n); d != nil && d["fatal"] == true {
			return d
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("queue did not freeze within %s", timeout)
	return nil
}

func mustSeedLedgerEntry(t *testing.T, path, id string) {
	t.Helper()
	payload, err := gproto.Marshal(&proto.Request{
		IdempotencyKey: id,
		Statements:     []*proto.Statement{{Sql: "UPDATE acct SET n = n + 1 WHERE id = 1"}},
	})
	if err != nil {
		t.Fatalf("marshal seed payload: %s", err)
	}
	l, err := durable.Open(path)
	if err != nil {
		t.Fatalf("open ledger for seeding: %s", err)
	}
	if _, _, _, err := l.Admit(id, payload); err != nil {
		t.Fatalf("seed admit %s: %s", id, err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("seed ledger close: %s", err)
	}
}

// Test_SingleNodeDurableQueued_ExactlyOnce verifies live queued writes,
// deterministic crash-window replay across service restart, and FSM-level
// deduplication of identical retries through real Raft: the balance ends up
// incremented exactly once per request id.
func Test_SingleNodeDurableQueued_ExactlyOnce(t *testing.T) {
	node := mustNewLeaderNode("durable-eo")
	defer node.Deprovision()
	if _, err := node.Execute("CREATE TABLE acct(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)"); err != nil {
		t.Fatalf("create table: %s", err)
	}
	if _, err := node.Execute("INSERT INTO acct VALUES(1, 0)"); err != nil {
		t.Fatalf("insert seed row: %s", err)
	}

	enableDurableService(t, node)

	// A normal live queued write with wait.
	if code, body := postQueued(t, node, "live-1", `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, true); code != http.StatusOK {
		t.Fatalf("live queued write failed: %d %s", code, body)
	}
	waitForBalance(t, node, "1", 5*time.Second)

	// Simulate "accepted and fsynced, not yet drained, process crashes":
	// stop the service and write Pending records directly to the ledger.
	node.Service.Close()
	for _, id := range []string{"crash-1", "crash-2", "crash-3"} {
		mustSeedLedgerEntry(t, durableLedgerPath(node), id)
	}
	enableDurableService(t, node) // reopen -> replay before serving

	waitForBalance(t, node, "4", 10*time.Second)

	// Replay recovery counter visible in status.
	d := queueDurabilityStatus(t, node)
	if toSysInt64(d["replayed"]) != 3 {
		t.Fatalf("expected 3 replayed records in status, got: %v", d["replayed"])
	}

	// Identical retries after commit must be deduplicated by the FSM, not
	// re-applied.
	for _, id := range []string{"live-1", "crash-1", "crash-2", "crash-3"} {
		if code, body := postQueued(t, node, id, `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, true); code != http.StatusOK {
			t.Fatalf("identical retry for %s failed: %d %s", id, code, body)
		}
	}
	waitForBalance(t, node, "4", 5*time.Second)
}

// Test_SingleNodeDurableQueued_ConflictFreezes verifies that reusing an id
// with different content freezes the queued channel while ordinary writes
// keep working and no partial effect reaches the database.
func Test_SingleNodeDurableQueued_ConflictFreezes(t *testing.T) {
	node := mustNewLeaderNode("durable-freeze")
	defer node.Deprovision()
	if _, err := node.Execute("CREATE TABLE acct(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)"); err != nil {
		t.Fatalf("create table: %s", err)
	}
	if _, err := node.Execute("INSERT INTO acct VALUES(1, 0)"); err != nil {
		t.Fatalf("insert seed row: %s", err)
	}

	enableDurableService(t, node)

	if code, body := postQueued(t, node, "c1", `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, true); code != http.StatusOK {
		t.Fatalf("first queued write: %d %s", code, body)
	}
	waitForBalance(t, node, "1", 5*time.Second)

	// Same id, different payload -> rejected and freezes at admission.
	code, _ := postQueued(t, node, "c1", `["UPDATE acct SET n = n + 50 WHERE id = 1"]`, false)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on id conflict, got %d", code)
	}
	d := waitForFrozen(t, node, 5*time.Second)
	if !strings.Contains(fmt.Sprint(d["fatal_reason"]), "different payload") {
		t.Fatalf("unexpected freeze reason: %v", d["fatal_reason"])
	}

	// Further queued writes are rejected outright.
	if code, _ := postQueued(t, node, "c2", `["UPDATE acct SET n = n + 1 WHERE id = 1"]`, false); code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on frozen queue, got %d", code)
	}

	// Ordinary non-queued writes still apply.
	if _, err := node.Execute("INSERT INTO acct VALUES(2, 2)"); err != nil {
		t.Fatalf("ordinary execute must keep working while queue is frozen: %s", err)
	}
	// The conflicting statement never applied.
	waitForBalance(t, node, "1", 2*time.Second)
}

// waitForDurabilityOK polls until the queue is not frozen.
func waitForDurabilityOK(t *testing.T, n *Node, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d := queueDurabilityStatus(t, n); d != nil && d["fatal"] == false {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("durable queue did not recover within %s", timeout)
}

// Test_SingleNodeDurableQueued_FailedStatementRepairReplay verifies a queued
// request whose statement fails inside the FSM has zero effect, stays Pending
// across restarts while frozen, and is applied exactly once after the
// operator repairs the missing precondition with an ordinary write.
func Test_SingleNodeDurableQueued_FailedStatementRepairReplay(t *testing.T) {
	node := mustNewLeaderNode("durable-repair")
	defer node.Deprovision()
	if _, err := node.Execute("CREATE TABLE acct(id INTEGER PRIMARY KEY, n INTEGER NOT NULL)"); err != nil {
		t.Fatalf("create table: %s", err)
	}
	enableDurableService(t, node)

	body := `["INSERT INTO later_tbl(x) VALUES(1)"]`

	// Statement fails (table does not exist): forced transaction rolls
	// back, the waiting client is woken with 503 and the channel freezes.
	if code, resp := postQueued(t, node, "fixme", body, true); code != http.StatusServiceUnavailable {
		t.Fatalf("failing statement should freeze with 503, got %d: %s", code, resp)
	}
	d := waitForFrozen(t, node, 5*time.Second)
	if ledger, ok := d["ledger"].(map[string]any); !ok || toSysInt64(ledger["pending"]) != 1 {
		t.Fatalf("failed request must remain Pending, status: %v", d)
	}
	if toSysInt64(d["committed"]) != 0 {
		t.Fatalf("failed request must not be committed, status: %v", d)
	}

	// Restart the service: the Pending record replays and fails again, so
	// the channel refreezes rather than silently dropping it.
	enableDurableService(t, node)
	d = waitForFrozen(t, node, 5*time.Second)
	if toSysInt64(d["committed"]) != 0 {
		t.Fatalf("still-failing replay must not commit, status: %v", d)
	}

	// Operator fixes the precondition with an ordinary (non-queued) write,
	// which must keep working while the queue is frozen.
	if _, err := node.Execute("CREATE TABLE later_tbl(x INTEGER)"); err != nil {
		t.Fatalf("ordinary repair write during freeze failed: %s", err)
	}

	// Restart again: the identical Pending payload now applies exactly once.
	enableDurableService(t, node)
	waitForDurabilityOK(t, node, 10*time.Second)
	want := `{"results":[{"columns":["COUNT(*)"],"types":["integer"],"values":[[1]]}]}`
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r, err := node.Query("SELECT COUNT(*) FROM later_tbl")
		if err == nil && r == want {
			break
		}
		if time.Now().Add(50 * time.Millisecond).After(deadline) {
			t.Fatalf("repaired replay never applied (last: %s)", r)
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Identical queued retry stays effective exactly once.
	if code, resp := postQueued(t, node, "fixme", body, true); code != http.StatusOK {
		t.Fatalf("post-repair retry failed: %d %s", code, resp)
	}
	r, err := node.Query("SELECT COUNT(*) FROM later_tbl")
	if err != nil || r != want {
		t.Fatalf("exactly-once violated after retry: %s %v", r, err)
	}
}

// Test_SingleNodeQueued_DefaultNotDurable verifies the legacy in-memory
// queued-write path is unchanged and creates no ledger file when the flag is
// off.
func Test_SingleNodeQueued_DefaultNotDurable(t *testing.T) {
	node := mustNewLeaderNode("plain-queue")
	defer node.Deprovision()

	r, err := node.ExecuteQueued("CREATE TABLE foo (id INTEGER NOT NULL PRIMARY KEY)", true)
	if err != nil {
		t.Fatalf("legacy queued write failed: %s", err)
	}
	if !QueuedResponseRegex.MatchString(r) {
		t.Fatalf("legacy queued response changed: %s", r)
	}
	if _, err := os.Stat(durableLedgerPath(node)); !os.IsNotExist(err) {
		t.Fatalf("no ledger file should exist with durability off, stat err: %v", err)
	}
}

func toSysInt64(v any) int64 {
	if n, ok := v.(float64); ok {
		return int64(n)
	}
	return -1
}
