package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	command "github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/proxy"
	"github.com/rqlite/rqlite/v10/queue/durable"
	bolt "go.etcd.io/bbolt"
	gproto "google.golang.org/protobuf/proto"
)

const durableTestBody = `["UPDATE acct SET n = n + 1 WHERE id = 1"]`

func newDurableTestService(t *testing.T, executeFn func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error)) *Service {
	t.Helper()
	m := &MockStore{
		leaderAddr: "foo:1234",
		executeFn:  executeFn,
	}
	c := &mockClusterService{apiAddr: "http://bar:5678"}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.QueueDurability = true
	s.QueueLedgerPath = filepath.Join(t.TempDir(), "queue-state.db")
	if err := s.Start(); err != nil {
		t.Fatalf("failed to start durable service: %s", err.Error())
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func postDurable(t *testing.T, host, id, body string, wait bool) (int, string) {
	t.Helper()
	u := fmt.Sprintf("http://%s/db/execute?queue&noleader", host)
	if id != "" {
		u += "&id=" + url.QueryEscape(id)
	}
	if wait {
		u += "&wait&timeout=5s"
	}
	resp, err := http.Post(u, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %s", err.Error())
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func postDurableAllowErr(t *testing.T, host, id, body string) (int, string) {
	t.Helper()
	u := fmt.Sprintf("http://%s/db/execute?queue&noleader&id=%s", host, url.QueryEscape(id))
	resp, err := http.Post(u, "application/json", strings.NewReader(body))
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func postPlainExecute(t *testing.T, host, body string) int {
	t.Helper()
	resp, err := http.Post(fmt.Sprintf("http://%s/db/execute?noleader", host),
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %s", err.Error())
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	return resp.StatusCode
}

func durableStatus(t *testing.T, host string) map[string]any {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://%s/status", host))
	if err != nil {
		t.Fatalf("GET status: %s", err.Error())
	}
	defer resp.Body.Close()
	var st map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		t.Fatalf("decode status: %s", err.Error())
	}
	httpSt, _ := st["http"].(map[string]any)
	q, _ := httpSt["queue"].(map[string]any)
	d, _ := q["durability"].(map[string]any)
	return d
}

func waitDurableFrozen(t *testing.T, host string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d := durableStatus(t, host); d != nil && d["fatal"] == true {
			return d
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("durable queue did not freeze within %s", timeout)
	return nil
}

func waitDurableCommitted(t *testing.T, host string, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		d := durableStatus(t, host)
		if toInt64(d["committed"]) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("committed did not reach %d within %s", want, timeout)
}

func toInt64(v any) int64 {
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	default:
		return -1
	}
}

// Test_DurableQueued_MissingID rejects id-less queued writes with 400.
func Test_DurableQueued_MissingID(t *testing.T) {
	s := newDurableTestService(t, nil)
	code, body := postDurable(t, s.Addr().String(), "", durableTestBody, false)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 without id, got %d: %s", code, body)
	}

	// Overlong id is also rejected.
	long := strings.Repeat("x", maxQueuedWriteIDLen+1)
	code, _ = postDurable(t, s.Addr().String(), long, durableTestBody, false)
	if code != http.StatusBadRequest {
		t.Fatalf("expected 400 for overlong id, got %d", code)
	}
}

// Test_DurableQueued_WaitAndIdempotentAdmit verifies wait completion and
// identical post-commit retries never re-propose.
func Test_DurableQueued_WaitAndIdempotentAdmit(t *testing.T) {
	var proposals atomic.Int64
	s := newDurableTestService(t, func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		proposals.Add(1)
		return nil, 0, nil
	})

	code, _ := postDurable(t, s.Addr().String(), "txn-1", durableTestBody, true)
	if code != http.StatusOK {
		t.Fatalf("first wait request failed: %d", code)
	}
	if got := proposals.Load(); got != 1 {
		t.Fatalf("expected 1 proposal, got %d", got)
	}

	// Same id, identical body: admitted as committed, no new proposal.
	code, _ = postDurable(t, s.Addr().String(), "txn-1", durableTestBody, true)
	if code != http.StatusOK {
		t.Fatalf("identical retry failed: %d", code)
	}
	if got := proposals.Load(); got != 1 {
		t.Fatalf("identical retry must not re-propose, got %d", got)
	}

	d := durableStatus(t, s.Addr().String())
	if d["fatal"] == true || toInt64(d["committed"]) != 1 {
		t.Fatalf("unexpected durability status: %v", d)
	}
}

// Test_DurableQueued_FSMConflictFreezes verifies an FSM id conflict freezes
// the queued-write channel while ordinary writes keep working.
func Test_DurableQueued_FSMConflictFreezes(t *testing.T) {
	s := newDurableTestService(t, func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		if er.Request.GetIdempotencyKey() == "conflict" {
			return nil, 0, fmt.Errorf("%s at raft index 42", db.ErrIdempotencyKeyConflict.Error())
		}
		return nil, 0, nil
	})

	code, _ := postDurable(t, s.Addr().String(), "conflict", durableTestBody, false)
	if code != http.StatusOK {
		t.Fatalf("admission itself should succeed, got %d", code)
	}

	d := waitDurableFrozen(t, s.Addr().String(), 5*time.Second)
	if !strings.Contains(fmt.Sprint(d["fatal_reason"]), "conflict") {
		t.Fatalf("fatal reason should mention conflict, got: %v", d["fatal_reason"])
	}

	// Frozen channel rejects new queued writes.
	code, body := postDurable(t, s.Addr().String(), "other", durableTestBody, false)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after freeze, got %d: %s", code, body)
	}

	// Ordinary, non-queued executes still work.
	if code := postPlainExecute(t, s.Addr().String(), `["CREATE TABLE foo (id INTEGER PRIMARY KEY)"]`); code != http.StatusOK {
		t.Fatalf("non-queued execute must still succeed while frozen, got %d", code)
	}
}

// Test_DurableQueued_CorruptAtStartup verifies ledger verification failure
// at start puts the channel in the frozen state without panicking.
func Test_DurableQueued_CorruptAtStartup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue-state.db")

	// Seed a real protobuf payload (the drainer will unmarshal it) and
	// tamper the record checksum directly.
	seedPayload, err := gproto.Marshal(&command.Request{
		IdempotencyKey: "seed",
		Statements:     []*command.Statement{{Sql: "SELECT 1"}},
	})
	if err != nil {
		t.Fatalf("seed marshal: %v", err)
	}
	l, err := durable.Open(path)
	if err != nil {
		t.Fatalf("seed open: %v", err)
	}
	if _, _, _, err := l.Admit("seed", seedPayload); err != nil {
		t.Fatalf("seed admit: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("seed close: %v", err)
	}
	raw, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("raw open: %v", err)
	}
	if err := raw.Update(func(tx *bolt.Tx) error {
		eb := tx.Bucket([]byte("entries"))
		k, v := eb.Cursor().First()
		marker := []byte(`"checksum":"`)
		idx := bytes.Index(v, marker)
		if idx < 0 {
			t.Fatalf("checksum marker not found")
		}
		pos := idx + len(marker)
		nv := append([]byte(nil), v...)
		if nv[pos] == '0' {
			nv[pos] = '1'
		} else {
			nv[pos] = '0'
		}
		return eb.Put(k, nv)
	}); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("raw close: %v", err)
	}

	m := &MockStore{leaderAddr: "foo:1234"}
	c := &mockClusterService{apiAddr: "http://bar:5678"}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.QueueDurability = true
	s.QueueLedgerPath = path
	if err := s.Start(); err != nil {
		t.Fatalf("service should start even when ledger is corrupt: %v", err)
	}
	defer s.Close()

	d := waitDurableFrozen(t, s.Addr().String(), 5*time.Second)
	if d["fatal"] != true {
		t.Fatalf("expected frozen: %v", d)
	}
	if reason := fmt.Sprint(d["fatal_reason"]); !strings.Contains(reason, "corrupt") &&
		!strings.Contains(reason, "verify") && !strings.Contains(reason, "open") {
		t.Fatalf("freeze reason should come from ledger verification, got: %s", reason)
	}
	code, _ := postDurable(t, s.Addr().String(), "a", durableTestBody, false)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 with corrupt ledger, got %d", code)
	}
}

// Test_DurableQueued_CapacityBlockAndClose verifies a full queue blocks and
// that closing the service unblocks the waiter instead of hanging.
func Test_DurableQueued_CapacityBlockAndClose(t *testing.T) {
	started := make(chan struct{}, 4)

	m := &MockStore{leaderAddr: "foo:1234"}
	c := &mockClusterService{apiAddr: "http://bar:5678"}
	s := New("127.0.0.1:0", m, c, proxy.New(m, c), nil)
	s.QueueDurability = true
	s.QueueLedgerPath = filepath.Join(t.TempDir(), "queue-state.db")
	s.DefaultQueueCap = 1
	// The proposal blocks until the service closes, simulating a leader
	// outage that keeps the admitted record Pending.
	m.executeFn = func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		started <- struct{}{}
		<-s.closeCh
		return nil, 0, context.Canceled
	}
	if err := s.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	// First request occupies the single in-flight slot.
	go func() { postDurable(t, s.Addr().String(), "first", durableTestBody, false) }()
	<-started

	// Second request blocks waiting for capacity; it must unblock when the
	// service closes (either a 503 response or a transport error).
	done := make(chan struct{}, 1)
	go func() {
		_, _ = postDurableAllowErr(t, s.Addr().String(), "second", durableTestBody)
		done <- struct{}{}
	}()
	select {
	case <-done:
		t.Fatalf("second request should block while queue is full")
	case <-time.After(150 * time.Millisecond):
	}

	s.Close()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("closing the service did not unblock the capacity waiter")
	}
}

// Test_DurableQueued_StatementFailureFreezes verifies that a queued request
// whose statements fail inside the FSM (forced transaction rolls back) is
// never marked committed, freezes the channel, and wakes the waiting client
// immediately with 503.
func Test_DurableQueued_StatementFailureFreezes(t *testing.T) {
	s := newDurableTestService(t, func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		if er.Request.GetIdempotencyKey() == "bad" {
			return []*command.ExecuteQueryResponse{{
				Result: &command.ExecuteQueryResponse_E{E: &command.ExecuteResult{
					Error: "UNIQUE constraint failed: acct.id",
				}},
			}}, 0, nil
		}
		return nil, 0, nil
	})

	done := make(chan int, 1)
	go func() {
		code, _ := postDurable(t, s.Addr().String(), "bad", durableTestBody, true)
		done <- code
	}()
	select {
	case code := <-done:
		if code != http.StatusServiceUnavailable {
			t.Fatalf("statement failure must freeze and return 503 to waiters, got %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("waiter was not woken when the channel froze")
	}

	d := waitDurableFrozen(t, s.Addr().String(), 2*time.Second)
	if !strings.Contains(fmt.Sprint(d["fatal_reason"]), "rolled back") {
		t.Fatalf("freeze reason should mention rollback, got: %v", d["fatal_reason"])
	}
	ledger, _ := d["ledger"].(map[string]any)
	if toInt64(ledger["pending"]) != 1 || toInt64(d["committed"]) != 0 {
		t.Fatalf("failed request must remain Pending and uncommitted, status: %v", d)
	}

	// Subsequent queued writes rejected; ordinary writes still work.
	if code, _ := postDurable(t, s.Addr().String(), "later", durableTestBody, false); code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 after freeze, got %d", code)
	}
	if code := postPlainExecute(t, s.Addr().String(), `["INSERT INTO plain VALUES(1)"]`); code != http.StatusOK {
		t.Fatalf("ordinary write must keep working, got %d", code)
	}
}

// faultLedger injects an arbitrary non-sentinel persistence error on Admit.
type faultLedger struct {
	admitErr error
}

func (f *faultLedger) Admit(string, []byte) (bool, bool, int64, error) {
	return false, false, 0, f.admitErr
}
func (f *faultLedger) MarkCommitted(string) (bool, error) { return true, nil }
func (f *faultLedger) Pending() ([]*durable.Entry, error) { return nil, nil }
func (f *faultLedger) PruneCommitted(int) (int, error)    { return 0, nil }
func (f *faultLedger) Stats() (map[string]any, error)     { return map[string]any{}, nil }
func (f *faultLedger) Close() error                       { return nil }

// Test_DurableQueued_AdmitIOFailureFreezes verifies non-sentinel ledger
// failures at admission freeze the channel rather than returning a plain 500.
func Test_DurableQueued_AdmitIOFailureFreezes(t *testing.T) {
	s := newDurableTestService(t, nil)
	real := s.durableQ.ledger
	fake := &faultLedger{admitErr: errors.New("fsync failed: read-only file system")}
	s.durableQ.ledger = fake
	if err := real.Close(); err != nil {
		t.Fatalf("close real ledger: %v", err)
	}

	code, _ := postDurable(t, s.Addr().String(), "iofail", durableTestBody, false)
	if code != http.StatusServiceUnavailable {
		t.Fatalf("admit I/O failure must yield 503, got %d", code)
	}
	d := waitDurableFrozen(t, s.Addr().String(), 2*time.Second)
	if !strings.Contains(fmt.Sprint(d["fatal_reason"]), "read-only") {
		t.Fatalf("freeze reason should carry the I/O error, got: %v", d["fatal_reason"])
	}
}

// Test_DurableQueueManager_FinishedBound verifies wait-finished markers are
// reclaimed past the high-water mark.
func Test_DurableQueueManager_FinishedBound(t *testing.T) {
	m := &durableQueueManager{
		waiters:  make(map[int64][]chan struct{}),
		finished: make(map[int64]bool),
	}
	for seq := int64(1); seq <= int64(durableFinishedMax+128); seq++ {
		m.notifyWaiter(seq)
	}
	if len(m.finished) != durableFinishedMax {
		t.Fatalf("finished map should be bounded at %d, got %d", durableFinishedMax, len(m.finished))
	}
}

// Test_DurableQueued_ReplayAfterRestart verifies a Pending record left over
// from a stopped drain is proposed exactly once by a fresh service, and that
// its committed retry does not propose again.
func Test_DurableQueued_ReplayAfterRestart(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "queue-state.db")
	s1close := make(chan struct{})
	s2close := make(chan struct{})

	// Service 1: proposal never completes before close.
	var p1 int64
	m1 := &MockStore{leaderAddr: "foo:1234", executeFn: func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		atomic.AddInt64(&p1, 1)
		<-s1close
		return nil, 0, context.Canceled
	}}
	c1 := &mockClusterService{apiAddr: "http://bar:5678"}
	s1 := New("127.0.0.1:0", m1, c1, proxy.New(m1, c1), nil)
	s1.QueueDurability = true
	s1.QueueLedgerPath = path
	if err := s1.Start(); err != nil {
		t.Fatalf("s1 start: %v", err)
	}
	code, _ := postDurable(t, s1.Addr().String(), "replay-1", durableTestBody, false)
	if code != http.StatusOK {
		t.Fatalf("admit failed: %d", code)
	}
	// Close while the proposal is stuck: MarkCommitted never ran.
	close(s1close)
	s1.Close()

	// Service 2 with the same ledger must replay the Pending record once.
	var p2 atomic.Int64
	m2 := &MockStore{leaderAddr: "foo:1234", executeFn: func(er *command.ExecuteRequest) ([]*command.ExecuteQueryResponse, uint64, error) {
		p2.Add(1)
		if er.Request.GetIdempotencyKey() == "dup" {
			return []*command.ExecuteQueryResponse{{Duplicated: true}}, 0, nil
		}
		return nil, 0, nil
	}}
	c2 := &mockClusterService{apiAddr: "http://bar:5678"}
	s2 := New("127.0.0.1:0", m2, c2, proxy.New(m2, c2), nil)
	s2.QueueDurability = true
	s2.QueueLedgerPath = path
	if err := s2.Start(); err != nil {
		t.Fatalf("s2 start: %v", err)
	}
	defer func() {
		close(s2close)
		s2.Close()
	}()

	waitDurableCommitted(t, s2.Addr().String(), 1, 5*time.Second)
	if got := p2.Load(); got != 1 {
		t.Fatalf("replay must propose exactly once, got %d", got)
	}
	if got := atomic.LoadInt64(&p1); got != 1 {
		t.Fatalf("service 1 should have proposed once, got %d", got)
	}

	// Same id again: ledger already committed locally, no new proposal.
	code, _ = postDurable(t, s2.Addr().String(), "replay-1", durableTestBody, true)
	if code != http.StatusOK {
		t.Fatalf("post-commit retry failed: %d", code)
	}
	if got := p2.Load(); got != 1 {
		t.Fatalf("post-commit retry must not propose, got %d", got)
	}
}
