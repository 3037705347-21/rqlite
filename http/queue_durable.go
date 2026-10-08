package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rqlite/rqlite/v10/command/proto"
	"github.com/rqlite/rqlite/v10/db"
	"github.com/rqlite/rqlite/v10/queue/durable"
	gproto "google.golang.org/protobuf/proto"
)

const (
	// maxQueuedWriteIDLen is the maximum accepted length, in bytes, of the
	// client-supplied durable queued-write identifier.
	maxQueuedWriteIDLen = 256

	// durablePruneKeep is the number of most-recent Committed ledger
	// records retained after a successful drain. The FSM keeps cluster-wide
	// dedup state indefinitely; the local ledger only needs a short window
	// for same-node retries.
	durablePruneKeep = 4096

	// durablePruneInterval limits how often the drainer performs the
	// full-ledger scan and extra fsync involved in pruning.
	durablePruneInterval = 1024

	// durableFinishedMax bounds the wait-finished marker map; only very
	// recent markers are needed to close the commit/register race.
	durableFinishedMax = 2*1024 + 64

	drainBackoffStart = time.Second
	drainBackoffMax   = 30 * time.Second
)

// durableLedger is the ledger surface used by the manager. *durable.Ledger
// satisfies it; the interface seam also allows fault injection in tests.
type durableLedger interface {
	Admit(id string, payload []byte) (already, committed bool, seq int64, err error)
	MarkCommitted(id string) (found bool, err error)
	Pending() ([]*durable.Entry, error)
	PruneCommitted(keep int) (int, error)
	Stats() (map[string]any, error)
	Close() error
}

// durableQueueManager owns the durable queued-write channel for a Service:
// the fsynced request ledger, the single in-order drain goroutine, the
// capacity semaphore, wait notifications, and the one-way freeze gate.
type durableQueueManager struct {
	s      *Service
	ledger durableLedger // nil when the ledger file could not be opened
	gate   *durable.Gate

	jobs   chan *durable.Entry
	sem    chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}

	// freezeSig is closed once when the channel is frozen; waiters use it
	// to wake up immediately instead of waiting for their timeout.
	freezeSig chan struct{}

	waitMu   sync.Mutex
	waiters  map[int64][]chan struct{}
	finished map[int64]bool

	admitted   atomic.Int64
	committed  atomic.Int64
	replayed   atomic.Int64
	pruned     atomic.Int64
	conflicts  atomic.Int64
	duplicated atomic.Int64
}

// startDurableQueue opens and recovers the ledger before the HTTP server
// starts serving. If the ledger cannot be opened or fails verification the
// channel starts in the frozen state; the HTTP server still starts so that
// queued-write callers receive an explicit error and the condition is
// visible through the status endpoint.
func (s *Service) startDurableQueue() {
	m := &durableQueueManager{
		s:         s,
		gate:      &durable.Gate{},
		jobs:      make(chan *durable.Entry, s.DefaultQueueCap+16),
		sem:       make(chan struct{}, s.DefaultQueueCap),
		stopCh:    s.closeCh,
		doneCh:    s.queueDone,
		freezeSig: make(chan struct{}),
		waiters:   make(map[int64][]chan struct{}),
		finished:  make(map[int64]bool),
	}
	s.durableQ = m

	var recovered []*durable.Entry
	l, err := durable.Open(s.QueueLedgerPath)
	if err != nil {
		if l != nil {
			m.ledger = l
		}
		m.freeze(fmt.Sprintf("failed to open/verify durable queue ledger: %s", err.Error()))
	} else {
		m.ledger = l
	}

	if fatal, _ := m.gate.Fatal(); !fatal {
		pending, perr := l.Pending()
		if perr != nil {
			m.freeze(fmt.Sprintf("failed to recover pending entries from ledger: %s", perr.Error()))
		} else {
			recovered = pending
			m.replayed.Store(int64(len(pending)))
			stats.Add(numQueuedDurableReplayed, int64(len(pending)))
			// Re-seed a capacity token for every recovered Pending entry
			// (best-effort if capacity was shrunk below the backlog).
			for range pending {
				select {
				case m.sem <- struct{}{}:
				default:
				}
			}
			s.logger.Printf("durable execute queue recovered %d pending request(s) from %s",
				len(pending), s.QueueLedgerPath)
		}
	}

	go m.run(recovered)
	s.logger.Printf("durable execute queue processing started with capacity %d, ledger %s",
		s.DefaultQueueCap, s.QueueLedgerPath)
}

// closeDurableQueue stops admits (HTTP server is already closed), lets the
// drain goroutine exit, then closes the ledger. An in-flight proposal that
// never reaches MarkCommitted is safely replayed on next start because the
// FSM deduplicates it.
func (s *Service) closeDurableQueue() {
	if s.durableQ == nil {
		return
	}
	select {
	case <-s.closeCh:
	default:
		close(s.closeCh)
	}
	<-s.queueDone
	if s.durableQ.ledger != nil {
		if err := s.durableQ.ledger.Close(); err != nil {
			s.logger.Printf("error closing durable queue ledger: %s", err.Error())
		}
	}
}

// durableAdmit handles a queued execute request under durability mode.
func (s *Service) durableAdmit(w http.ResponseWriter, r *http.Request, qp QueryParams, stmts []*proto.Statement) {
	m := s.durableQ
	resp := NewResponse()

	id := qp.ID()
	if len(id) == 0 || len(id) > maxQueuedWriteIDLen {
		http.Error(w, fmt.Sprintf(
			"durable queued writes require a non-empty 'id' query parameter of at most %d bytes",
			maxQueuedWriteIDLen), http.StatusBadRequest)
		return
	}
	if fatal, reason := m.gate.Fatal(); fatal {
		http.Error(w, "durable queued writes unavailable: channel is frozen: "+reason,
			http.StatusServiceUnavailable)
		return
	}

	// The marshaled Request is the canonical, byte-stable payload that is
	// persisted and replayed verbatim through Raft.
	payload, err := gproto.Marshal(&proto.Request{
		Statements:     stmts,
		Transaction:    s.DefaultQueueTx,
		IdempotencyKey: id,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to encode queued request: %s", err.Error()),
			http.StatusInternalServerError)
		return
	}

	if err := m.acquire(r.Context()); err != nil {
		if errors.Is(err, errDurableQueueStopped) {
			http.Error(w, "durable queued writes unavailable: node is shutting down or channel is frozen",
				http.StatusServiceUnavailable)
		} else {
			http.Error(w, fmt.Sprintf("interrupted while waiting for durable queue capacity: %s", err.Error()),
				http.StatusServiceUnavailable)
		}
		return
	}

	already, committed, seq, err := m.ledger.Admit(id, payload)
	if err != nil {
		m.release()
		switch {
		case errors.Is(err, durable.ErrIDConflict):
			m.conflicts.Add(1)
			m.freeze(fmt.Sprintf("request id %q admitted with different payload", id))
			http.Error(w, "durable queued writes unavailable: id reused with different payload; channel is frozen",
				http.StatusServiceUnavailable)
		case errors.Is(err, durable.ErrCorrupt):
			m.freeze(fmt.Sprintf("ledger integrity failure while admitting id %q: %s", id, err.Error()))
			http.Error(w, "durable queued writes unavailable: ledger is corrupt; channel is frozen",
				http.StatusServiceUnavailable)
		case errors.Is(err, durable.ErrClosed):
			http.Error(w, "durable queued writes unavailable: node is shutting down",
				http.StatusServiceUnavailable)
		default:
			// Any other ledger failure is a persistence (fsync/I/O) fault: the
			// request cannot be proven durable, so freeze rather than risk
			// accepting unconfirmed writes.
			m.freeze(fmt.Sprintf("ledger failure while admitting id %q: %s", id, err.Error()))
			http.Error(w, "durable queued writes unavailable: ledger persistence failure; channel is frozen",
				http.StatusServiceUnavailable)
		}
		return
	}

	if already {
		// Duplicate admission performs no new work; do not consume a
		// capacity slot for it.
		m.release()
	} else {
		stats.Add(numQueuedDurableRx, 1)
		m.admitted.Add(1)
		m.enqueue(&durable.Entry{ID: id, Seq: seq, Payload: payload})
	}
	resp.SequenceNum = seq

	if qp.Wait() && !committed {
		ch := m.registerWaiter(seq)
		timer := time.NewTimer(qp.Timeout(defaultTimeout))
		select {
		case <-ch:
		case <-m.freezeSig:
			timer.Stop()
			_, reason := m.gate.Fatal()
			http.Error(w, "durable queued write did not complete because the channel is frozen: "+reason,
				http.StatusServiceUnavailable)
			return
		case <-timer.C:
			stats.Add(numQueuedExecutionsWaitTimeout, 1)
			http.Error(w, "queue wait timeout", http.StatusRequestTimeout)
			return
		}
	}

	resp.end = time.Now()
	s.writeResponse(w, qp, resp)
}

var errDurableQueueStopped = errors.New("durable queue stopped")

func (m *durableQueueManager) acquire(ctx context.Context) error {
	if fatal, _ := m.gate.Fatal(); fatal {
		return errDurableQueueStopped
	}
	select {
	case m.sem <- struct{}{}:
		return nil
	case <-m.stopCh:
		return errDurableQueueStopped
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *durableQueueManager) release() {
	select {
	case <-m.sem:
	default:
	}
}

func (m *durableQueueManager) enqueue(e *durable.Entry) {
	select {
	case m.jobs <- e:
	case <-m.stopCh:
	}
}

func (m *durableQueueManager) registerWaiter(seq int64) chan struct{} {
	ch := make(chan struct{})
	m.waitMu.Lock()
	defer m.waitMu.Unlock()
	if m.finished[seq] {
		close(ch)
		return ch
	}
	m.waiters[seq] = append(m.waiters[seq], ch)
	return ch
}

func (m *durableQueueManager) notifyWaiter(seq int64) {
	m.waitMu.Lock()
	chans := m.waiters[seq]
	delete(m.waiters, seq)
	m.finished[seq] = true
	m.evictFinishedLocked()
	m.waitMu.Unlock()
	for _, c := range chans {
		close(c)
	}
}

// evictFinishedLocked bounds the finished-marker map. A late register only
// ever accompanies a brand-new (highest) seq; markers far below the current
// watermark are no longer needed because committed retries short-circuit in
// Admit.
func (m *durableQueueManager) evictFinishedLocked() {
	if len(m.finished) <= durableFinishedMax {
		return
	}
	seqs := make([]int64, 0, len(m.finished))
	for s := range m.finished {
		seqs = append(seqs, s)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	drop := len(seqs) - durableFinishedMax
	for _, s := range seqs[:drop] {
		delete(m.finished, s)
	}
}

// run drains recovered Pending entries first, strictly in ledger sequence
// order, before serving live admissions.
func (m *durableQueueManager) run(recovered []*durable.Entry) {
	defer close(m.doneCh)
	for _, e := range recovered {
		select {
		case <-m.stopCh:
			return
		default:
		}
		m.process(e)
		if fatal, _ := m.gate.Fatal(); fatal {
			m.s.logger.Printf("durable execute queue drain stopped during recovery after fatal condition, uncommitted records retained")
			return
		}
	}
	for {
		select {
		case <-m.stopCh:
			return
		case e := <-m.jobs:
			m.process(e)
			if fatal, _ := m.gate.Fatal(); fatal {
				m.s.logger.Printf("durable execute queue drain stopped after fatal condition, uncommitted records retained")
				return
			}
		}
	}
}

func (m *durableQueueManager) process(e *durable.Entry) {
	defer m.release()

	if fatal, _ := m.gate.Fatal(); fatal {
		return
	}

	req := &proto.Request{}
	if err := gproto.Unmarshal(e.Payload, req); err != nil {
		m.freeze(fmt.Sprintf("failed to decode ledger payload for id %q: %s", e.ID, err.Error()))
		return
	}
	er := &proto.ExecuteRequest{Request: req}

	backoff := drainBackoffStart
	for {
		if fatal, _ := m.gate.Fatal(); fatal {
			return
		}
		select {
		case <-m.stopCh:
			return
		default:
		}

		results, _, _, err := m.s.proxy.Execute(context.Background(), er, nil, defaultTimeout, 0, false)
		if err == nil {
			dup := false
			for _, r := range results {
				if r.GetDuplicated() {
					dup = true
				}
			}
			if dup {
				m.duplicated.Add(1)
				stats.Add(numQueuedDurableDuplicated, 1)
			}
			// A per-statement error means the forced transaction rolled
			// back, including the dedup record: the request had zero
			// effect. Retrying the same command would fail again and the
			// request must never be marked committed, so freeze pending
			// operator intervention.
			if sqlErr := queuedApplySQLError(results); sqlErr != "" {
				m.freeze(fmt.Sprintf("queued request id %q failed to apply; its transaction rolled back so it has no effect, record retained as Pending: %s",
					e.ID, sqlErr))
				return
			}
			break
		}
		if isIdempotencyConflict(err) {
			m.conflicts.Add(1)
			m.freeze(fmt.Sprintf("FSM rejected id %q as previously applied with different content: %s",
				e.ID, err.Error()))
			return
		}

		m.s.logger.Printf("durable execute queue write for id %q seq %d not committed yet (%s); retrying in %s",
			e.ID, e.Seq, err.Error(), backoff)
		stats.Add(numQueuedExecutionsFailed, 1)
		select {
		case <-m.stopCh:
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > drainBackoffMax {
			backoff = drainBackoffMax
		}
	}

	if _, err := m.ledger.MarkCommitted(e.ID); err != nil {
		m.freeze(fmt.Sprintf("failed to mark id %q committed in ledger: %s", e.ID, err.Error()))
		return
	}
	m.committed.Add(1)
	stats.Add(numQueuedDurableCommitted, 1)
	m.notifyWaiter(e.Seq)

	// Throttle pruning: it scans and fsyncs the whole ledger, so run it
	// only periodically rather than after every commit.
	if m.committed.Load()%durablePruneInterval != 0 {
		return
	}
	if n, err := m.ledger.PruneCommitted(durablePruneKeep); err != nil {
		m.s.logger.Printf("durable execute queue ledger prune failed: %s", err.Error())
	} else if n > 0 {
		m.pruned.Add(int64(n))
		stats.Add(numQueuedDurablePruned, int64(n))
	}
}

// queuedApplySQLError returns the first per-statement error reported by the
// FSM. rqlite carries ordinary SQL failures in the responses (with a nil Go
// error); for forced-transaction keyed requests any such failure means the
// entire request, including the dedup record, was rolled back.
func queuedApplySQLError(rr []*proto.ExecuteQueryResponse) string {
	for _, r := range rr {
		if e := r.GetE().GetError(); e != "" {
			return e
		}
		if q := r.GetQ().GetError(); q != "" {
			return q
		}
	}
	return ""
}

// freeze trips the one-way fatal gate, wakes waiters, and logs the cause
// prominently.
func (m *durableQueueManager) freeze(reason string) {
	if m.gate.Trip(reason) {
		close(m.freezeSig)
		stats.Add(numQueuedDurableFrozen, 1)
		m.s.logger.Printf("FATAL: durable queued-write channel FROZEN on node %s: %s; ledger: %s; "+
			"no further queued writes will be accepted or submitted until the ledger is repaired and the node is restarted",
			m.s.Addr().String(), reason, m.s.QueueLedgerPath)
	}
}

// stats reports the durable queue state for the /status endpoint.
func (m *durableQueueManager) stats() map[string]any {
	fatal, reason := m.gate.Fatal()
	st := map[string]any{
		"enabled":      true,
		"fatal":        fatal,
		"fatal_reason": reason,
		"admitted":     m.admitted.Load(),
		"committed":    m.committed.Load(),
		"replayed":     m.replayed.Load(),
		"pruned":       m.pruned.Load(),
		"conflicts":    m.conflicts.Load(),
		"duplicated":   m.duplicated.Load(),
		"ledger_path":  m.s.QueueLedgerPath,
	}
	if m.ledger != nil {
		if ls, err := m.ledger.Stats(); err == nil {
			st["ledger"] = ls
			st["pending"] = ls["pending"]
		}
	}
	return st
}

// isIdempotencyConflict recognizes the FSM key-conflict error both in
// process-local execution and in errors forwarded over the cluster API
// (which arrive as strings).
func isIdempotencyConflict(err error) bool {
	return errors.Is(err, db.ErrIdempotencyKeyConflict) ||
		strings.Contains(err.Error(), db.ErrIdempotencyKeyConflict.Error())
}
